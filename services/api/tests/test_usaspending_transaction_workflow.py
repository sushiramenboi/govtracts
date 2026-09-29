from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import os
import tempfile
import threading
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, inspect, select
from sqlalchemy.engine import make_url

from app.db.models import (
    UsaSpendingIngestionCheckpoint,
    UsaSpendingTransaction,
    UsaSpendingTransactionIngestionAttempt,
)
from app.usaspending.client import (
    BulkExportJob,
    BulkExportStatus,
    UsaSpendingClient,
    UsaSpendingError,
)
from app.usaspending.transaction_export import EXPECTED_TRANSACTION_HEADERS
from app.usaspending.transaction_ingestion import (
    TransactionIngestionLoader,
    TransactionIngestionResult,
)
from app.usaspending.transaction_workflow import (
    FIXED_ARCHIVE_FILENAME,
    TransactionIngestionWorkflow,
    TransactionWorkflowError,
)


PERIOD_START = date(2024, 10, 1)
PERIOD_END = date(2024, 10, 31)
FISCAL_YEAR = 2025
NOW = datetime(2024, 11, 2, 12, 0, tzinfo=timezone.utc)
STATUS_URL = "https://api.usaspending.gov/api/v2/download/status/example"
FILE_URL = "https://files.usaspending.gov/generated_downloads/example.zip"
REMOTE_FILE_NAME = "../../remote-name-must-not-be-used.zip"
BASE_ROW = {
    "contract_transaction_unique_key": "transaction-1",
    "contract_award_unique_key": "CONT_AWD_1",
    "award_id_piid": "PIID-1",
    "award_type_code": "A",
    "action_date": "2024-10-15",
    "federal_action_obligation": "-12.34",
    "recipient_name": "Example Recipient",
    "recipient_uei": "ABC123DEF456",
    "recipient_parent_name": "Example Parent",
    "recipient_parent_uei": "PARENT123456",
    "awarding_agency_name": "Example Agency",
    "awarding_sub_agency_name": "Example Subagency",
    "naics_code": "541512",
    "product_or_service_code": "D310",
    "transaction_description": "Example transaction",
    "last_modified_date": "2024-11-01T12:34:56Z",
}


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def archive_bytes(*, row: dict[str, str] | None = None) -> bytes:
    output = io.BytesIO()
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(EXPECTED_TRANSACTION_HEADERS)
    values = row or BASE_ROW
    writer.writerow([values.get(header, "") for header in EXPECTED_TRANSACTION_HEADERS])
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("transactions.csv", stream.getvalue().encode("utf-8"))
    return output.getvalue()


class StubClient:
    def __init__(
        self,
        *,
        archive: bytes,
        count_error: UsaSpendingError | None = None,
        submit_error: UsaSpendingError | None = None,
        poll_error: UsaSpendingError | None = None,
        download_error: UsaSpendingError | None = None,
    ) -> None:
        self.archive = archive
        self.count_error = count_error
        self.submit_error = submit_error
        self.poll_error = poll_error
        self.download_error = download_error
        self.count_calls = 0
        self.submit_calls = 0
        self.poll_calls = 0
        self.download_calls = 0
        self.destinations: list[Path] = []
        self._lock = threading.Lock()

    def get_expected_transaction_count(self, start: date, end: date) -> int:
        assert (start, end) == (PERIOD_START, PERIOD_END)
        with self._lock:
            self.count_calls += 1
        if self.count_error is not None:
            raise self.count_error
        return 1

    def submit_bulk_export(self, start: date, end: date) -> BulkExportJob:
        assert (start, end) == (PERIOD_START, PERIOD_END)
        with self._lock:
            self.submit_calls += 1
        if self.submit_error is not None:
            raise self.submit_error
        return BulkExportJob(
            status_url=STATUS_URL,
            file_url=FILE_URL,
            file_name=REMOTE_FILE_NAME,
        )

    async def poll_bulk_export(self, job: BulkExportJob) -> BulkExportStatus:
        assert job.status_url == STATUS_URL
        with self._lock:
            self.poll_calls += 1
        if self.poll_error is not None:
            raise self.poll_error
        return BulkExportStatus(
            status="finished",
            status_url=job.status_url,
            file_url=job.file_url,
            file_name=job.file_name,
            message=None,
            seconds_elapsed="1.0",
        )

    async def retrieve_bulk_export(
        self,
        completed: BulkExportStatus,
        destination: str | os.PathLike[str],
        **_: Any,
    ) -> int:
        assert completed.status == "finished"
        with self._lock:
            self.download_calls += 1
        destination_path = Path(destination)
        self.destinations.append(destination_path)
        if self.download_error is not None:
            raise self.download_error
        destination_path.write_bytes(self.archive)
        return len(self.archive)

    @property
    def total_calls(self) -> int:
        return (
            self.count_calls
            + self.submit_calls
            + self.poll_calls
            + self.download_calls
        )


class CountBarrierClient(StubClient):
    def __init__(self, *, barrier: threading.Barrier, archive: bytes) -> None:
        super().__init__(
            archive=archive,
            poll_error=UsaSpendingError("timeout_or_network"),
        )
        self._count_barrier = barrier

    def get_expected_transaction_count(self, start: date, end: date) -> int:
        result = super().get_expected_transaction_count(start, end)
        self._count_barrier.wait(timeout=5)
        return result


class TrackingTemporaryDirectories:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: list[Path] = []

    @contextmanager
    def __call__(self) -> Iterator[Path]:
        with tempfile.TemporaryDirectory(
            prefix="workflow-test-",
            dir=self.root,
        ) as name:
            path = Path(name)
            self.paths.append(path)
            yield path


class ExplodingLoader:
    def load_with_connection(self, *_: Any, **__: Any) -> TransactionIngestionResult:
        raise AssertionError("loader must not be called")


def test_invalid_fiscal_period_fails_before_database_or_network() -> None:
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    client = StubClient(archive=archive_bytes())
    workflow = TransactionIngestionWorkflow(  # type: ignore[arg-type]
        engine,
        client,  # type: ignore[arg-type]
        ExplodingLoader(),  # type: ignore[arg-type]
    )

    with pytest.raises(TransactionWorkflowError, match="invalid_fiscal_period"):
        run(
            workflow.run(
                fiscal_year=2024,
                calendar_year=2024,
                calendar_month=10,
            )
        )

    assert client.total_calls == 0


@pytest.fixture
def disposable_postgres_engine() -> Iterator[Any]:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not set")
    url = make_url(value)
    if not url.database or not url.database.endswith("_test"):
        pytest.skip("TEST_DATABASE_URL must name a disposable database ending in _test")

    admin_engine = create_engine(url)
    schema = f"usaspending_workflow_{uuid4().hex}"
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped_engine = create_engine(
        url,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with scoped_engine.begin() as connection:
            UsaSpendingIngestionCheckpoint.__table__.create(connection)
            UsaSpendingTransaction.__table__.create(connection)
            UsaSpendingTransactionIngestionAttempt.__table__.create(connection)
        yield scoped_engine
    finally:
        scoped_engine.dispose()
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin_engine.dispose()


def checkpoint_values(checkpoint_id: UUID) -> dict[str, Any]:
    return {
        "id": checkpoint_id,
        "source": "usaspending",
        "fiscal_year": FISCAL_YEAR,
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "status": "complete",
        "status_url": STATUS_URL,
        "remote_file_name": REMOTE_FILE_NAME,
        "archive_sha256": "a" * 64,
        "expected_rows": 1,
        "loaded_rows": 1,
        "started_at": NOW,
        "completed_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }


def attempt_values(
    status: str,
    *,
    attempt_id: UUID | None = None,
    archive: bytes | None = None,
    status_url: str = STATUS_URL,
    file_url: str = FILE_URL,
    checkpoint_id: UUID | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": attempt_id or uuid4(),
        "source": "usaspending",
        "fiscal_year": FISCAL_YEAR,
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "status": status,
        "failure_count": 0,
        "started_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    if status not in {"created", "failed"}:
        values["expected_rows"] = 1
    if status in {
        "submitted",
        "export_finished",
        "archive_hashed",
        "loading",
        "completed",
    }:
        values.update(
            status_url=status_url,
            file_url=file_url,
            remote_file_name=REMOTE_FILE_NAME,
        )
    if status in {"archive_hashed", "loading", "completed"}:
        assert archive is not None
        values.update(
            archive_sha256=hashlib.sha256(archive).hexdigest(),
            archive_bytes=len(archive),
        )
    if status == "submission_unknown":
        values["last_error_code"] = "submission_outcome_unknown"
    if status == "failed":
        values.update(
            last_error_code="deterministic_failure",
            completed_at=NOW,
        )
    if status == "completed":
        assert checkpoint_id is not None
        values.update(
            loaded_rows=1,
            signed_obligation_total=Decimal("-12.34"),
            checkpoint_id=checkpoint_id,
            completed_at=NOW,
        )
    return values


def seed_attempt(
    engine: Any,
    status: str,
    *,
    archive: bytes | None = None,
    status_url: str = STATUS_URL,
    file_url: str = FILE_URL,
) -> UUID:
    checkpoint_id = uuid4() if status == "completed" else None
    values = attempt_values(
        status,
        archive=archive,
        status_url=status_url,
        file_url=file_url,
        checkpoint_id=checkpoint_id,
    )
    with engine.begin() as connection:
        if checkpoint_id is not None:
            connection.execute(
                UsaSpendingIngestionCheckpoint.__table__.insert(),
                checkpoint_values(checkpoint_id),
            )
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(),
            values,
        )
    return values["id"]


def workflow(
    engine: Any,
    client: StubClient | UsaSpendingClient,
    temporary_directories: TrackingTemporaryDirectories,
    *,
    loader: Any | None = None,
) -> TransactionIngestionWorkflow:
    return TransactionIngestionWorkflow(
        engine,
        client,
        loader or TransactionIngestionLoader(engine, clock=lambda: NOW),
        clock=lambda: NOW,
        temporary_directory_factory=temporary_directories,
    )


def run_period(subject: TransactionIngestionWorkflow) -> Any:
    return run(
        subject.run(
            fiscal_year=FISCAL_YEAR,
            calendar_year=2024,
            calendar_month=10,
        )
    )


@pytest.mark.integration
def test_complete_workflow_uses_all_valid_transitions_and_safe_temporary_path(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    client = StubClient(archive=archive)
    directories = TrackingTemporaryDirectories(tmp_path)

    result = run_period(workflow(engine, client, directories))

    assert result.status == "completed"
    assert result.expected_rows == result.loaded_rows == 1
    assert result.signed_obligation_total == Decimal("-12.34")
    assert result.checkpoint_id is not None
    assert (
        client.count_calls,
        client.submit_calls,
        client.poll_calls,
        client.download_calls,
    ) == (1, 1, 1, 1)
    assert [path.name for path in client.destinations] == [FIXED_ARCHIVE_FILENAME]
    assert REMOTE_FILE_NAME not in str(client.destinations[0])
    assert directories.paths and all(not path.exists() for path in directories.paths)

    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingTransaction)
        ) == 1
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingIngestionCheckpoint)
        ) == 1
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert attempt["status"] == "completed"
        assert attempt["checkpoint_id"] == result.checkpoint_id
        assert set(inspect(connection).get_table_names()) == {
            "usaspending_ingestion_checkpoints",
            "usaspending_transactions",
            "usaspending_transaction_ingestion_attempts",
        }


@pytest.mark.integration
def test_concurrent_creators_converge_on_one_active_attempt(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    client = StubClient(
        archive=archive_bytes(),
        count_error=UsaSpendingError("timeout_or_network"),
    )

    def invoke(index: int) -> Any:
        directories = TrackingTemporaryDirectories(tmp_path / f"creator-{index}")
        directories.root.mkdir()
        return run_period(workflow(engine, client, directories))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, range(2)))

    assert results[0].attempt_id == results[1].attempt_id
    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(
                UsaSpendingTransactionIngestionAttempt
            )
        ) == 1


@pytest.mark.integration
def test_concurrent_creators_do_not_follow_another_workers_submission_claim(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    client = CountBarrierClient(
        barrier=threading.Barrier(2),
        archive=archive_bytes(),
    )

    def invoke(index: int) -> Any:
        directories = TrackingTemporaryDirectories(tmp_path / f"fresh-{index}")
        directories.root.mkdir()
        return run_period(
            workflow(engine, client, directories, loader=ExplodingLoader())
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(invoke, range(2)))

    assert client.count_calls == 2
    assert client.submit_calls == 1
    with engine.connect() as connection:
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert attempt["status"] == "submitted"
        assert attempt["last_error_code"] == "timeout_or_network"


class CountedBarrierWorkflow(TransactionIngestionWorkflow):
    def __init__(self, *args: Any, barrier: threading.Barrier, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._barrier = barrier

    def _locate_or_create_attempt(self, **kwargs: Any) -> dict[str, Any]:
        attempt = super()._locate_or_create_attempt(**kwargs)
        if attempt["status"] == "counted":
            self._barrier.wait(timeout=5)
        return attempt


@pytest.mark.integration
def test_concurrent_counted_claims_submit_exactly_once(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "counted")
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError("timeout_or_network"),
    )
    barrier = threading.Barrier(2)

    def invoke(index: int) -> Any:
        directories = TrackingTemporaryDirectories(tmp_path / f"claim-{index}")
        directories.root.mkdir()
        subject = CountedBarrierWorkflow(
            engine,
            client,  # type: ignore[arg-type]
            ExplodingLoader(),  # type: ignore[arg-type]
            clock=lambda: NOW,
            temporary_directory_factory=directories,
            barrier=barrier,
        )
        return run_period(subject)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(invoke, range(2)))

    assert client.submit_calls == 1
    with engine.connect() as connection:
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert attempt["status"] == "submitted"


@pytest.mark.integration
def test_submitting_restart_fails_closed_without_resubmission(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitting")
    client = StubClient(archive=archive_bytes())
    directories = TrackingTemporaryDirectories(tmp_path)

    result = run_period(
        workflow(engine, client, directories, loader=ExplodingLoader())
    )

    assert result.status == "submission_unknown"
    assert result.operator_resolution_required is True
    assert result.error_code == "submission_outcome_unknown"
    assert client.submit_calls == 0


@pytest.mark.integration
def test_ambiguous_submission_failure_becomes_submission_unknown(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "counted")
    client = StubClient(
        archive=archive_bytes(),
        submit_error=UsaSpendingError("timeout_or_network"),
    )

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "submission_unknown"
    assert result.operator_resolution_required is True
    assert client.submit_calls == 1


@pytest.mark.integration
@pytest.mark.parametrize(
    "resume_status",
    ["submitted", "export_finished", "archive_hashed", "loading"],
)
def test_resumes_every_persisted_post_submission_state_without_resubmitting(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    resume_status: str,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    seed_attempt(engine, resume_status, archive=archive)
    client = StubClient(archive=archive)

    result = run_period(
        workflow(engine, client, TrackingTemporaryDirectories(tmp_path))
    )

    assert result.status == "completed"
    assert client.submit_calls == 0
    assert client.poll_calls == (1 if resume_status == "submitted" else 0)
    assert client.download_calls == 1


@pytest.mark.integration
def test_transient_poll_failure_retains_submitted_state(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitted")
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError("bulk_export_timeout"),
    )

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "submitted"
    assert result.error_code == "bulk_export_timeout"
    with engine.connect() as connection:
        assert connection.scalar(
            select(UsaSpendingTransactionIngestionAttempt.failure_count)
        ) == 1


@pytest.mark.integration
def test_transient_download_failure_retains_export_finished_state(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "export_finished")
    client = StubClient(
        archive=archive_bytes(),
        download_error=UsaSpendingError("timeout_or_network"),
    )
    directories = TrackingTemporaryDirectories(tmp_path)

    result = run_period(
        workflow(engine, client, directories, loader=ExplodingLoader())
    )

    assert result.status == "export_finished"
    assert result.error_code == "timeout_or_network"
    assert directories.paths and all(not path.exists() for path in directories.paths)


@pytest.mark.integration
@pytest.mark.parametrize("remote_status", ["failed", "rejected"])
def test_remote_rejection_is_retained_as_terminal_failure(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    remote_status: str,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitted")
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError(f"bulk_export_{remote_status}"),
    )

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "failed"
    assert result.error_code == f"bulk_export_{remote_status}"
    with engine.connect() as connection:
        assert connection.scalar(
            select(UsaSpendingTransactionIngestionAttempt.completed_at)
        ) == NOW


@pytest.mark.integration
def test_parser_validation_failure_is_retained_and_rolls_back_loader_changes(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    invalid_archive = b"not-a-zip"
    seed_attempt(engine, "loading", archive=invalid_archive)
    client = StubClient(archive=invalid_archive)

    result = run_period(
        workflow(engine, client, TrackingTemporaryDirectories(tmp_path))
    )

    assert result.status == "failed"
    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingTransaction)
        ) == 0
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingIngestionCheckpoint)
        ) == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    ("status", "status_url", "file_url", "expected_code"),
    [
        (
            "submitted",
            "https://api.usaspending.gov.attacker/status",
            FILE_URL,
            "unsafe_status_url",
        ),
        (
            "export_finished",
            STATUS_URL,
            "https://files.usaspending.gov.attacker/archive.zip",
            "unsafe_file_url",
        ),
    ],
)
def test_persisted_malicious_urls_use_real_client_validation(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    status: str,
    status_url: str,
    file_url: str,
    expected_code: str,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(
        engine,
        status,
        status_url=status_url,
        file_url=file_url,
    )
    real_client = object.__new__(UsaSpendingClient)

    result = run_period(
        workflow(
            engine,
            real_client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "failed"
    assert result.error_code == expected_code


@pytest.mark.integration
@pytest.mark.parametrize("mismatch", ["hash", "bytes"])
def test_restart_redownload_must_match_persisted_archive_identity(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    mismatch: str,
) -> None:
    engine = disposable_postgres_engine
    original = archive_bytes()
    if mismatch == "hash":
        replacement = bytearray(original)
        replacement[-1] ^= 1
        downloaded = bytes(replacement)
    else:
        downloaded = original + b"x"
    seed_attempt(engine, "archive_hashed", archive=original)
    client = StubClient(archive=downloaded)

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "failed"
    assert result.error_code == (
        "archive_sha256_mismatch"
        if mismatch == "hash"
        else "archive_byte_count_mismatch"
    )


@pytest.mark.integration
def test_loader_and_attempt_completion_share_one_atomic_transaction(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    seed_attempt(engine, "loading", archive=archive)
    client = StubClient(archive=archive)

    result = run_period(
        workflow(engine, client, TrackingTemporaryDirectories(tmp_path))
    )

    with engine.connect() as connection:
        transaction = connection.execute(
            select(UsaSpendingTransaction)
        ).mappings().one()
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert result.status == attempt["status"] == "completed"
        assert transaction["ingestion_checkpoint_id"] == attempt["checkpoint_id"]


@pytest.mark.integration
def test_failure_after_loader_work_rolls_back_loader_and_attempt_completion(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    seed_attempt(engine, "loading", archive=archive)
    client = StubClient(archive=archive)
    subject = workflow(engine, client, TrackingTemporaryDirectories(tmp_path))

    def fail_completion(*_: Any, **__: Any) -> None:
        raise TransactionWorkflowError("injected_completion_failure")

    monkeypatch.setattr(subject, "_complete_attempt", fail_completion)

    result = run_period(subject)

    assert result.status == "loading"
    assert result.error_code == "database_operation_failed"
    with engine.connect() as connection:
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingTransaction)
        ) == 0
        assert connection.scalar(
            select(func.count()).select_from(UsaSpendingIngestionCheckpoint)
        ) == 0
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert attempt["status"] == "loading"
        assert attempt["checkpoint_id"] is None
        assert attempt["failure_count"] == 1


@pytest.mark.integration
@pytest.mark.parametrize("terminal_status", ["completed", "submission_unknown"])
def test_terminal_or_operator_blocked_attempt_returns_without_activity(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    terminal_status: str,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    seed_attempt(
        engine,
        terminal_status,
        archive=archive if terminal_status == "completed" else None,
    )
    client = StubClient(archive=archive)
    directories = TrackingTemporaryDirectories(tmp_path)
    before = None
    with engine.connect() as connection:
        before = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()

    result = run_period(
        workflow(
            engine,
            client,
            directories,
            loader=ExplodingLoader(),
        )
    )

    assert result.status == terminal_status
    assert result.operator_resolution_required is (
        terminal_status == "submission_unknown"
    )
    assert client.total_calls == 0
    assert directories.paths == []
    with engine.connect() as connection:
        after = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        assert after == before


@pytest.mark.integration
def test_failed_history_allows_a_new_retry_attempt(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    failed_id = seed_attempt(engine, "failed")
    client = StubClient(
        archive=archive_bytes(),
        count_error=UsaSpendingError("timeout_or_network"),
    )

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "created"
    assert result.attempt_id != failed_id
    with engine.connect() as connection:
        statuses = connection.scalars(
            select(UsaSpendingTransactionIngestionAttempt.status).order_by(
                UsaSpendingTransactionIngestionAttempt.created_at,
                UsaSpendingTransactionIngestionAttempt.id,
            )
        ).all()
        assert sorted(statuses) == ["created", "failed"]
