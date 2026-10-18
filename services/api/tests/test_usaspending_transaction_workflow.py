from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import os
import tempfile
import threading
import zipfile
from collections.abc import Callable, Iterator
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
from sqlalchemy.exc import SQLAlchemyError

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
        pre_submission_rows: int = 1,
        export_rows: int = 1,
        export_columns: int | None = 16,
        poll_observer: Callable[[BulkExportJob, bool], None] | None = None,
    ) -> None:
        self.archive = archive
        self.count_error = count_error
        self.submit_error = submit_error
        self.poll_error = poll_error
        self.download_error = download_error
        self.pre_submission_rows = pre_submission_rows
        self.export_rows = export_rows
        self.export_columns = export_columns
        self.poll_observer = poll_observer
        self.count_calls = 0
        self.submit_calls = 0
        self.poll_calls = 0
        self.poll_grace_values: list[bool] = []
        self.download_calls = 0
        self.destinations: list[Path] = []
        self._lock = threading.Lock()

    def get_expected_transaction_count(self, start: date, end: date) -> int:
        assert (start, end) == (PERIOD_START, PERIOD_END)
        with self._lock:
            self.count_calls += 1
        if self.count_error is not None:
            raise self.count_error
        return self.pre_submission_rows

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

    async def poll_bulk_export(
        self,
        job: BulkExportJob,
        *,
        initial_status_404_grace: bool = False,
    ) -> BulkExportStatus:
        assert job.status_url == STATUS_URL
        with self._lock:
            self.poll_calls += 1
            self.poll_grace_values.append(initial_status_404_grace)
        if self.poll_observer is not None:
            self.poll_observer(job, initial_status_404_grace)
        if self.poll_error is not None:
            raise self.poll_error
        return BulkExportStatus(
            status="finished",
            status_url=job.status_url,
            file_url=job.file_url,
            file_name=job.file_name,
            total_rows=self.export_rows,
            total_columns=self.export_columns,
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
        assert completed.total_rows == self.export_rows
        assert completed.total_columns == self.export_columns
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


class RecordingLoader:
    def __init__(self, *, loaded_rows: int) -> None:
        self.loaded_rows = loaded_rows
        self.expected_counts: list[int] = []

    def load_with_connection(
        self,
        connection: Any,
        *,
        expected_count: int,
        expected_archive_sha256: str,
        **_: Any,
    ) -> TransactionIngestionResult:
        self.expected_counts.append(expected_count)
        checkpoint_id = uuid4()
        connection.execute(
            UsaSpendingIngestionCheckpoint.__table__.insert(),
            {
                "id": checkpoint_id,
                "source": "usaspending",
                "fiscal_year": FISCAL_YEAR,
                "period_start": PERIOD_START,
                "period_end": PERIOD_END,
                "status": "complete",
                "archive_sha256": expected_archive_sha256,
                "expected_rows": expected_count,
                "loaded_rows": self.loaded_rows,
                "started_at": NOW,
                "completed_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        return TransactionIngestionResult(
            checkpoint_id=checkpoint_id,
            archive_sha256=expected_archive_sha256,
            loaded_rows=self.loaded_rows,
            signed_obligation_total=Decimal("-12.34"),
            no_op=False,
        )


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
    if status in {"export_finished", "archive_hashed", "loading", "completed"}:
        values.update(export_rows=1, export_columns=16)
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


def run_child_period(
    subject: TransactionIngestionWorkflow,
    *,
    period_start: date,
    period_end: date,
    stop_after_count: bool = False,
    capacity_guard: Any | None = None,
    retry_parent_attempt_id: UUID | None = None,
    submission_observer: Any | None = None,
) -> Any:
    return run(
        subject.run_period(
            fiscal_year=FISCAL_YEAR,
            period_start=period_start,
            period_end=period_end,
            stop_after_count=stop_after_count,
            capacity_guard=capacity_guard,
            retry_parent_attempt_id=retry_parent_attempt_id,
            submission_observer=submission_observer,
        )
    )


CLAIM_FILE_NAME = "recovery.zip"
CLAIM_STATUS_URL = (
    "https://api.usaspending.gov/api/v2/download/status?file_name=recovery.zip"
)
CLAIM_FILE_URL = "https://files.usaspending.gov/generated_downloads/recovery.zip"


class ClaimOnlyClient(UsaSpendingClient):
    def __init__(self) -> None:
        from app.core.config import Settings

        super().__init__(
            Settings(database_url="postgresql+psycopg://unused:unused@invalid/unused_test")
        )

    def get_expected_transaction_count(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim attempted a count request")

    def submit_bulk_export(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim attempted submission")

    async def poll_bulk_export(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim attempted polling")

    async def retrieve_bulk_export(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim attempted downloading")

    def _http_client(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim opened a sync HTTP client")

    def _async_http_client(self, *_: Any, **__: Any) -> Any:
        pytest.fail("recovery claim opened an async HTTP client")


def claim_workflow(engine: Any) -> TransactionIngestionWorkflow:
    def no_filesystem() -> Any:
        pytest.fail("recovery claim accessed the filesystem")

    return TransactionIngestionWorkflow(
        engine,
        ClaimOnlyClient(),
        ExplodingLoader(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        temporary_directory_factory=no_filesystem,
    )


def claim_source_values(**changes: Any) -> dict[str, Any]:
    values = attempt_values("failed")
    values.update(
        expected_rows=7,
        status_url=CLAIM_STATUS_URL,
        file_url=CLAIM_FILE_URL,
        remote_file_name=CLAIM_FILE_NAME,
        last_error_code="http_404",
        failure_count=1,
    )
    values.update(changes)
    return values


@pytest.mark.integration
def test_claim_failed_job_recovery_creates_one_exact_offline_copy(
    disposable_postgres_engine: Any,
) -> None:
    from app.usaspending.transaction_workflow import RecoveryClaim

    engine = disposable_postgres_engine
    source = claim_source_values()
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    with engine.begin() as connection:
        connection.execute(attempts.insert(), source)
        before = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())

    claim = claim_workflow(engine).claim_failed_job_recovery(
        source["id"], FISCAL_YEAR, PERIOD_START, PERIOD_END
    )

    assert isinstance(claim, RecoveryClaim)
    assert claim.failed_attempt_id == source["id"]
    assert claim.recovery_attempt_id != source["id"]
    assert claim.created is True
    with engine.connect() as connection:
        after = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())
        linked = connection.execute(
            select(attempts).where(attempts.c.recovery_of_attempt_id == source["id"])
        ).mappings().all()
    assert after == before
    assert len(linked) == 1
    recovery = dict(linked[0])
    assert recovery["id"] == claim.recovery_attempt_id
    assert recovery["status"] == "submitted"
    assert recovery["recovery_of_attempt_id"] == source["id"]
    assert recovery["failure_count"] == 0
    for field in (
        "source", "fiscal_year", "period_start", "period_end", "expected_rows",
        "status_url", "file_url", "remote_file_name", "parent_attempt_id",
    ):
        assert recovery[field] == before[field]
    for field in (
        "completed_at", "last_error_code", "last_error_detail", "export_rows",
        "export_columns", "archive_sha256", "archive_bytes", "loaded_rows",
        "signed_obligation_total", "checkpoint_id",
    ):
        assert recovery[field] is None
    assert dict(claim.attempt) == recovery
    with pytest.raises(TypeError):
        claim.attempt["status"] = "failed"  # type: ignore[index]


def _seed_claim_source(engine: Any, **changes: Any) -> dict[str, Any]:
    values = claim_source_values(**changes)
    with engine.begin() as connection:
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), values)
    return values


def _claim(engine: Any, source_id: UUID) -> Any:
    return claim_workflow(engine).claim_failed_job_recovery(
        source_id, FISCAL_YEAR, PERIOD_START, PERIOD_END
    )


@pytest.mark.integration
def test_claim_is_idempotent_and_preserves_parent_and_source_snapshot(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    parent = attempt_values("failed", attempt_id=uuid4())
    parent.update(period_start=date(2024, 9, 1), period_end=date(2024, 9, 30), fiscal_year=2024)
    source = claim_source_values(parent_attempt_id=parent["id"])
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    with engine.begin() as connection:
        connection.execute(attempts.insert(), parent)
        connection.execute(attempts.insert(), source)
        original = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())
    first = _claim(engine, source["id"])
    second = _claim(engine, source["id"])
    with engine.connect() as connection:
        after = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())
        linked = connection.execute(select(attempts).where(attempts.c.recovery_of_attempt_id == source["id"])).mappings().all()
    assert first.created is True and second.created is False
    assert first.recovery_attempt_id == second.recovery_attempt_id
    assert len(linked) == 1
    assert linked[0]["parent_attempt_id"] == parent["id"]
    assert linked[0]["recovery_of_attempt_id"] == source["id"]
    assert after == original


@pytest.mark.integration
@pytest.mark.parametrize(
    "changes,code",
    [
        ({"status_url": None, "file_url": None, "remote_file_name": None}, "recovery_job_absent"),
        ({"status_url": None}, "recovery_job_corrupt"),
        ({"remote_file_name": "../unsafe.zip"}, "recovery_job_unsafe"),
        ({"status_url": "https://evil.example/status"}, "recovery_job_unsafe"),
        ({"expected_rows": 500001}, "recovery_source_ineligible"),
        ({"source": "other"}, "recovery_source_ineligible"),
        ({"fiscal_year": 2024}, "recovery_source_ineligible"),
        ({"period_start": date(2024, 10, 2)}, "recovery_source_ineligible"),
        ({"period_end": date(2024, 10, 30)}, "recovery_source_ineligible"),
        ({"status": "submission_unknown", "completed_at": None}, "recovery_source_ineligible"),
        ({"last_error_code": "http_500"}, "recovery_source_ineligible"),
    ],
)
def test_claim_rejects_ineligible_source_without_creating_recovery(
    disposable_postgres_engine: Any,
    changes: dict[str, Any],
    code: str,
) -> None:
    engine = disposable_postgres_engine
    source = _seed_claim_source(engine, **changes)
    with pytest.raises(TransactionWorkflowError, match=f"^{code}$"):
        _claim(engine, source["id"])
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    with engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(attempts)) == 1


@pytest.mark.integration
def test_claim_rejects_unknown_and_recovery_source_ids(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    with pytest.raises(TransactionWorkflowError, match="^recovery_source_ineligible$"):
        _claim(engine, uuid4())
    parent = claim_source_values()
    child = claim_source_values(recovery_of_attempt_id=parent["id"])
    with engine.begin() as connection:
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), parent)
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), child)
    with pytest.raises(TransactionWorkflowError, match="^recovery_source_ineligible$"):
        _claim(engine, child["id"])


@pytest.mark.parametrize(
    "failed_id,year,start,end,code",
    [
        ("not-a-uuid", FISCAL_YEAR, PERIOD_START, PERIOD_END, "invalid_failed_attempt_id"),
        (uuid4(), True, PERIOD_START, PERIOD_END, "invalid_fiscal_period"),
        (uuid4(), FISCAL_YEAR, datetime(2024, 10, 1, tzinfo=timezone.utc), PERIOD_END, "invalid_fiscal_period"),
        (uuid4(), FISCAL_YEAR, PERIOD_END, PERIOD_START, "invalid_fiscal_period"),
    ],
)
def test_claim_rejects_invalid_inputs_before_database(
    failed_id: Any, year: Any, start: Any, end: Any, code: str,
) -> None:
    fake = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    subject = claim_workflow(fake)
    with pytest.raises(TransactionWorkflowError, match=f"^{code}$"):
        subject.claim_failed_job_recovery(failed_id, year, start, end)


@pytest.mark.integration
@pytest.mark.parametrize(
    "field,value",
    [
        ("export_rows", 1),
        ("export_columns", 16),
        ("archive_sha256", "a" * 64),
        ("archive_bytes", 1),
        ("loaded_rows", 1),
        ("signed_obligation_total", Decimal("1.00")),
    ],
)
def test_claim_rejects_each_persistable_dirty_source_field(
    disposable_postgres_engine: Any, field: str, value: Any,
) -> None:
    engine = disposable_postgres_engine
    source = _seed_claim_source(engine, **{field: value})
    with pytest.raises(TransactionWorkflowError, match="^recovery_source_dirty$"):
        _claim(engine, source["id"])


@pytest.mark.integration
def test_claim_rejects_source_checkpoint_field(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    checkpoint_id = uuid4()
    with engine.begin() as connection:
        connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint_values(checkpoint_id))
        source = claim_source_values(checkpoint_id=checkpoint_id)
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), source)
    with pytest.raises(TransactionWorkflowError, match="^recovery_source_dirty$"):
        _claim(engine, source["id"])


def _claim_test_transaction(checkpoint_id: UUID, action_date: date) -> dict[str, Any]:
    return {
        "stable_transaction_id": str(uuid4()),
        "ingestion_checkpoint_id": checkpoint_id,
        "generated_award_id": "CONT_AWD_1",
        "display_award_id": "PIID-1",
        "award_type_code": "A",
        "action_date": action_date,
        "fiscal_year": 2025 if action_date.month == 10 else 2024,
        "federal_action_obligation": Decimal("1.00"),
        "recipient_name": "Test recipient",
        "recipient_uei": "ABC123DEF456",
        "awarding_agency_name": "Test agency",
        "awarding_subagency_name": "Test subagency",
        "transaction_description": "Test transaction",
        "source_updated_at": NOW,
    }


@pytest.mark.integration
@pytest.mark.parametrize("obstacle", ["checkpoint", "in_period_row", "active_attempt"])
def test_claim_rejects_period_ownership_obstacles(
    disposable_postgres_engine: Any, obstacle: str,
) -> None:
    engine = disposable_postgres_engine
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    source = claim_source_values()
    with engine.begin() as connection:
        if obstacle in {"checkpoint", "in_period_row"}:
            checkpoint_id = uuid4()
            checkpoint = checkpoint_values(checkpoint_id)
            if obstacle == "in_period_row":
                checkpoint.update(period_start=date(2024, 9, 1), period_end=date(2024, 9, 30), fiscal_year=2024)
            connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint)
            if obstacle == "in_period_row":
                connection.execute(UsaSpendingTransaction.__table__.insert(), _claim_test_transaction(checkpoint_id, PERIOD_START))
        connection.execute(attempts.insert(), source)
        if obstacle == "active_attempt":
            connection.execute(attempts.insert(), attempt_values("created"))
    with pytest.raises(TransactionWorkflowError, match="^(recovery_period_not_empty|recovery_period_conflict)$"):
        _claim(engine, source["id"])


@pytest.mark.integration
def test_out_of_period_transaction_does_not_block_claim(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    checkpoint_id = uuid4()
    checkpoint = checkpoint_values(checkpoint_id)
    checkpoint.update(period_start=date(2024, 9, 1), period_end=date(2024, 9, 30), fiscal_year=2024)
    with engine.begin() as connection:
        connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint)
        connection.execute(UsaSpendingTransaction.__table__.insert(), _claim_test_transaction(checkpoint_id, date(2024, 9, 15)))
        source = claim_source_values()
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), source)
    assert _claim(engine, source["id"]).created is True


def _linked_claim_values(source: dict[str, Any], status: str, **changes: Any) -> dict[str, Any]:
    values = {
        "id": uuid4(), "source": source["source"], "fiscal_year": source["fiscal_year"],
        "period_start": source["period_start"], "period_end": source["period_end"],
        "status": status, "expected_rows": source["expected_rows"],
        "status_url": source["status_url"], "file_url": source["file_url"],
        "remote_file_name": source["remote_file_name"],
        "parent_attempt_id": source.get("parent_attempt_id"),
        "recovery_of_attempt_id": source["id"], "failure_count": 0,
        "started_at": NOW, "created_at": NOW, "updated_at": NOW,
    }
    if status in {"export_finished", "archive_hashed", "loading", "completed"}:
        values.update(export_rows=1, export_columns=16)
    if status in {"archive_hashed", "loading", "completed"}:
        values.update(archive_sha256="a" * 64, archive_bytes=123)
    if status == "completed":
        values.update(loaded_rows=1, signed_obligation_total=Decimal("1.00"),
                      completed_at=NOW)
    if status == "failed":
        values.update(last_error_code="http_500", completed_at=NOW)
    values.update(changes)
    return values


@pytest.mark.integration
@pytest.mark.parametrize("status", ["submitted", "export_finished", "archive_hashed", "loading"])
def test_valid_nonterminal_linked_recovery_is_reused(
    disposable_postgres_engine: Any, status: str,
) -> None:
    engine = disposable_postgres_engine
    source = claim_source_values()
    linked = _linked_claim_values(source, status)
    with engine.begin() as connection:
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), source)
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), linked)
    claim = _claim(engine, source["id"])
    assert claim.recovery_attempt_id == linked["id"]
    assert claim.created is False


@pytest.mark.integration
@pytest.mark.parametrize("obstacle", ["checkpoint", "in_period_row"])
def test_nonterminal_linked_recovery_still_requires_empty_period(
    disposable_postgres_engine: Any, obstacle: str,
) -> None:
    engine = disposable_postgres_engine
    source = claim_source_values()
    linked = _linked_claim_values(source, "submitted")
    checkpoint_id = uuid4()
    checkpoint = checkpoint_values(checkpoint_id)
    if obstacle == "in_period_row":
        checkpoint.update(period_start=date(2024, 9, 1), period_end=date(2024, 9, 30), fiscal_year=2024)
    with engine.begin() as connection:
        connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint)
        if obstacle == "in_period_row":
            connection.execute(UsaSpendingTransaction.__table__.insert(), _claim_test_transaction(checkpoint_id, PERIOD_START))
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), source)
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), linked)
    with pytest.raises(TransactionWorkflowError, match="^recovery_period_not_empty$"):
        _claim(engine, source["id"])


@pytest.mark.integration
@pytest.mark.parametrize(
    "status,changes",
    [
        ("submitted", {"expected_rows": 8}),
        ("submitted", {"parent_attempt_id": uuid4()}),
        ("submitted", {"status_url": "https://api.usaspending.gov/other"}),
        ("submitted", {"export_rows": 1}),
        ("export_finished", {"archive_bytes": 10}),
        ("archive_hashed", {"loaded_rows": 1}),
        ("loading", {"completed_at": None, "checkpoint_id": uuid4()}),
        ("counted", {}),
        ("failed", {}),
    ],
)
def test_inconsistent_or_illegal_linked_recovery_blocks_another(
    disposable_postgres_engine: Any, status: str, changes: dict[str, Any],
) -> None:
    engine = disposable_postgres_engine
    source = claim_source_values()
    linked = _linked_claim_values(source, status, **changes)
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    with engine.begin() as connection:
        connection.execute(attempts.insert(), source)
        if linked["parent_attempt_id"] is not None:
            parent = attempt_values("failed", attempt_id=linked["parent_attempt_id"])
            parent.update(period_start=date(2024, 9, 1), period_end=date(2024, 9, 30), fiscal_year=2024)
            connection.execute(attempts.insert(), parent)
        if linked.get("checkpoint_id") is not None:
            connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint_values(linked["checkpoint_id"]))
        connection.execute(attempts.insert(), linked)
    with pytest.raises(TransactionWorkflowError, match="^recovery_link_invalid$"):
        _claim(engine, source["id"])
    with engine.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(attempts).where(attempts.c.recovery_of_attempt_id == source["id"])) == 1


@pytest.mark.integration
def test_completed_linked_recovery_with_owned_rows_is_reused(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    source = claim_source_values()
    checkpoint_id = uuid4()
    linked = _linked_claim_values(source, "completed", checkpoint_id=checkpoint_id)
    checkpoint = checkpoint_values(checkpoint_id)
    with engine.begin() as connection:
        connection.execute(UsaSpendingIngestionCheckpoint.__table__.insert(), checkpoint)
        connection.execute(UsaSpendingTransaction.__table__.insert(), _claim_test_transaction(checkpoint_id, PERIOD_START))
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), source)
        connection.execute(UsaSpendingTransactionIngestionAttempt.__table__.insert(), linked)
    claim = _claim(engine, source["id"])
    assert claim.recovery_attempt_id == linked["id"]
    assert claim.created is False


@pytest.mark.integration
def test_two_workers_claim_one_recovery_without_mutating_source(
    disposable_postgres_engine: Any,
) -> None:
    engine = disposable_postgres_engine
    source = _seed_claim_source(engine)
    attempts = UsaSpendingTransactionIngestionAttempt.__table__
    with engine.connect() as connection:
        before = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())
    gate = threading.Barrier(2)

    def worker() -> Any:
        subject = claim_workflow(engine)
        gate.wait(timeout=10)
        return subject.claim_failed_job_recovery(source["id"], FISCAL_YEAR, PERIOD_START, PERIOD_END)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker) for _ in range(2)]
        claims = [future.result(timeout=20) for future in futures]
    with engine.connect() as connection:
        after = dict(connection.execute(select(attempts).where(attempts.c.id == source["id"])).mappings().one())
        linked = connection.execute(select(attempts.c.id).where(attempts.c.recovery_of_attempt_id == source["id"])).all()
    assert len(linked) == 1
    assert {claim.recovery_attempt_id for claim in claims} == {linked[0].id}
    assert sorted(claim.created for claim in claims) == [False, True]
    assert after == before


@pytest.mark.integration
def test_stop_after_count_persists_count_without_submission(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    client = StubClient(archive=archive_bytes(), pre_submission_rows=25)
    subject = workflow(
        disposable_postgres_engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
        loader=ExplodingLoader(),
    )

    result = run_child_period(
        subject,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        stop_after_count=True,
    )

    assert result.status == "counted"
    assert result.pre_submission_rows == 25
    assert (client.count_calls, client.submit_calls, client.poll_calls) == (1, 0, 0)


@pytest.mark.integration
def test_submission_observer_fires_after_durable_submitted_transition(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    observed_statuses: list[str] = []
    observed_poll_metadata: list[tuple[str, str, str, str, bool]] = []

    def observe_submission() -> None:
        with disposable_postgres_engine.connect() as connection:
            status = connection.scalar(
                select(UsaSpendingTransactionIngestionAttempt.status)
            )
        assert isinstance(status, str)
        observed_statuses.append(status)

    def observe_poll(job: BulkExportJob, grace_enabled: bool) -> None:
        with disposable_postgres_engine.connect() as connection:
            persisted = connection.execute(
                select(
                    UsaSpendingTransactionIngestionAttempt.status,
                    UsaSpendingTransactionIngestionAttempt.status_url,
                    UsaSpendingTransactionIngestionAttempt.file_url,
                    UsaSpendingTransactionIngestionAttempt.remote_file_name,
                )
            ).one()
        observed_poll_metadata.append((*persisted, grace_enabled))
        assert (job.status_url, job.file_url, job.file_name) == (
            persisted.status_url,
            persisted.file_url,
            persisted.remote_file_name,
        )

    client = StubClient(archive=archive_bytes(), poll_observer=observe_poll)
    subject = workflow(
        disposable_postgres_engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
    )

    result = run_child_period(
        subject,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        submission_observer=observe_submission,
    )

    assert result.status == "completed"
    assert observed_statuses == ["submitted"]
    assert observed_poll_metadata == [
        ("submitted", STATUS_URL, FILE_URL, REMOTE_FILE_NAME, True)
    ]
    assert client.submit_calls == 1


@pytest.mark.integration
def test_oversized_counted_period_can_never_submit(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    client = StubClient(archive=archive_bytes(), pre_submission_rows=500001)
    subject = workflow(
        disposable_postgres_engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
        loader=ExplodingLoader(),
    )

    first = run_period(subject)
    second = run_period(subject)

    assert first.status == second.status == "counted"
    assert first.attempt_id == second.attempt_id
    assert first.pre_submission_rows == 500001
    assert (client.count_calls, client.submit_calls, client.poll_calls) == (1, 0, 0)


@pytest.mark.integration
def test_valid_child_period_counts_exact_bounds_and_invalid_cross_month_stops(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    class ChildClient(StubClient):
        def get_expected_transaction_count(self, start: date, end: date) -> int:
            assert (start, end) == (date(2024, 10, 1), date(2024, 10, 16))
            self.count_calls += 1
            return 1

    client = ChildClient(archive=archive_bytes())
    subject = workflow(
        disposable_postgres_engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
        loader=ExplodingLoader(),
    )

    result = run_child_period(
        subject,
        period_start=date(2024, 10, 1),
        period_end=date(2024, 10, 16),
        stop_after_count=True,
    )
    assert result.status == "counted"
    assert client.count_calls == 1

    with pytest.raises(TransactionWorkflowError, match="invalid_fiscal_period"):
        run_child_period(
            subject,
            period_start=date(2024, 10, 31),
            period_end=date(2024, 11, 1),
            stop_after_count=True,
        )


@pytest.mark.integration
def test_subdivided_attempt_is_no_activity_state(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    attempt_id = uuid4()
    with disposable_postgres_engine.begin() as connection:
        connection.execute(
            UsaSpendingTransactionIngestionAttempt.__table__.insert(),
            {
                **attempt_values("created", attempt_id=attempt_id),
                "status": "subdivided",
                "expected_rows": 500001,
                "completed_at": NOW,
            },
        )
    client = StubClient(archive=archive_bytes())
    result = run_period(
        workflow(
            disposable_postgres_engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=ExplodingLoader(),
        )
    )

    assert result.status == "subdivided"
    assert result.attempt_id == attempt_id
    assert client.total_calls == 0


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
    assert result.pre_submission_rows == result.export_rows == result.loaded_rows == 1
    assert result.count_drift == 0
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
def test_export_bound_count_drives_loader_checkpoint_and_completion(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    client = StubClient(
        archive=archive,
        pre_submission_rows=490382,
        export_rows=490381,
    )
    loader = RecordingLoader(loaded_rows=490381)

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=loader,
        )
    )

    assert result.status == "completed"
    assert result.pre_submission_rows == 490382
    assert result.export_rows == result.loaded_rows == 490381
    assert result.count_drift == -1
    assert loader.expected_counts == [490381]
    with engine.connect() as connection:
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
        checkpoint = connection.execute(
            select(UsaSpendingIngestionCheckpoint)
        ).mappings().one()
    assert attempt["expected_rows"] == 490382
    assert attempt["export_rows"] == attempt["loaded_rows"] == 490381
    assert attempt["export_columns"] == 16
    assert checkpoint["expected_rows"] == checkpoint["loaded_rows"] == 490381


@pytest.mark.integration
@pytest.mark.parametrize(
    ("advisory_rows", "export_rows"),
    [(1, 500_000), (500_000, 1)],
)
def test_export_bound_count_is_guarded_for_drift_in_both_directions(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    advisory_rows: int,
    export_rows: int,
) -> None:
    archive = archive_bytes()
    client = StubClient(
        archive=archive,
        pre_submission_rows=advisory_rows,
        export_rows=export_rows,
    )
    loader = RecordingLoader(loaded_rows=export_rows)
    guarded: list[int] = []
    subject = workflow(
        disposable_postgres_engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
        loader=loader,
    )

    result = run_child_period(
        subject,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        capacity_guard=guarded.append,
    )

    assert result.status == "completed"
    assert guarded == [export_rows]
    assert client.download_calls == 1
    assert loader.expected_counts == [export_rows]


@pytest.mark.integration
@pytest.mark.parametrize("loaded_rows", [490380, 490382])
def test_loader_count_mismatch_against_export_rolls_back_atomically(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    loaded_rows: int,
) -> None:
    engine = disposable_postgres_engine
    archive = archive_bytes()
    client = StubClient(
        archive=archive,
        pre_submission_rows=490382,
        export_rows=490381,
    )
    loader = RecordingLoader(loaded_rows=loaded_rows)

    result = run_period(
        workflow(
            engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
            loader=loader,
        )
    )

    assert result.status == "failed"
    assert result.error_code == "loaded_row_count_mismatch"
    assert result.pre_submission_rows == 490382
    assert result.export_rows == 490381
    assert result.loaded_rows is None
    assert result.count_drift == -1
    assert loader.expected_counts == [490381]
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
    assert attempt["status"] == "failed"
    assert attempt["checkpoint_id"] is None
    assert attempt["loaded_rows"] is None


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
    assert client.poll_grace_values == ([False] if resume_status == "submitted" else [])
    assert client.download_calls == 1


@pytest.mark.integration
def test_fresh_submission_404_exhaustion_preserves_metadata_without_download(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError("http_404"),
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
    assert result.error_code == "http_404"
    assert client.submit_calls == 1
    assert client.poll_calls == 1
    assert client.poll_grace_values == [True]
    assert client.download_calls == 0
    with engine.connect() as connection:
        persisted = connection.execute(
            select(
                UsaSpendingTransactionIngestionAttempt.status,
                UsaSpendingTransactionIngestionAttempt.status_url,
                UsaSpendingTransactionIngestionAttempt.file_url,
                UsaSpendingTransactionIngestionAttempt.remote_file_name,
            )
        ).one()
    assert persisted == ("failed", STATUS_URL, FILE_URL, REMOTE_FILE_NAME)


@pytest.mark.integration
def test_resumed_submission_404_is_terminal_without_grace_or_resubmission(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitted")
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError("http_404"),
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
    assert result.error_code == "http_404"
    assert client.submit_calls == 0
    assert client.poll_calls == 1
    assert client.poll_grace_values == [False]
    assert client.download_calls == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    ("resume_status", "expected_guard_calls"),
    [
        ("submitted", [500_000, 1]),
        ("export_finished", [1]),
        ("archive_hashed", [1]),
        ("loading", [1]),
    ],
)
def test_every_post_submission_restart_guards_before_download_or_load(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    resume_status: str,
    expected_guard_calls: list[int],
) -> None:
    archive = archive_bytes()
    seed_attempt(disposable_postgres_engine, resume_status, archive=archive)
    client = StubClient(archive=archive)
    guarded: list[int] = []

    result = run_child_period(
        workflow(
            disposable_postgres_engine,
            client,
            TrackingTemporaryDirectories(tmp_path),
        ),
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        stop_after_count=True,
        capacity_guard=guarded.append,
    )

    assert result.status == "completed"
    assert guarded == expected_guard_calls
    assert client.download_calls == 1


@pytest.mark.integration
def test_submitted_restart_cannot_complete_when_capacity_guard_rejects(
    disposable_postgres_engine: Any,
    tmp_path: Path,
) -> None:
    archive = archive_bytes()
    seed_attempt(disposable_postgres_engine, "submitted", archive=archive)
    client = StubClient(archive=archive)
    calls: list[int] = []

    def reject(rows: int) -> None:
        calls.append(rows)
        raise RuntimeError("capacity rejected")

    with pytest.raises(RuntimeError, match="capacity rejected"):
        run_child_period(
            workflow(
                disposable_postgres_engine,
                client,
                TrackingTemporaryDirectories(tmp_path),
                loader=ExplodingLoader(),
            ),
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            stop_after_count=True,
            capacity_guard=reject,
        )

    assert calls == [500_000]
    assert (client.poll_calls, client.download_calls) == (0, 0)


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
def test_terminal_metadata_transition_failure_remains_submitted(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitted")
    client = StubClient(archive=archive_bytes(), export_rows=490381)
    subject = workflow(
        engine,
        client,
        TrackingTemporaryDirectories(tmp_path),
        loader=ExplodingLoader(),
    )
    original_transition = subject._transition

    def fail_export_finished_transition(
        attempt: dict[str, Any],
        *,
        expected_status: str,
        next_status: str,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        if expected_status == "submitted" and next_status == "export_finished":
            raise SQLAlchemyError("injected transition failure")
        return original_transition(
            attempt,
            expected_status=expected_status,
            next_status=next_status,
            values=values,
        )

    monkeypatch.setattr(subject, "_transition", fail_export_finished_transition)

    result = run_period(subject)

    assert result.status == "submitted"
    assert result.export_rows is None
    assert result.error_code == "database_operation_failed"
    assert client.poll_calls == 1
    assert client.download_calls == 0
    with engine.connect() as connection:
        attempt = connection.execute(
            select(UsaSpendingTransactionIngestionAttempt)
        ).mappings().one()
    assert attempt["status"] == "submitted"
    assert attempt["export_rows"] is None
    assert attempt["export_columns"] is None


@pytest.mark.integration
@pytest.mark.parametrize(
    "error_code",
    ["invalid_bulk_export_total_rows", "invalid_bulk_export_total_columns"],
)
def test_invalid_terminal_count_metadata_fails_before_download(
    disposable_postgres_engine: Any,
    tmp_path: Path,
    error_code: str,
) -> None:
    engine = disposable_postgres_engine
    seed_attempt(engine, "submitted")
    client = StubClient(
        archive=archive_bytes(),
        poll_error=UsaSpendingError(error_code),
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
    assert result.error_code == error_code
    assert client.download_calls == 0


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
