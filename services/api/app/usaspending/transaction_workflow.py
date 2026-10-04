"""Restartable orchestration for one USAspending transaction-export month."""

from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, and_, insert, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.db.models import UsaSpendingTransactionIngestionAttempt
from app.usaspending.client import (
    BULK_TRANSACTION_FIELDS,
    BulkExportJob,
    BulkExportStatus,
    UsaSpendingClient,
    UsaSpendingError,
)
from app.usaspending.fiscal_years import fiscal_month_bounds
from app.usaspending.transaction_export import DEFAULT_MAX_ROWS, TransactionExportError
from app.usaspending.transaction_ingestion import (
    BulkJobMetadata,
    TransactionIngestionError,
    TransactionIngestionLoader,
    TransactionIngestionResult,
)


SOURCE = "usaspending"
FIXED_ARCHIVE_FILENAME = "transactions.zip"
TERMINAL_STATUSES = frozenset({"completed", "failed"})
SAFE_ERROR_CODE = re.compile(r"[a-z0-9_]{1,128}\Z")
TRANSIENT_UPSTREAM_ERRORS = frozenset(
    {
        "timeout_or_network",
        "bulk_export_timeout",
        "bulk_export_transfer_timeout",
        "bulk_export_destination_error",
        "bulk_export_destination_write_failed",
        "bulk_export_partial_cleanup_failed",
    }
)
DETERMINISTIC_UPSTREAM_ERRORS = frozenset(
    {
        "bulk_export_failed",
        "bulk_export_rejected",
        "invalid_bulk_export_status",
        "invalid_bulk_export_total_rows",
        "invalid_bulk_export_total_columns",
        "bulk_export_not_finished",
        "invalid_content_length",
        "bulk_export_archive_too_large",
        "unsafe_status_url",
        "unsafe_file_url",
        "invalid_redirect",
        "too_many_redirects",
    }
)
AttemptRow = dict[str, Any]
TemporaryDirectoryFactory = Callable[[], AbstractContextManager[Path | str]]


class TransactionWorkflowError(RuntimeError):
    """A stable workflow error that never contains upstream response data."""


class _DeterministicFailure(TransactionWorkflowError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class TransactionWorkflowResult:
    """Persisted workflow state returned after one bounded invocation."""

    attempt_id: UUID
    status: str
    pre_submission_rows: int | None
    export_rows: int | None
    loaded_rows: int | None
    count_drift: int | None
    signed_obligation_total: Decimal | None
    checkpoint_id: UUID | None
    error_code: str | None
    operator_resolution_required: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def _private_temporary_directory() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="govtracts-usaspending-workflow-") as name:
        yield Path(name)


class TransactionIngestionWorkflow:
    """Advance one FY/month attempt through the restartable state machine."""

    def __init__(
        self,
        engine: Engine,
        client: UsaSpendingClient,
        loader: TransactionIngestionLoader,
        *,
        clock: Callable[[], datetime] = _utc_now,
        temporary_directory_factory: TemporaryDirectoryFactory = (
            _private_temporary_directory
        ),
    ) -> None:
        if engine.dialect.name != "postgresql":
            raise TransactionWorkflowError("postgresql_engine_required")
        self._engine = engine
        self._client = client
        self._loader = loader
        self._clock = clock
        self._temporary_directory_factory = temporary_directory_factory
        self._attempts = UsaSpendingTransactionIngestionAttempt.__table__

    async def run(
        self,
        *,
        fiscal_year: int,
        calendar_year: int,
        calendar_month: int,
    ) -> TransactionWorkflowResult:
        """Run or resume exactly one validated fiscal calendar month."""
        try:
            period_start, period_end = fiscal_month_bounds(
                fiscal_year,
                calendar_year=calendar_year,
                calendar_month=calendar_month,
            )
        except (TypeError, ValueError):
            raise TransactionWorkflowError("invalid_fiscal_period") from None

        try:
            attempt = self._locate_or_create_attempt(
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
            )
        except SQLAlchemyError:
            raise TransactionWorkflowError("database_operation_failed") from None

        submission_claimed = False
        local_archive: Path | None = None
        local_sha256: str | None = None
        local_bytes: int | None = None

        with ExitStack() as resources:
            for _ in range(12):
                status = attempt["status"]
                if status == "completed":
                    return self._result(attempt)
                if status == "failed":
                    return self._result(attempt)
                if status == "submission_unknown":
                    return self._result(attempt)

                if status == "created":
                    try:
                        expected_rows = await asyncio.to_thread(
                            self._client.get_expected_transaction_count,
                            period_start,
                            period_end,
                        )
                    except UsaSpendingError as error:
                        attempt = self._record_resumable_failure(
                            attempt,
                            expected_status="created",
                            code=_safe_upstream_code(error, "count_request_failed"),
                            detail="Expected-count request did not complete.",
                        )
                        return self._result(attempt)
                    attempt, transitioned, database_failed = self._transition_safely(
                        attempt,
                        expected_status="created",
                        next_status="counted",
                        values={
                            "expected_rows": expected_rows,
                            "last_error_code": None,
                            "last_error_detail": None,
                        },
                    )
                    if database_failed:
                        return self._result(attempt)
                    if not transitioned:
                        return self._result(attempt)
                    continue

                if status == "counted":
                    (
                        attempt,
                        submission_claimed,
                        database_failed,
                    ) = self._transition_safely(
                        attempt,
                        expected_status="counted",
                        next_status="submitting",
                        values={
                            "last_error_code": None,
                            "last_error_detail": None,
                        },
                    )
                    if database_failed:
                        return self._result(attempt)
                    if not submission_claimed:
                        return self._result(attempt)
                    continue

                if status == "submitting":
                    if not submission_claimed:
                        attempt = self._mark_submission_unknown(attempt)
                        return self._result(attempt)
                    try:
                        job = await asyncio.to_thread(
                            self._client.submit_bulk_export,
                            period_start,
                            period_end,
                        )
                    except UsaSpendingError:
                        attempt = self._mark_submission_unknown(attempt)
                        return self._result(attempt)
                    try:
                        attempt, persisted = self._transition(
                            attempt,
                            expected_status="submitting",
                            next_status="submitted",
                            values={
                                "status_url": job.status_url,
                                "file_url": job.file_url,
                                "remote_file_name": job.file_name,
                                "last_error_code": None,
                                "last_error_detail": None,
                            },
                        )
                    except SQLAlchemyError:
                        attempt = self._mark_submission_unknown(attempt)
                        return self._result(attempt)
                    submission_claimed = False
                    if not persisted:
                        return self._result(attempt)
                    continue

                if status == "submitted":
                    try:
                        job = self._job_from_attempt(attempt)
                        completed = await self._client.poll_bulk_export(job)
                    except _DeterministicFailure as error:
                        attempt = self._mark_failed(
                            attempt,
                            expected_status="submitted",
                            code=error.code,
                            detail=error.detail,
                        )
                        return self._result(attempt)
                    except UsaSpendingError as error:
                        code = _safe_upstream_code(error, "poll_failed")
                        if _is_transient_upstream(code):
                            attempt = self._record_resumable_failure(
                                attempt,
                                expected_status="submitted",
                                code=code,
                                detail="Bulk-export polling did not complete.",
                            )
                        else:
                            attempt = self._mark_failed(
                                attempt,
                                expected_status="submitted",
                                code=code,
                                detail="Bulk export ended in a deterministic failure.",
                            )
                        return self._result(attempt)
                    attempt, transitioned, database_failed = self._transition_safely(
                        attempt,
                        expected_status="submitted",
                        next_status="export_finished",
                        values={
                            "status_url": completed.status_url,
                            "file_url": completed.file_url,
                            "remote_file_name": completed.file_name,
                            "export_rows": completed.total_rows,
                            "export_columns": completed.total_columns,
                            "last_error_code": None,
                            "last_error_detail": None,
                        },
                    )
                    if database_failed:
                        return self._result(attempt)
                    if not transitioned:
                        return self._result(attempt)
                    continue

                if status in {"export_finished", "archive_hashed", "loading"}:
                    if local_archive is None:
                        try:
                            (
                                local_archive,
                                local_sha256,
                                local_bytes,
                            ) = await self._download_archive(resources, attempt)
                        except _DeterministicFailure as error:
                            attempt = self._mark_failed(
                                attempt,
                                expected_status=status,
                                code=error.code,
                                detail=error.detail,
                            )
                            return self._result(attempt)
                        except UsaSpendingError as error:
                            code = _safe_upstream_code(error, "download_failed")
                            if code in DETERMINISTIC_UPSTREAM_ERRORS:
                                attempt = self._mark_failed(
                                    attempt,
                                    expected_status=status,
                                    code=code,
                                    detail="Archive retrieval failed deterministic validation.",
                                )
                            else:
                                attempt = self._record_resumable_failure(
                                    attempt,
                                    expected_status=status,
                                    code=code,
                                    detail="Archive retrieval did not complete.",
                                )
                            return self._result(attempt)
                        except OSError:
                            attempt = self._record_resumable_failure(
                                attempt,
                                expected_status=status,
                                code="archive_read_failed",
                                detail="Downloaded archive could not be read.",
                            )
                            return self._result(attempt)

                    assert local_sha256 is not None
                    assert local_bytes is not None
                    if status == "export_finished":
                        (
                            attempt,
                            transitioned,
                            database_failed,
                        ) = self._transition_safely(
                            attempt,
                            expected_status="export_finished",
                            next_status="archive_hashed",
                            values={
                                "archive_sha256": local_sha256,
                                "archive_bytes": local_bytes,
                                "last_error_code": None,
                                "last_error_detail": None,
                            },
                        )
                        if database_failed:
                            return self._result(attempt)
                        if not transitioned:
                            return self._result(attempt)
                        continue

                    try:
                        self._validate_archive_identity(
                            attempt,
                            archive_sha256=local_sha256,
                            archive_bytes=local_bytes,
                        )
                    except _DeterministicFailure as error:
                        attempt = self._mark_failed(
                            attempt,
                            expected_status=status,
                            code=error.code,
                            detail=error.detail,
                        )
                        return self._result(attempt)

                    if status == "archive_hashed":
                        (
                            attempt,
                            transitioned,
                            database_failed,
                        ) = self._transition_safely(
                            attempt,
                            expected_status="archive_hashed",
                            next_status="loading",
                            values={
                                "last_error_code": None,
                                "last_error_detail": None,
                            },
                        )
                        if database_failed:
                            return self._result(attempt)
                        if not transitioned:
                            return self._result(attempt)
                        continue

                    try:
                        result = self._load_atomically(
                            attempt_id=attempt["id"],
                            archive_path=local_archive,
                            archive_sha256=local_sha256,
                            archive_bytes=local_bytes,
                            fiscal_year=fiscal_year,
                            period_start=period_start,
                            period_end=period_end,
                        )
                    except (TransactionExportError, TransactionIngestionError) as error:
                        attempt = self._mark_failed(
                            attempt,
                            expected_status="loading",
                            code=_safe_validation_code(error),
                            detail="Archive or transaction validation failed.",
                        )
                        return self._result(attempt)
                    except _DeterministicFailure as error:
                        attempt = self._mark_failed(
                            attempt,
                            expected_status="loading",
                            code=error.code,
                            detail=error.detail,
                        )
                        return self._result(attempt)
                    except (SQLAlchemyError, TransactionWorkflowError):
                        attempt = self._record_resumable_failure(
                            attempt,
                            expected_status="loading",
                            code="database_operation_failed",
                            detail="Atomic transaction loading did not complete.",
                        )
                        return self._result(attempt)
                    attempt = self._read_attempt(attempt["id"])
                    return self._result(attempt)

                raise TransactionWorkflowError("invalid_attempt_state")

        raise TransactionWorkflowError("workflow_transition_limit_exceeded")

    def _locate_or_create_attempt(
        self,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> AttemptRow:
        lock_key = _attempt_lock_key(SOURCE, period_start, period_end)
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_key)"),
                    {"lock_key": lock_key},
                )
                active = connection.execute(
                    self._period_attempt_statement(
                        period_start,
                        period_end,
                        active_only=True,
                    ).with_for_update()
                ).mappings().one_or_none()
                if active is not None:
                    return dict(active)

                completed = connection.execute(
                    self._period_attempt_statement(
                        period_start,
                        period_end,
                        completed_only=True,
                    )
                ).mappings().first()
                if completed is not None:
                    return dict(completed)

                attempt_id = uuid4()
                now = self._aware_now()
                connection.execute(
                    insert(self._attempts),
                    {
                        "id": attempt_id,
                        "source": SOURCE,
                        "fiscal_year": fiscal_year,
                        "period_start": period_start,
                        "period_end": period_end,
                        "status": "created",
                        "failure_count": 0,
                        "started_at": now,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                return self._read_attempt_on_connection(connection, attempt_id)
        except IntegrityError:
            with self._engine.begin() as connection:
                active = connection.execute(
                    self._period_attempt_statement(
                        period_start,
                        period_end,
                        active_only=True,
                    )
                ).mappings().one_or_none()
                if active is None:
                    raise
                return dict(active)

    def _period_attempt_statement(
        self,
        period_start: date,
        period_end: date,
        *,
        active_only: bool = False,
        completed_only: bool = False,
    ) -> Any:
        statement = select(self._attempts).where(
            and_(
                self._attempts.c.source == SOURCE,
                self._attempts.c.period_start == period_start,
                self._attempts.c.period_end == period_end,
            )
        )
        if active_only:
            statement = statement.where(
                self._attempts.c.status.not_in(TERMINAL_STATUSES)
            )
        if completed_only:
            statement = statement.where(self._attempts.c.status == "completed")
        return statement.order_by(
            self._attempts.c.created_at.desc(),
            self._attempts.c.id.desc(),
        ).limit(1)

    def _transition(
        self,
        attempt: Mapping[str, Any],
        *,
        expected_status: str,
        next_status: str,
        values: Mapping[str, Any],
    ) -> tuple[AttemptRow, bool]:
        now = self._aware_now()
        with self._engine.begin() as connection:
            result = connection.execute(
                update(self._attempts)
                .where(
                    and_(
                        self._attempts.c.id == attempt["id"],
                        self._attempts.c.status == expected_status,
                    )
                )
                .values(status=next_status, updated_at=now, **dict(values))
            )
            transitioned = result.rowcount == 1
            current = self._read_attempt_on_connection(connection, attempt["id"])
        return current, transitioned

    def _transition_safely(
        self,
        attempt: Mapping[str, Any],
        *,
        expected_status: str,
        next_status: str,
        values: Mapping[str, Any],
    ) -> tuple[AttemptRow, bool, bool]:
        try:
            current, transitioned = self._transition(
                attempt,
                expected_status=expected_status,
                next_status=next_status,
                values=values,
            )
            return current, transitioned, False
        except SQLAlchemyError:
            try:
                current = self._record_resumable_failure(
                    attempt,
                    expected_status=expected_status,
                    code="database_operation_failed",
                    detail="Workflow state transition did not complete.",
                )
            except SQLAlchemyError:
                raise TransactionWorkflowError("database_operation_failed") from None
            return current, False, True

    def _mark_submission_unknown(self, attempt: Mapping[str, Any]) -> AttemptRow:
        now = self._aware_now()
        with self._engine.begin() as connection:
            connection.execute(
                update(self._attempts)
                .where(
                    and_(
                        self._attempts.c.id == attempt["id"],
                        self._attempts.c.status == "submitting",
                    )
                )
                .values(
                    status="submission_unknown",
                    last_error_code="submission_outcome_unknown",
                    last_error_detail=(
                        "Bulk-export submission outcome requires operator resolution."
                    ),
                    failure_count=self._attempts.c.failure_count + 1,
                    updated_at=now,
                )
            )
            return self._read_attempt_on_connection(connection, attempt["id"])

    def _record_resumable_failure(
        self,
        attempt: Mapping[str, Any],
        *,
        expected_status: str,
        code: str,
        detail: str,
    ) -> AttemptRow:
        now = self._aware_now()
        with self._engine.begin() as connection:
            connection.execute(
                update(self._attempts)
                .where(
                    and_(
                        self._attempts.c.id == attempt["id"],
                        self._attempts.c.status == expected_status,
                    )
                )
                .values(
                    last_error_code=_bounded_error_code(code),
                    last_error_detail=_bounded_detail(detail),
                    failure_count=self._attempts.c.failure_count + 1,
                    updated_at=now,
                )
            )
            return self._read_attempt_on_connection(connection, attempt["id"])

    def _mark_failed(
        self,
        attempt: Mapping[str, Any],
        *,
        expected_status: str,
        code: str,
        detail: str,
    ) -> AttemptRow:
        now = self._aware_now()
        with self._engine.begin() as connection:
            connection.execute(
                update(self._attempts)
                .where(
                    and_(
                        self._attempts.c.id == attempt["id"],
                        self._attempts.c.status == expected_status,
                    )
                )
                .values(
                    status="failed",
                    last_error_code=_bounded_error_code(code),
                    last_error_detail=_bounded_detail(detail),
                    failure_count=self._attempts.c.failure_count + 1,
                    completed_at=now,
                    updated_at=now,
                )
            )
            return self._read_attempt_on_connection(connection, attempt["id"])

    async def _download_archive(
        self,
        resources: ExitStack,
        attempt: Mapping[str, Any],
    ) -> tuple[Path, str, int]:
        completed = self._completed_status_from_attempt(attempt)
        directory_value = resources.enter_context(
            self._temporary_directory_factory()
        )
        directory = Path(directory_value)
        if not directory.is_dir():
            raise _DeterministicFailure(
                "invalid_temporary_directory",
                "Private temporary directory is unavailable.",
            )
        destination = directory / FIXED_ARCHIVE_FILENAME
        reported_bytes = await self._client.retrieve_bulk_export(
            completed,
            destination,
        )
        archive_sha256, archive_bytes = _archive_identity(destination)
        if reported_bytes != archive_bytes:
            raise _DeterministicFailure(
                "archive_byte_count_mismatch",
                "Downloaded archive byte count did not reconcile.",
            )
        return destination, archive_sha256, archive_bytes

    def _load_atomically(
        self,
        *,
        attempt_id: UUID,
        archive_path: Path,
        archive_sha256: str,
        archive_bytes: int,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> TransactionIngestionResult:
        with self._engine.begin() as connection:
            attempt = connection.execute(
                select(self._attempts)
                .where(self._attempts.c.id == attempt_id)
                .with_for_update()
            ).mappings().one()
            self._validate_loading_attempt(
                attempt,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
                archive_sha256=archive_sha256,
                archive_bytes=archive_bytes,
            )
            result = self._loader.load_with_connection(
                connection,
                archive_path=archive_path,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
                expected_count=attempt["export_rows"],
                bulk_job=BulkJobMetadata(
                    status_url=attempt["status_url"],
                    file_name=attempt["remote_file_name"],
                ),
                expected_archive_sha256=attempt["archive_sha256"],
            )
            if result.loaded_rows != attempt["export_rows"]:
                raise _DeterministicFailure(
                    "loaded_row_count_mismatch",
                    "Loaded rows differ from the persisted export count.",
                )
            self._complete_attempt(connection, attempt_id, result)
            return result

    def _complete_attempt(
        self,
        connection: Connection,
        attempt_id: UUID,
        result: TransactionIngestionResult,
    ) -> None:
        now = self._aware_now()
        updated = connection.execute(
            update(self._attempts)
            .where(
                and_(
                    self._attempts.c.id == attempt_id,
                    self._attempts.c.status == "loading",
                )
            )
            .values(
                status="completed",
                loaded_rows=result.loaded_rows,
                signed_obligation_total=result.signed_obligation_total,
                checkpoint_id=result.checkpoint_id,
                last_error_code=None,
                last_error_detail=None,
                completed_at=now,
                updated_at=now,
            )
        )
        if updated.rowcount != 1:
            raise TransactionWorkflowError("attempt_completion_conflict")

    def _validate_loading_attempt(
        self,
        attempt: Mapping[str, Any],
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        archive_sha256: str,
        archive_bytes: int,
    ) -> None:
        if (
            attempt["source"] != SOURCE
            or attempt["status"] != "loading"
            or attempt["fiscal_year"] != fiscal_year
            or attempt["period_start"] != period_start
            or attempt["period_end"] != period_end
            or not isinstance(attempt["expected_rows"], int)
            or isinstance(attempt["expected_rows"], bool)
            or attempt["expected_rows"] < 0
            or not isinstance(attempt["export_rows"], int)
            or isinstance(attempt["export_rows"], bool)
            or attempt["export_rows"] < 0
            or attempt["export_rows"] > DEFAULT_MAX_ROWS
            or (
                attempt.get("export_columns") is not None
                and (
                    not isinstance(attempt["export_columns"], int)
                    or isinstance(attempt["export_columns"], bool)
                    or attempt["export_columns"] != len(BULK_TRANSACTION_FIELDS)
                )
            )
            or not _present_string(attempt["status_url"])
            or not _present_string(attempt["file_url"])
            or not _present_string(attempt["remote_file_name"])
        ):
            raise _DeterministicFailure(
                "invalid_loading_attempt",
                "Persisted loading metadata is inconsistent.",
            )
        self._validate_archive_identity(
            attempt,
            archive_sha256=archive_sha256,
            archive_bytes=archive_bytes,
        )

    @staticmethod
    def _validate_archive_identity(
        attempt: Mapping[str, Any],
        *,
        archive_sha256: str,
        archive_bytes: int,
    ) -> None:
        if attempt.get("archive_bytes") != archive_bytes:
            raise _DeterministicFailure(
                "archive_byte_count_mismatch",
                "Downloaded archive byte count differs from the persisted value.",
            )
        if attempt.get("archive_sha256") != archive_sha256:
            raise _DeterministicFailure(
                "archive_sha256_mismatch",
                "Downloaded archive hash differs from the persisted value.",
            )

    @staticmethod
    def _job_from_attempt(attempt: Mapping[str, Any]) -> BulkExportJob:
        if not all(
            _present_string(attempt.get(name))
            for name in ("status_url", "file_url", "remote_file_name")
        ):
            raise _DeterministicFailure(
                "invalid_persisted_job_metadata",
                "Persisted bulk-export job metadata is incomplete.",
            )
        return BulkExportJob(
            status_url=attempt["status_url"],
            file_url=attempt["file_url"],
            file_name=attempt["remote_file_name"],
        )

    @staticmethod
    def _completed_status_from_attempt(
        attempt: Mapping[str, Any],
    ) -> BulkExportStatus:
        job = TransactionIngestionWorkflow._job_from_attempt(attempt)
        total_rows = attempt.get("export_rows")
        total_columns = attempt.get("export_columns")
        if (
            not isinstance(total_rows, int)
            or isinstance(total_rows, bool)
            or total_rows < 0
            or total_rows > DEFAULT_MAX_ROWS
        ):
            raise _DeterministicFailure(
                "invalid_persisted_export_rows",
                "Persisted export row metadata is invalid.",
            )
        if total_columns is not None and (
            not isinstance(total_columns, int)
            or isinstance(total_columns, bool)
            or total_columns != len(BULK_TRANSACTION_FIELDS)
        ):
            raise _DeterministicFailure(
                "invalid_persisted_export_columns",
                "Persisted export column metadata is invalid.",
            )
        return BulkExportStatus(
            status="finished",
            status_url=job.status_url,
            file_url=job.file_url,
            file_name=job.file_name,
            total_rows=total_rows,
            total_columns=total_columns,
            message=None,
            seconds_elapsed=None,
        )

    def _read_attempt(self, attempt_id: UUID) -> AttemptRow:
        with self._engine.connect() as connection:
            row = connection.execute(
                select(self._attempts).where(self._attempts.c.id == attempt_id)
            ).mappings().one()
            return dict(row)

    def _read_attempt_on_connection(
        self,
        connection: Connection,
        attempt_id: UUID,
    ) -> AttemptRow:
        row = connection.execute(
            select(self._attempts).where(self._attempts.c.id == attempt_id)
        ).mappings().one()
        return dict(row)

    def _aware_now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise TransactionWorkflowError("clock_must_return_aware_datetime")
        return value.astimezone(timezone.utc)

    @staticmethod
    def _result(attempt: Mapping[str, Any]) -> TransactionWorkflowResult:
        pre_submission_rows = attempt.get("expected_rows")
        export_rows = attempt.get("export_rows")
        count_drift = (
            export_rows - pre_submission_rows
            if isinstance(pre_submission_rows, int)
            and not isinstance(pre_submission_rows, bool)
            and isinstance(export_rows, int)
            and not isinstance(export_rows, bool)
            else None
        )
        return TransactionWorkflowResult(
            attempt_id=attempt["id"],
            status=attempt["status"],
            pre_submission_rows=pre_submission_rows,
            export_rows=export_rows,
            loaded_rows=attempt.get("loaded_rows"),
            count_drift=count_drift,
            signed_obligation_total=attempt.get("signed_obligation_total"),
            checkpoint_id=attempt.get("checkpoint_id"),
            error_code=attempt.get("last_error_code"),
            operator_resolution_required=(
                attempt["status"] == "submission_unknown"
            ),
        )


def _attempt_lock_key(source: str, period_start: date, period_end: date) -> int:
    digest = hashlib.sha256(
        f"attempt:{source}:{period_start.isoformat()}:{period_end.isoformat()}".encode(
            "ascii"
        )
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _archive_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _present_string(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _safe_upstream_code(error: UsaSpendingError, default: str) -> str:
    value = str(error)
    if (
        value in TRANSIENT_UPSTREAM_ERRORS
        or value in DETERMINISTIC_UPSTREAM_ERRORS
        or re.fullmatch(r"http_[45][0-9]{2}", value) is not None
    ):
        return value
    return default


def _is_transient_upstream(code: str) -> bool:
    if code in TRANSIENT_UPSTREAM_ERRORS:
        return True
    if code == "http_429":
        return True
    if code.startswith("http_5") and len(code) == 8:
        return True
    return False


def _safe_validation_code(
    error: TransactionExportError | TransactionIngestionError,
) -> str:
    del error
    return "transaction_validation_failed"


def _bounded_error_code(value: str) -> str:
    if SAFE_ERROR_CODE.fullmatch(value):
        return value
    return "workflow_error"


def _bounded_detail(value: str) -> str:
    return value[:512]
