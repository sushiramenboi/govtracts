"""Restartable orchestration for one USAspending transaction-export month."""

from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, and_, func, insert, or_, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.db.models import (
    UsaSpendingIngestionCheckpoint,
    UsaSpendingTransaction,
    UsaSpendingTransactionIngestionAttempt,
)
from app.usaspending.client import (
    BULK_TRANSACTION_FIELDS,
    BulkExportJob,
    BulkExportStatus,
    UsaSpendingClient,
    UsaSpendingError,
)
from app.usaspending.fiscal_years import (
    FiscalPeriod,
    fiscal_month_bounds,
    split_period,
    validate_subperiod,
)
from app.usaspending.transaction_export import DEFAULT_MAX_ROWS, TransactionExportError
from app.usaspending.transaction_ingestion import (
    BulkJobMetadata,
    TransactionIngestionError,
    TransactionIngestionLoader,
    TransactionIngestionResult,
    probe_private_directory,
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
CapacityGuard = Callable[[int], None]
SubmissionObserver = Callable[[], None]


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


@dataclass(frozen=True)
class RecoveryClaim:
    """An immutable, detached receipt for one already-submitted recovery job."""

    failed_attempt_id: UUID
    recovery_attempt_id: UUID
    attempt: Mapping[str, Any]
    created: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def _private_temporary_directory() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="govtracts-usaspending-workflow-") as name:
        yield Path(name)


def assert_owned_period_state_on_connection(
    connection: Connection,
    period: FiscalPeriod,
    *,
    exact_attempt: Mapping[str, Any] | None,
) -> None:
    """Validate overlapping attempts and data using the claim's transaction."""
    attempts_table = UsaSpendingTransactionIngestionAttempt.__table__
    checkpoints = UsaSpendingIngestionCheckpoint.__table__
    transactions = UsaSpendingTransaction.__table__
    attempts = connection.execute(
        select(attempts_table).where(
            and_(
                attempts_table.c.source == SOURCE,
                attempts_table.c.period_start <= period.end,
                attempts_table.c.period_end >= period.start,
            )
        )
    ).mappings().all()
    allowed_attempt_ids = _owned_attempt_ids(
        attempts, exact_attempt=exact_attempt, period=period
    )
    if any(row["id"] not in allowed_attempt_ids for row in attempts):
        raise TransactionWorkflowError("unexpected_overlapping_state")

    allowed_checkpoint_ids = {
        row["checkpoint_id"]
        for row in attempts
        if row["id"] in allowed_attempt_ids
        and isinstance(row["checkpoint_id"], UUID)
    }
    checkpoint_ids = set(
        connection.scalars(
            select(checkpoints.c.id).where(
                and_(
                    checkpoints.c.source == SOURCE,
                    checkpoints.c.period_start <= period.end,
                    checkpoints.c.period_end >= period.start,
                )
            )
        )
    )
    if checkpoint_ids - allowed_checkpoint_ids:
        raise TransactionWorkflowError("unexpected_overlapping_state")

    transaction_filter = transactions.c.action_date.between(period.start, period.end)
    if allowed_checkpoint_ids:
        transaction_filter = and_(
            transaction_filter,
            or_(
                transactions.c.ingestion_checkpoint_id.is_(None),
                transactions.c.ingestion_checkpoint_id.not_in(allowed_checkpoint_ids),
            ),
        )
    if connection.scalar(select(func.count()).where(transaction_filter)):
        raise TransactionWorkflowError("unexpected_overlapping_state")


def _owned_attempt_ids(
    rows: Sequence[Mapping[str, Any]],
    *,
    exact_attempt: Mapping[str, Any] | None,
    period: FiscalPeriod,
) -> set[UUID]:
    if exact_attempt is None:
        return set()
    exact_id = exact_attempt.get("id")
    if not isinstance(exact_id, UUID):
        raise TransactionWorkflowError("unexpected_overlapping_state")
    by_id = {row["id"]: row for row in rows}
    root = by_id.get(exact_id)
    if root is None:
        raise TransactionWorkflowError("unexpected_overlapping_state")

    allowed: set[UUID] = set()

    def allow_failed_peers(
        current: Mapping[str, Any], current_period: FiscalPeriod
    ) -> None:
        for peer in rows:
            if (
                peer["id"] != current["id"]
                and peer["status"] == "failed"
                and peer["parent_attempt_id"] == current["parent_attempt_id"]
                and peer["period_start"] == current_period.start
                and peer["period_end"] == current_period.end
            ):
                allowed.add(peer["id"])

    ancestor = root
    seen_ancestor_ids = {exact_id}
    while isinstance(ancestor["parent_attempt_id"], UUID):
        parent_id = ancestor["parent_attempt_id"]
        if parent_id in seen_ancestor_ids:
            raise TransactionWorkflowError("unexpected_overlapping_state")
        parent = by_id.get(parent_id)
        if parent is None or parent["status"] != "subdivided":
            raise TransactionWorkflowError("unexpected_overlapping_state")
        parent_period = FiscalPeriod(
            parent["fiscal_year"], parent["period_start"], parent["period_end"]
        )
        if (
            ancestor["period_start"],
            ancestor["period_end"],
        ) not in split_period(parent_period.start, parent_period.end):
            raise TransactionWorkflowError("unexpected_overlapping_state")
        allowed.add(parent_id)
        allow_failed_peers(parent, parent_period)
        seen_ancestor_ids.add(parent_id)
        ancestor = parent

    def visit(current: Mapping[str, Any], current_period: FiscalPeriod) -> None:
        current_id = current["id"]
        allowed.add(current_id)
        allow_failed_peers(current, current_period)
        children = [row for row in rows if row["parent_attempt_id"] == current_id]
        if current["status"] != "subdivided":
            if children:
                raise TransactionWorkflowError("unexpected_overlapping_state")
            return
        expected_bounds = split_period(current_period.start, current_period.end)
        for child_start, child_end in expected_bounds:
            candidates = [
                row for row in children
                if row["period_start"] == child_start
                and row["period_end"] == child_end
            ]
            current_candidates = [
                row for row in candidates if row["status"] != "failed"
            ]
            if len(current_candidates) > 1 or not candidates:
                raise TransactionWorkflowError("unexpected_overlapping_state")
            selected = (
                current_candidates[0]
                if current_candidates
                else max(candidates, key=lambda row: (row["created_at"], row["id"]))
            )
            allowed.update(row["id"] for row in candidates if row["status"] == "failed")
            visit(selected, FiscalPeriod(period.fiscal_year, child_start, child_end))
        if any(
            (row["period_start"], row["period_end"]) not in expected_bounds
            for row in children
        ):
            raise TransactionWorkflowError("unexpected_overlapping_state")

    visit(root, period)
    return allowed


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

    def claim_failed_job_recovery(
        self,
        failed_attempt_id: UUID,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> RecoveryClaim:
        """Atomically claim an eligible failed HTTP-404 job without remote I/O."""
        if not isinstance(failed_attempt_id, UUID):
            raise TransactionWorkflowError("invalid_failed_attempt_id")
        try:
            validate_subperiod(fiscal_year, period_start, period_end)
        except (TypeError, ValueError):
            raise TransactionWorkflowError("invalid_fiscal_period") from None

        lock_key = _attempt_lock_key(SOURCE, period_start, period_end)
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_key)"),
                    {"lock_key": lock_key},
                )
                source = self._claim_source_on_connection(
                    connection, failed_attempt_id, fiscal_year, period_start, period_end
                )
                linked = self._linked_recovery_on_connection(connection, source)
                if linked is not None:
                    self._validate_linked_recovery(connection, source, linked)
                    if linked["status"] != "completed":
                        self._require_empty_recovery_period(connection, source)
                    return self._recovery_claim(source, linked, created=False)

                conflict = connection.execute(
                    select(self._attempts.c.id).where(
                        self._attempts.c.source == SOURCE,
                        self._attempts.c.period_start == period_start,
                        self._attempts.c.period_end == period_end,
                        self._attempts.c.status.not_in(TERMINAL_STATUSES),
                    ).limit(1)
                ).first()
                if conflict is not None:
                    raise TransactionWorkflowError("recovery_period_conflict")
                self._require_empty_recovery_period(connection, source)
                assert_owned_period_state_on_connection(
                    connection,
                    FiscalPeriod(fiscal_year, period_start, period_end),
                    exact_attempt=source,
                )

                recovery_id = uuid4()
                now = self._aware_now()
                connection.execute(
                    insert(self._attempts),
                    {
                        "id": recovery_id,
                        "source": source["source"],
                        "fiscal_year": source["fiscal_year"],
                        "period_start": source["period_start"],
                        "period_end": source["period_end"],
                        "status": "submitted",
                        "expected_rows": source["expected_rows"],
                        "status_url": source["status_url"],
                        "file_url": source["file_url"],
                        "remote_file_name": source["remote_file_name"],
                        "parent_attempt_id": source["parent_attempt_id"],
                        "recovery_of_attempt_id": source["id"],
                        "failure_count": 0,
                        "started_at": now,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                recovery = self._read_attempt_on_connection(connection, recovery_id)
                return self._recovery_claim(source, recovery, created=True)
        except IntegrityError:
            # The original transaction has rolled back. A concurrent writer may
            # have won the unique recovery lineage; accept only that exact row.
            try:
                with self._engine.begin() as connection:
                    connection.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_key)"),
                        {"lock_key": lock_key},
                    )
                    source = self._claim_source_on_connection(
                        connection, failed_attempt_id, fiscal_year,
                        period_start, period_end,
                    )
                    linked = self._linked_recovery_on_connection(connection, source)
                    if linked is None:
                        raise TransactionWorkflowError("recovery_claim_conflict")
                    self._validate_linked_recovery(connection, source, linked)
                    if linked["status"] != "completed":
                        self._require_empty_recovery_period(connection, source)
                    return self._recovery_claim(source, linked, created=False)
            except (SQLAlchemyError, TransactionWorkflowError):
                raise TransactionWorkflowError("recovery_claim_conflict") from None
        except SQLAlchemyError:
            raise TransactionWorkflowError("database_operation_failed") from None

    def _claim_source_on_connection(
        self,
        connection: Connection,
        failed_attempt_id: UUID,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> AttemptRow:
        row = connection.execute(
            select(self._attempts)
            .where(self._attempts.c.id == failed_attempt_id)
            .with_for_update()
        ).mappings().one_or_none()
        if row is None:
            raise TransactionWorkflowError("recovery_source_ineligible")
        source = dict(row)
        if (
            source["source"] != SOURCE
            or source["fiscal_year"] != fiscal_year
            or source["period_start"] != period_start
            or source["period_end"] != period_end
            or source["status"] != "failed"
            or source["last_error_code"] != "http_404"
            or source["completed_at"] is None
            or source["recovery_of_attempt_id"] is not None
            or not _valid_recovery_count(source["expected_rows"])
        ):
            raise TransactionWorkflowError("recovery_source_ineligible")
        identity = tuple(source[name] for name in (
            "status_url", "file_url", "remote_file_name"
        ))
        if all(value is None for value in identity):
            raise TransactionWorkflowError("recovery_job_absent")
        if not all(_present_string(value) for value in identity):
            raise TransactionWorkflowError("recovery_job_corrupt")
        try:
            self._client.validate_persisted_bulk_export_job(
                BulkExportJob(identity[0], identity[1], identity[2])
            )
        except (UsaSpendingError, TypeError, ValueError):
            raise TransactionWorkflowError("recovery_job_unsafe") from None
        if any(source[name] is not None for name in (
            "export_rows", "export_columns", "archive_sha256", "archive_bytes",
            "loaded_rows", "signed_obligation_total", "checkpoint_id",
        )):
            raise TransactionWorkflowError("recovery_source_dirty")
        return source

    def _linked_recovery_on_connection(
        self, connection: Connection, source: Mapping[str, Any]
    ) -> AttemptRow | None:
        rows = connection.execute(
            select(self._attempts)
            .where(self._attempts.c.recovery_of_attempt_id == source["id"])
            .limit(2)
        ).mappings().all()
        if len(rows) > 1:
            raise TransactionWorkflowError("recovery_link_conflict")
        return dict(rows[0]) if rows else None

    def _require_empty_recovery_period(
        self, connection: Connection, source: Mapping[str, Any]
    ) -> None:
        checkpoints = UsaSpendingIngestionCheckpoint.__table__
        transactions = UsaSpendingTransaction.__table__
        checkpoint = connection.execute(
            select(checkpoints.c.id).where(
                checkpoints.c.source == SOURCE,
                checkpoints.c.period_start == source["period_start"],
                checkpoints.c.period_end == source["period_end"],
            ).limit(1)
        ).first()
        transaction = connection.execute(
            select(transactions.c.stable_transaction_id).where(
                transactions.c.action_date >= source["period_start"],
                transactions.c.action_date <= source["period_end"],
            ).limit(1)
        ).first()
        if checkpoint is not None or transaction is not None:
            raise TransactionWorkflowError("recovery_period_not_empty")

    def _validate_linked_recovery(
        self,
        connection: Connection,
        source: Mapping[str, Any],
        recovery: Mapping[str, Any],
    ) -> None:
        copied = (
            "source", "fiscal_year", "period_start", "period_end",
            "parent_attempt_id", "expected_rows", "status_url", "file_url",
            "remote_file_name",
        )
        if (
            recovery["recovery_of_attempt_id"] != source["id"]
            or recovery["id"] == source["id"]
            or any(recovery[name] != source[name] for name in copied)
            or recovery["status"] not in {
                "submitted", "export_finished", "archive_hashed", "loading", "completed"
            }
        ):
            raise TransactionWorkflowError("recovery_link_invalid")
        status = recovery["status"]
        if status == "submitted":
            if any(recovery[name] is not None for name in (
                "export_rows", "export_columns", "archive_sha256", "archive_bytes",
                "loaded_rows", "signed_obligation_total", "checkpoint_id", "completed_at",
            )):
                raise TransactionWorkflowError("recovery_link_invalid")
            return
        if (
            not _valid_recovery_count(recovery["export_rows"])
            or recovery["export_columns"] not in (None, len(BULK_TRANSACTION_FIELDS))
        ):
            raise TransactionWorkflowError("recovery_link_invalid")
        if status == "export_finished":
            if any(recovery[name] is not None for name in (
                "archive_sha256", "archive_bytes", "loaded_rows",
                "signed_obligation_total", "checkpoint_id", "completed_at",
            )):
                raise TransactionWorkflowError("recovery_link_invalid")
            return
        if (
            not isinstance(recovery["archive_sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", recovery["archive_sha256"]) is None
            or not isinstance(recovery["archive_bytes"], int)
            or isinstance(recovery["archive_bytes"], bool)
            or recovery["archive_bytes"] < 0
        ):
            raise TransactionWorkflowError("recovery_link_invalid")
        if status in {"archive_hashed", "loading"}:
            if any(recovery[name] is not None for name in (
                "loaded_rows", "signed_obligation_total", "checkpoint_id", "completed_at",
            )):
                raise TransactionWorkflowError("recovery_link_invalid")
            return
        if (
            recovery["loaded_rows"] != recovery["export_rows"]
            or not isinstance(recovery["signed_obligation_total"], Decimal)
            or not isinstance(recovery["checkpoint_id"], UUID)
            or recovery["completed_at"] is None
            or recovery["last_error_code"] is not None
            or recovery["last_error_detail"] is not None
        ):
            raise TransactionWorkflowError("recovery_link_invalid")
        checkpoints = UsaSpendingIngestionCheckpoint.__table__
        checkpoint = connection.execute(
            select(checkpoints).where(checkpoints.c.id == recovery["checkpoint_id"])
        ).mappings().one_or_none()
        if (
            checkpoint is None
            or checkpoint["source"] != SOURCE
            or checkpoint["fiscal_year"] != source["fiscal_year"]
            or checkpoint["period_start"] != source["period_start"]
            or checkpoint["period_end"] != source["period_end"]
            or checkpoint["status"] != "complete"
            or checkpoint["loaded_rows"] != recovery["loaded_rows"]
        ):
            raise TransactionWorkflowError("recovery_link_invalid")
        transactions = UsaSpendingTransaction.__table__
        owned_rows = connection.scalar(
            select(func.count()).select_from(transactions).where(
                transactions.c.ingestion_checkpoint_id == recovery["checkpoint_id"],
                transactions.c.action_date >= source["period_start"],
                transactions.c.action_date <= source["period_end"],
            )
        )
        if owned_rows != recovery["loaded_rows"]:
            raise TransactionWorkflowError("recovery_link_invalid")

    @staticmethod
    def _recovery_claim(
        source: Mapping[str, Any], recovery: Mapping[str, Any], *, created: bool
    ) -> RecoveryClaim:
        return RecoveryClaim(
            failed_attempt_id=source["id"],
            recovery_attempt_id=recovery["id"],
            attempt=MappingProxyType(dict(recovery)),
            created=created,
        )

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

        return await self.run_period(
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
        )

    async def run_period(
        self,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        stop_after_count: bool = False,
        capacity_guard: CapacityGuard | None = None,
        retry_parent_attempt_id: UUID | None = None,
        submission_observer: SubmissionObserver | None = None,
    ) -> TransactionWorkflowResult:
        """Run or resume one validated same-month, same-FY inclusive period."""
        if not isinstance(stop_after_count, bool):
            raise TransactionWorkflowError("invalid_stop_after_count")
        if capacity_guard is not None and not callable(capacity_guard):
            raise TransactionWorkflowError("invalid_capacity_guard")
        if submission_observer is not None and not callable(submission_observer):
            raise TransactionWorkflowError("invalid_submission_observer")
        if retry_parent_attempt_id is not None and not isinstance(
            retry_parent_attempt_id, UUID
        ):
            raise TransactionWorkflowError("invalid_retry_parent")
        try:
            validate_subperiod(fiscal_year, period_start, period_end)
        except (TypeError, ValueError):
            raise TransactionWorkflowError("invalid_fiscal_period") from None

        try:
            attempt = self._locate_or_create_attempt(
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
                parent_attempt_id=retry_parent_attempt_id,
            )
        except SQLAlchemyError:
            raise TransactionWorkflowError("database_operation_failed") from None

        if attempt.get("recovery_of_attempt_id") is not None:
            raise TransactionWorkflowError("recovery_authorization_required")

        submission_claimed = False
        submitted_in_this_invocation = False

        with ExitStack():
            for transition_index in range(12):
                status = attempt["status"]
                if status == "completed":
                    return self._result(attempt)
                if status == "failed":
                    return self._result(attempt)
                if status == "submission_unknown":
                    return self._result(attempt)
                if status == "subdivided":
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
                    expected_rows = attempt.get("expected_rows")
                    if (
                        not isinstance(expected_rows, int)
                        or isinstance(expected_rows, bool)
                        or expected_rows < 0
                    ):
                        raise TransactionWorkflowError("invalid_expected_count")
                    if stop_after_count or expected_rows > DEFAULT_MAX_ROWS:
                        return self._result(attempt)
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
                    submitted_in_this_invocation = True
                    if submission_observer is not None:
                        submission_observer()
                    continue

                if status in {"submitted", "export_finished", "archive_hashed", "loading"}:
                    return await self._resume_persisted_attempt(
                        attempt,
                        fiscal_year=fiscal_year,
                        period_start=period_start,
                        period_end=period_end,
                        capacity_guard=capacity_guard,
                        initial_status_404_grace=submitted_in_this_invocation,
                        require_persisted_identity=False,
                        max_transitions=12 - transition_index,
                    )

                raise TransactionWorkflowError("invalid_attempt_state")

        raise TransactionWorkflowError("workflow_transition_limit_exceeded")

    async def resume_failed_job_recovery(
        self,
        *,
        recovery_attempt_id: UUID,
        expected_failed_attempt_id: UUID,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        capacity_guard: CapacityGuard | None = None,
    ) -> TransactionWorkflowResult:
        """Resume one exact persisted recovery job without a new export submission."""
        if not isinstance(recovery_attempt_id, UUID):
            raise TransactionWorkflowError("invalid_recovery_attempt_id")
        if not isinstance(expected_failed_attempt_id, UUID):
            raise TransactionWorkflowError("invalid_failed_attempt_id")
        if capacity_guard is not None and not callable(capacity_guard):
            raise TransactionWorkflowError("invalid_capacity_guard")
        try:
            validate_subperiod(fiscal_year, period_start, period_end)
        except (TypeError, ValueError):
            raise TransactionWorkflowError("invalid_fiscal_period") from None

        try:
            with self._engine.begin() as connection:
                source = self._claim_source_on_connection(
                    connection,
                    expected_failed_attempt_id,
                    fiscal_year,
                    period_start,
                    period_end,
                )
                row = connection.execute(
                    select(self._attempts)
                    .where(self._attempts.c.id == recovery_attempt_id)
                    .with_for_update()
                ).mappings().one_or_none()
                if row is None:
                    raise TransactionWorkflowError("recovery_attempt_not_found")
                recovery = dict(row)
                if recovery["recovery_of_attempt_id"] != source["id"]:
                    raise TransactionWorkflowError("recovery_lineage_mismatch")
                self._validate_linked_recovery(connection, source, recovery)
                if recovery["status"] != "completed":
                    self._require_empty_recovery_period(connection, source)
                try:
                    job = self._job_from_attempt(recovery)
                    self._client.validate_persisted_bulk_export_job(job)
                except (_DeterministicFailure, UsaSpendingError, TypeError, ValueError):
                    raise TransactionWorkflowError("recovery_job_unsafe") from None
        except SQLAlchemyError:
            raise TransactionWorkflowError("database_operation_failed") from None

        if recovery["status"] == "completed":
            return self._result(recovery)
        return await self._resume_persisted_attempt(
            recovery,
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
            capacity_guard=capacity_guard,
            initial_status_404_grace=False,
            require_persisted_identity=True,
        )

    async def _resume_persisted_attempt(
        self,
        attempt: AttemptRow,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        capacity_guard: CapacityGuard | None,
        initial_status_404_grace: bool,
        require_persisted_identity: bool,
        max_transitions: int = 12,
    ) -> TransactionWorkflowResult:
        """Advance an already-selected submitted job through persisted states."""
        local_archive: Path | None = None
        local_sha256: str | None = None
        local_bytes: int | None = None
        guarded_export_rows: int | None = None
        guarded_restart_without_export_rows = False

        with ExitStack() as resources:
            for _ in range(max_transitions):
                status = attempt["status"]
                if status in TERMINAL_STATUSES:
                    return self._result(attempt)

                if status == "submitted":
                    if (
                        capacity_guard is not None
                        and not guarded_restart_without_export_rows
                        and attempt.get("export_rows") is None
                        and not initial_status_404_grace
                    ):
                        capacity_guard(DEFAULT_MAX_ROWS)
                        guarded_restart_without_export_rows = True
                    try:
                        job = self._job_from_attempt(attempt)
                        completed = await self._client.poll_bulk_export(
                            job,
                            initial_status_404_grace=initial_status_404_grace,
                        )
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
                    if require_persisted_identity and (
                        completed.status_url != attempt["status_url"]
                        or completed.file_url != attempt["file_url"]
                        or completed.file_name != attempt["remote_file_name"]
                    ):
                        attempt = self._mark_failed(
                            attempt,
                            expected_status="submitted",
                            code="recovery_job_identity_mismatch",
                            detail="Persisted recovery job identity changed.",
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
                    export_rows = attempt.get("export_rows")
                    if (
                        not isinstance(export_rows, int)
                        or isinstance(export_rows, bool)
                        or not 0 <= export_rows <= DEFAULT_MAX_ROWS
                    ):
                        raise TransactionWorkflowError(
                            "invalid_persisted_export_rows"
                        )
                    if (
                        capacity_guard is not None
                        and guarded_export_rows != export_rows
                    ):
                        capacity_guard(export_rows)
                        guarded_export_rows = export_rows
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
        parent_attempt_id: UUID | None = None,
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
                        "parent_attempt_id": parent_attempt_id,
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
        try:
            probe_private_directory(directory)
        except TransactionIngestionError:
            raise _DeterministicFailure(
                "invalid_temporary_directory",
                "Private temporary directory is unavailable.",
            ) from None
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


def _valid_recovery_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= DEFAULT_MAX_ROWS


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
