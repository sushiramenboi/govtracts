"""Controlled, serial USAspending transaction-backfill coordination."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, and_, case, func, insert, or_, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.db.models import (
    UsaSpendingIngestionCheckpoint,
    UsaSpendingTransaction,
    UsaSpendingTransactionIngestionAttempt,
)
from app.usaspending.client import DEFAULT_MAX_ARCHIVE_BYTES
from app.usaspending.fiscal_years import (
    FiscalPeriod,
    assert_exact_period_coverage,
    fiscal_month_bounds,
    split_period,
    validate_backfill_period,
)
from app.usaspending.transaction_export import (
    DEFAULT_MAX_ROWS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
)
from app.usaspending.transaction_ingestion import (
    TemporaryDirectoryIdentity,
    TransactionIngestionError,
    probe_private_directory,
)
from app.usaspending.transaction_workflow import (
    SOURCE,
    TransactionIngestionWorkflow,
    TransactionWorkflowError,
    TransactionWorkflowResult,
    assert_owned_period_state_on_connection,
)


PILOT_PERIOD = FiscalPeriod(2025, date(2024, 10, 1), date(2024, 10, 31))
PILOT_ATTEMPT_ID = UUID("c6b44991-c217-4e36-bdf8-905690d6bd2d")
PILOT_CHECKPOINT_ID = UUID("ce639524-be6d-4cba-8d82-07657c5c09a9")
PILOT_ROWS = 490381
PILOT_SIGNED_TOTAL = Decimal("48714624177.57")
MODELED_BYTES_PER_ROW = 550
GLOBAL_INGESTION_LEASE_KEY = int.from_bytes(
    hashlib.sha256(b"govtracts:usaspending:transaction-ingestion:global:v1").digest()[:8],
    byteorder="big",
    signed=True,
)
SAFE_CODE = re.compile(r"[a-z0-9_]{1,128}\Z")
BACKUP_REFERENCE = re.compile(
    r"backup-([0-9]{8})t([0-9]{6})z-([0-9a-f]{16,64})\Z",
    re.IGNORECASE,
)
ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
BLOCKING_STATUSES = frozenset({"failed", "submission_unknown"})
POST_SUBMISSION_STATUSES = frozenset(
    {"submitting", "submitted", "export_finished", "archive_hashed", "loading"}
)
MAX_STORAGE_BYTES = (1 << 63) - 1
SIMULTANEOUS_ARCHIVE_COPIES = 2
LOCAL_ARCHIVE_HEADROOM_BYTES = (
    SIMULTANEOUS_ARCHIVE_COPIES * DEFAULT_MAX_ARCHIVE_BYTES
    + DEFAULT_MAX_UNCOMPRESSED_BYTES
)
WAL_BYTES_PER_RELATION_BYTE = 1


class BackfillError(RuntimeError):
    """Fail-closed backfill error containing only a stable safe code."""

    def __init__(self, code: str) -> None:
        safe = code if SAFE_CODE.fullmatch(code) else "backfill_operation_failed"
        super().__init__(safe)
        self.code = safe


class _ExecutionBoundReached(RuntimeError):
    pass


@dataclass(frozen=True)
class BackfillLimits:
    """Finite execution controls for one live invocation."""

    max_periods: int | None = None
    max_leaf_submissions: int | None = None
    max_estimated_new_bytes: int | None = None

    def __post_init__(self) -> None:
        values = (
            self.max_periods,
            self.max_leaf_submissions,
            self.max_estimated_new_bytes,
        )
        if all(value is None for value in values):
            raise BackfillError("finite_execution_bound_required")
        for value in values:
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
            ):
                raise BackfillError("invalid_execution_bound")


@dataclass(frozen=True)
class StorageDecision:
    estimated_new_bytes: int
    database_relation_bytes: int
    database_free_bytes: int
    local_free_bytes: int
    estimated_relation_bytes: int = 0
    estimated_wal_bytes: int = 0
    database_budget_remaining_bytes: int = 0
    local_required_bytes: int = 0


@dataclass
class StorageGuard:
    """Fail-closed local and database-volume capacity guard."""

    engine: Engine
    local_path: Path
    database_capacity_bytes: int
    database_free_bytes: int
    database_reserve_bytes: int
    local_reserve_bytes: int
    conservative_factor: Decimal
    disk_usage: Callable[[Path], Any] = shutil.disk_usage
    database_free_bytes_provider: Callable[[], int] | None = None
    _database_budget_remaining_bytes: int = field(default=0, init=False, repr=False)
    _database_reservations: dict[object, int] = field(
        default_factory=dict, init=False, repr=False
    )
    _database_relation_reservations: dict[object, int] = field(
        default_factory=dict, init=False, repr=False
    )
    _anonymous_reservation: int = field(default=0, init=False, repr=False)
    _trusted_database_free_bytes: int | None = field(
        default=None, init=False, repr=False
    )
    _local_identity: TemporaryDirectoryIdentity | None = field(
        default=None, init=False, repr=False
    )

    def __post_init__(self) -> None:
        integer_values = (
            self.database_capacity_bytes,
            self.database_free_bytes,
            self.database_reserve_bytes,
            self.local_reserve_bytes,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > MAX_STORAGE_BYTES
            for value in integer_values
        ):
            raise BackfillError("invalid_storage_input")
        if (
            self.database_capacity_bytes <= 0
            or self.database_free_bytes > self.database_capacity_bytes
            or self.database_free_bytes <= self.database_reserve_bytes
            or self.database_reserve_bytes >= self.database_capacity_bytes
            or self.conservative_factor < Decimal("1")
            or not self.conservative_factor.is_finite()
            or self.local_reserve_bytes
            > MAX_STORAGE_BYTES - LOCAL_ARCHIVE_HEADROOM_BYTES
        ):
            raise BackfillError("invalid_storage_input")
        try:
            self._local_identity = probe_private_directory(self.local_path)
        except TransactionIngestionError:
            raise BackfillError("invalid_temporary_directory")
        self._trusted_database_free_bytes = self.database_free_bytes
        self._database_budget_remaining_bytes = (
            self.database_free_bytes - self.database_reserve_bytes
        )

    @property
    def local_identity(self) -> TemporaryDirectoryIdentity:
        assert self._local_identity is not None
        return self._local_identity

    def check(
        self,
        expected_rows: int,
        *,
        reservation_key: object | None = None,
    ) -> StorageDecision:
        if (
            isinstance(expected_rows, bool)
            or not isinstance(expected_rows, int)
            or not 0 <= expected_rows <= DEFAULT_MAX_ROWS
        ):
            raise BackfillError("invalid_storage_row_count")
        try:
            probe_private_directory(
                self.local_path,
                expected_identity=self.local_identity,
            )
            local = self.disk_usage(self.local_path)
        except (OSError, TransactionIngestionError):
            raise BackfillError("local_storage_measurement_unavailable") from None
        local_free = int(local.free)
        if local_free < 0:
            raise BackfillError("local_storage_measurement_unavailable")

        with self.engine.connect() as connection:
            relation_bytes = int(
                connection.scalar(
                    text(
                        "SELECT COALESCE(SUM(pg_total_relation_size(oid)), 0) "
                        "FROM pg_class WHERE oid IN ("
                        "to_regclass('usaspending_transactions'), "
                        "to_regclass('usaspending_ingestion_checkpoints'), "
                        "to_regclass('usaspending_transaction_ingestion_attempts'))"
                    )
                )
                or 0
            )
            completed_rows = int(
                connection.scalar(
                    select(func.coalesce(func.sum(
                        UsaSpendingTransactionIngestionAttempt.loaded_rows
                    ), 0)).where(
                        UsaSpendingTransactionIngestionAttempt.status == "completed"
                    )
                )
                or 0
            )
        if relation_bytes < 0 or relation_bytes > MAX_STORAGE_BYTES:
            raise BackfillError("database_storage_measurement_unavailable")
        observed_per_row = (
            math.ceil(relation_bytes / completed_rows) if completed_rows else 0
        )
        bytes_per_row = max(MODELED_BYTES_PER_ROW, observed_per_row)
        estimated_relation = math.ceil(
            Decimal(bytes_per_row)
            * Decimal(expected_rows)
            * self.conservative_factor
        )
        estimated_wal = estimated_relation * WAL_BYTES_PER_RELATION_BYTE
        estimated = estimated_relation + estimated_wal
        if estimated > MAX_STORAGE_BYTES:
            raise BackfillError("storage_estimate_overflow")
        local_needed = LOCAL_ARCHIVE_HEADROOM_BYTES + self.local_reserve_bytes

        trusted_free = self._trusted_database_free_bytes
        assert trusted_free is not None
        if self.database_free_bytes_provider is not None:
            try:
                refreshed = self.database_free_bytes_provider()
            except Exception:
                raise BackfillError(
                    "database_storage_measurement_unavailable"
                ) from None
            if (
                isinstance(refreshed, bool)
                or not isinstance(refreshed, int)
                or refreshed < 0
                or refreshed > self.database_capacity_bytes
            ):
                raise BackfillError("database_storage_measurement_unavailable")
            trusted_free = min(trusted_free, refreshed)
            refreshed_usable = (
                refreshed
                - self.database_reserve_bytes
                - sum(self._database_reservations.values())
            )
            self._database_budget_remaining_bytes = min(
                self._database_budget_remaining_bytes,
                refreshed_usable,
            )
        self._trusted_database_free_bytes = trusted_free

        if reservation_key is None:
            self._anonymous_reservation += 1
            reservation_key = ("anonymous", self._anonymous_reservation)
        previous_estimated = self._database_reservations.get(reservation_key, 0)
        previous_relation = self._database_relation_reservations.get(
            reservation_key, 0
        )
        charged_estimated = max(previous_estimated, estimated)
        charged_relation = max(previous_relation, estimated_relation)
        additional_estimated = charged_estimated - previous_estimated
        database_usable = (
            self._database_budget_remaining_bytes - additional_estimated
        )
        database_ceiling = self.database_capacity_bytes - self.database_reserve_bytes
        outstanding_relation = (
            sum(self._database_relation_reservations.values())
            - previous_relation
            + charged_relation
        )
        if local_free < local_needed:
            raise BackfillError("local_storage_reserve_failed")
        if (
            database_usable < 0
            or relation_bytes + outstanding_relation > database_ceiling
        ):
            raise BackfillError("database_storage_reserve_failed")
        self._database_budget_remaining_bytes = database_usable
        self._database_reservations[reservation_key] = charged_estimated
        self._database_relation_reservations[reservation_key] = charged_relation
        return StorageDecision(
            estimated_new_bytes=estimated,
            database_relation_bytes=relation_bytes,
            database_free_bytes=trusted_free,
            local_free_bytes=local_free,
            estimated_relation_bytes=estimated_relation,
            estimated_wal_bytes=estimated_wal,
            database_budget_remaining_bytes=database_usable,
            local_required_bytes=local_needed,
        )

    def complete(self, reservation_key: object) -> None:
        """Remove a reflected reservation without replenishing the trusted budget."""
        self._database_reservations.pop(reservation_key, None)
        self._database_relation_reservations.pop(reservation_key, None)


@dataclass(frozen=True)
class CompletedVerification:
    attempt_id: UUID
    checkpoint_id: UUID
    fiscal_year: int
    period_start: date
    period_end: date
    rows: int
    signed_obligation_total: Decimal


class CompletedPeriodVerifier:
    """Read-only relational verification required before a completed skip."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._attempts = UsaSpendingTransactionIngestionAttempt.__table__
        self._checkpoints = UsaSpendingIngestionCheckpoint.__table__
        self._transactions = UsaSpendingTransaction.__table__

    def verify(
        self,
        attempt_id: UUID,
        *,
        expected_period: FiscalPeriod | None = None,
    ) -> CompletedVerification:
        if not isinstance(attempt_id, UUID):
            raise BackfillError("invalid_completed_attempt")
        try:
            with self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as connection:
                with connection.begin():
                    connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                    attempt = connection.execute(
                        select(self._attempts).where(self._attempts.c.id == attempt_id)
                    ).mappings().one_or_none()
                    if attempt is None:
                        raise BackfillError("completed_attempt_missing")
                    return self._verify_on_connection(
                        connection,
                        attempt,
                        expected_period=expected_period,
                    )
        except BackfillError:
            raise
        except SQLAlchemyError:
            raise BackfillError("completed_verification_database_failed") from None

    def _verify_on_connection(
        self,
        connection: Connection,
        attempt: Mapping[str, Any],
        *,
        expected_period: FiscalPeriod | None,
    ) -> CompletedVerification:
        if (
            attempt["source"] != SOURCE
            or attempt["status"] != "completed"
            or not isinstance(attempt["checkpoint_id"], UUID)
            or not isinstance(attempt["export_rows"], int)
            or isinstance(attempt["export_rows"], bool)
            or not isinstance(attempt["loaded_rows"], int)
            or isinstance(attempt["loaded_rows"], bool)
            or attempt["export_rows"] != attempt["loaded_rows"]
            or not isinstance(attempt["signed_obligation_total"], Decimal)
            or not isinstance(attempt["archive_sha256"], str)
        ):
            raise BackfillError("completed_attempt_corrupt")
        actual_period = FiscalPeriod(
            attempt["fiscal_year"],
            attempt["period_start"],
            attempt["period_end"],
        )
        try:
            validate_backfill_period(
                actual_period.fiscal_year,
                actual_period.start,
                actual_period.end,
            )
        except (TypeError, ValueError):
            raise BackfillError("completed_period_corrupt") from None
        if expected_period is not None and actual_period != expected_period:
            raise BackfillError("completed_period_mismatch")

        checkpoint = connection.execute(
            select(self._checkpoints).where(
                self._checkpoints.c.id == attempt["checkpoint_id"]
            )
        ).mappings().one_or_none()
        if (
            checkpoint is None
            or checkpoint["source"] != SOURCE
            or checkpoint["status"] != "complete"
            or checkpoint["fiscal_year"] != actual_period.fiscal_year
            or checkpoint["period_start"] != actual_period.start
            or checkpoint["period_end"] != actual_period.end
            or checkpoint["archive_sha256"] != attempt["archive_sha256"]
            or checkpoint["expected_rows"] != attempt["export_rows"]
            or checkpoint["loaded_rows"] != attempt["loaded_rows"]
        ):
            raise BackfillError("completed_checkpoint_corrupt")

        row_count, distinct_count, signed_total, fiscal_mismatch = connection.execute(
            select(
                func.count(),
                func.count(func.distinct(self._transactions.c.stable_transaction_id)),
                func.coalesce(
                    func.sum(self._transactions.c.federal_action_obligation),
                    Decimal("0.00"),
                ),
                func.count().filter(
                    self._transactions.c.fiscal_year != actual_period.fiscal_year
                ),
            ).where(
                self._transactions.c.action_date.between(
                    actual_period.start,
                    actual_period.end,
                )
            )
        ).one()
        out_of_range_owned = connection.scalar(
            select(func.count()).where(
                and_(
                    self._transactions.c.ingestion_checkpoint_id
                    == attempt["checkpoint_id"],
                    or_(
                        self._transactions.c.action_date < actual_period.start,
                        self._transactions.c.action_date > actual_period.end,
                    ),
                )
            )
        )
        wrong_checkpoint_in_range = connection.scalar(
            select(func.count()).where(
                and_(
                    self._transactions.c.action_date.between(
                        actual_period.start,
                        actual_period.end,
                    ),
                    self._transactions.c.ingestion_checkpoint_id
                    != attempt["checkpoint_id"],
                )
            )
        )
        if (
            row_count != attempt["loaded_rows"]
            or distinct_count != attempt["loaded_rows"]
            or Decimal(signed_total) != attempt["signed_obligation_total"]
            or fiscal_mismatch
            or out_of_range_owned
            or wrong_checkpoint_in_range
        ):
            raise BackfillError("completed_transactions_corrupt")
        return CompletedVerification(
            attempt_id=attempt["id"],
            checkpoint_id=attempt["checkpoint_id"],
            fiscal_year=actual_period.fiscal_year,
            period_start=actual_period.start,
            period_end=actual_period.end,
            rows=attempt["loaded_rows"],
            signed_obligation_total=attempt["signed_obligation_total"],
        )

    def verify_pilot(self) -> CompletedVerification:
        result = self.verify(PILOT_ATTEMPT_ID, expected_period=PILOT_PERIOD)
        if (
            result.checkpoint_id != PILOT_CHECKPOINT_ID
            or result.rows != PILOT_ROWS
            or result.signed_obligation_total != PILOT_SIGNED_TOTAL
        ):
            raise BackfillError("pilot_anchor_mismatch")
        return result


@dataclass(frozen=True)
class BackfillResult:
    runner_id: UUID
    status: str
    processed_periods: int
    submitted_leaves: int
    skipped_completed: int
    subdivided_periods: int
    estimated_new_bytes: int
    records: tuple[dict[str, object], ...] = field(default_factory=tuple)


class BackfillCoordinator:
    """Traverse durable attempts chronologically and process one leaf at a time."""

    def __init__(
        self,
        engine: Engine,
        workflow: TransactionIngestionWorkflow,
        storage_guard: StorageGuard,
        *,
        verifier: CompletedPeriodVerifier | None = None,
        clock: Callable[[], datetime] | None = None,
        progress: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        if engine.dialect.name != "postgresql":
            raise BackfillError("postgresql_engine_required")
        self._engine = engine
        self._workflow = workflow
        self._storage_guard = storage_guard
        self._verifier = verifier or CompletedPeriodVerifier(engine)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._progress = progress or (lambda _record: None)
        self._attempts = UsaSpendingTransactionIngestionAttempt.__table__
        self._records: list[dict[str, object]] = []
        self._processed = 0
        self._submitted = 0
        self._skipped = 0
        self._subdivided = 0
        self._estimated = 0
        self._estimated_by_attempt: dict[UUID, int] = {}

    async def run(
        self,
        roots: Sequence[FiscalPeriod],
        *,
        limits: BackfillLimits,
        backup_identifier: str,
        retry_failed_attempt_id: UUID | None = None,
        recover_failed_job_attempt_id: UUID | None = None,
        require_pilot: bool = True,
    ) -> BackfillResult:
        self._validate_invocation(
            roots,
            limits=limits,
            backup_identifier=backup_identifier,
            retry_failed_attempt_id=retry_failed_attempt_id,
            recover_failed_job_attempt_id=recover_failed_job_attempt_id,
        )
        runner_id = uuid4()
        if require_pilot:
            pilot = self._verifier.verify_pilot()
            self._record("pilot_verified", PILOT_PERIOD, attempt_id=pilot.attempt_id)

        status = "completed"
        for root in roots:
            if limits.max_periods is not None and self._processed >= limits.max_periods:
                status = "bounded"
                break
            completed = await self._process_period(
                root,
                limits=limits,
                retry_failed_attempt_id=retry_failed_attempt_id,
                recover_failed_job_attempt_id=recover_failed_job_attempt_id,
            )
            if not completed:
                status = "bounded"
                break
        return BackfillResult(
            runner_id=runner_id,
            status=status,
            processed_periods=self._processed,
            submitted_leaves=self._submitted,
            skipped_completed=self._skipped,
            subdivided_periods=self._subdivided,
            estimated_new_bytes=self._estimated,
            records=tuple(self._records),
        )

    def _validate_invocation(
        self,
        roots: Sequence[FiscalPeriod],
        *,
        limits: BackfillLimits,
        backup_identifier: str,
        retry_failed_attempt_id: UUID | None,
        recover_failed_job_attempt_id: UUID | None,
    ) -> None:
        canonicalize_backup_identifier(backup_identifier)
        if retry_failed_attempt_id is not None and not isinstance(
            retry_failed_attempt_id, UUID
        ):
            raise BackfillError("invalid_retry_authorization")
        if recover_failed_job_attempt_id is not None:
            if not isinstance(recover_failed_job_attempt_id, UUID):
                raise BackfillError("invalid_recovery_authorization")
            if retry_failed_attempt_id is not None:
                raise BackfillError("conflicting_retry_authorizations")
            if len(roots) != 1 or limits.max_periods != 1:
                raise BackfillError("recovery_requires_one_period")
        if not roots:
            raise BackfillError("empty_backfill_selection")
        periods: list[tuple[date, date]] = []
        previous: FiscalPeriod | None = None
        for root in roots:
            if not isinstance(root, FiscalPeriod):
                raise BackfillError("invalid_backfill_period")
            try:
                validate_backfill_period(root.fiscal_year, root.start, root.end)
            except (TypeError, ValueError):
                raise BackfillError("invalid_backfill_period") from None
            if previous is not None and root.start <= previous.end:
                raise BackfillError("unordered_backfill_periods")
            periods.append((root.start, root.end))
            previous = root
        if len(roots) > 1:
            assert_exact_period_coverage(roots[0].start, roots[-1].end, periods)

    async def _process_period(
        self,
        period: FiscalPeriod,
        *,
        limits: BackfillLimits,
        retry_failed_attempt_id: UUID | None,
        recover_failed_job_attempt_id: UUID | None = None,
    ) -> bool:
        if limits.max_periods is not None and self._processed >= limits.max_periods:
            return False
        self._processed += 1
        if recover_failed_job_attempt_id is not None:
            return await self._run_same_job_recovery(
                period,
                failed_attempt_id=recover_failed_job_attempt_id,
                limits=limits,
            )
        attempt = self._period_attempt(period)
        if attempt is None:
            month_bounds = fiscal_month_bounds(
                period.fiscal_year,
                calendar_year=period.start.year,
                calendar_month=period.start.month,
            )
            if (period.start, period.end) != month_bounds:
                raise BackfillError("selected_leaf_missing")
        if attempt is not None and attempt.get("recovery_of_attempt_id") is not None:
            if attempt["status"] != "completed":
                raise BackfillError("same_job_recovery_required")
        if attempt is not None and attempt["status"] == "failed":
            job_identity = tuple(
                attempt.get(name)
                for name in ("status_url", "file_url", "remote_file_name")
            )
            if any(value is not None for value in job_identity):
                if not all(value is not None for value in job_identity):
                    raise BackfillError("failed_job_identity_partial")
                raise BackfillError("same_job_recovery_required")
        self._assert_no_overlapping_state(period, exact_attempt=attempt)
        if attempt is not None and attempt["status"] == "completed":
            self._verifier.verify(attempt["id"], expected_period=period)
            self._skipped += 1
            self._record("completed_verified", period, attempt_id=attempt["id"])
            return True
        if attempt is not None and attempt["status"] == "subdivided":
            children = self._verified_children(attempt, period)
            for child in children:
                if not await self._process_period(
                    child,
                    limits=limits,
                    retry_failed_attempt_id=retry_failed_attempt_id,
                ):
                    return False
            return True
        if attempt is not None and attempt["status"] == "submission_unknown":
            raise BackfillError("submission_unknown_requires_operator")
        if attempt is not None and attempt["status"] == "failed":
            if attempt["id"] != retry_failed_attempt_id:
                raise BackfillError("failed_attempt_requires_authorization")
            self._record("failed_retry_authorized", period, attempt_id=attempt["id"])
        retry_parent_attempt_id = (
            attempt.get("parent_attempt_id")
            if attempt is not None and attempt["status"] == "failed"
            else None
        )

        if attempt is not None and attempt["status"] in POST_SUBMISSION_STATUSES:
            if limits.max_leaf_submissions is not None and self._submitted >= limits.max_leaf_submissions:
                return False
            initial_rows = attempt.get("export_rows")
            if initial_rows is None:
                initial_rows = DEFAULT_MAX_ROWS
            if not self._reserve_storage(
                attempt["id"],
                period,
                initial_rows,
                limits=limits,
            ):
                return False
            return await self._run_leaf(
                period,
                attempt_id=attempt["id"],
                limits=limits,
                retry_parent_attempt_id=None,
                counts_as_submission=False,
            )

        counted = await self._workflow.run_period(
            fiscal_year=period.fiscal_year,
            period_start=period.start,
            period_end=period.end,
            stop_after_count=True,
            retry_parent_attempt_id=retry_parent_attempt_id,
        )
        if counted.status == "completed":
            self._verifier.verify(counted.attempt_id, expected_period=period)
            self._skipped += 1
            self._record("completed_verified", period, attempt_id=counted.attempt_id)
            return True
        if counted.status == "subdivided":
            current = self._read_attempt(counted.attempt_id)
            for child in self._verified_children(current, period):
                if not await self._process_period(
                    child,
                    limits=limits,
                    retry_failed_attempt_id=retry_failed_attempt_id,
                ):
                    return False
            return True
        if counted.status in BLOCKING_STATUSES:
            code = (
                "submission_unknown_requires_operator"
                if counted.status == "submission_unknown"
                else "period_failed"
            )
            raise BackfillError(code)
        if counted.status in POST_SUBMISSION_STATUSES:
            raise BackfillError("post_submission_resume_incomplete")
        if counted.status != "counted":
            raise BackfillError("period_count_incomplete")
        expected_rows = counted.pre_submission_rows
        if (
            not isinstance(expected_rows, int)
            or isinstance(expected_rows, bool)
            or expected_rows < 0
        ):
            raise BackfillError("invalid_persisted_count")

        if expected_rows > DEFAULT_MAX_ROWS:
            if period.start == period.end:
                self._record(
                    "oversized_day_blocked",
                    period,
                    attempt_id=counted.attempt_id,
                    expected_rows=expected_rows,
                )
                raise BackfillError("oversized_single_day")
            children = self._subdivide(counted.attempt_id, period, expected_rows)
            self._subdivided += 1
            self._record(
                "period_subdivided",
                period,
                attempt_id=counted.attempt_id,
                expected_rows=expected_rows,
            )
            for child in children:
                if not await self._process_period(
                    child,
                    limits=limits,
                    retry_failed_attempt_id=retry_failed_attempt_id,
                ):
                    return False
            return True

        if (
            limits.max_leaf_submissions is not None
            and self._submitted >= limits.max_leaf_submissions
        ):
            return False
        if not self._reserve_storage(
            counted.attempt_id,
            period,
            expected_rows,
            limits=limits,
        ):
            return False
        return await self._run_leaf(
            period,
            attempt_id=counted.attempt_id,
            limits=limits,
            retry_parent_attempt_id=retry_parent_attempt_id,
            counts_as_submission=True,
        )

    async def _run_same_job_recovery(
        self,
        period: FiscalPeriod,
        *,
        failed_attempt_id: UUID,
        limits: BackfillLimits,
    ) -> bool:
        try:
            claim = self._workflow.claim_failed_job_recovery(
                failed_attempt_id,
                period.fiscal_year,
                period.start,
                period.end,
            )
        except TransactionWorkflowError as exc:
            if str(exc) == "unexpected_overlapping_state":
                raise BackfillError("unexpected_overlapping_state") from None
            raise
        recovery_id = claim.recovery_attempt_id
        if claim.failed_attempt_id != failed_attempt_id or not isinstance(
            recovery_id, UUID
        ) or claim.attempt.get("id") != recovery_id:
            raise BackfillError("recovery_claim_mismatch")
        self._record(
            "same_job_recovery_claimed" if claim.created else "same_job_recovery_resumed",
            period,
            attempt_id=recovery_id,
            recovery_of_attempt_id=failed_attempt_id,
        )
        self._assert_no_overlapping_state(period, exact_attempt=claim.attempt)
        persisted_rows = claim.attempt.get("export_rows")
        initial_rows = (
            persisted_rows
            if isinstance(persisted_rows, int) and not isinstance(persisted_rows, bool)
            else DEFAULT_MAX_ROWS
        )
        if not self._reserve_storage(recovery_id, period, initial_rows, limits=limits):
            return False

        def capacity_guard(export_rows: int) -> None:
            if not self._reserve_storage(
                recovery_id, period, export_rows, limits=limits
            ):
                raise _ExecutionBoundReached

        try:
            result = await self._workflow.resume_failed_job_recovery(
                recovery_attempt_id=recovery_id,
                expected_failed_attempt_id=failed_attempt_id,
                fiscal_year=period.fiscal_year,
                period_start=period.start,
                period_end=period.end,
                capacity_guard=capacity_guard,
            )
        except _ExecutionBoundReached:
            return False
        if result.attempt_id != recovery_id:
            raise BackfillError("recovery_attempt_mismatch")
        if result.status != "completed":
            if result.status == "failed":
                raise BackfillError("period_failed")
            raise BackfillError("leaf_incomplete")
        if (
            not isinstance(result.export_rows, int)
            or isinstance(result.export_rows, bool)
            or not 0 <= result.export_rows <= DEFAULT_MAX_ROWS
        ):
            raise BackfillError("invalid_persisted_export_rows")
        verified = self._verifier.verify(recovery_id, expected_period=period)
        self._complete_storage(recovery_id)
        self._record(
            "leaf_completed",
            period,
            attempt_id=recovery_id,
            recovery_of_attempt_id=failed_attempt_id,
            checkpoint_id=verified.checkpoint_id,
            expected_rows=result.export_rows,
            loaded_rows=verified.rows,
            signed_obligation_total=str(verified.signed_obligation_total),
            estimated_new_bytes=self._estimated_by_attempt[recovery_id],
        )
        return True

    async def _run_leaf(
        self,
        period: FiscalPeriod,
        *,
        attempt_id: UUID,
        limits: BackfillLimits,
        retry_parent_attempt_id: UUID | None,
        counts_as_submission: bool,
    ) -> bool:
        submission_counted = False

        def record_submission() -> None:
            nonlocal submission_counted
            if counts_as_submission and not submission_counted:
                self._submitted += 1
                submission_counted = True

        def capacity_guard(export_rows: int) -> None:
            if not self._reserve_storage(
                attempt_id,
                period,
                export_rows,
                limits=limits,
            ):
                raise _ExecutionBoundReached

        try:
            result = await self._workflow.run_period(
                fiscal_year=period.fiscal_year,
                period_start=period.start,
                period_end=period.end,
                stop_after_count=False,
                capacity_guard=capacity_guard,
                retry_parent_attempt_id=retry_parent_attempt_id,
                submission_observer=record_submission,
            )
        except _ExecutionBoundReached:
            return False
        if result.status != "completed":
            if result.status == "submission_unknown":
                raise BackfillError("submission_unknown_requires_operator")
            if result.status == "failed":
                raise BackfillError("period_failed")
            raise BackfillError("leaf_incomplete")
        if (
            not isinstance(result.export_rows, int)
            or isinstance(result.export_rows, bool)
            or not 0 <= result.export_rows <= DEFAULT_MAX_ROWS
        ):
            raise BackfillError("invalid_persisted_export_rows")
        verified = self._verifier.verify(result.attempt_id, expected_period=period)
        self._complete_storage(attempt_id)
        self._record(
            "leaf_completed",
            period,
            attempt_id=result.attempt_id,
            checkpoint_id=verified.checkpoint_id,
            expected_rows=result.export_rows,
            loaded_rows=verified.rows,
            signed_obligation_total=str(verified.signed_obligation_total),
            estimated_new_bytes=self._estimated_by_attempt[attempt_id],
        )
        return True

    def _reserve_storage(
        self,
        attempt_id: UUID,
        period: FiscalPeriod,
        row_count: int,
        *,
        limits: BackfillLimits,
    ) -> bool:
        storage = self._storage_guard.check(
            row_count,
            reservation_key=attempt_id,
        )
        previous = self._estimated_by_attempt.get(attempt_id, 0)
        charged = max(previous, storage.estimated_new_bytes)
        additional = charged - previous
        if (
            limits.max_estimated_new_bytes is not None
            and self._estimated + additional > limits.max_estimated_new_bytes
        ):
            return False
        self._estimated += additional
        self._estimated_by_attempt[attempt_id] = charged
        self._record(
            "leaf_storage_reserved",
            period,
            expected_rows=row_count,
            estimated_new_bytes=storage.estimated_new_bytes,
            estimated_relation_bytes=storage.estimated_relation_bytes,
            estimated_wal_bytes=storage.estimated_wal_bytes,
            database_relation_bytes=storage.database_relation_bytes,
            database_free_bytes=storage.database_free_bytes,
            database_budget_remaining_bytes=storage.database_budget_remaining_bytes,
            local_free_bytes=storage.local_free_bytes,
            local_required_bytes=storage.local_required_bytes,
        )
        return True

    def _complete_storage(self, attempt_id: UUID) -> None:
        complete = getattr(self._storage_guard, "complete", None)
        if callable(complete):
            complete(attempt_id)

    def _period_attempt(self, period: FiscalPeriod) -> Mapping[str, Any] | None:
        with self._engine.connect() as connection:
            rows = connection.execute(
                select(self._attempts)
                .where(
                    and_(
                        self._attempts.c.source == SOURCE,
                        self._attempts.c.period_start == period.start,
                        self._attempts.c.period_end == period.end,
                    )
                )
                .order_by(
                    case(
                        (self._attempts.c.status.not_in(("completed", "failed")), 0),
                        (self._attempts.c.status == "completed", 1),
                        else_=2,
                    ),
                    self._attempts.c.created_at.desc(),
                    self._attempts.c.id.desc(),
                )
            ).mappings().all()
        linked_recoveries = [
            row for row in rows if row["recovery_of_attempt_id"] is not None
        ]
        if len(linked_recoveries) > 1:
            raise BackfillError("unexpected_overlapping_state")
        if linked_recoveries:
            return dict(linked_recoveries[0])
        return dict(rows[0]) if rows else None

    def _assert_no_overlapping_state(
        self,
        period: FiscalPeriod,
        *,
        exact_attempt: Mapping[str, Any] | None,
    ) -> None:
        """Reject overlap except the exact attempt's verified tree-owned state."""
        with self._engine.connect() as connection:
            try:
                assert_owned_period_state_on_connection(
                    connection, period, exact_attempt=exact_attempt
                )
            except TransactionWorkflowError as exc:
                if str(exc) == "unexpected_overlapping_state":
                    raise BackfillError("unexpected_overlapping_state") from None
                raise

    def _read_attempt(self, attempt_id: UUID) -> Mapping[str, Any]:
        with self._engine.connect() as connection:
            row = connection.execute(
                select(self._attempts).where(self._attempts.c.id == attempt_id)
            ).mappings().one()
        return dict(row)

    def _subdivide(
        self,
        attempt_id: UUID,
        period: FiscalPeriod,
        expected_rows: int,
    ) -> tuple[FiscalPeriod, FiscalPeriod]:
        child_bounds = split_period(period.start, period.end)
        children = tuple(
            FiscalPeriod(period.fiscal_year, start, end)
            for start, end in child_bounds
        )
        now = self._aware_now()
        try:
            with self._engine.begin() as connection:
                parent = connection.execute(
                    select(self._attempts)
                    .where(self._attempts.c.id == attempt_id)
                    .with_for_update()
                ).mappings().one()
                if parent["status"] == "subdivided":
                    return self._verified_children_on_connection(
                        connection,
                        parent,
                        period,
                    )
                if (
                    parent["source"] != SOURCE
                    or parent["status"] != "counted"
                    or parent["fiscal_year"] != period.fiscal_year
                    or parent["period_start"] != period.start
                    or parent["period_end"] != period.end
                    or parent["expected_rows"] != expected_rows
                    or expected_rows <= DEFAULT_MAX_ROWS
                    or any(
                        parent[name] is not None
                        for name in (
                            "export_rows",
                            "export_columns",
                            "status_url",
                            "file_url",
                            "remote_file_name",
                            "archive_sha256",
                            "archive_bytes",
                            "loaded_rows",
                            "signed_obligation_total",
                            "checkpoint_id",
                        )
                    )
                ):
                    raise BackfillError("subdivision_parent_conflict")
                for child in children:
                    child_id = uuid4()
                    connection.execute(
                        insert(self._attempts),
                        {
                            "id": child_id,
                            "source": SOURCE,
                            "fiscal_year": child.fiscal_year,
                            "period_start": child.start,
                            "period_end": child.end,
                            "status": "created",
                            "failure_count": 0,
                            "parent_attempt_id": attempt_id,
                            "started_at": now,
                            "created_at": now,
                            "updated_at": now,
                        },
                    )
                updated = connection.execute(
                    update(self._attempts)
                    .where(
                        and_(
                            self._attempts.c.id == attempt_id,
                            self._attempts.c.status == "counted",
                        )
                    )
                    .values(
                        status="subdivided",
                        completed_at=now,
                        last_error_code=None,
                        last_error_detail=None,
                        updated_at=now,
                    )
                )
                if updated.rowcount != 1:
                    raise BackfillError("subdivision_parent_conflict")
        except BackfillError:
            raise
        except IntegrityError:
            raise BackfillError("subdivision_concurrency_conflict") from None
        except SQLAlchemyError:
            raise BackfillError("subdivision_database_failed") from None
        return children  # type: ignore[return-value]

    def _verified_children(
        self,
        parent: Mapping[str, Any],
        period: FiscalPeriod,
    ) -> tuple[FiscalPeriod, FiscalPeriod]:
        with self._engine.connect() as connection:
            return self._verified_children_on_connection(connection, parent, period)

    def _verified_children_on_connection(
        self,
        connection: Connection,
        parent: Mapping[str, Any],
        period: FiscalPeriod,
    ) -> tuple[FiscalPeriod, FiscalPeriod]:
        if (
            parent["status"] != "subdivided"
            or parent["source"] != SOURCE
            or parent["fiscal_year"] != period.fiscal_year
            or parent["period_start"] != period.start
            or parent["period_end"] != period.end
            or not isinstance(parent["expected_rows"], int)
            or parent["expected_rows"] <= DEFAULT_MAX_ROWS
        ):
            raise BackfillError("subdivision_parent_corrupt")
        rows = connection.execute(
            select(self._attempts)
            .where(self._attempts.c.parent_attempt_id == parent["id"])
            .order_by(self._attempts.c.period_start, self._attempts.c.period_end)
        ).mappings().all()
        expected = split_period(period.start, period.end)
        selected_rows: list[Mapping[str, Any]] = []
        for child_start, child_end in expected:
            candidates = [
                row
                for row in rows
                if row["period_start"] == child_start
                and row["period_end"] == child_end
            ]
            current = [row for row in candidates if row["status"] != "failed"]
            if not candidates or len(current) > 1:
                raise BackfillError("subdivision_children_corrupt")
            selected_rows.append(
                current[0]
                if current
                else max(
                    candidates,
                    key=lambda row: (row["created_at"], row["id"]),
                )
            )
        if any(
            (row["period_start"], row["period_end"]) not in expected
            for row in rows
        ):
            raise BackfillError("subdivision_children_corrupt")
        children = tuple(
            FiscalPeriod(row["fiscal_year"], row["period_start"], row["period_end"])
            for row in selected_rows
        )
        if any(
            row["source"] != SOURCE
            or row["fiscal_year"] != period.fiscal_year
            or row["parent_attempt_id"] != parent["id"]
            for row in rows
        ):
            raise BackfillError("subdivision_children_corrupt")
        assert_exact_period_coverage(period.start, period.end, expected)
        return children  # type: ignore[return-value]

    def _record(
        self,
        event: str,
        period: FiscalPeriod,
        **values: object,
    ) -> None:
        if not SAFE_CODE.fullmatch(event):
            raise BackfillError("unsafe_progress_record")
        record: dict[str, object] = {
            "event": event,
            "fiscal_year": period.fiscal_year,
            "period_start": period.start.isoformat(),
            "period_end": period.end.isoformat(),
        }
        for name, value in values.items():
            if isinstance(value, UUID):
                record[name] = str(value)
            elif value is None or isinstance(value, (str, int, bool)):
                record[name] = value
            else:
                raise BackfillError("unsafe_progress_record")
        safe_record = sanitize_progress_record(record)
        self._records.append(safe_record)
        self._progress(dict(safe_record))

    def _aware_now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise BackfillError("clock_must_return_aware_datetime")
        return value.astimezone(timezone.utc)


@contextmanager
def global_ingestion_lease(engine: Engine) -> Iterator[None]:
    """Hold the shared PostgreSQL session lease for one operator invocation."""
    if engine.dialect.name != "postgresql":
        raise BackfillError("postgresql_engine_required")
    connection = engine.connect()
    acquired = False
    try:
        acquired = bool(
            connection.scalar(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": GLOBAL_INGESTION_LEASE_KEY},
            )
        )
        if not acquired:
            raise BackfillError("ingestion_lease_busy")
        yield
    finally:
        if acquired:
            try:
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": GLOBAL_INGESTION_LEASE_KEY},
                )
            except SQLAlchemyError:
                pass
        connection.close()


def _encoded_manifest(payload: Mapping[str, object]) -> bytes:
    try:
        return (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        raise BackfillError("unsafe_manifest_record") from None


class ExclusiveManifestReservation:
    """Own one exclusive manifest inode until it is committed or abandoned."""

    def __init__(
        self,
        path: Path,
        descriptor: int,
        created_stat: os.stat_result,
    ) -> None:
        self.path = path
        self._descriptor: int | None = descriptor
        self._created_stat = created_stat
        self._complete = False

    def _assert_path_identity(self) -> None:
        current = self.path.lstat()
        if (
            current.st_dev != self._created_stat.st_dev
            or current.st_ino != self._created_stat.st_ino
        ):
            raise OSError("manifest destination identity changed")

    def write(self, payload: Mapping[str, object]) -> None:
        """Commit a complete deterministic payload to the reserved inode."""
        if self._descriptor is None or self._complete:
            raise BackfillError("manifest_write_failed")
        try:
            encoded = _encoded_manifest(payload)
        except BackfillError:
            self.abort()
            raise
        try:
            self._assert_path_identity()
            offset = 0
            while offset < len(encoded):
                remaining = len(encoded) - offset
                try:
                    written = os.write(self._descriptor, encoded[offset:])
                except InterruptedError:
                    continue
                if (
                    isinstance(written, bool)
                    or not isinstance(written, int)
                    or written <= 0
                    or written > remaining
                ):
                    raise OSError("manifest write returned an invalid count")
                offset += written
            os.fsync(self._descriptor)
            self._assert_path_identity()
            os.close(self._descriptor)
            self._descriptor = None
            directory = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self._complete = True
        except OSError:
            self.abort()
            raise BackfillError("manifest_write_failed") from None

    def abort(self) -> None:
        """Remove only the still-owned incomplete reservation."""
        if self._descriptor is not None:
            try:
                os.close(self._descriptor)
            except OSError:
                pass
            self._descriptor = None
        if not self._complete:
            try:
                self._assert_path_identity()
                self.path.unlink()
            except OSError:
                pass


def reserve_manifest_exclusive(path: Path) -> ExclusiveManifestReservation:
    """Reserve and validate one mode-0600 manifest destination."""
    if not isinstance(path, Path):
        raise BackfillError("invalid_manifest_path")
    descriptor: int | None = None
    reservation: ExclusiveManifestReservation | None = None
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        reservation = ExclusiveManifestReservation(
            path,
            descriptor,
            os.fstat(descriptor),
        )
        reservation._assert_path_identity()
        return reservation
    except FileExistsError:
        raise BackfillError("manifest_exists") from None
    except OSError:
        if reservation is not None:
            reservation.abort()
        elif descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise BackfillError("manifest_write_failed") from None


def write_manifest_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    """Write one deterministic mode-0600 manifest without overwriting."""
    reserve_manifest_exclusive(path).write(payload)


def canonicalize_backup_identifier(value: object) -> str:
    """Return the strict non-secret backup reference allowed in output."""
    if not isinstance(value, str) or value != value.strip():
        raise BackfillError("invalid_backup_identifier")
    match = BACKUP_REFERENCE.fullmatch(value)
    if match is None:
        raise BackfillError("invalid_backup_identifier")
    try:
        datetime.strptime(f"{match.group(1)}{match.group(2)}", "%Y%m%d%H%M%S")
    except ValueError:
        raise BackfillError("invalid_backup_identifier") from None
    return value.lower()


def safe_result_manifest(
    result: BackfillResult,
    *,
    backup_identifier: str | None,
) -> dict[str, object]:
    """Return a deterministic result containing only reviewed safe fields."""
    safe_backup_identifier = (
        canonicalize_backup_identifier(backup_identifier)
        if backup_identifier is not None
        else None
    )
    if result.status not in {"completed", "bounded", "stopped", "interrupted"}:
        raise BackfillError("unsafe_manifest_record")
    return {
        "runner_id": str(result.runner_id),
        "status": result.status,
        "processed_periods": result.processed_periods,
        "submitted_leaves": result.submitted_leaves,
        "skipped_completed": result.skipped_completed,
        "subdivided_periods": result.subdivided_periods,
        "estimated_new_bytes": result.estimated_new_bytes,
        "backup_identifier": safe_backup_identifier,
        "records": [sanitize_progress_record(record) for record in result.records],
    }


def sanitize_progress_record(record: Mapping[str, object]) -> dict[str, object]:
    """Validate and copy one progress record using an explicit safe-field schema."""
    required = {"event", "fiscal_year", "period_start", "period_end"}
    optional_ints = {
        "database_budget_remaining_bytes",
        "database_free_bytes",
        "database_relation_bytes",
        "expected_rows",
        "estimated_relation_bytes",
        "loaded_rows",
        "estimated_new_bytes",
        "estimated_wal_bytes",
        "local_free_bytes",
        "local_required_bytes",
    }
    optional_uuids = {"attempt_id", "checkpoint_id", "recovery_of_attempt_id"}
    allowed = required | optional_ints | optional_uuids | {
        "signed_obligation_total"
    }
    if set(record) - allowed or not required <= set(record):
        raise BackfillError("unsafe_manifest_record")
    event = record["event"]
    fiscal_year = record["fiscal_year"]
    start = record["period_start"]
    end = record["period_end"]
    if (
        not isinstance(event, str)
        or not SAFE_CODE.fullmatch(event)
        or isinstance(fiscal_year, bool)
        or not isinstance(fiscal_year, int)
        or fiscal_year not in range(2020, 2027)
        or not isinstance(start, str)
        or not ISO_DATE.fullmatch(start)
        or not isinstance(end, str)
        or not ISO_DATE.fullmatch(end)
    ):
        raise BackfillError("unsafe_manifest_record")
    try:
        period_start = date.fromisoformat(start)
        period_end = date.fromisoformat(end)
        validate_backfill_period(fiscal_year, period_start, period_end)
    except (TypeError, ValueError):
        raise BackfillError("unsafe_manifest_record") from None
    for name in optional_ints & set(record):
        value = record[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BackfillError("unsafe_manifest_record")
    for name in optional_uuids & set(record):
        value = record[name]
        if not isinstance(value, str):
            raise BackfillError("unsafe_manifest_record")
        try:
            UUID(value)
        except ValueError:
            raise BackfillError("unsafe_manifest_record") from None
    if "signed_obligation_total" in record:
        value = record["signed_obligation_total"]
        if not isinstance(value, str):
            raise BackfillError("unsafe_manifest_record")
        try:
            parsed = Decimal(value)
        except Exception:
            raise BackfillError("unsafe_manifest_record") from None
        if not parsed.is_finite():
            raise BackfillError("unsafe_manifest_record")
    return dict(record)
