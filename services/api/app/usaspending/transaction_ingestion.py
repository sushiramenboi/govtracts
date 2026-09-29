"""Atomic persistence for one validated USAspending transaction-export month."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Engine,
    Table,
    and_,
    column,
    delete,
    exists,
    func,
    insert,
    or_,
    select,
    table,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.engine import Connection

from app.db.models import UsaSpendingIngestionCheckpoint, UsaSpendingTransaction
from app.usaspending.fiscal_years import fiscal_month_bounds
from app.usaspending.transaction_export import (
    TransactionExportParser,
    TransactionRow,
    ValidationTotals,
)


SOURCE = "usaspending"
COMPLETE_STATUS = "complete"
LOADING_STATUS = "loading"
TEMPORARY_STAGE_TABLE = "usaspending_transaction_stage"
DEFAULT_BATCH_SIZE = 2_000
ZERO_MONEY = Decimal("0.00")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class TransactionIngestionError(RuntimeError):
    """A deterministic validation or reconciliation failure for one period load."""


@dataclass(frozen=True)
class BulkJobMetadata:
    """Bulk-export metadata retained by the period checkpoint."""

    status_url: str
    file_name: str


@dataclass(frozen=True)
class TransactionIngestionResult:
    """Committed result for a period replacement or an identical-archive no-op."""

    checkpoint_id: UUID
    archive_sha256: str
    loaded_rows: int
    signed_obligation_total: Decimal
    no_op: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TransactionIngestionLoader:
    """Load one complete FY/month export as an atomic replacement snapshot."""

    def __init__(
        self,
        engine: Engine,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise TransactionIngestionError("invalid_batch_size")
        if engine.dialect.name != "postgresql":
            raise TransactionIngestionError("postgresql_engine_required")
        self._engine = engine
        self._batch_size = batch_size
        self._clock = clock
        self._target = UsaSpendingTransaction.__table__
        self._checkpoints = UsaSpendingIngestionCheckpoint.__table__
        self._stage = _stage_table(self._target)

    def load(
        self,
        *,
        archive_path: str | os.PathLike[str],
        fiscal_year: int,
        period_start: date,
        period_end: date,
        expected_count: int,
        bulk_job: BulkJobMetadata,
    ) -> TransactionIngestionResult:
        """Validate, reconcile, and atomically replace one exact calendar month."""
        self._validate_load_inputs(
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
            expected_count=expected_count,
            bulk_job=bulk_job,
        )
        with self._engine.begin() as connection:
            return self.load_with_connection(
                connection,
                archive_path=archive_path,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
                expected_count=expected_count,
                bulk_job=bulk_job,
            )

    def load_with_connection(
        self,
        connection: Connection,
        *,
        archive_path: str | os.PathLike[str],
        fiscal_year: int,
        period_start: date,
        period_end: date,
        expected_count: int,
        bulk_job: BulkJobMetadata,
        expected_archive_sha256: str | None = None,
    ) -> TransactionIngestionResult:
        """Load through a caller-owned active PostgreSQL transaction."""
        self._validate_connection(connection)
        self._validate_load_inputs(
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
            expected_count=expected_count,
            bulk_job=bulk_job,
        )
        if expected_archive_sha256 is not None and (
            not isinstance(expected_archive_sha256, str)
            or SHA256_PATTERN.fullmatch(expected_archive_sha256) is None
        ):
            raise TransactionIngestionError("invalid_expected_archive_sha256")

        lock_key = _advisory_lock_key(SOURCE, period_start, period_end)
        with _private_archive_copy(Path(archive_path)) as (
            immutable_archive_path,
            archive_sha256,
        ):
            if (
                expected_archive_sha256 is not None
                and archive_sha256 != expected_archive_sha256
            ):
                raise TransactionIngestionError("archive_sha256_mismatch")
            return self._load_snapshot(
                connection,
                archive_path=immutable_archive_path,
                archive_sha256=archive_sha256,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
                expected_count=expected_count,
                bulk_job=bulk_job,
                lock_key=lock_key,
            )

    @staticmethod
    def _validate_connection(connection: Connection) -> None:
        if connection.dialect.name != "postgresql":
            raise TransactionIngestionError("postgresql_connection_required")
        if not connection.in_transaction():
            raise TransactionIngestionError("active_transaction_required")

    def _validate_load_inputs(
        self,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        expected_count: int,
        bulk_job: BulkJobMetadata,
    ) -> None:
        self._validate_period(fiscal_year, period_start, period_end)
        if (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count < 0
        ):
            raise TransactionIngestionError("invalid_expected_count")
        self._validate_checkpoint_metadata(bulk_job)

    def _load_snapshot(
        self,
        connection: Any,
        *,
        archive_path: Path,
        archive_sha256: str,
        fiscal_year: int,
        period_start: date,
        period_end: date,
        expected_count: int,
        bulk_job: BulkJobMetadata,
        lock_key: int,
    ) -> TransactionIngestionResult:
        started_at = self._aware_now()
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )
        existing = connection.execute(
            select(
                self._checkpoints.c.id,
                self._checkpoints.c.status,
                self._checkpoints.c.archive_sha256,
                self._checkpoints.c.expected_rows,
                self._checkpoints.c.loaded_rows,
            )
            .where(
                and_(
                    self._checkpoints.c.source == SOURCE,
                    self._checkpoints.c.period_start == period_start,
                    self._checkpoints.c.period_end == period_end,
                )
            )
            .with_for_update()
        ).mappings().one_or_none()

        checkpoint_id = existing["id"] if existing is not None else uuid4()
        connection.execute(_create_stage_table_statement())
        parser = TransactionExportParser(
            archive_path,
            period_start=period_start,
            period_end=period_end,
            expected_count=expected_count,
        )
        self._stream_to_stage(
            connection,
            parser,
            checkpoint_id=checkpoint_id,
            fiscal_year=fiscal_year,
            fetched_at=started_at,
        )
        parser_totals = parser.totals
        self._reconcile_stage(connection, parser_totals, expected_count)
        self._validate_stage_ownership(
            connection,
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
        )

        if existing is None:
            connection.execute(
                insert(self._checkpoints),
                {
                    "id": checkpoint_id,
                    "source": SOURCE,
                    "fiscal_year": fiscal_year,
                    "period_start": period_start,
                    "period_end": period_end,
                    "status": LOADING_STATUS,
                    "status_url": bulk_job.status_url,
                    "remote_file_name": bulk_job.file_name,
                    "archive_sha256": archive_sha256,
                    "expected_rows": expected_count,
                    "loaded_rows": 0,
                    "started_at": started_at,
                    "completed_at": None,
                    "created_at": started_at,
                    "updated_at": started_at,
                },
            )

        connection.execute(
            _upsert_from_stage_statement(
                self._target,
                self._stage,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
            )
        )
        self._validate_stage_ownership(
            connection,
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
        )
        self._delete_disappeared_transactions(connection, period_start, period_end)
        self._reconcile_target(
            connection,
            checkpoint_id=checkpoint_id,
            period_start=period_start,
            period_end=period_end,
            totals=parser_totals,
        )

        completed_at = self._aware_now()
        connection.execute(
            update(self._checkpoints).where(self._checkpoints.c.id == checkpoint_id),
            {
                "fiscal_year": fiscal_year,
                "status": COMPLETE_STATUS,
                "status_url": bulk_job.status_url,
                "remote_file_name": bulk_job.file_name,
                "archive_sha256": archive_sha256,
                "expected_rows": expected_count,
                "loaded_rows": parser_totals.row_count,
                "started_at": started_at,
                "completed_at": completed_at,
                "updated_at": completed_at,
            },
        )

        return TransactionIngestionResult(
            checkpoint_id=checkpoint_id,
            archive_sha256=archive_sha256,
            loaded_rows=parser_totals.row_count,
            signed_obligation_total=parser_totals.signed_obligation_total,
            no_op=False,
        )

    def _validate_period(
        self,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> None:
        if type(period_start) is not date or type(period_end) is not date:
            raise TransactionIngestionError("invalid_fiscal_period")
        try:
            expected_start, expected_end = fiscal_month_bounds(
                fiscal_year,
                calendar_year=period_start.year,
                calendar_month=period_start.month,
            )
        except (TypeError, ValueError):
            raise TransactionIngestionError("invalid_fiscal_period") from None
        if (period_start, period_end) != (expected_start, expected_end):
            raise TransactionIngestionError("invalid_fiscal_period")

    def _validate_checkpoint_metadata(self, bulk_job: BulkJobMetadata) -> None:
        if not isinstance(bulk_job, BulkJobMetadata):
            raise TransactionIngestionError("invalid_bulk_job_metadata")
        if (
            not isinstance(bulk_job.status_url, str)
            or not isinstance(bulk_job.file_name, str)
            or not bulk_job.status_url
            or not bulk_job.file_name
        ):
            raise TransactionIngestionError("invalid_bulk_job_metadata")
        self._validate_string_width(
            self._checkpoints,
            "status_url",
            bulk_job.status_url,
            "bulk_job_status_url_too_long",
        )
        self._validate_string_width(
            self._checkpoints,
            "remote_file_name",
            bulk_job.file_name,
            "bulk_job_file_name_too_long",
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise TransactionIngestionError("clock_must_return_aware_datetime")
        return value.astimezone(timezone.utc)

    def _stream_to_stage(
        self,
        connection: Any,
        parser: TransactionExportParser,
        *,
        checkpoint_id: UUID,
        fiscal_year: int,
        fetched_at: datetime,
    ) -> None:
        batch: list[dict[str, Any]] = []
        with parser.open_rows() as rows:
            for row in rows:
                if row.fiscal_year != fiscal_year:
                    raise TransactionIngestionError("transaction_fiscal_year_mismatch")
                values = self._row_values(
                    row,
                    checkpoint_id=checkpoint_id,
                    fetched_at=fetched_at,
                )
                self._validate_row_widths(values)
                batch.append(values)
                if len(batch) >= self._batch_size:
                    connection.execute(insert(self._stage), batch)
                    batch.clear()
            if batch:
                connection.execute(insert(self._stage), batch)

    def _row_values(
        self,
        row: TransactionRow,
        *,
        checkpoint_id: UUID,
        fetched_at: datetime,
    ) -> dict[str, Any]:
        return {
            "stable_transaction_id": row.stable_transaction_id,
            "ingestion_checkpoint_id": checkpoint_id,
            "generated_award_id": row.generated_award_id,
            "display_award_id": row.display_award_id,
            "award_type_code": row.award_type_code,
            "action_date": row.action_date,
            "fiscal_year": row.fiscal_year,
            "federal_action_obligation": row.federal_action_obligation,
            "recipient_name": row.recipient_name,
            "recipient_uei": row.recipient_uei,
            "recipient_parent_name": row.recipient_parent_name,
            "recipient_parent_uei": row.recipient_parent_uei,
            "awarding_agency_name": row.awarding_agency_name,
            "awarding_subagency_name": row.awarding_subagency_name,
            "naics_code": row.naics_code,
            "psc_code": row.psc_code,
            "transaction_description": row.transaction_description,
            "source_updated_at": row.source_updated_at,
            "fetched_at": fetched_at,
            "created_at": fetched_at,
            "updated_at": fetched_at,
        }

    def _validate_row_widths(self, values: Mapping[str, Any]) -> None:
        for name, value in values.items():
            if isinstance(value, str):
                self._validate_string_width(
                    self._target,
                    name,
                    value,
                    f"{name}_too_long",
                )

    @staticmethod
    def _validate_string_width(
        table_: Table,
        column_name: str,
        value: str,
        error: str,
    ) -> None:
        length = getattr(table_.c[column_name].type, "length", None)
        if length is not None and len(value) > length:
            raise TransactionIngestionError(error)

    def _reconcile_stage(
        self,
        connection: Any,
        totals: ValidationTotals,
        expected_count: int,
    ) -> None:
        row_count, distinct_count, obligation_total = connection.execute(
            _period_totals_statement(self._stage)
        ).one()
        normalized_total = _exact_decimal(obligation_total)
        if row_count != expected_count or row_count != totals.row_count:
            raise TransactionIngestionError("staging_row_count_mismatch")
        if distinct_count != totals.distinct_transaction_count:
            raise TransactionIngestionError("staging_distinct_count_mismatch")
        if normalized_total != totals.signed_obligation_total:
            raise TransactionIngestionError("staging_signed_total_mismatch")

    def _reconcile_target(
        self,
        connection: Any,
        *,
        checkpoint_id: UUID,
        period_start: date,
        period_end: date,
        totals: ValidationTotals,
    ) -> None:
        statement = _period_totals_statement(
            self._target,
            period_start=period_start,
            period_end=period_end,
            checkpoint_id=checkpoint_id,
        )
        row_count, distinct_count, obligation_total, other_checkpoint_rows = (
            connection.execute(statement).one()
        )
        if (
            row_count != totals.row_count
            or distinct_count != totals.distinct_transaction_count
        ):
            raise TransactionIngestionError("committed_period_count_mismatch")
        if _exact_decimal(obligation_total) != totals.signed_obligation_total:
            raise TransactionIngestionError("committed_period_signed_total_mismatch")
        if other_checkpoint_rows:
            raise TransactionIngestionError("committed_period_checkpoint_mismatch")

    def _validate_stage_ownership(
        self,
        connection: Any,
        *,
        fiscal_year: int,
        period_start: date,
        period_end: date,
    ) -> None:
        (conflict_count,) = connection.execute(
            _stage_ownership_conflicts_statement(
                self._target,
                self._stage,
                fiscal_year=fiscal_year,
                period_start=period_start,
                period_end=period_end,
            )
        ).one()
        if conflict_count:
            raise TransactionIngestionError(
                "cross_period_transaction_ownership_conflict"
            )

    def _delete_disappeared_transactions(
        self,
        connection: Any,
        period_start: date,
        period_end: date,
    ) -> None:
        connection.execute(
            _delete_disappeared_statement(
                self._target,
                self._stage,
                period_start=period_start,
                period_end=period_end,
            )
        )

def _stage_table(target: Table) -> Any:
    return table(
        TEMPORARY_STAGE_TABLE,
        *(column(item.name, item.type) for item in target.columns),
    )


def _create_stage_table_statement() -> Any:
    return text(
        f"""
        CREATE TEMPORARY TABLE {TEMPORARY_STAGE_TABLE} (
            LIKE usaspending_transactions INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
            PRIMARY KEY (stable_transaction_id)
        ) ON COMMIT DROP
        """
    )


def _period_totals_statement(
    table_: Any,
    *,
    period_start: date | None = None,
    period_end: date | None = None,
    checkpoint_id: UUID | None = None,
) -> Any:
    fields: list[Any] = [
        func.count(),
        func.count(func.distinct(table_.c.stable_transaction_id)),
        func.coalesce(func.sum(table_.c.federal_action_obligation), ZERO_MONEY),
    ]
    if checkpoint_id is not None:
        fields.append(
            func.count().filter(table_.c.ingestion_checkpoint_id != checkpoint_id)
        )
    statement = select(*fields).select_from(table_)
    if period_start is not None and period_end is not None:
        statement = statement.where(
            table_.c.action_date.between(period_start, period_end)
        )
    return statement


def _stage_ownership_conflicts_statement(
    target: Table,
    stage: Any,
    *,
    fiscal_year: int,
    period_start: date,
    period_end: date,
) -> Any:
    return (
        select(func.count())
        .select_from(
            target.join(
                stage,
                target.c.stable_transaction_id == stage.c.stable_transaction_id,
            )
        )
        .where(
            or_(
                ~target.c.action_date.between(period_start, period_end),
                target.c.fiscal_year != fiscal_year,
            )
        )
    )


def _upsert_from_stage_statement(
    target: Table,
    stage: Any,
    *,
    fiscal_year: int,
    period_start: date,
    period_end: date,
) -> Any:
    insert_columns = [item.name for item in target.columns]
    source_rows = select(*(stage.c[name] for name in insert_columns))
    statement = postgresql_insert(target).from_select(insert_columns, source_rows)
    mutable_columns = [
        name
        for name in insert_columns
        if name not in {"stable_transaction_id", "created_at"}
    ]
    return statement.on_conflict_do_update(
        index_elements=[target.c.stable_transaction_id],
        set_={name: getattr(statement.excluded, name) for name in mutable_columns},
        where=and_(
            target.c.action_date.between(period_start, period_end),
            target.c.fiscal_year == fiscal_year,
        ),
    )


def _delete_disappeared_statement(
    target: Table,
    stage: Any,
    *,
    period_start: date,
    period_end: date,
) -> Any:
    staged_id = select(stage.c.stable_transaction_id).where(
        stage.c.stable_transaction_id == target.c.stable_transaction_id
    )
    return delete(target).where(
        target.c.action_date.between(period_start, period_end),
        ~exists(staged_id),
    )


def _advisory_lock_key(source: str, period_start: date, period_end: date) -> int:
    digest = hashlib.sha256(
        f"{source}:{period_start.isoformat()}:{period_end.isoformat()}".encode("ascii")
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


@contextmanager
def _private_archive_copy(path: Path) -> Iterator[tuple[Path, str]]:
    digest = hashlib.sha256()
    file_descriptor: int | None = None
    temporary_path: Path | None = None
    try:
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix="govtracts-usaspending-",
            suffix=".zip",
        )
        temporary_path = Path(temporary_name)
        os.fchmod(file_descriptor, stat.S_IRUSR | stat.S_IWUSR)
        with path.open("rb") as source, os.fdopen(
            file_descriptor,
            "wb",
            closefd=True,
        ) as destination:
            file_descriptor = None
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
                destination.write(chunk)
        temporary_path.chmod(stat.S_IRUSR)
    except OSError:
        if file_descriptor is not None:
            os.close(file_descriptor)
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise TransactionIngestionError("archive_snapshot_failed") from None

    try:
        yield temporary_path, digest.hexdigest()
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            raise TransactionIngestionError("archive_snapshot_cleanup_failed") from None


def _exact_decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise TransactionIngestionError("invalid_database_obligation_total")
    return value
