from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import CheckConstraint, Numeric, create_engine, select, update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from app.db import models
from app.db.base import Base


NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
PERIOD_START = date(2024, 10, 1)
PERIOD_END = date(2024, 10, 31)


def _database() -> Engine:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


def _checkpoint(connection: Connection) -> UUID:
    checkpoint_id = uuid4()
    connection.execute(
        models.UsaSpendingIngestionCheckpoint.__table__.insert(),
        {
            "id": checkpoint_id,
            "source": "usaspending",
            "fiscal_year": 2025,
            "period_start": PERIOD_START,
            "period_end": PERIOD_END,
            "status": "complete",
            "expected_rows": 1,
            "loaded_rows": 1,
            "started_at": NOW,
            "completed_at": NOW,
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    return checkpoint_id


def _attempt_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "id": uuid4(),
        "source": "usaspending",
        "fiscal_year": 2025,
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "status": "created",
        "failure_count": 0,
        "started_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(overrides)
    return values


def test_attempt_model_declares_restart_state_contract() -> None:
    table = models.UsaSpendingTransactionIngestionAttempt.__table__

    assert [column.name for column in table.primary_key.columns] == ["id"]
    assert {column.name for column in table.c} == {
        "id",
        "source",
        "fiscal_year",
        "period_start",
        "period_end",
        "status",
        "expected_rows",
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
        "last_error_code",
        "last_error_detail",
        "failure_count",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    }
    amount_type = table.c.signed_obligation_total.type
    assert isinstance(amount_type, Numeric)
    assert (amount_type.precision, amount_type.scale) == (20, 2)
    assert {
        foreign_key.constraint.name: (foreign_key.target_fullname, foreign_key.ondelete)
        for foreign_key in table.foreign_keys
    } == {
        "fk_usaspending_attempt_checkpoint": (
            "usaspending_ingestion_checkpoints.id",
            "RESTRICT",
        )
    }
    assert {index.name: index.unique for index in table.indexes} == {
        "ix_usaspending_attempts_period_status": False,
        "uq_usaspending_attempts_active_period": True,
    }
    active_index = next(
        index
        for index in table.indexes
        if index.name == "uq_usaspending_attempts_active_period"
    )
    assert str(active_index.dialect_options["postgresql"]["where"]) == (
        "status NOT IN ('completed', 'failed')"
    )
    assert {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    } == {
        "ck_usaspending_transaction_ingestion_attempts_archive_metadata",
        "ck_usaspending_transaction_ingestion_attempts_count_metadata",
        "ck_usaspending_transaction_ingestion_attempts_counts_nonneg",
        "ck_usaspending_transaction_ingestion_attempts_error_metadata",
        "ck_usaspending_transaction_ingestion_attempts_export_metadata",
        "ck_usaspending_transaction_ingestion_attempts_fiscal_year_range",
        "ck_usaspending_transaction_ingestion_attempts_hash_lower_hex",
        "ck_usaspending_transaction_ingestion_attempts_job_metadata",
        "ck_usaspending_transaction_ingestion_attempts_period_order",
        "ck_usaspending_transaction_ingestion_attempts_status_allowed",
        "ck_usaspending_transaction_ingestion_attempts_terminal_fields",
    }


def test_only_one_nonterminal_attempt_can_exist_for_a_period() -> None:
    engine = _database()
    table = models.UsaSpendingTransactionIngestionAttempt.__table__
    try:
        first = _attempt_values()
        with engine.begin() as connection:
            checkpoint_id = _checkpoint(connection)
            connection.execute(table.insert(), first)

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(table.insert(), _attempt_values())

        with engine.begin() as connection:
            connection.execute(
                update(table)
                .where(table.c.id == first["id"])
                .values(
                    status="completed",
                    expected_rows=1,
                    export_rows=1,
                    export_columns=16,
                    status_url="https://api.usaspending.gov/status/1",
                    file_url="https://files.usaspending.gov/export.zip",
                    remote_file_name="export.zip",
                    archive_sha256="a" * 64,
                    archive_bytes=123,
                    loaded_rows=1,
                    signed_obligation_total=Decimal("10.25"),
                    checkpoint_id=checkpoint_id,
                    completed_at=NOW,
                    updated_at=NOW,
                )
            )
            connection.execute(
                table.insert(),
                _attempt_values(
                    status="submission_unknown",
                    expected_rows=1,
                    last_error_code="submission_unknown",
                ),
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(table.insert(), _attempt_values())

        with engine.connect() as connection:
            statuses = connection.scalars(
                select(table.c.status).order_by(table.c.status)
            ).all()
        assert statuses == ["completed", "submission_unknown"]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "unknown"},
        {"period_end": date(2024, 9, 30)},
        {"expected_rows": -1},
        {"export_rows": -1},
        {"export_rows": 500001},
        {"export_columns": 15},
        {"archive_sha256": "short"},
        {"archive_sha256": "A" * 64},
        {"archive_sha256": "a" * 32 + "A" * 32},
        {"archive_sha256": "a" * 63 + "-"},
        {"archive_sha256": "a" * 63 + " "},
        {"archive_sha256": "g" * 64},
        {"status": "counted"},
        {"status": "submitted"},
        {
            "status": "export_finished",
            "expected_rows": 1,
            "status_url": "https://api.usaspending.gov/status/1",
            "file_url": "https://files.usaspending.gov/export.zip",
            "remote_file_name": "export.zip",
        },
        {
            "status": "archive_hashed",
            "expected_rows": 1,
            "status_url": "https://api.usaspending.gov/status/1",
            "file_url": "https://files.usaspending.gov/export.zip",
            "remote_file_name": "export.zip",
            "archive_sha256": "a" * 64,
            "archive_bytes": 123,
        },
        {"status": "failed", "completed_at": NOW},
        {"status": "submission_unknown"},
    ],
)
def test_attempt_checks_reject_inconsistent_state(
    overrides: dict[str, object],
) -> None:
    engine = _database()
    table = models.UsaSpendingTransactionIngestionAttempt.__table__
    try:
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(table.insert(), _attempt_values(**overrides))
    finally:
        engine.dispose()


def test_completed_attempt_uses_export_count_and_allows_pre_count_drift() -> None:
    engine = _database()
    table = models.UsaSpendingTransactionIngestionAttempt.__table__
    try:
        with engine.begin() as connection:
            checkpoint_id = _checkpoint(connection)

        completed = _attempt_values(
            status="completed",
            expected_rows=490382,
            export_rows=490381,
            export_columns=16,
            status_url="https://api.usaspending.gov/status/1",
            file_url="https://files.usaspending.gov/export.zip",
            remote_file_name="export.zip",
            archive_sha256="0123456789abcdef" * 4,
            archive_bytes=123,
            loaded_rows=490381,
            signed_obligation_total=Decimal("10.25"),
            checkpoint_id=checkpoint_id,
            completed_at=NOW,
        )
        with engine.begin() as connection:
            connection.execute(table.insert(), completed)

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    table.insert(),
                    _attempt_values(
                        **{
                            **completed,
                            "id": uuid4(),
                            "loaded_rows": 490380,
                        }
                    ),
                )

        with engine.connect() as connection:
            row = connection.execute(
                select(
                    table.c.expected_rows,
                    table.c.export_rows,
                    table.c.loaded_rows,
                ).where(table.c.id == completed["id"])
            ).one()
        assert row == (490382, 490381, 490381)
    finally:
        engine.dispose()


def test_historical_failed_attempt_accepts_null_export_metadata() -> None:
    engine = _database()
    table = models.UsaSpendingTransactionIngestionAttempt.__table__
    try:
        failed = _attempt_values(
            status="failed",
            expected_rows=490382,
            status_url="https://api.usaspending.gov/status/1",
            file_url="https://files.usaspending.gov/export.zip",
            remote_file_name="export.zip",
            archive_sha256="0123456789abcdef" * 4,
            archive_bytes=123,
            last_error_code="transaction_count_mismatch",
            completed_at=NOW,
        )
        with engine.begin() as connection:
            connection.execute(table.insert(), failed)
        with engine.connect() as connection:
            row = connection.execute(
                select(table.c.status, table.c.export_rows, table.c.export_columns).where(
                    table.c.id == failed["id"]
                )
            ).one()
        assert row == ("failed", None, None)
    finally:
        engine.dispose()
