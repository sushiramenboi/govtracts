from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import CheckConstraint, Numeric, UniqueConstraint, create_engine, select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from app.db import models
from app.db.base import Base


def _database() -> Engine:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


def _seed_dependencies(connection: Connection) -> tuple[UUID, datetime]:
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)
    checkpoint_id = uuid4()
    connection.execute(
        models.UsaSpendingIngestionCheckpoint.__table__.insert().values(
            id=checkpoint_id,
            source="usaspending",
            fiscal_year=2025,
            period_start=date(2024, 10, 1),
            period_end=date(2024, 10, 31),
            status="complete",
            loaded_rows=1,
            started_at=now,
            completed_at=now,
            created_at=now,
            updated_at=now,
        )
    )
    connection.execute(
        models.Award.__table__.insert().values(
            id=uuid4(),
            usa_generated_id="CONT_AWD_1",
            award_id="PIID-1",
            fetched_at=now,
            created_at=now,
            updated_at=now,
        )
    )
    return checkpoint_id, now


def _transaction_values(checkpoint_id: UUID, now: datetime) -> dict[str, object]:
    return {
        "stable_transaction_id": "CONT_TX_1",
        "ingestion_checkpoint_id": checkpoint_id,
        "generated_award_id": "CONT_AWD_1",
        "display_award_id": "PIID-1",
        "award_type_code": "A",
        "action_date": date(2024, 10, 15),
        "fiscal_year": 2025,
        "federal_action_obligation": Decimal("-25.50"),
        "recipient_name": "Example Recipient",
        "recipient_uei": "ABCDEF123456",
        "awarding_agency_name": "Example Agency",
        "awarding_subagency_name": "Example Subagency",
        "naics_code": "541512",
        "psc_code": "DA01",
        "transaction_description": "Corrective deobligation",
        "source_updated_at": now,
        "fetched_at": now,
        "created_at": now,
        "updated_at": now,
    }


def test_usaspending_schema_declares_exact_grain_constraints_and_indexes() -> None:
    transaction = Base.metadata.tables["usaspending_transactions"]
    checkpoint = Base.metadata.tables["usaspending_ingestion_checkpoints"]
    preset_match = Base.metadata.tables["usaspending_transaction_preset_matches"]
    aggregate = Base.metadata.tables["usaspending_period_aggregates"]

    assert [column.name for column in transaction.primary_key.columns] == ["stable_transaction_id"]
    amount_type = transaction.c.federal_action_obligation.type
    assert isinstance(amount_type, Numeric)
    assert (amount_type.precision, amount_type.scale) == (20, 2)
    assert transaction.c.federal_action_obligation.nullable is False
    assert transaction.c.recipient_parent_uei.nullable is True
    assert transaction.c.naics_code.nullable is True
    assert transaction.c.psc_code.nullable is True

    transaction_foreign_keys = {
        foreign_key.constraint.name: foreign_key.target_fullname for foreign_key in transaction.foreign_keys
    }
    assert transaction_foreign_keys == {
        "fk_usaspending_tx_checkpoint": "usaspending_ingestion_checkpoints.id",
    }
    assert transaction.c.generated_award_id.nullable is False
    assert transaction.c.generated_award_id.type.length == 512
    assert {index.name for index in transaction.indexes} == {
        "ix_usaspending_transactions_award_action",
        "ix_usaspending_transactions_checkpoint",
        "ix_usaspending_transactions_fiscal_action",
        "ix_usaspending_transactions_fiscal_agency",
        "ix_usaspending_transactions_fiscal_recipient",
        "ix_usaspending_transactions_fiscal_naics",
        "ix_usaspending_transactions_fiscal_psc",
    }

    checkpoint_uniques = {
        constraint.name for constraint in checkpoint.constraints if isinstance(constraint, UniqueConstraint)
    }
    aggregate_uniques = {
        constraint.name for constraint in aggregate.constraints if isinstance(constraint, UniqueConstraint)
    }
    assert checkpoint_uniques == {"uq_usaspending_checkpoints_source_period"}
    assert aggregate_uniques == {"uq_usaspending_period_aggregates_grain"}
    assert [column.name for column in preset_match.primary_key.columns] == [
        "stable_transaction_id",
        "preset_key",
        "preset_version",
    ]
    assert {foreign_key.target_fullname for foreign_key in preset_match.foreign_keys} == {
        "usaspending_transactions.stable_transaction_id"
    }
    assert {index.name for index in preset_match.indexes} == {
        "ix_usaspending_preset_matches_preset_transaction"
    }
    preset_match_checks = {
        constraint.name
        for constraint in preset_match.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_usaspending_transaction_preset_matches_version_positive" in preset_match_checks
    assert {index.name for index in aggregate.indexes} == {
        "ix_usaspending_period_aggregates_ranking"
    }

    transaction_checks = {
        constraint.name for constraint in transaction.constraints if isinstance(constraint, CheckConstraint)
    }
    aggregate_checks = {
        constraint.name for constraint in aggregate.constraints if isinstance(constraint, CheckConstraint)
    }
    assert {
        "ck_usaspending_transactions_prime_award_type",
        "ck_usaspending_transactions_fiscal_year_range",
    } <= transaction_checks
    assert {
        "ck_usaspending_period_aggregates_gross_positive_nonnegative",
        "ck_usaspending_period_aggregates_signed_deob_nonpositive",
        "ck_usaspending_period_aggregates_net_obligations_formula",
        "ck_usaspending_period_aggregates_transaction_count_nonnegative",
        "ck_usaspending_period_aggregates_award_count_nonnegative",
    } <= aggregate_checks


def test_signed_obligation_round_trips_and_duplicate_transaction_is_rejected() -> None:
    engine = _database()
    try:
        with engine.begin() as connection:
            checkpoint_id, now = _seed_dependencies(connection)
            values = _transaction_values(checkpoint_id, now)
            connection.execute(models.UsaSpendingTransaction.__table__.insert().values(**values))

        with engine.connect() as connection:
            obligation = connection.scalar(
                select(models.UsaSpendingTransaction.federal_action_obligation).where(
                    models.UsaSpendingTransaction.stable_transaction_id == "CONT_TX_1"
                )
            )
        assert obligation == Decimal("-25.50")

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(models.UsaSpendingTransaction.__table__.insert().values(**values))
    finally:
        engine.dispose()


def test_database_checks_reject_invalid_transactions_and_aggregate_math() -> None:
    engine = _database()
    try:
        with engine.begin() as connection:
            checkpoint_id, now = _seed_dependencies(connection)

        invalid_transaction = _transaction_values(checkpoint_id, now)
        invalid_transaction["stable_transaction_id"] = "CONT_TX_BAD_TYPE"
        invalid_transaction["award_type_code"] = "E"
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    models.UsaSpendingTransaction.__table__.insert().values(**invalid_transaction)
                )

        valid_aggregate = {
            "id": uuid4(),
            "fiscal_year": 2025,
            "period_start": date(2024, 10, 1),
            "period_end": date(2024, 10, 31),
            "preset_key": "technology",
            "preset_version": 1,
            "dimension_type": "total",
            "dimension_key": "all",
            "gross_positive_obligations": Decimal("125.50"),
            "signed_deobligations": Decimal("-25.50"),
            "net_obligations": Decimal("100.00"),
            "transaction_count": 2,
            "distinct_award_count": 1,
            "computed_at": now,
            "created_at": now,
            "updated_at": now,
        }
        with engine.begin() as connection:
            connection.execute(models.UsaSpendingPeriodAggregate.__table__.insert().values(**valid_aggregate))
            stored = connection.execute(
                select(
                    models.UsaSpendingPeriodAggregate.gross_positive_obligations,
                    models.UsaSpendingPeriodAggregate.signed_deobligations,
                    models.UsaSpendingPeriodAggregate.net_obligations,
                )
            ).one()
        assert stored == (Decimal("125.50"), Decimal("-25.50"), Decimal("100.00"))

        invalid_aggregate = dict(valid_aggregate)
        invalid_aggregate["id"] = uuid4()
        invalid_aggregate["dimension_key"] = "bad-math"
        invalid_aggregate["net_obligations"] = Decimal("99.99")
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    models.UsaSpendingPeriodAggregate.__table__.insert().values(**invalid_aggregate)
                )
    finally:
        engine.dispose()
