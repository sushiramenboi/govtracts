import io
import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect


API_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20260927_0002_usaspending_transaction_foundation.py"
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("usaspending_transaction_foundation", MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_phase1a_migration_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata = MetaData()
    Table(
        "awards",
        metadata,
        Column("usa_generated_id", String(512), primary_key=True),
    )

    try:
        with engine.begin() as connection:
            metadata.create_all(connection)
            migration = _load_migration()
            operations = Operations(MigrationContext.configure(connection))
            monkeypatch.setattr(migration, "op", operations)

            migration.upgrade()

            inspector = inspect(connection)
            assert {
                "awards",
                "usaspending_ingestion_checkpoints",
                "usaspending_transactions",
                "usaspending_transaction_preset_matches",
                "usaspending_period_aggregates",
            } == set(inspector.get_table_names())
            assert {
                index["name"] for index in inspector.get_indexes("usaspending_transactions")
            } == {
                "ix_usaspending_transactions_award_action",
                "ix_usaspending_transactions_checkpoint",
                "ix_usaspending_transactions_fiscal_action",
                "ix_usaspending_transactions_fiscal_agency",
                "ix_usaspending_transactions_fiscal_recipient",
                "ix_usaspending_transactions_fiscal_naics",
                "ix_usaspending_transactions_fiscal_psc",
            }
            assert {
                constraint["name"]
                for constraint in inspector.get_check_constraints("usaspending_period_aggregates")
            } >= {
                "ck_usaspending_period_aggregates_gross_positive_nonnegative",
                "ck_usaspending_period_aggregates_signed_deob_nonpositive",
                "ck_usaspending_period_aggregates_net_obligations_formula",
            }

            migration.downgrade()

            assert inspect(connection).get_table_names() == ["awards"]
    finally:
        engine.dispose()


def test_phase1a_migration_compiles_for_postgresql_without_overlength_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    migration = _load_migration()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    monkeypatch.setattr(migration, "op", Operations(context))

    migration.upgrade()

    ddl = output.getvalue()
    identifiers = re.findall(r'\b(?:CONSTRAINT|INDEX)\s+"?([a-z0-9_]+)"?', ddl, re.IGNORECASE)
    assert identifiers
    assert max(map(len, identifiers)) <= 63
