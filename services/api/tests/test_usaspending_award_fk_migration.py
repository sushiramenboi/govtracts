import io
import importlib.util
import os
import re
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, call
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url


API_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20260927_0003_decouple_transaction_awards.py"
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "decouple_usaspending_transaction_awards",
        MIGRATION_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_upgrade_drops_only_the_transaction_award_foreign_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    operations.drop_constraint.assert_called_once_with(
        "fk_usaspending_tx_award",
        "usaspending_transactions",
        type_="foreignkey",
    )
    assert operations.method_calls == [
        call.drop_constraint(
            "fk_usaspending_tx_award",
            "usaspending_transactions",
            type_="foreignkey",
        )
    ]


def test_clean_downgrade_recreates_the_original_named_foreign_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    operations = Mock()
    operations.get_bind.return_value.scalar.return_value = False
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    operations.create_foreign_key.assert_called_once_with(
        "fk_usaspending_tx_award",
        "usaspending_transactions",
        "awards",
        ["generated_award_id"],
        ["usa_generated_id"],
        ondelete="RESTRICT",
    )
    statement = operations.get_bind.return_value.scalar.call_args.args[0]
    sql = str(statement)
    assert "LEFT JOIN awards" in sql
    assert "award.usa_generated_id = tx.generated_award_id" in sql


def test_orphan_aware_downgrade_fails_without_modifying_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    operations = Mock()
    operations.get_bind.return_value.scalar.return_value = True
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(RuntimeError, match="generated_award_id values absent from awards"):
        migration.downgrade()

    operations.create_foreign_key.assert_not_called()
    assert not operations.execute.called
    statement = operations.get_bind.return_value.scalar.call_args.args[0]
    assert not re.search(r"\b(?:DELETE|INSERT|UPDATE)\b", str(statement), re.IGNORECASE)


def test_migration_compiles_for_postgresql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    migration = _load_migration()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    monkeypatch.setattr(migration, "op", Operations(context))
    monkeypatch.setattr(migration, "_has_orphan_transaction_awards", lambda: False)

    migration.upgrade()
    migration.downgrade()

    ddl = output.getvalue()
    assert "DROP CONSTRAINT fk_usaspending_tx_award" in ddl
    assert "ADD CONSTRAINT fk_usaspending_tx_award" in ddl
    assert "FOREIGN KEY(generated_award_id)" in ddl
    assert "REFERENCES awards (usa_generated_id) ON DELETE RESTRICT" in ddl


@pytest.mark.integration
def test_live_postgresql_upgrade_and_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL is not set")

    database_name = make_url(test_database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a disposable database ending in _test")

    migration = _load_migration()
    schema = f"phase1b4a_{uuid4().hex}"
    engine = create_engine(test_database_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            connection.exec_driver_sql(
                "CREATE TABLE awards ("
                "usa_generated_id VARCHAR(512) UNIQUE NOT NULL"
                ")"
            )
            connection.exec_driver_sql(
                "CREATE TABLE usaspending_transactions ("
                "stable_transaction_id VARCHAR(512) PRIMARY KEY, "
                "generated_award_id VARCHAR(512) NOT NULL, "
                "CONSTRAINT fk_usaspending_tx_award "
                "FOREIGN KEY (generated_award_id) "
                "REFERENCES awards (usa_generated_id) ON DELETE RESTRICT"
                ")"
            )
            monkeypatch.setattr(
                migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )

            migration.upgrade()
            assert inspect(connection).get_foreign_keys(
                "usaspending_transactions",
                schema=schema,
            ) == []

            migration.downgrade()
            foreign_keys = inspect(connection).get_foreign_keys(
                "usaspending_transactions",
                schema=schema,
            )
            assert [foreign_key["name"] for foreign_key in foreign_keys] == [
                "fk_usaspending_tx_award"
            ]

            migration.upgrade()
            connection.exec_driver_sql(
                "INSERT INTO usaspending_transactions "
                "(stable_transaction_id, generated_award_id) "
                "VALUES ('transaction-1', 'missing-award')"
            )
            with pytest.raises(RuntimeError, match="generated_award_id values absent from awards"):
                migration.downgrade()
            assert connection.exec_driver_sql(
                "SELECT generated_award_id FROM usaspending_transactions"
            ).scalar_one() == "missing-award"
            assert connection.exec_driver_sql("SELECT count(*) FROM awards").scalar_one() == 0
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        engine.dispose()
