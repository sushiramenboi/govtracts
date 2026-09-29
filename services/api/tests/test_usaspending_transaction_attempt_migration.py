import io
import importlib.util
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.db.models import UsaSpendingTransactionIngestionAttempt


API_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20260927_0004_usaspending_transaction_attempts.py"
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "usaspending_transaction_attempts",
        MIGRATION_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _normalize_sql(value: object) -> str:
    return " ".join(str(value).split())


def _check_definitions(items: tuple[object, ...] | list[object]) -> dict[str, str]:
    return {
        item.name: _normalize_sql(item.sqltext)
        for item in items
        if isinstance(item, sa.CheckConstraint)
    }


def _foreign_key_definitions(
    items: tuple[object, ...] | list[object],
    *,
    migration_metadata: bool,
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str | None, ...]]]:
    definitions = {}
    for item in items:
        if not isinstance(item, sa.ForeignKeyConstraint):
            continue
        targets = tuple(
            element._colspec if migration_metadata else element.target_fullname
            for element in item.elements
        )
        definitions[item.name] = (
            tuple(item.column_keys),
            targets,
            tuple(element.ondelete for element in item.elements),
        )
    return definitions


def _migration_index_definitions(
    calls: list[object],
) -> dict[str, tuple[str, tuple[str, ...], bool, str | None]]:
    return {
        call.args[0]: (
            call.args[1],
            tuple(call.args[2]),
            bool(call.kwargs.get("unique", False)),
            _normalize_sql(call.kwargs["postgresql_where"])
            if call.kwargs.get("postgresql_where") is not None
            else None,
        )
        for call in calls
    }


def _model_index_definitions(
    table: sa.Table,
) -> dict[str, tuple[str, tuple[str, ...], bool, str | None]]:
    return {
        index.name: (
            table.name,
            tuple(expression.name for expression in index.expressions),
            bool(index.unique),
            _normalize_sql(index.dialect_options["postgresql"]["where"])
            if index.dialect_options["postgresql"]["where"] is not None
            else None,
        )
        for index in table.indexes
    }


def _assert_schema_parity(operations: Mock) -> None:
    table_call = operations.create_table.call_args
    assert table_call.args[0] == "usaspending_transaction_ingestion_attempts"
    migration_items = table_call.args[1:]
    model_table = UsaSpendingTransactionIngestionAttempt.__table__

    migration_columns = {
        item.name: item for item in migration_items if isinstance(item, sa.Column)
    }
    assert set(migration_columns) == {column.name for column in model_table.c}
    for name, model_column in model_table.c.items():
        migration_column = migration_columns[name]
        assert migration_column.nullable == model_column.nullable
        assert str(migration_column.type) == str(model_column.type)

    assert _check_definitions(migration_items) == _check_definitions(
        list(model_table.constraints)
    )
    assert _foreign_key_definitions(
        migration_items, migration_metadata=True
    ) == _foreign_key_definitions(
        list(model_table.constraints), migration_metadata=False
    )
    assert _migration_index_definitions(
        operations.create_index.call_args_list
    ) == _model_index_definitions(model_table)


def test_migration_matches_model_columns_constraints_and_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()
    _assert_schema_parity(operations)


def test_schema_parity_rejects_same_named_semantic_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)
    migration.upgrade()

    hash_constraint = next(
        item
        for item in operations.create_table.call_args.args[1:]
        if isinstance(item, sa.CheckConstraint)
        and item.name == "ck_usaspending_transaction_ingestion_attempts_hash_lower_hex"
    )
    hash_constraint.sqltext = sa.text("archive_sha256 IS NULL")

    with pytest.raises(AssertionError):
        _assert_schema_parity(operations)


def test_migration_compiles_for_postgresql_with_bounded_identifiers(
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
    migration.downgrade()

    ddl = output.getvalue()
    identifiers = re.findall(
        r'\b(?:CONSTRAINT|INDEX)\s+"?([a-z0-9_]+)"?',
        ddl,
        re.IGNORECASE,
    )
    assert identifiers
    assert max(map(len, identifiers)) <= 63
    assert "CREATE TABLE usaspending_transaction_ingestion_attempts" in ddl
    assert "CREATE UNIQUE INDEX uq_usaspending_attempts_active_period" in ddl
    assert "WHERE status NOT IN ('completed', 'failed')" in ddl
    assert "archive_sha256 = lower(archive_sha256)" in ddl
    assert "replace(replace(replace" in ddl
    assert "expected_rows = loaded_rows" in ddl
    assert "TIMESTAMP WITH TIME ZONE" in ddl
    assert "REFERENCES usaspending_ingestion_checkpoints (id) ON DELETE RESTRICT" in ddl
    assert ddl.rstrip().endswith(
        "DROP TABLE usaspending_transaction_ingestion_attempts;"
    )


def _test_database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not set")
    url = make_url(value)
    if url.drivername != "postgresql+psycopg":
        pytest.fail("TEST_DATABASE_URL must use the postgresql+psycopg driver")
    database_name = url.database or ""
    if not database_name.endswith("_test"):
        pytest.fail(
            "TEST_DATABASE_URL must point to a disposable database ending in _test"
        )
    return value


def _prepare_live_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[sa.Engine, str, ModuleType]:
    engine = create_engine(_test_database_url())
    schema = f"phase1b4c_attempts_{uuid4().hex}"
    migration = _load_migration()
    with engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
        connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
        connection.exec_driver_sql(
            "CREATE TABLE usaspending_ingestion_checkpoints (id UUID PRIMARY KEY)"
        )
        monkeypatch.setattr(
            migration,
            "op",
            Operations(MigrationContext.configure(connection)),
        )
        migration.upgrade()
    return engine, schema, migration


def _drop_live_schema(engine: sa.Engine, schema: str) -> None:
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
            )
    finally:
        engine.dispose()


@pytest.mark.integration
def test_live_postgresql_upgrade_and_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, migration = _prepare_live_migration(monkeypatch)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            assert "usaspending_transaction_ingestion_attempts" in inspect(
                connection
            ).get_table_names()
            monkeypatch.setattr(
                migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            migration.downgrade()
            assert inspect(connection).get_table_names() == [
                "usaspending_ingestion_checkpoints"
            ]
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_constraints_and_active_attempt_uniqueness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _ = _prepare_live_migration(monkeypatch)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            checkpoint_id = uuid4()
            connection.exec_driver_sql(
                "INSERT INTO usaspending_ingestion_checkpoints (id) VALUES (%s)",
                (checkpoint_id,),
            )
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            now = datetime(2026, 9, 27, tzinfo=timezone.utc)
            base = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "period_start": date(2024, 10, 1),
                "period_end": date(2024, 10, 31),
                "status": "created",
                "failure_count": 0,
                "started_at": now,
                "created_at": now,
                "updated_at": now,
            }
            first_id = uuid4()
            connection.execute(attempts.insert(), {"id": first_id, **base})
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {"id": uuid4(), **base},
                    )

            connection.execute(
                attempts.update()
                .where(attempts.c.id == first_id)
                .values(
                    status="completed",
                    expected_rows=1,
                    status_url="https://api.usaspending.gov/status/1",
                    file_url="https://files.usaspending.gov/export.zip",
                    remote_file_name="export.zip",
                    archive_sha256="a" * 64,
                    archive_bytes=123,
                    loaded_rows=1,
                    signed_obligation_total=Decimal("10.25"),
                    checkpoint_id=checkpoint_id,
                    completed_at=now,
                )
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    **base,
                    "status": "completed",
                    "expected_rows": 1,
                    "status_url": "https://api.usaspending.gov/status/2",
                    "file_url": "https://files.usaspending.gov/export-2.zip",
                    "remote_file_name": "export-2.zip",
                    "archive_sha256": "0123456789abcdef" * 4,
                    "archive_bytes": 456,
                    "loaded_rows": 1,
                    "signed_obligation_total": Decimal("-0.01"),
                    "checkpoint_id": checkpoint_id,
                    "completed_at": now,
                },
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    **base,
                    "status": "failed",
                    "last_error_code": "download_failed",
                    "completed_at": now,
                },
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    **base,
                    "status": "submission_unknown",
                    "expected_rows": 1,
                    "last_error_code": "submission_unknown",
                },
            )

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {"id": uuid4(), **base},
                    )

            statuses = connection.scalars(
                sa.select(attempts.c.status).order_by(attempts.c.status)
            ).all()
            assert statuses == [
                "completed",
                "completed",
                "failed",
                "submission_unknown",
            ]

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": uuid4(),
                            **base,
                            "period_start": date(2024, 11, 1),
                            "period_end": date(2024, 11, 30),
                            "status": "completed",
                            "completed_at": now,
                        },
                    )
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_persistent_state_constraints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _ = _prepare_live_migration(monkeypatch)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            checkpoint_id = uuid4()
            connection.exec_driver_sql(
                "INSERT INTO usaspending_ingestion_checkpoints (id) VALUES (%s)",
                (checkpoint_id,),
            )
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            now = datetime(2026, 9, 27, 19, 15, tzinfo=timezone.utc)
            base = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "period_start": date(2024, 10, 1),
                "period_end": date(2024, 10, 31),
                "status": "completed",
                "expected_rows": 1,
                "status_url": "https://api.usaspending.gov/status/1",
                "file_url": "https://files.usaspending.gov/export.zip",
                "remote_file_name": "export.zip",
                "archive_sha256": "0123456789abcdef" * 4,
                "archive_bytes": 123,
                "loaded_rows": 1,
                "checkpoint_id": checkpoint_id,
                "failure_count": 0,
                "started_at": now,
                "completed_at": now,
                "created_at": now,
                "updated_at": now,
            }

            positive_id = uuid4()
            negative_id = uuid4()
            connection.execute(
                attempts.insert(),
                {
                    "id": positive_id,
                    **base,
                    "signed_obligation_total": Decimal("999999999999999999.99"),
                },
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": negative_id,
                    **base,
                    "signed_obligation_total": Decimal("-999999999999999999.99"),
                },
            )

            rows = connection.execute(
                sa.select(
                    attempts.c.id,
                    attempts.c.signed_obligation_total,
                    attempts.c.started_at,
                    attempts.c.completed_at,
                ).where(attempts.c.id.in_([positive_id, negative_id]))
            ).all()
            amounts = {row.id: row.signed_obligation_total for row in rows}
            assert amounts == {
                positive_id: Decimal("999999999999999999.99"),
                negative_id: Decimal("-999999999999999999.99"),
            }
            assert all(row.started_at.tzinfo is not None for row in rows)
            assert all(row.completed_at.tzinfo is not None for row in rows)
            assert all(row.started_at == now for row in rows)
            assert all(row.completed_at == now for row in rows)

            for invalid_hash in (
                "A" * 64,
                "a" * 32 + "A" * 32,
                "a" * 63 + "-",
                "a" * 63 + " ",
                "g" * 64,
            ):
                with pytest.raises(IntegrityError):
                    with connection.begin_nested():
                        connection.execute(
                            attempts.insert(),
                            {
                                "id": uuid4(),
                                **base,
                                "archive_sha256": invalid_hash,
                                "signed_obligation_total": Decimal("0.00"),
                            },
                        )

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": uuid4(),
                            **base,
                            "loaded_rows": 2,
                            "signed_obligation_total": Decimal("0.00"),
                        },
                    )

            with pytest.raises(DBAPIError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": uuid4(),
                            **base,
                            "signed_obligation_total": Decimal(
                                "1000000000000000000.00"
                            ),
                        },
                    )

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": uuid4(),
                            **base,
                            "checkpoint_id": uuid4(),
                            "signed_obligation_total": Decimal("0.00"),
                        },
                    )

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.exec_driver_sql(
                        "DELETE FROM usaspending_ingestion_checkpoints WHERE id = %s",
                        (checkpoint_id,),
                    )
    finally:
        _drop_live_schema(engine, schema)
