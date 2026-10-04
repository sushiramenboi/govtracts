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
OLD_MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20260927_0004_usaspending_transaction_attempts.py"
)
NEW_MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20261003_0005_usaspending_export_bound_counts.py"
)


def _load_migration(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        path,
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


def _assert_incremental_schema_parity(operations: Mock) -> None:
    model_table = UsaSpendingTransactionIngestionAttempt.__table__
    migration_columns = {
        call.args[1].name: call.args[1]
        for call in operations.add_column.call_args_list
    }
    assert set(migration_columns) == {"export_rows", "export_columns"}
    for name, migration_column in migration_columns.items():
        model_column = model_table.c[name]
        assert migration_column.nullable == model_column.nullable
        assert str(migration_column.type) == str(model_column.type)

    created_checks = {
        call.args[0]: _normalize_sql(call.args[2])
        for call in operations.create_check_constraint.call_args_list
    }
    model_checks = _check_definitions(list(model_table.constraints))
    assert created_checks == {
        name: model_checks[name]
        for name in (
            "ck_usaspending_transaction_ingestion_attempts_counts_nonneg",
            "ck_usaspending_transaction_ingestion_attempts_export_metadata",
            "ck_usaspending_transaction_ingestion_attempts_terminal_fields",
        )
    }


def test_incremental_migration_matches_model_columns_and_constraints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(NEW_MIGRATION_PATH, "export_bound_counts")
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()
    _assert_incremental_schema_parity(operations)


def test_schema_parity_rejects_same_named_semantic_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(NEW_MIGRATION_PATH, "export_bound_counts_mismatch")
    migration.COUNTS_CHECK = "export_rows IS NULL"
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)
    migration.upgrade()

    with pytest.raises(AssertionError):
        _assert_incremental_schema_parity(operations)


def test_migration_compiles_for_postgresql_with_bounded_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    old_migration = _load_migration(OLD_MIGRATION_PATH, "attempts_0004_ddl")
    new_migration = _load_migration(NEW_MIGRATION_PATH, "attempts_0005_ddl")
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    monkeypatch.setattr(old_migration, "op", operations)
    monkeypatch.setattr(new_migration, "op", operations)

    old_migration.upgrade()
    new_migration.upgrade()
    new_migration.downgrade()
    old_migration.downgrade()

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
    assert "ADD COLUMN export_rows BIGINT" in ddl
    assert "ADD COLUMN export_columns SMALLINT" in ddl
    assert "export_rows BETWEEN 0 AND 500000" in ddl
    assert "export_rows = loaded_rows" in ddl
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
    *,
    upgrade_to_0005: bool = True,
) -> tuple[sa.Engine, str, ModuleType, ModuleType]:
    engine = create_engine(_test_database_url())
    schema = f"phase1b4c_attempts_{uuid4().hex}"
    old_migration = _load_migration(OLD_MIGRATION_PATH, f"attempts_0004_{schema}")
    new_migration = _load_migration(NEW_MIGRATION_PATH, f"attempts_0005_{schema}")
    with engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
        connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
        connection.exec_driver_sql(
            "CREATE TABLE usaspending_ingestion_checkpoints (id UUID PRIMARY KEY)"
        )
        monkeypatch.setattr(
            old_migration,
            "op",
            Operations(MigrationContext.configure(connection)),
        )
        old_migration.upgrade()
        if upgrade_to_0005:
            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.upgrade()
    return engine, schema, old_migration, new_migration


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
    engine, schema, old_migration, new_migration = _prepare_live_migration(
        monkeypatch
    )
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            assert "usaspending_transaction_ingestion_attempts" in inspect(
                connection
            ).get_table_names()
            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.downgrade()
            columns = {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            assert "export_rows" not in columns
            assert "export_columns" not in columns
            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.upgrade()
            columns = {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            assert {"export_rows", "export_columns"} <= columns
            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.downgrade()
            monkeypatch.setattr(
                old_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            old_migration.downgrade()
            assert inspect(connection).get_table_names() == [
                "usaspending_ingestion_checkpoints"
            ]
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_upgrade_backfills_completed_and_preserves_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _, new_migration = _prepare_live_migration(
        monkeypatch,
        upgrade_to_0005=False,
    )
    completed_id = uuid4()
    failed_id = uuid4()
    now = datetime(2026, 10, 3, tzinfo=timezone.utc)
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
            base = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "period_start": date(2024, 10, 1),
                "period_end": date(2024, 10, 31),
                "failure_count": 0,
                "started_at": now,
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(
                attempts.insert(),
                {
                    "id": completed_id,
                    **base,
                    "status": "completed",
                    "expected_rows": 7,
                    "status_url": "https://api.usaspending.gov/status/completed",
                    "file_url": "https://files.usaspending.gov/completed.zip",
                    "remote_file_name": "completed.zip",
                    "archive_sha256": "a" * 64,
                    "archive_bytes": 123,
                    "loaded_rows": 7,
                    "signed_obligation_total": Decimal("10.25"),
                    "checkpoint_id": checkpoint_id,
                    "completed_at": now,
                },
            )
            failed_values = {
                "id": failed_id,
                **base,
                "status": "failed",
                "expected_rows": 490382,
                "status_url": "https://api.usaspending.gov/status/failed",
                "file_url": "https://files.usaspending.gov/failed.zip",
                "remote_file_name": "failed.zip",
                "archive_sha256": "0123456789abcdef" * 4,
                "archive_bytes": 456,
                "last_error_code": "transaction_count_mismatch",
                "last_error_detail": "historical evidence",
                "failure_count": 1,
                "completed_at": now,
            }
            connection.execute(attempts.insert(), failed_values)
            failed_before = dict(
                connection.execute(
                    sa.select(attempts).where(attempts.c.id == failed_id)
                ).mappings().one()
            )

            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.upgrade()
            upgraded = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            completed = connection.execute(
                sa.select(upgraded).where(upgraded.c.id == completed_id)
            ).mappings().one()
            failed = dict(
                connection.execute(
                    sa.select(upgraded).where(upgraded.c.id == failed_id)
                ).mappings().one()
            )
            assert completed.export_rows == 7
            assert completed.export_columns is None
            assert failed.pop("export_rows") is None
            assert failed.pop("export_columns") is None
            assert failed == failed_before

            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.downgrade()
            monkeypatch.setattr(
                new_migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            new_migration.upgrade()
            reupgraded = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            assert connection.scalar(
                sa.select(reupgraded.c.export_rows).where(
                    reupgraded.c.id == completed_id
                )
            ) == 7
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_upgrade_rejects_active_post_export_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _, new_migration = _prepare_live_migration(
        monkeypatch,
        upgrade_to_0005=False,
    )
    now = datetime(2026, 10, 3, tzinfo=timezone.utc)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    "source": "usaspending",
                    "fiscal_year": 2025,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 31),
                    "status": "export_finished",
                    "expected_rows": 1,
                    "status_url": "https://api.usaspending.gov/status/1",
                    "file_url": "https://files.usaspending.gov/export.zip",
                    "remote_file_name": "export.zip",
                    "failure_count": 0,
                    "started_at": now,
                    "created_at": now,
                    "updated_at": now,
                },
            )

        with pytest.raises(DBAPIError, match="unreconstructable active"):
            with engine.begin() as connection:
                connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
                monkeypatch.setattr(
                    new_migration,
                    "op",
                    Operations(MigrationContext.configure(connection)),
                )
                new_migration.upgrade()

        with engine.connect() as connection:
            connection.exec_driver_sql(f'SET search_path TO "{schema}"')
            columns = {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
        assert "export_rows" not in columns
        assert "export_columns" not in columns
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
@pytest.mark.parametrize("unsafe_history", ["completed_drift", "failed_metadata"])
def test_live_postgresql_downgrade_guards_before_schema_mutation(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_history: str,
) -> None:
    engine, schema, _, new_migration = _prepare_live_migration(monkeypatch)
    now = datetime(2026, 10, 3, tzinfo=timezone.utc)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            values = {
                "id": uuid4(),
                "source": "usaspending",
                "fiscal_year": 2025,
                "period_start": date(2024, 10, 1),
                "period_end": date(2024, 10, 31),
                "status": "failed",
                "expected_rows": 2,
                "export_rows": 1,
                "last_error_code": "transaction_count_mismatch",
                "failure_count": 1,
                "started_at": now,
                "completed_at": now,
                "created_at": now,
                "updated_at": now,
            }
            if unsafe_history == "completed_drift":
                checkpoint_id = uuid4()
                connection.exec_driver_sql(
                    "INSERT INTO usaspending_ingestion_checkpoints (id) VALUES (%s)",
                    (checkpoint_id,),
                )
                values.update(
                    status="completed",
                    export_columns=16,
                    status_url="https://api.usaspending.gov/status/1",
                    file_url="https://files.usaspending.gov/export.zip",
                    remote_file_name="export.zip",
                    archive_sha256="a" * 64,
                    archive_bytes=123,
                    loaded_rows=1,
                    signed_obligation_total=Decimal("10.25"),
                    checkpoint_id=checkpoint_id,
                    last_error_code=None,
                    failure_count=0,
                )
            connection.execute(attempts.insert(), values)

        expected_error = (
            "completed USAspending count drift"
            if unsafe_history == "completed_drift"
            else "failed USAspending export metadata"
        )
        with pytest.raises(DBAPIError, match=expected_error):
            with engine.begin() as connection:
                connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
                monkeypatch.setattr(
                    new_migration,
                    "op",
                    Operations(MigrationContext.configure(connection)),
                )
                new_migration.downgrade()

        with engine.connect() as connection:
            connection.exec_driver_sql(f'SET search_path TO "{schema}"')
            columns = {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
        assert {"export_rows", "export_columns"} <= columns
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_constraints_and_active_attempt_uniqueness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _, _ = _prepare_live_migration(monkeypatch)
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
                    "export_rows": 1,
                    "export_columns": 16,
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
    engine, schema, _, _ = _prepare_live_migration(monkeypatch)
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
                "export_rows": 1,
                "export_columns": 16,
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
