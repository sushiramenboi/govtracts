import io
import importlib.util
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock
from uuid import UUID, uuid4

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
SUBDIVISION_MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20261004_0006_usaspending_backfill_subdivisions.py"
)
RECOVERY_MIGRATION_PATH = (
    API_ROOT
    / "alembic"
    / "versions"
    / "20261017_0007_usaspending_same_job_recovery.py"
)
MIGRATION_0005_TERMINAL_CHECK = (
    "((status IN ('completed', 'failed')) = (completed_at IS NOT NULL)) AND "
    "(status != 'completed' OR "
    "(checkpoint_id IS NOT NULL AND expected_rows IS NOT NULL "
    "AND export_rows IS NOT NULL AND loaded_rows IS NOT NULL "
    "AND export_rows = loaded_rows "
    "AND signed_obligation_total IS NOT NULL))"
)
EXPECTED_0006_INDEX_PREDICATES = {
    "uq_usaspending_attempts_active_period": (
        "status NOT IN ('completed', 'failed')"
    ),
    "uq_usaspending_attempts_parent_period": (
        "(parent_attempt_id IS NOT NULL) AND (status != 'failed')"
    ),
}
EXPECTED_0006_CHECK_EXPRESSIONS = {
    "ck_usaspending_transaction_ingestion_attempts_status_allowed": (
        "status IN ('created', 'counted', 'submitting', 'submitted', "
        "'export_finished', 'archive_hashed', 'loading', 'completed', "
        "'failed', 'submission_unknown', 'subdivided')"
    ),
    "ck_usaspending_transaction_ingestion_attempts_count_metadata": (
        "(status NOT IN ('counted', 'submitting', 'submitted', "
        "'export_finished', 'archive_hashed', 'loading', 'completed', "
        "'submission_unknown', 'subdivided')) OR expected_rows IS NOT NULL"
    ),
    "ck_usaspending_transaction_ingestion_attempts_terminal_fields": (
        "(status IN ('completed', 'failed', 'subdivided')) = "
        "(completed_at IS NOT NULL) AND "
        "(status != 'completed' OR checkpoint_id IS NOT NULL "
        "AND expected_rows IS NOT NULL AND export_rows IS NOT NULL "
        "AND loaded_rows IS NOT NULL AND export_rows = loaded_rows "
        "AND signed_obligation_total IS NOT NULL)"
    ),
    "ck_usaspending_transaction_ingestion_attempts_not_self_parent": (
        "parent_attempt_id IS NULL OR parent_attempt_id != id"
    ),
    "ck_usaspending_transaction_ingestion_attempts_subdivision": (
        "status != 'subdivided' OR expected_rows > 500000 "
        "AND export_rows IS NULL AND export_columns IS NULL "
        "AND status_url IS NULL AND file_url IS NULL "
        "AND remote_file_name IS NULL AND archive_sha256 IS NULL "
        "AND archive_bytes IS NULL AND loaded_rows IS NULL "
        "AND signed_obligation_total IS NULL AND checkpoint_id IS NULL "
        "AND last_error_code IS NULL AND last_error_detail IS NULL"
    ),
}


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
        "ck_usaspending_transaction_ingestion_attempts_counts_nonneg": model_checks[
            "ck_usaspending_transaction_ingestion_attempts_counts_nonneg"
        ],
        "ck_usaspending_transaction_ingestion_attempts_export_metadata": model_checks[
            "ck_usaspending_transaction_ingestion_attempts_export_metadata"
        ],
        "ck_usaspending_transaction_ingestion_attempts_terminal_fields": (
            _normalize_sql(MIGRATION_0005_TERMINAL_CHECK)
        ),
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


def test_subdivision_migration_matches_model_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(SUBDIVISION_MIGRATION_PATH, "subdivisions_0006")
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()
    table = UsaSpendingTransactionIngestionAttempt.__table__
    assert migration.revision == "20261004_0006"
    assert migration.down_revision == "20261003_0005"
    assert operations.add_column.call_args.args[1].name == "parent_attempt_id"
    assert operations.create_foreign_key.call_args.args[:3] == (
        "fk_usaspending_attempt_parent",
        table.name,
        table.name,
    )
    created_indexes = {call.args[0]: call for call in operations.create_index.call_args_list}
    assert created_indexes["uq_usaspending_attempts_parent_period"].args == (
        "uq_usaspending_attempts_parent_period",
        table.name,
        ["parent_attempt_id", "period_start", "period_end"],
    )
    assert created_indexes["uq_usaspending_attempts_parent_period"].kwargs[
        "unique"
    ] is True
    assert _normalize_sql(
        created_indexes["uq_usaspending_attempts_parent_period"].kwargs[
            "postgresql_where"
        ]
    ) == "parent_attempt_id IS NOT NULL AND status != 'failed'"
    assert created_indexes["ix_usaspending_attempts_parent"].args == (
        "ix_usaspending_attempts_parent",
        table.name,
        ["parent_attempt_id"],
    )
    model_checks = _check_definitions(list(table.constraints))
    created_checks = {
        call.args[0]: _normalize_sql(call.args[2])
        for call in operations.create_check_constraint.call_args_list
    }
    for name in (
        "ck_usaspending_transaction_ingestion_attempts_status_allowed",
        "ck_usaspending_transaction_ingestion_attempts_count_metadata",
        "ck_usaspending_transaction_ingestion_attempts_terminal_fields",
        "ck_usaspending_transaction_ingestion_attempts_not_self_parent",
        "ck_usaspending_transaction_ingestion_attempts_subdivision",
    ):
        assert created_checks[name] == model_checks[name]


def test_subdivision_migration_downgrade_guard_precedes_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(
        SUBDIVISION_MIGRATION_PATH,
        "subdivisions_0006_downgrade",
    )
    operations = Mock()
    events: list[str] = []
    operations.execute.side_effect = lambda *_: events.append("guard")
    operations.drop_constraint.side_effect = lambda *_args, **_kwargs: events.append(
        "mutation"
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    assert events[0] == "guard"


def test_recovery_migration_matches_model_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(RECOVERY_MIGRATION_PATH, "recovery_0007")
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    table = UsaSpendingTransactionIngestionAttempt.__table__
    column = operations.add_column.call_args.args[1]
    assert migration.revision == "20261017_0007"
    assert migration.down_revision == "20261004_0006"
    assert column.name == "recovery_of_attempt_id"
    assert column.nullable == table.c.recovery_of_attempt_id.nullable
    assert str(column.type) == str(table.c.recovery_of_attempt_id.type)
    assert operations.create_foreign_key.call_args.args == (
        "fk_usaspending_attempt_recovery_of",
        table.name,
        table.name,
        ["recovery_of_attempt_id"],
        ["id"],
    )
    assert operations.create_foreign_key.call_args.kwargs == {
        "ondelete": "RESTRICT"
    }
    checks = _check_definitions(list(table.constraints))
    assert operations.create_check_constraint.call_args.args == (
        "ck_usaspending_transaction_ingestion_attempts_not_self_recovery",
        table.name,
        checks["ck_usaspending_transaction_ingestion_attempts_not_self_recovery"],
    )
    index = next(
        index
        for index in table.indexes
        if index.name == "uq_usaspending_attempts_recovery_of"
    )
    assert operations.create_index.call_args.args == (
        index.name,
        table.name,
        ["recovery_of_attempt_id"],
    )
    assert operations.create_index.call_args.kwargs["unique"] is True
    assert _normalize_sql(
        operations.create_index.call_args.kwargs["postgresql_where"]
    ) == _normalize_sql(index.dialect_options["postgresql"]["where"])
    assert [call[0] for call in operations.mock_calls] == [
        "add_column",
        "create_foreign_key",
        "create_check_constraint",
        "create_index",
    ]
    assert all(
        len(name.encode("utf-8")) <= 63
        for name in (
            "fk_usaspending_attempt_recovery_of",
            "ck_usaspending_transaction_ingestion_attempts_not_self_recovery",
            "uq_usaspending_attempts_recovery_of",
        )
    )


def test_recovery_migration_downgrade_guard_precedes_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(RECOVERY_MIGRATION_PATH, "recovery_0007_guard")
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    assert operations.mock_calls[0][0] == "execute"
    guard_sql = str(operations.execute.call_args.args[0])
    assert "recovery_of_attempt_id IS NOT NULL" in guard_sql
    assert "used USAspending recovery lineage cannot be downgraded safely" in guard_sql
    assert [call[0] for call in operations.mock_calls[1:]] == [
        "drop_index",
        "drop_constraint",
        "drop_constraint",
        "drop_column",
    ]


def test_recovery_migration_compiles_for_postgresql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration(RECOVERY_MIGRATION_PATH, "recovery_0007_ddl")
    output = io.StringIO()
    operations = Operations(
        MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": output},
        )
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()
    migration.downgrade()

    ddl = output.getvalue()
    assert "ADD COLUMN recovery_of_attempt_id UUID" in ddl
    assert "ON DELETE RESTRICT" in ddl
    assert "recovery_of_attempt_id IS NOT NULL" in ddl
    assert "used USAspending recovery lineage cannot be downgraded safely" in ddl
    assert all(
        len(identifier.encode("utf-8")) <= 63
        for identifier in re.findall(
            r'\b(?:CONSTRAINT|INDEX)\s+"?([a-z0-9_]+)"?', ddl,
        )
    )


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


def _prepare_live_migration_0006(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[sa.Engine, str, ModuleType]:
    engine, schema, _, _ = _prepare_live_migration(monkeypatch)
    migration = _load_migration(
        SUBDIVISION_MIGRATION_PATH,
        f"attempts_0006_{schema}",
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
        monkeypatch.setattr(
            migration,
            "op",
            Operations(MigrationContext.configure(connection)),
        )
        migration.upgrade()
    return engine, schema, migration


def _normalize_driver_value(value: object) -> object:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else value
    if isinstance(value, Decimal):
        return value.normalize()
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, dict):
        return {
            key: _normalize_driver_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return tuple(_normalize_driver_value(item) for item in value)
    return value


def _read_normalized_attempt_rows(
    connection: sa.Connection,
    table: sa.Table,
) -> dict[object, dict[str, object]]:
    rows = (
        {
            key: _normalize_driver_value(value)
            for key, value in row.items()
        }
        for row in connection.execute(sa.select(table).order_by(table.c.id)).mappings()
    )
    return {row["id"]: row for row in rows}


def _assert_legacy_attempt_rows_preserved(
    original_rows: dict[object, dict[str, object]],
    upgraded_rows: dict[object, dict[str, object]],
) -> None:
    assert set(upgraded_rows) == set(original_rows)
    for attempt_id, original in original_rows.items():
        upgraded = upgraded_rows[attempt_id]
        assert upgraded["parent_attempt_id"] is None
        assert {
            key: value
            for key, value in upgraded.items()
            if key != "parent_attempt_id"
        } == original


_POSTGRESQL_EXPRESSION_TOKEN = re.compile(
    r"'(?:''|[^'])*'|<>|!=|>=|<=|::|=|>|<|[A-Za-z_][A-Za-z0-9_]*|\d+|[(),\[\]]"
)


def _tokenize_postgresql_expression(value: object) -> list[str]:
    expression = str(value)
    tokens = []
    position = 0
    for match in _POSTGRESQL_EXPRESSION_TOKEN.finditer(expression):
        assert not expression[position : match.start()].strip()
        token = match.group(0)
        if token[0].isalpha() or token[0] == "_":
            token = token.lower()
        elif token == "!=":
            token = "<>"
        tokens.append(token)
        position = match.end()
    assert not expression[position:].strip()
    return tokens


def _remove_postgresql_type_casts(tokens: list[str]) -> list[str]:
    without_casts = []
    position = 0
    while position < len(tokens):
        if tokens[position] != "::":
            without_casts.append(tokens[position])
            position += 1
            continue

        position += 1
        if tokens[position : position + 2] == ["character", "varying"]:
            position += 2
        else:
            assert tokens[position : position + 1] == ["text"]
            position += 1
        if tokens[position : position + 2] == ["[", "]"]:
            position += 2
    return without_casts


def _remove_parenthesized_identifiers(tokens: list[str]) -> list[str]:
    identifier = re.compile(r"[a-z_][a-z0-9_]*\Z")
    normalized = tokens
    while True:
        for position in range(len(normalized) - 2):
            if (
                normalized[position] == "("
                and identifier.fullmatch(normalized[position + 1])
                and normalized[position + 2] == ")"
            ):
                normalized = [
                    *normalized[:position],
                    normalized[position + 1],
                    *normalized[position + 3 :],
                ]
                break
        else:
            return normalized


def _quantified_array_end(
    tokens: list[str],
    quantifier_position: int,
) -> tuple[int, list[str]] | None:
    position = quantifier_position + 1
    if tokens[position : position + 1] != ["("]:
        return None
    position += 1

    wrapper_count = 0
    while tokens[position : position + 1] == ["("]:
        wrapper_count += 1
        position += 1
    if tokens[position : position + 2] != ["array", "["]:
        return None
    position += 2

    item_start = position
    bracket_depth = 1
    while position < len(tokens) and bracket_depth:
        if tokens[position] == "[":
            bracket_depth += 1
        elif tokens[position] == "]":
            bracket_depth -= 1
        position += 1
    assert bracket_depth == 0
    items = tokens[item_start : position - 1]

    assert tokens[position : position + wrapper_count] == [")"] * wrapper_count
    position += wrapper_count
    assert tokens[position : position + 1] == [")"]
    return position + 1, items


def _normalize_postgresql_quantified_arrays(tokens: list[str]) -> list[str]:
    normalized = []
    position = 0
    while position < len(tokens):
        if (
            position + 2 < len(tokens)
            and re.fullmatch(r"[a-z_][a-z0-9_]*", tokens[position])
            and tokens[position + 1] in {"=", "<>"}
            and tokens[position + 2] in {"any", "all"}
        ):
            parsed = _quantified_array_end(tokens, position + 2)
            if parsed is not None:
                end, items = parsed
                operator = (tokens[position + 1], tokens[position + 2])
                if operator == ("=", "any"):
                    normalized.extend(
                        [tokens[position], "in", "(", *items, ")"]
                    )
                    position = end
                    continue
                if operator == ("<>", "all"):
                    normalized.extend(
                        [tokens[position], "not", "in", "(", *items, ")"]
                    )
                    position = end
                    continue
        normalized.append(tokens[position])
        position += 1
    return normalized


def _matching_parenthesis(tokens: list[str], start: int) -> int | None:
    depth = 0
    for position in range(start, len(tokens)):
        if tokens[position] == "(":
            depth += 1
        elif tokens[position] == ")":
            depth -= 1
            assert depth >= 0
            if depth == 0:
                return position
    assert depth == 0
    return None


def _strip_redundant_outer_parentheses(tokens: list[str]) -> list[str]:
    normalized = tokens
    while (
        len(normalized) >= 2
        and normalized[0] == "("
        and _matching_parenthesis(normalized, 0) == len(normalized) - 1
    ):
        normalized = normalized[1:-1]
    return normalized


def _canonical_postgresql_expression(value: object) -> str:
    tokens = _tokenize_postgresql_expression(value)
    tokens = _remove_postgresql_type_casts(tokens)
    tokens = _remove_parenthesized_identifiers(tokens)
    tokens = _normalize_postgresql_quantified_arrays(tokens)
    tokens = _strip_redundant_outer_parentheses(tokens)
    expression = " ".join(tokens)
    expression = re.sub(r"\s+([,\)\]])", r"\1", expression)
    return re.sub(r"([\(\[])\s+", r"\1", expression)


def _assert_canonical_expression(actual: object, expected: str) -> None:
    assert _canonical_postgresql_expression(actual) == (
        _canonical_postgresql_expression(expected)
    )


def _postgresql_index_predicate(index: dict[str, object]) -> object | None:
    dialect_options = index.get("dialect_options", {})
    assert isinstance(dialect_options, dict)
    return dialect_options.get("postgresql_where")


def _assert_0006_index_predicates(
    indexes: dict[str, dict[str, object]],
) -> None:
    for name, expected in EXPECTED_0006_INDEX_PREDICATES.items():
        actual = _postgresql_index_predicate(indexes[name])
        assert actual is not None
        _assert_canonical_expression(actual, expected)

    for name in (
        "ix_usaspending_attempts_period_status",
        "ix_usaspending_attempts_parent",
    ):
        assert _postgresql_index_predicate(indexes[name]) is None


def _assert_0006_check_expressions(checks: dict[str, object]) -> None:
    assert set(EXPECTED_0006_CHECK_EXPRESSIONS) <= set(checks)
    for name, expected in EXPECTED_0006_CHECK_EXPRESSIONS.items():
        _assert_canonical_expression(checks[name], expected)


@pytest.mark.parametrize(
    ("index_name", "predicate"),
    [
        (
            "uq_usaspending_attempts_active_period",
            "status NOT IN ('completed', 'failed', 'created')",
        ),
        (
            "uq_usaspending_attempts_parent_period",
            "(parent_attempt_id IS NOT NULL) OR (status != 'failed')",
        ),
        (
            "ix_usaspending_attempts_period_status",
            "status != 'failed'",
        ),
        (
            "ix_usaspending_attempts_parent",
            "parent_attempt_id IS NOT NULL",
        ),
    ],
)
def test_0006_index_predicate_assertions_reject_semantic_changes(
    index_name: str,
    predicate: str,
) -> None:
    indexes = {
        name: {"dialect_options": {"postgresql_where": expected}}
        for name, expected in EXPECTED_0006_INDEX_PREDICATES.items()
    }
    indexes.update(
        {
            "ix_usaspending_attempts_period_status": {
                "dialect_options": {"postgresql_where": None}
            },
            "ix_usaspending_attempts_parent": {
                "dialect_options": {"postgresql_where": None}
            },
        }
    )
    indexes[index_name]["dialect_options"]["postgresql_where"] = predicate

    with pytest.raises(AssertionError):
        _assert_0006_index_predicates(indexes)


@pytest.mark.parametrize(
    ("constraint_name", "expression"),
    [
        (
            "ck_usaspending_transaction_ingestion_attempts_status_allowed",
            EXPECTED_0006_CHECK_EXPRESSIONS[
                "ck_usaspending_transaction_ingestion_attempts_status_allowed"
            ].replace("'subdivided')", "'subdivided', 'unexpected')"),
        ),
        (
            "ck_usaspending_transaction_ingestion_attempts_status_allowed",
            EXPECTED_0006_CHECK_EXPRESSIONS[
                "ck_usaspending_transaction_ingestion_attempts_status_allowed"
            ].replace(", 'subdivided'", ""),
        ),
        (
            "ck_usaspending_transaction_ingestion_attempts_terminal_fields",
            EXPECTED_0006_CHECK_EXPRESSIONS[
                "ck_usaspending_transaction_ingestion_attempts_terminal_fields"
            ].replace(") AND (status", ") OR (status"),
        ),
        (
            "ck_usaspending_transaction_ingestion_attempts_subdivision",
            EXPECTED_0006_CHECK_EXPRESSIONS[
                "ck_usaspending_transaction_ingestion_attempts_subdivision"
            ].replace(
                "OR expected_rows > 500000",
                "AND expected_rows > 500000",
            ),
        ),
    ],
)
def test_0006_check_assertions_reject_semantic_changes(
    constraint_name: str,
    expression: str,
) -> None:
    checks = dict(EXPECTED_0006_CHECK_EXPRESSIONS)
    checks[constraint_name] = expression

    with pytest.raises(AssertionError):
        _assert_0006_check_expressions(checks)


def _assert_live_0006_schema(connection: sa.Connection) -> None:
    table_name = "usaspending_transaction_ingestion_attempts"
    inspector = inspect(connection)
    expected_nullable = {
        "id": False,
        "source": False,
        "fiscal_year": False,
        "period_start": False,
        "period_end": False,
        "status": False,
        "expected_rows": True,
        "status_url": True,
        "file_url": True,
        "remote_file_name": True,
        "archive_sha256": True,
        "archive_bytes": True,
        "loaded_rows": True,
        "signed_obligation_total": True,
        "checkpoint_id": True,
        "last_error_code": True,
        "last_error_detail": True,
        "failure_count": False,
        "started_at": False,
        "completed_at": True,
        "created_at": False,
        "updated_at": False,
        "export_rows": True,
        "export_columns": True,
        "parent_attempt_id": True,
    }

    columns = {
        column["name"]: column
        for column in inspector.get_columns(table_name)
    }
    assert set(columns) == set(expected_nullable)
    for name, nullable in expected_nullable.items():
        assert columns[name]["nullable"] is nullable

    foreign_keys = {
        foreign_key["name"]: foreign_key
        for foreign_key in inspector.get_foreign_keys(table_name)
    }
    parent_foreign_key = foreign_keys["fk_usaspending_attempt_parent"]
    assert parent_foreign_key["constrained_columns"] == ["parent_attempt_id"]
    assert parent_foreign_key["referred_table"] == table_name
    assert parent_foreign_key["referred_columns"] == ["id"]
    assert parent_foreign_key["options"]["ondelete"].upper() == "RESTRICT"

    indexes = {
        index["name"]: index
        for index in inspector.get_indexes(table_name)
    }
    expected_indexes = {
        "uq_usaspending_attempts_active_period": (
            ["source", "period_start", "period_end"],
            True,
        ),
        "ix_usaspending_attempts_period_status": (
            ["source", "period_start", "period_end", "status"],
            False,
        ),
        "ix_usaspending_attempts_parent": (["parent_attempt_id"], False),
        "uq_usaspending_attempts_parent_period": (
            ["parent_attempt_id", "period_start", "period_end"],
            True,
        ),
    }
    assert set(indexes) == set(expected_indexes)
    for name, (column_names, unique) in expected_indexes.items():
        assert indexes[name]["column_names"] == column_names
        assert indexes[name]["unique"] is unique
    _assert_0006_index_predicates(indexes)

    checks = {
        check["name"]: check["sqltext"]
        for check in inspector.get_check_constraints(table_name)
    }
    _assert_0006_check_expressions(checks)

    primary_key = inspector.get_pk_constraint(table_name)
    identifiers = [
        table_name,
        *columns,
        *indexes,
        *foreign_keys,
        *checks,
        primary_key["name"],
    ]
    assert all(identifier is not None for identifier in identifiers)
    assert max(len(identifier) for identifier in identifiers if identifier) <= 63


@pytest.mark.integration
def test_live_postgresql_0006_preserves_populated_0005_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _, _ = _prepare_live_migration(monkeypatch)
    migration = _load_migration(
        SUBDIVISION_MIGRATION_PATH,
        f"attempts_0006_populated_{schema}",
    )
    now = datetime(2026, 10, 4, tzinfo=timezone.utc)
    checkpoint_id = uuid4()
    completed_id = uuid4()
    failed_id = uuid4()
    submitted_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            connection.execute(
                sa.text(
                    "INSERT INTO usaspending_ingestion_checkpoints (id) "
                    "VALUES (:checkpoint_id)"
                ),
                {"checkpoint_id": checkpoint_id},
            )
            attempts_0005 = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            common = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "expected_rows": None,
                "status_url": None,
                "file_url": None,
                "remote_file_name": None,
                "archive_sha256": None,
                "archive_bytes": None,
                "loaded_rows": None,
                "signed_obligation_total": None,
                "checkpoint_id": None,
                "last_error_code": None,
                "last_error_detail": None,
                "failure_count": 0,
                "started_at": now,
                "completed_at": None,
                "created_at": now,
                "updated_at": now,
                "export_rows": None,
                "export_columns": None,
            }
            connection.execute(
                attempts_0005.insert(),
                [
                    {
                        "id": completed_id,
                        **common,
                        "period_start": date(2024, 10, 1),
                        "period_end": date(2024, 10, 31),
                        "status": "completed",
                        "expected_rows": 7,
                        "export_rows": 7,
                        "export_columns": 16,
                        "status_url": "https://example.invalid/status/completed",
                        "file_url": "https://example.invalid/file/completed",
                        "remote_file_name": "completed.zip",
                        "archive_sha256": "a" * 64,
                        "archive_bytes": 123,
                        "loaded_rows": 7,
                        "signed_obligation_total": Decimal("12.34"),
                        "checkpoint_id": checkpoint_id,
                        "completed_at": now,
                    },
                    {
                        "id": failed_id,
                        **common,
                        "period_start": date(2024, 11, 1),
                        "period_end": date(2024, 11, 30),
                        "status": "failed",
                        "expected_rows": 3,
                        "last_error_code": "test_failure",
                        "last_error_detail": "stable test detail",
                        "failure_count": 1,
                        "completed_at": now,
                    },
                    {
                        "id": submitted_id,
                        **common,
                        "period_start": date(2024, 12, 1),
                        "period_end": date(2024, 12, 31),
                        "status": "submitted",
                        "expected_rows": 4,
                        "status_url": "https://example.invalid/status/submitted",
                        "file_url": "https://example.invalid/file/submitted",
                        "remote_file_name": "submitted.zip",
                    },
                ],
            )
            original_rows = _read_normalized_attempt_rows(
                connection,
                attempts_0005,
            )

            monkeypatch.setattr(
                migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            migration.upgrade()

            attempts_0006 = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            upgraded_rows = _read_normalized_attempt_rows(
                connection,
                attempts_0006,
            )
            _assert_legacy_attempt_rows_preserved(original_rows, upgraded_rows)
            _assert_live_0006_schema(connection)

            migration.downgrade()
            attempts_downgraded = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            downgraded_rows = _read_normalized_attempt_rows(
                connection,
                attempts_downgraded,
            )
            assert downgraded_rows == original_rows

            migration.upgrade()
            attempts_reupgraded = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            reupgraded_rows = _read_normalized_attempt_rows(
                connection,
                attempts_reupgraded,
            )
            _assert_legacy_attempt_rows_preserved(original_rows, reupgraded_rows)
            _assert_live_0006_schema(connection)
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_0006_round_trip_and_used_downgrade_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, migration = _prepare_live_migration_0006(monkeypatch)
    now = datetime(2026, 10, 4, tzinfo=timezone.utc)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            assert "parent_attempt_id" in {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            monkeypatch.setattr(
                migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            migration.downgrade()
            assert "parent_attempt_id" not in {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            migration.upgrade()

            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            parent_id = uuid4()
            base = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "failure_count": 0,
                "started_at": now,
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(
                attempts.insert(),
                {
                    "id": parent_id,
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 31),
                    "status": "subdivided",
                    "expected_rows": 500001,
                    "completed_at": now,
                },
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 16),
                    "status": "created",
                    "parent_attempt_id": parent_id,
                },
            )

        with pytest.raises(DBAPIError, match="used USAspending subdivisions"):
            with engine.begin() as connection:
                connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
                monkeypatch.setattr(
                    migration,
                    "op",
                    Operations(MigrationContext.configure(connection)),
                )
                migration.downgrade()

        with engine.connect() as connection:
            connection.exec_driver_sql(f'SET search_path TO "{schema}"')
            assert "parent_attempt_id" in {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            assert connection.scalar(
                sa.select(sa.func.count()).select_from(
                    sa.Table(
                        "usaspending_transaction_ingestion_attempts",
                        sa.MetaData(),
                        autoload_with=connection,
                    )
                )
            ) == 2
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_0006_enforces_self_parent_and_parent_delete_restriction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _migration = _prepare_live_migration_0006(monkeypatch)
    now = datetime(2026, 10, 4, tzinfo=timezone.utc)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            parent_id = uuid4()
            base = {
                "source": "usaspending",
                "fiscal_year": 2025,
                "failure_count": 0,
                "started_at": now,
                "created_at": now,
                "updated_at": now,
            }
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": parent_id,
                            **base,
                            "period_start": date(2024, 10, 1),
                            "period_end": date(2024, 10, 31),
                            "status": "created",
                            "parent_attempt_id": parent_id,
                        },
                    )

            connection.execute(
                attempts.insert(),
                {
                    "id": parent_id,
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 31),
                    "status": "subdivided",
                    "expected_rows": 500001,
                    "completed_at": now,
                },
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": uuid4(),
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 16),
                    "status": "created",
                    "parent_attempt_id": parent_id,
                },
            )
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.delete().where(attempts.c.id == parent_id)
                    )
    finally:
        _drop_live_schema(engine, schema)


@pytest.mark.integration
def test_live_postgresql_0007_preserves_and_enforces_recovery_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, schema, _ = _prepare_live_migration_0006(monkeypatch)
    migration = _load_migration(
        RECOVERY_MIGRATION_PATH,
        f"attempts_0007_{schema}",
    )
    now = datetime(2026, 10, 17, tzinfo=timezone.utc)
    source_id = uuid4()
    recovery_id = uuid4()
    base = {
        "source": "usaspending",
        "fiscal_year": 2025,
        "failure_count": 0,
        "started_at": now,
        "created_at": now,
        "updated_at": now,
    }
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            attempts_0006 = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            connection.execute(
                attempts_0006.insert(),
                {
                    "id": source_id,
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 16),
                    "status": "failed",
                    "last_error_code": "http_404",
                    "completed_at": now,
                },
            )
            original = _read_normalized_attempt_rows(connection, attempts_0006)
            monkeypatch.setattr(
                migration,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            migration.upgrade()
            attempts_0007 = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            assert _read_normalized_attempt_rows(connection, attempts_0007) == {
                key: {**row, "recovery_of_attempt_id": None}
                for key, row in original.items()
            }

            migration.downgrade()
            assert "recovery_of_attempt_id" not in {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
            migration.upgrade()
            attempts = sa.Table(
                "usaspending_transaction_ingestion_attempts",
                sa.MetaData(),
                autoload_with=connection,
            )
            connection.execute(
                attempts.insert(),
                {
                    "id": recovery_id,
                    **base,
                    "period_start": date(2024, 10, 1),
                    "period_end": date(2024, 10, 16),
                    "status": "submitted",
                    "expected_rows": 1,
                    "status_url": "https://example.invalid/status/recovery",
                    "file_url": "https://example.invalid/file/recovery",
                    "remote_file_name": "recovery.zip",
                    "recovery_of_attempt_id": source_id,
                },
            )
            assert connection.scalar(
                sa.select(attempts.c.recovery_of_attempt_id).where(
                    attempts.c.id == recovery_id
                )
            ) == source_id

            invalid_cases = (
                (uuid4(), source_id),
                (uuid4(), uuid4()),
            )
            for candidate_id, target_id in invalid_cases:
                with pytest.raises(IntegrityError):
                    with connection.begin_nested():
                        connection.execute(
                            attempts.insert(),
                            {
                                "id": candidate_id,
                                **base,
                                "period_start": date(2024, 11, 1),
                                "period_end": date(2024, 11, 30),
                                "status": "failed",
                                "last_error_code": "test_failure",
                                "completed_at": now,
                                "recovery_of_attempt_id": target_id,
                            },
                        )

            self_id = uuid4()
            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.insert(),
                        {
                            "id": self_id,
                            **base,
                            "period_start": date(2024, 12, 1),
                            "period_end": date(2024, 12, 31),
                            "status": "failed",
                            "last_error_code": "test_failure",
                            "completed_at": now,
                            "recovery_of_attempt_id": self_id,
                        },
                    )

            with pytest.raises(IntegrityError):
                with connection.begin_nested():
                    connection.execute(
                        attempts.delete().where(attempts.c.id == source_id)
                    )
            assert connection.scalar(sa.select(sa.func.count()).select_from(attempts)) == 2

        with pytest.raises(DBAPIError, match="used USAspending recovery lineage"):
            with engine.begin() as connection:
                connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
                monkeypatch.setattr(
                    migration,
                    "op",
                    Operations(MigrationContext.configure(connection)),
                )
                migration.downgrade()

        with engine.connect() as connection:
            connection.exec_driver_sql(f'SET search_path TO "{schema}"')
            assert "recovery_of_attempt_id" in {
                column["name"]
                for column in inspect(connection).get_columns(
                    "usaspending_transaction_ingestion_attempts"
                )
            }
    finally:
        _drop_live_schema(engine, schema)


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
