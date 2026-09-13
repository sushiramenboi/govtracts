import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url

from app.core.config import load_settings

API_ROOT = Path(__file__).resolve().parents[1]


def test_initial_migration_is_the_only_head_revision() -> None:
    config = Config(str(API_ROOT / "alembic.ini"))
    script = ScriptDirectory.from_config(config)

    assert script.get_heads() == ["20260913_0001"]


@pytest.mark.integration
def test_upgrade_on_explicit_disposable_postgres_database(monkeypatch: pytest.MonkeyPatch) -> None:
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL is not set")

    database_name = make_url(test_database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a disposable database ending in _test")

    monkeypatch.setenv("DATABASE_URL", test_database_url)
    load_settings.cache_clear()
    config = Config(str(API_ROOT / "alembic.ini"))
    try:
        command.upgrade(config, "head")
        engine = create_engine(test_database_url)
        try:
            assert {"agencies", "vendors", "awards", "opportunities", "upstream_cache", "ingestion_runs"} <= set(
                inspect(engine).get_table_names()
            )
        finally:
            engine.dispose()
    finally:
        load_settings.cache_clear()
