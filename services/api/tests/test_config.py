import pytest

from app.core.config import Settings, load_settings


def test_settings_omit_database_url_from_representation() -> None:
    settings = Settings(database_url="postgresql+psycopg://user:secret@db.invalid:5432/govtracts")

    assert "secret" not in repr(settings)
    assert settings.allowed_origins == []


def test_load_settings_reports_missing_database_url_without_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    load_settings.cache_clear()

    with pytest.raises(RuntimeError) as error:
        load_settings()

    assert "DATABASE_URL" in str(error.value)
    load_settings.cache_clear()


def test_load_settings_hides_invalid_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    invalid_url = "postgresql://user:should-not-appear@db.invalid/govtracts"
    monkeypatch.setenv("DATABASE_URL", invalid_url)
    load_settings.cache_clear()

    with pytest.raises(RuntimeError) as error:
        load_settings()

    assert invalid_url not in str(error.value)
    load_settings.cache_clear()
