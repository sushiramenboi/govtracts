from collections.abc import Generator

from fastapi.testclient import TestClient

from app.api.dependencies import get_database
from app.core.config import Settings
from app.main import create_app


class ReadyConnection:
    def __enter__(self) -> "ReadyConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, _: object) -> None:
        return None


class ReadyEngine:
    def connect(self) -> ReadyConnection:
        return ReadyConnection()


class UnavailableEngine:
    def connect(self) -> None:
        raise OSError("database connection failed")


class FakeDatabase:
    def __init__(self, engine: object) -> None:
        self.engine = engine


def test_liveness_does_not_require_database_connection() -> None:
    app = create_app(Settings(database_url="postgresql+psycopg://test:password@db.invalid:5432/govtracts_test"))

    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_ready_when_database_query_succeeds() -> None:
    app = create_app(Settings(database_url="postgresql+psycopg://test:password@db.invalid:5432/govtracts_test"))
    app.dependency_overrides[get_database] = lambda: FakeDatabase(ReadyEngine())

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readiness_returns_safe_error_when_database_is_unavailable() -> None:
    app = create_app(Settings(database_url="postgresql+psycopg://test:password@db.invalid:5432/govtracts_test"))
    app.dependency_overrides[get_database] = lambda: FakeDatabase(UnavailableEngine())

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": {"status": "not_ready"}}
    assert "db.invalid" not in response.text
    assert "password" not in response.text
