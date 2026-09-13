from datetime import date

import httpx
import pytest

from app.core.config import Settings
from app.usaspending.client import UsaSpendingClient, UsaSpendingError
from app.usaspending.ingestion import normalize_name, parse_date, parse_decimal
from app.usaspending.preset import CYBER_IT_V1, as_public_dict


def settings() -> Settings:
    return Settings(database_url="postgresql+psycopg://test:password@db.invalid:5432/govtracts_test")


def test_cyber_it_preset_is_public_and_explicit() -> None:
    public = as_public_dict()

    assert public["id"] == "cyber-it-v1"
    assert "does not identify all cybersecurity spending" in str(public["disclaimer"])
    assert "541512" in public["rules"]["naics_codes"]
    assert "D310" in public["rules"]["psc_codes"]


def test_normalizers_handle_missing_and_valid_values() -> None:
    assert normalize_name("  Example   Vendor  ") == "example vendor"
    assert parse_date("2025-01-31") == date(2025, 1, 31)
    assert parse_date("not-a-date") is None
    assert parse_decimal("123.456") == parse_decimal("123.46")
    assert parse_decimal("bad") is None


def test_client_paginates_using_safe_request_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeClient:
        def __init__(self, **_: object) -> None:
            pass

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def post(self, _: str, json: dict[str, object]) -> httpx.Response:
            calls.append(json)
            page = int(json["page"])
            return httpx.Response(200, json={"results": [], "page_metadata": {"hasNext": page == 1}})

    monkeypatch.setattr(httpx, "Client", FakeClient)
    pages = list(UsaSpendingClient(settings()).iter_award_pages({"award_type_codes": ["A"]}, page_size=1))

    assert len(pages) == 2
    assert calls[0]["fields"]
    assert calls[0]["page"] == 1
    assert calls[1]["page"] == 2


def test_client_exposes_only_safe_http_status(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __init__(self, **_: object) -> None:
            pass

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def post(self, *_: object, **__: object) -> httpx.Response:
            return httpx.Response(400, text="secret remote detail")

    monkeypatch.setattr(httpx, "Client", FakeClient)
    with pytest.raises(UsaSpendingError, match="http_400"):
        list(UsaSpendingClient(settings()).iter_award_pages({"award_type_codes": ["A"]}, page_size=1))
