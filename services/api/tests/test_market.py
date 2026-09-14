from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.api.routes.market import market_overview
from app.db.models import Award


class Result:
    def __init__(self, *, one: tuple[object, ...] | None = None, rows: list[object] | None = None) -> None:
        self._one = one
        self._rows = rows or []

    def one(self) -> tuple[object, ...]:
        assert self._one is not None
        return self._one

    def all(self) -> list[object]:
        return self._rows


class RecentAwardRow:
    """Matches SQLAlchemy positional result access without an ``Award`` attribute."""

    def __init__(self, award: Award, agency_name: str, vendor_name: str) -> None:
        self._mapping = {
            "usa_generated_id": award.usa_generated_id,
            "award_id": award.award_id,
            "naics_code": award.naics_code,
            "psc_code": award.psc_code,
            "obligation_amount": award.obligation_amount,
            "base_obligation_date": award.base_obligation_date,
            "award_type": award.award_type,
            "source_url": award.source_url,
        }
        self.agency_name = agency_name
        self.vendor_name = vendor_name


class Connection:
    def __init__(self, results: list[Result]) -> None:
        self._results = results

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, _: object) -> Result:
        return self._results.pop(0)

    def scalar(self, _: object) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class Database:
    def __init__(self, connection: Connection) -> None:
        self.engine = SimpleNamespace(connect=lambda: connection)


def test_market_overview_serializes_positional_award_result() -> None:
    award = Award(
        usa_generated_id="generated-award-id",
        award_id="CONT-001",
        obligation_amount=Decimal("125.50"),
        base_obligation_date=date(2026, 1, 15),
    )
    connection = Connection(
        [
            Result(one=(Decimal("125.50"), 1, date(2026, 1, 15), date(2026, 1, 15))),
            Result(rows=[(datetime(2026, 1, 1, tzinfo=timezone.utc), Decimal("125.50"))]),
            Result(rows=[SimpleNamespace(name="Example Agency", external_code="123", amount=Decimal("125.50"), count=1)]),
            Result(rows=[SimpleNamespace(canonical_name="Example Vendor", amount=Decimal("125.50"), count=1)]),
            Result(rows=[RecentAwardRow(award, "Example Agency", "Example Vendor")]),
        ]
    )

    response = market_overview(Database(connection))  # type: ignore[arg-type]

    assert response["award_count"] == 1
    assert response["recent_awards"] == [
        {
            "id": "generated-award-id",
            "award_id": "CONT-001",
            "agency": "Example Agency",
            "vendor": "Example Vendor",
            "naics_code": None,
            "psc_code": None,
            "obligation_amount": Decimal("125.50"),
            "base_obligation_date": date(2026, 1, 15),
            "award_type": None,
            "source_url": None,
        }
    ]
