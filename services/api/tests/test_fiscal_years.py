from datetime import date

import pytest

from app.usaspending.fiscal_years import (
    fiscal_month_bounds,
    fiscal_year_bounds,
    fiscal_year_for_date,
)


@pytest.mark.parametrize(
    (("action_date", "expected_fiscal_year")),
    [
        (date(2025, 9, 30), 2025),
        (date(2025, 10, 1), 2026),
        (date(2024, 12, 31), 2025),
        (date(2025, 1, 1), 2025),
    ],
)
def test_fiscal_year_for_date_uses_action_date(
    action_date: date,
    expected_fiscal_year: int,
) -> None:
    assert fiscal_year_for_date(action_date) == expected_fiscal_year


def test_fiscal_year_bounds_are_inclusive_and_cross_calendar_years() -> None:
    assert fiscal_year_bounds(2025) == (date(2024, 10, 1), date(2025, 9, 30))


@pytest.mark.parametrize(
    (("fiscal_year", "calendar_year", "calendar_month", "expected")),
    [
        (2025, 2024, 10, (date(2024, 10, 1), date(2024, 10, 31))),
        (2025, 2025, 4, (date(2025, 4, 1), date(2025, 4, 30))),
        (2024, 2024, 2, (date(2024, 2, 1), date(2024, 2, 29))),
        (2025, 2025, 2, (date(2025, 2, 1), date(2025, 2, 28))),
    ],
)
def test_fiscal_month_bounds_return_exact_calendar_months(
    fiscal_year: int,
    calendar_year: int,
    calendar_month: int,
    expected: tuple[date, date],
) -> None:
    assert (
        fiscal_month_bounds(
            fiscal_year,
            calendar_year=calendar_year,
            calendar_month=calendar_month,
        )
        == expected
    )


@pytest.mark.parametrize(
    (("fiscal_year", "calendar_year", "calendar_month")),
    [
        (2025, 2025, 10),
        (2025, 2024, 9),
    ],
)
def test_fiscal_month_bounds_reject_inconsistent_fiscal_year(
    fiscal_year: int,
    calendar_year: int,
    calendar_month: int,
) -> None:
    with pytest.raises(ValueError, match="belongs to FY"):
        fiscal_month_bounds(
            fiscal_year,
            calendar_year=calendar_year,
            calendar_month=calendar_month,
        )


@pytest.mark.parametrize(
    (("fiscal_year", "calendar_year", "calendar_month")),
    [
        (1, 2024, 10),
        (2025, 0, 10),
        (2025, 2024, 0),
        (2025, 2024, 13),
        (True, 2024, 10),
    ],
)
def test_fiscal_month_bounds_reject_invalid_inputs(
    fiscal_year: int,
    calendar_year: int,
    calendar_month: int,
) -> None:
    with pytest.raises(ValueError):
        fiscal_month_bounds(
            fiscal_year,
            calendar_year=calendar_year,
            calendar_month=calendar_month,
        )


def test_fiscal_year_for_date_rejects_non_date_input() -> None:
    with pytest.raises(TypeError, match="action_date must be a date"):
        fiscal_year_for_date("2025-10-01")  # type: ignore[arg-type]
