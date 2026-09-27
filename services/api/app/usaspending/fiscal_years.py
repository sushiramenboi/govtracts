"""Federal fiscal-year and ingestion-month date utilities."""

from calendar import monthrange
from datetime import date


def fiscal_year_for_date(action_date: date) -> int:
    """Return the federal fiscal year containing an action date."""
    if not isinstance(action_date, date):
        raise TypeError("action_date must be a date")
    return action_date.year + 1 if action_date.month >= 10 else action_date.year


def fiscal_year_bounds(fiscal_year: int) -> tuple[date, date]:
    """Return the inclusive October 1 through September 30 fiscal-year bounds."""
    _validate_year("fiscal_year", fiscal_year, minimum=2)
    return date(fiscal_year - 1, 10, 1), date(fiscal_year, 9, 30)


def fiscal_month_bounds(
    fiscal_year: int,
    *,
    calendar_year: int,
    calendar_month: int,
) -> tuple[date, date]:
    """Return one exact calendar month after validating its fiscal-year selection."""
    _validate_year("fiscal_year", fiscal_year, minimum=2)
    _validate_year("calendar_year", calendar_year, minimum=1)
    if (
        isinstance(calendar_month, bool)
        or not isinstance(calendar_month, int)
        or not 1 <= calendar_month <= 12
    ):
        raise ValueError("calendar_month must be an integer from 1 through 12")

    start = date(calendar_year, calendar_month, 1)
    selected_fiscal_year = fiscal_year_for_date(start)
    if selected_fiscal_year != fiscal_year:
        raise ValueError(
            f"{calendar_year:04d}-{calendar_month:02d} belongs to "
            f"FY{selected_fiscal_year}, not FY{fiscal_year}"
        )

    end = date(calendar_year, calendar_month, monthrange(calendar_year, calendar_month)[1])
    return start, end


def _validate_year(name: str, value: int, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= 9999:
        raise ValueError(f"{name} must be an integer from {minimum} through 9999")
