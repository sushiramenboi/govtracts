"""Federal fiscal-year and ingestion-month date utilities."""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta


BACKFILL_FIRST_FISCAL_YEAR = 2020
BACKFILL_LAST_FISCAL_YEAR = 2026


@dataclass(frozen=True, order=True)
class FiscalPeriod:
    """One inclusive period wholly contained in a federal fiscal year."""

    fiscal_year: int
    start: date
    end: date


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


def backfill_month_roots() -> tuple[FiscalPeriod, ...]:
    """Return the 84 ordered calendar-month roots for FY2020 through FY2026."""
    roots: list[FiscalPeriod] = []
    for fiscal_year in range(
        BACKFILL_FIRST_FISCAL_YEAR,
        BACKFILL_LAST_FISCAL_YEAR + 1,
    ):
        fiscal_start, fiscal_end = fiscal_year_bounds(fiscal_year)
        cursor = fiscal_start
        while cursor <= fiscal_end:
            start, end = fiscal_month_bounds(
                fiscal_year,
                calendar_year=cursor.year,
                calendar_month=cursor.month,
            )
            roots.append(FiscalPeriod(fiscal_year, start, end))
            cursor = end + timedelta(days=1)
    assert_exact_period_coverage(
        date(2019, 10, 1),
        date(2026, 9, 30),
        ((period.start, period.end) for period in roots),
    )
    return tuple(roots)


def validate_subperiod(
    fiscal_year: int,
    period_start: date,
    period_end: date,
) -> tuple[date, date]:
    """Validate a nonempty inclusive period inside one month and one FY."""
    _validate_year("fiscal_year", fiscal_year, minimum=2)
    if type(period_start) is not date or type(period_end) is not date:
        raise TypeError("period bounds must be dates")
    if period_end < period_start:
        raise ValueError("period_end must not precede period_start")
    if (
        period_start.year != period_end.year
        or period_start.month != period_end.month
    ):
        raise ValueError("period must remain within one calendar month")
    if (
        fiscal_year_for_date(period_start) != fiscal_year
        or fiscal_year_for_date(period_end) != fiscal_year
    ):
        raise ValueError("period must remain within one fiscal year")
    return period_start, period_end


def validate_backfill_period(
    fiscal_year: int,
    period_start: date,
    period_end: date,
) -> tuple[date, date]:
    """Validate one period and constrain it to the controlled backfill years."""
    validated = validate_subperiod(fiscal_year, period_start, period_end)
    if not BACKFILL_FIRST_FISCAL_YEAR <= fiscal_year <= BACKFILL_LAST_FISCAL_YEAR:
        raise ValueError("period fiscal year is outside FY2020 through FY2026")
    return validated


def split_period(
    period_start: date,
    period_end: date,
) -> tuple[tuple[date, date], tuple[date, date]]:
    """Split an inclusive multi-day range into deterministic balanced children."""
    if type(period_start) is not date or type(period_end) is not date:
        raise TypeError("period bounds must be dates")
    days = (period_end - period_start).days + 1
    if days <= 1:
        raise ValueError("period must contain at least two days")
    left_days = (days + 1) // 2
    left_end = period_start + timedelta(days=left_days - 1)
    children = ((period_start, left_end), (left_end + timedelta(days=1), period_end))
    assert_exact_period_coverage(period_start, period_end, children)
    return children


def assert_exact_period_coverage(
    expected_start: date,
    expected_end: date,
    periods: object,
) -> None:
    """Raise when ordered inclusive periods have a gap, overlap, or wrong bounds."""
    if type(expected_start) is not date or type(expected_end) is not date:
        raise TypeError("coverage bounds must be dates")
    if expected_end < expected_start:
        raise ValueError("coverage end must not precede coverage start")
    try:
        normalized = tuple(periods)  # type: ignore[arg-type]
    except TypeError:
        raise TypeError("periods must be iterable") from None
    if not normalized:
        raise ValueError("period coverage must not be empty")
    cursor = expected_start
    for item in normalized:
        if (
            not isinstance(item, tuple)
            or len(item) != 2
            or type(item[0]) is not date
            or type(item[1]) is not date
        ):
            raise TypeError("each period must be a pair of dates")
        start, end = item
        if end < start:
            raise ValueError("period end must not precede period start")
        if start != cursor:
            raise ValueError("period coverage contains a gap or overlap")
        cursor = end + timedelta(days=1)
    if normalized[0][0] != expected_start or normalized[-1][1] != expected_end:
        raise ValueError("period coverage does not match expected bounds")


def _validate_year(name: str, value: int, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= 9999:
        raise ValueError(f"{name} must be an integer from {minimum} through 9999")
