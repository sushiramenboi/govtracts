from datetime import date

import pytest

from app.usaspending.fiscal_years import (
    assert_exact_period_coverage,
    backfill_month_roots,
    fiscal_month_bounds,
    fiscal_year_bounds,
    fiscal_year_for_date,
    split_period,
    validate_backfill_period,
    validate_subperiod,
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


def test_backfill_month_roots_cover_exactly_fy2020_through_fy2026() -> None:
    roots = backfill_month_roots()

    assert len(roots) == 84
    assert (roots[0].fiscal_year, roots[0].start, roots[0].end) == (
        2020,
        date(2019, 10, 1),
        date(2019, 10, 31),
    )
    assert (roots[-1].fiscal_year, roots[-1].start, roots[-1].end) == (
        2026,
        date(2026, 9, 1),
        date(2026, 9, 30),
    )
    assert {root.fiscal_year for root in roots} == set(range(2020, 2027))
    assert all(root.end <= date(2026, 9, 30) for root in roots)
    assert_exact_period_coverage(
        date(2019, 10, 1),
        date(2026, 9, 30),
        ((root.start, root.end) for root in roots),
    )


def test_backfill_roots_include_both_leap_februaries() -> None:
    roots = {(root.start, root.end) for root in backfill_month_roots()}
    assert (date(2020, 2, 1), date(2020, 2, 29)) in roots
    assert (date(2024, 2, 1), date(2024, 2, 29)) in roots


@pytest.mark.parametrize("days", [28, 29, 30, 31])
def test_balanced_split_is_adjacent_gap_free_and_deterministic(days: int) -> None:
    start = date(2024, 1, 1)
    end = date(2024, 1, days)
    left, right = split_period(start, end)

    assert left[0] == start
    assert right[1] == end
    assert (left[1] - left[0]).days + 1 == (days + 1) // 2
    assert (right[1] - right[0]).days + 1 == days // 2
    assert_exact_period_coverage(start, end, (left, right))
    assert split_period(start, end) == (left, right)


def test_recursive_splits_converge_to_exact_individual_days() -> None:
    pending = [(date(2024, 2, 1), date(2024, 2, 29))]
    leaves: list[tuple[date, date]] = []
    while pending:
        start, end = pending.pop(0)
        if start == end:
            leaves.append((start, end))
        else:
            pending[0:0] = list(split_period(start, end))

    assert len(leaves) == 29
    assert_exact_period_coverage(date(2024, 2, 1), date(2024, 2, 29), leaves)


@pytest.mark.parametrize(
    ("fiscal_year", "start", "end"),
    [
        (2025, date(2024, 10, 10), date(2024, 10, 20)),
        (2024, date(2024, 2, 29), date(2024, 2, 29)),
    ],
)
def test_validate_subperiod_accepts_same_month_same_fiscal_year(
    fiscal_year: int,
    start: date,
    end: date,
) -> None:
    assert validate_subperiod(fiscal_year, start, end) == (start, end)


@pytest.mark.parametrize(
    ("fiscal_year", "start", "end"),
    [
        (2025, date(2024, 10, 2), date(2024, 10, 1)),
        (2025, date(2024, 10, 31), date(2024, 11, 1)),
        (2025, date(2025, 9, 30), date(2025, 10, 1)),
    ],
)
def test_validate_subperiod_rejects_reversal_or_cross_boundary(
    fiscal_year: int,
    start: date,
    end: date,
) -> None:
    with pytest.raises(ValueError):
        validate_subperiod(fiscal_year, start, end)


def test_backfill_period_rejects_fy2027_without_narrowing_general_validation() -> None:
    period = (date(2026, 10, 1), date(2026, 10, 1))
    assert validate_subperiod(2027, *period) == period
    with pytest.raises(ValueError, match="outside FY2020"):
        validate_backfill_period(2027, *period)


@pytest.mark.parametrize(
    "periods",
    [
        ((date(2024, 1, 1), date(2024, 1, 1)), (date(2024, 1, 3), date(2024, 1, 3))),
        ((date(2024, 1, 1), date(2024, 1, 2)), (date(2024, 1, 2), date(2024, 1, 3))),
        ((date(2024, 1, 1), date(2024, 1, 3)), (date(2024, 1, 4), date(2024, 1, 3))),
    ],
)
def test_coverage_proof_rejects_gaps_overlaps_and_reversals(
    periods: tuple[tuple[date, date], ...],
) -> None:
    with pytest.raises(ValueError):
        assert_exact_period_coverage(date(2024, 1, 1), date(2024, 1, 3), periods)


def test_one_day_period_cannot_be_split() -> None:
    with pytest.raises(ValueError, match="at least two days"):
        split_period(date(2024, 1, 1), date(2024, 1, 1))
