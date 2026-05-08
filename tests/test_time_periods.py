"""
Unit tests for src/time_periods.py — the day-type / time-period buckets used
by the RouteDetail filter (NOTES-41). Covers the late-night-wrap case for
`is_hour_in_period`, valid-token contracts, and the period lookup.
"""

from __future__ import annotations

import pytest

from src.time_periods import (
    ALL_DAY_TYPES,
    ALL_HOURS,
    TIME_PERIODS,
    VALID_DAY_TYPES,
    VALID_PERIOD_KEYS,
    get_period,
    hour_range_for_period,
    is_hour_in_period,
)


class TestIsHourInPeriodAll:
    """`ALL_HOURS` is the no-filter sentinel — every hour qualifies."""

    @pytest.mark.parametrize("hour", list(range(24)))
    def test_every_hour_qualifies(self, hour):
        assert is_hour_in_period(hour, ALL_HOURS) is True


class TestIsHourInPeriodAmPeak:
    """AM Peak is 6-10 (inclusive start, exclusive end)."""

    @pytest.mark.parametrize("hour", [6, 7, 8, 9])
    def test_in_period(self, hour):
        assert is_hour_in_period(hour, "am_peak") is True

    @pytest.mark.parametrize("hour", [5, 10, 11, 0, 22])
    def test_out_of_period(self, hour):
        assert is_hour_in_period(hour, "am_peak") is False


class TestIsHourInPeriodMidday:
    """Midday is 10-15."""

    @pytest.mark.parametrize("hour", [10, 11, 12, 13, 14])
    def test_in_period(self, hour):
        assert is_hour_in_period(hour, "midday") is True

    @pytest.mark.parametrize("hour", [9, 15, 16, 0, 23])
    def test_out_of_period(self, hour):
        assert is_hour_in_period(hour, "midday") is False


class TestIsHourInPeriodPmPeak:
    """PM Peak is 15-19."""

    @pytest.mark.parametrize("hour", [15, 16, 17, 18])
    def test_in_period(self, hour):
        assert is_hour_in_period(hour, "pm_peak") is True

    @pytest.mark.parametrize("hour", [14, 19, 20, 0])
    def test_out_of_period(self, hour):
        assert is_hour_in_period(hour, "pm_peak") is False


class TestIsHourInPeriodEvening:
    """Evening is 19-22."""

    @pytest.mark.parametrize("hour", [19, 20, 21])
    def test_in_period(self, hour):
        assert is_hour_in_period(hour, "evening") is True

    @pytest.mark.parametrize("hour", [18, 22, 23, 0])
    def test_out_of_period(self, hour):
        assert is_hour_in_period(hour, "evening") is False


class TestIsHourInPeriodLate:
    """Late wraps midnight — 22, 23, 0..5 qualify; 6..21 do not."""

    @pytest.mark.parametrize("hour", [22, 23, 0, 1, 2, 3, 4, 5])
    def test_late_night_includes(self, hour):
        assert is_hour_in_period(hour, "late") is True

    @pytest.mark.parametrize("hour", [6, 7, 12, 18, 21])
    def test_late_night_excludes(self, hour):
        assert is_hour_in_period(hour, "late") is False


class TestIsHourInPeriodInvalid:
    """Unknown period keys return False (defensive — endpoint validates first)."""

    def test_unknown_key_returns_false(self):
        assert is_hour_in_period(12, "garbage") is False

    def test_empty_string_returns_false(self):
        assert is_hour_in_period(12, "") is False


class TestGetPeriod:
    """`get_period` returns the named tuple, or None for `all` / unknown."""

    def test_all_hours_returns_none(self):
        assert get_period(ALL_HOURS) is None

    def test_known_key_returns_named_tuple(self):
        am = get_period("am_peak")
        assert am is not None
        assert am.key == "am_peak"
        assert am.start_hour == 6
        assert am.end_hour == 10
        assert am.wraps_midnight is False

    def test_late_wraps_midnight(self):
        late = get_period("late")
        assert late is not None
        assert late.wraps_midnight is True
        assert late.start_hour == 22
        assert late.end_hour == 6

    def test_unknown_returns_none(self):
        assert get_period("nope") is None


class TestHourRangeForPeriod:
    """Returns a 3-tuple (start, end, wraps) or None for `all`."""

    def test_all_returns_none(self):
        assert hour_range_for_period(ALL_HOURS) is None

    def test_am_peak_returns_range(self):
        assert hour_range_for_period("am_peak") == (6, 10, False)

    def test_late_returns_wrap_flag(self):
        assert hour_range_for_period("late") == (22, 6, True)


class TestValidTokens:
    """Valid-token tuples pin the API contract — kept here so an accidental
    re-key of TIME_PERIODS that drops the `all` sentinel is caught loudly."""

    def test_all_period_keys_present(self):
        # ALL_HOURS plus one entry per TIME_PERIODS row.
        assert set(VALID_PERIOD_KEYS) == {ALL_HOURS, *(p.key for p in TIME_PERIODS)}

    def test_day_type_tokens(self):
        assert set(VALID_DAY_TYPES) == {ALL_DAY_TYPES, "weekday", "saturday", "sunday"}

    def test_five_periods_defined(self):
        # Brief: AM Peak / Midday / PM Peak / Evening / Late.
        assert len(TIME_PERIODS) == 5
        keys = [p.key for p in TIME_PERIODS]
        assert keys == ["am_peak", "midday", "pm_peak", "evening", "late"]
