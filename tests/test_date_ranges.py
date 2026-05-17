"""Unit tests for ``src/date_ranges.py`` helpers.

Verifies inclusivity, ordering, single-date edge cases, and input
validation for both ``iter_eastern_dates`` and
``iter_recent_eastern_dates``.
"""

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from src.date_ranges import iter_eastern_dates, iter_recent_eastern_dates


class TestIterEasternDates:
    """Tests for ``iter_eastern_dates(start, end)``."""

    def test_range_is_inclusive_on_both_endpoints(self):
        """Both ``start`` and ``end`` are yielded."""
        start = date(2026, 5, 1)
        end = date(2026, 5, 3)
        result = list(iter_eastern_dates(start, end))
        assert result == [date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3)]

    def test_ascending_order(self):
        """Dates are yielded in chronological order."""
        result = list(iter_eastern_dates(date(2026, 5, 10), date(2026, 5, 15)))
        for i in range(1, len(result)):
            assert result[i] > result[i - 1]

    def test_single_day_range(self):
        """``start == end`` yields exactly that one date."""
        d = date(2026, 5, 17)
        assert list(iter_eastern_dates(d, d)) == [d]

    def test_crosses_month_boundary(self):
        """Range spanning month boundary yields all dates correctly."""
        result = list(iter_eastern_dates(date(2026, 4, 29), date(2026, 5, 2)))
        assert result == [
            date(2026, 4, 29),
            date(2026, 4, 30),
            date(2026, 5, 1),
            date(2026, 5, 2),
        ]

    def test_crosses_year_boundary(self):
        """Range spanning year boundary yields all dates correctly."""
        result = list(iter_eastern_dates(date(2025, 12, 30), date(2026, 1, 2)))
        assert result == [
            date(2025, 12, 30),
            date(2025, 12, 31),
            date(2026, 1, 1),
            date(2026, 1, 2),
        ]

    def test_end_before_start_raises(self):
        """``end < start`` raises ValueError rather than silently empty."""
        with pytest.raises(ValueError, match="must be >="):
            list(iter_eastern_dates(date(2026, 5, 10), date(2026, 5, 1)))

    def test_returns_iterator_not_list(self):
        """Result is a generator/iterator (lazy)."""
        result = iter_eastern_dates(date(2026, 5, 1), date(2026, 5, 3))
        assert not isinstance(result, list)
        # Verify it's iterable
        assert iter(result) is result


class TestIterRecentEasternDates:
    """Tests for ``iter_recent_eastern_dates(lookback_days)``."""

    def test_lookback_one_yields_only_today(self):
        """``lookback_days=1`` yields only the current Eastern date."""
        fake_today = date(2026, 5, 17)
        with patch("src.date_ranges.eastern_today", return_value=fake_today):
            result = list(iter_recent_eastern_dates(1))
        assert result == [fake_today]

    def test_lookback_seven_yields_seven_dates_ending_today(self):
        """``lookback_days=7`` yields 7 dates with today last."""
        fake_today = date(2026, 5, 17)
        with patch("src.date_ranges.eastern_today", return_value=fake_today):
            result = list(iter_recent_eastern_dates(7))
        assert len(result) == 7
        assert result[-1] == fake_today
        assert result[0] == fake_today - timedelta(days=6)

    def test_ascending_order(self):
        """Dates are yielded oldest-first."""
        fake_today = date(2026, 5, 17)
        with patch("src.date_ranges.eastern_today", return_value=fake_today):
            result = list(iter_recent_eastern_dates(5))
        for i in range(1, len(result)):
            assert result[i] > result[i - 1]

    def test_lookback_zero_raises(self):
        """``lookback_days=0`` raises ValueError (empty window not allowed)."""
        with pytest.raises(ValueError, match="must be >= 1"):
            list(iter_recent_eastern_dates(0))

    def test_lookback_negative_raises(self):
        """Negative lookback raises ValueError."""
        with pytest.raises(ValueError, match="must be >= 1"):
            list(iter_recent_eastern_dates(-3))

    def test_returns_iterator_not_list(self):
        """Result is a generator/iterator (lazy)."""
        fake_today = date(2026, 5, 17)
        with patch("src.date_ranges.eastern_today", return_value=fake_today):
            result = iter_recent_eastern_dates(3)
            assert not isinstance(result, list)
