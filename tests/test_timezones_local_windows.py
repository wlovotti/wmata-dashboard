"""Tests for the timezone-generic ``local_*`` siblings added for NOTES-100.

Mirrors ``tests/test_timezones.py`` (the Eastern-only originals) but
parameterized by IANA zone name, so the SFMTA (Pacific) derivation path
gets the same DST-transition and offset coverage WMATA already has.
"""

from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src.timezones import (
    local_day_bounds_utc,
    local_midnight_as_utc,
    local_service_date_position_window_utc,
    local_today,
)

EASTERN = "America/New_York"
PACIFIC = "America/Los_Angeles"


def test_local_midnight_matches_eastern_midnight_as_utc_for_eastern():
    """local_midnight_as_utc(d, "America/New_York") == eastern_midnight_as_utc(d)."""
    from src.timezones import eastern_midnight_as_utc

    d = date(2026, 7, 2)
    assert local_midnight_as_utc(d, EASTERN) == eastern_midnight_as_utc(d)


def test_local_midnight_pacific_pdt_offset():
    """July (PDT, UTC-7): Pacific midnight is 07:00 UTC."""
    assert local_midnight_as_utc(date(2026, 7, 2), PACIFIC) == datetime(2026, 7, 2, 7, 0, 0)


def test_local_midnight_pacific_pst_offset():
    """January (PST, UTC-8): Pacific midnight is 08:00 UTC."""
    assert local_midnight_as_utc(date(2026, 1, 15), PACIFIC) == datetime(2026, 1, 15, 8, 0, 0)


def test_local_day_bounds_pacific_spans_24h_normal_day():
    start, end = local_day_bounds_utc(date(2026, 7, 2), PACIFIC)
    assert start == datetime(2026, 7, 2, 7, 0, 0)
    assert end == datetime(2026, 7, 3, 7, 0, 0)


def test_local_day_bounds_matches_eastern_day_bounds_utc_for_eastern():
    from src.timezones import eastern_day_bounds_utc

    d = date(2026, 3, 8)  # DST spring-forward
    assert local_day_bounds_utc(d, EASTERN) == eastern_day_bounds_utc(d)


def test_position_window_pacific_spans_48h_from_pacific_midnight():
    """Pacific sibling of test_position_window_edt_spans_48h_from_eastern_midnight."""
    start, end = local_service_date_position_window_utc(date(2026, 7, 2), PACIFIC)
    assert start == datetime(2026, 7, 2, 7, 0, 0)
    assert end == datetime(2026, 7, 4, 7, 0, 0)


def test_position_window_matches_eastern_helper_for_eastern():
    from src.timezones import service_date_position_window_utc

    d = date(2026, 1, 15)
    assert local_service_date_position_window_utc(d, EASTERN) == service_date_position_window_utc(d)


def test_local_today_returns_current_date_in_named_zone():
    """05:30 UTC on 2026-07-22 is still 2026-07-21 in Pacific but already
    2026-07-22 in Eastern -- same split as test_local_date_pacific_vs_eastern_split
    in tests/test_timezones_local_date.py, but for "today" resolution
    instead of a stored-timestamp conversion."""
    fixed_utc = datetime(2026, 7, 22, 5, 30, 0, tzinfo=ZoneInfo("UTC"))
    with patch("src.timezones.datetime") as mock_dt:
        mock_dt.now.side_effect = lambda tz: fixed_utc.astimezone(tz)
        assert local_today(EASTERN) == date(2026, 7, 22)
        assert local_today(PACIFIC) == date(2026, 7, 21)
