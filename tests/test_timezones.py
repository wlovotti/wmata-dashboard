"""Tests for src.timezones service-date window helpers."""

from datetime import date, datetime

from src.timezones import service_date_position_window_utc


def test_position_window_edt_spans_48h_from_eastern_midnight():
    """July (EDT, UTC-4): window starts at Eastern midnight of the service
    date and ends 48 hours later, expressed as naive UTC.

    The 48h width is deliberate: GTFS service days extend past midnight
    (stop_times hours can exceed 24), so positions for a service date can
    land on the next calendar day. Two full Eastern days is generous
    enough for any real service while still excluding phantom timestamps
    months away.
    """
    start, end = service_date_position_window_utc(date(2026, 7, 2))
    assert start == datetime(2026, 7, 2, 4, 0, 0)
    assert end == datetime(2026, 7, 4, 4, 0, 0)


def test_position_window_est_uses_5h_utc_offset():
    """January (EST, UTC-5): Eastern midnight is 05:00 UTC."""
    start, end = service_date_position_window_utc(date(2026, 1, 15))
    assert start == datetime(2026, 1, 15, 5, 0, 0)
    assert end == datetime(2026, 1, 17, 5, 0, 0)


def test_position_window_spring_forward_shortens_window():
    """DST spring-forward (2026-03-08): the first day is 23 hours long, so
    the window is 47 hours — correct service-day semantics, matching
    eastern_day_bounds_utc's behavior on transition days."""
    start, end = service_date_position_window_utc(date(2026, 3, 8))
    assert start == datetime(2026, 3, 8, 5, 0, 0)  # EST midnight
    assert end == datetime(2026, 3, 10, 4, 0, 0)  # EDT midnight two days on
