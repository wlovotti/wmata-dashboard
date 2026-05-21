"""Tests for the ``_service_date_for_row`` helper in src/wmata_collector.

The helper decides which calendar date each trip-update row belongs to.
That decision drives the ``service_date`` column of ``trip_update_state``
and therefore which day's stop_events get derived from each row. Getting
this wrong silently mis-attributes observations to the wrong day.
"""

from datetime import date, datetime

import pytest

from src.wmata_collector import _service_date_for_row


@pytest.mark.smoke
def test_uses_trip_start_date_when_present():
    """When WMATA populates trip.start_date, that wins over snapshot-day inference.

    A snapshot at 03:00 UTC on 5/19 is 23:00 ET on 5/18 — by ET-day
    inference it'd be 5/18. The explicit ``trip_start_date=20260518``
    agrees in this case, but the test guards against a future
    implementation that uses the snapshot's calendar day instead of
    the explicit field.
    """
    row = {
        "trip_start_date": "20260518",
        "snapshot_ts": datetime(2026, 5, 19, 3, 0, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 18)


@pytest.mark.smoke
def test_falls_back_to_snapshot_ts_when_missing():
    """No trip_start_date -> Eastern day of snapshot_ts."""
    # 18:00 UTC on 5/19 = 14:00 ET on 5/19.
    row = {
        "trip_start_date": None,
        "snapshot_ts": datetime(2026, 5, 19, 18, 0, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)


@pytest.mark.smoke
def test_falls_back_to_snapshot_ts_when_unparseable():
    """Garbage trip_start_date -> snapshot_ts fallback, not an exception.

    Defends against malformed feed payloads. If WMATA ever emits a
    non-YYYYMMDD start_date, we silently fall back rather than crash
    the collector.
    """
    row = {
        "trip_start_date": "bogus",
        "snapshot_ts": datetime(2026, 5, 19, 18, 0, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)


@pytest.mark.smoke
def test_eastern_day_boundary_late_evening():
    """A 23:30 UTC snapshot on 5/19 = 19:30 ET on 5/19, still the 19th."""
    row = {
        "trip_start_date": None,
        "snapshot_ts": datetime(2026, 5, 19, 23, 30, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)


@pytest.mark.smoke
def test_eastern_day_boundary_post_midnight_utc():
    """A 03:30 UTC snapshot on 5/20 = 23:30 ET on 5/19 — Eastern day is 5/19."""
    row = {
        "trip_start_date": None,
        "snapshot_ts": datetime(2026, 5, 20, 3, 30, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)
