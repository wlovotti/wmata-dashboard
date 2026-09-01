"""Post-replay canary: feed-trip_id vs current-GTFS-trip_id match rate.

NOTES-134: on 2026-08-30 Muni's fall service change landed a new
trip_id space while the loaded GTFS snapshot was still the old one —
feed trip_ids matched 0 of 3,657 current trips, so derivation silently
produced near-zero stop_events while exiting 0. This canary computes
the same intersection derive_stop_events_from_state.py's
``active_trip_ids`` gate relies on (observed VehiclePosition trip_ids
∩ is_current Trip.trip_id) and fails loudly when it collapses.
"""

from datetime import date, timedelta

import pytest

from src.models import Trip, VehiclePosition
from src.timezones import local_midnight_as_utc


def _vp(trip_id: str, service_date: date, tz_name: str = "America/New_York") -> VehiclePosition:
    """Build a VehiclePosition row timestamped inside ``service_date``'s local window."""
    ts = local_midnight_as_utc(service_date, tz_name) + timedelta(hours=8)
    return VehiclePosition(
        vehicle_id="v1",
        route_id="R1",
        trip_id=trip_id,
        trip_start_date=service_date.strftime("%Y%m%d"),
        latitude=0.0,
        longitude=0.0,
        timestamp=ts,
    )


def _trip(trip_id: str, is_current: bool = True) -> Trip:
    """Build a minimal current (or superseded) Trip row."""
    return Trip(trip_id=trip_id, route_id="R1", is_current=is_current)


def test_no_observed_trips_returns_none_rate(db_session):
    """No vehicle_positions for the date means nothing to compare — not a collapse."""
    from scripts.gtfs_trip_match_canary import compute_trip_match_rate

    result = compute_trip_match_rate(db_session, date(2026, 8, 30), "America/New_York")
    assert result.observed_count == 0
    assert result.rate is None


def test_full_match_rate_is_one(db_session):
    """Every observed trip_id matches a current GTFS trip -> rate 1.0."""
    from scripts.gtfs_trip_match_canary import compute_trip_match_rate

    d = date(2026, 8, 30)
    db_session.add_all([_trip("t1"), _trip("t2"), _vp("t1", d), _vp("t2", d)])
    db_session.commit()

    result = compute_trip_match_rate(db_session, d, "America/New_York")
    assert result.observed_count == 2
    assert result.matched_count == 2
    assert result.rate == 1.0


def test_collapsed_match_rate_is_near_zero(db_session):
    """A new trip_id space (service change) -> zero of the observed ids match."""
    from scripts.gtfs_trip_match_canary import compute_trip_match_rate

    d = date(2026, 8, 30)
    # Old (superseded) trip is not is_current; observed feed trip_ids are
    # entirely from the new (unloaded) trip_id space.
    db_session.add_all([_trip("old-t1", is_current=False), _vp("new-t1", d), _vp("new-t2", d)])
    db_session.commit()

    result = compute_trip_match_rate(db_session, d, "America/New_York")
    assert result.observed_count == 2
    assert result.matched_count == 0
    assert result.rate == 0.0


def test_check_trip_match_rate_passes_above_threshold(db_session):
    """A healthy match rate does not raise."""
    from scripts.gtfs_trip_match_canary import check_trip_match_rate

    d = date(2026, 8, 30)
    db_session.add_all([_trip("t1"), _vp("t1", d)])
    db_session.commit()

    # Should not raise.
    check_trip_match_rate(db_session, "wmata", d, "America/New_York", threshold=0.05)


def test_check_trip_match_rate_fails_loudly_on_collapse(db_session):
    """A collapsed match rate raises with agency/date/rate in the message."""
    from scripts.gtfs_trip_match_canary import MatchRateCollapseError, check_trip_match_rate

    d = date(2026, 8, 30)
    db_session.add_all([_trip("old-t1", is_current=False), _vp("new-t1", d)])
    db_session.commit()

    with pytest.raises(MatchRateCollapseError) as exc_info:
        check_trip_match_rate(db_session, "sfmta", d, "America/New_York", threshold=0.05)

    message = str(exc_info.value)
    assert "sfmta" in message
    assert "2026-08-30" in message
    assert "0.0" in message or "0%" in message


def test_check_trip_match_rate_skips_when_no_data(db_session):
    """No observed trips (e.g. a not-yet-replayed date) is a skip, not a failure."""
    from scripts.gtfs_trip_match_canary import check_trip_match_rate

    d = date(2026, 8, 30)
    # No VehiclePosition rows at all for this date.
    check_trip_match_rate(db_session, "wmata", d, "America/New_York", threshold=0.05)
