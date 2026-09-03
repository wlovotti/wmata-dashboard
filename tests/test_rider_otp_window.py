"""
Tests for the rider-experience OTP window (NOTES-144).

Covers the two request-time OTP endpoints wired for `otp_window`:
- GET /api/routes/{route_id}/trend?metric=otp
- GET /api/routes/{route_id}/stops

Seeds three proximity `stop_events` deviations (-90s, +120s, +300s) that
straddle both windows so official (-2min/+7min) and rider (-1min/+3min)
disagree on on-time classification:
    -90s:  on-time under official (>= -120), EARLY under rider (< -60)
    +120s: on-time under both (<= 420 and <= 180)
    +300s: on-time under official (<= 420), LATE under rider (> 180)
So official OTP = 3/3 = 100%, rider OTP = 1/3 = 33.33...%.

Also covers `otp_window_bounds` (src/otp_constants.py), invalid
`otp_window` -> 422, and cache isolation between the two windows on the
`/stops` endpoint's `_stop_diagnostics_cache`.
"""

from datetime import date, datetime, timedelta

import pytest

from api import aggregations as agg
from src.models import Route, Stop, StopEvent, StopTime, Trip
from src.otp_constants import (
    OTP_EARLY_SEC,
    OTP_LATE_SEC,
    RIDER_OTP_EARLY_SEC,
    RIDER_OTP_LATE_SEC,
    otp_window_bounds,
)

ROUTE = "R144"
STOP_ID = "S144_1"
TRIP_ID = "T144_D0"
DIRECTION = 0
SERVICE_DATE = date(2026, 5, 5)  # Tuesday — weekday, EDT (UTC-4)
SERVICE_DATE_STR = SERVICE_DATE.isoformat()

# Straddles both windows: -90 is early under rider but on-time under
# official; +300 is late under rider but on-time under official; +120 is
# on-time under both.
DEVIATIONS = [-90, 120, 300]


@pytest.fixture(autouse=True)
def _freeze_eastern_today(monkeypatch):
    """Pin `eastern_today()` so both endpoints' windows include SERVICE_DATE.

    Both `get_route_trend_data` and `compute_route_stop_diagnostics` derive
    their window's `end_date` from `eastern_today()` via a local import —
    pinning it here (module-attribute patch) makes the seeded date fall
    inside the window deterministically.
    """
    import src.timezones as tz

    monkeypatch.setattr(tz, "eastern_today", lambda: SERVICE_DATE)


@pytest.fixture(autouse=True)
def _clear_stop_diagnostics_cache():
    """Drop the module-level stop-diagnostics cache before and after each test.

    Otherwise a warm entry from an earlier test (or an earlier otp_window
    within the same test) could mask a cache-key bug.
    """
    agg._stop_diagnostics_cache.clear()
    yield
    agg._stop_diagnostics_cache.clear()


def _seed_route_with_stop_events(db) -> None:
    """Seed one route/trip/stop plus the three straddling deviations.

    Builds the minimal canonical stop sequence `/stops` needs (one trip,
    one stop, one direction) and three proximity `stop_events` rows at
    that stop with `DEVIATIONS`. `/trend` only needs the `stop_events`
    rows (it doesn't consult GTFS), but seeding both once keeps the two
    endpoint tests sharing one fixture.
    """
    db.add(
        Route(
            route_id=ROUTE,
            route_short_name=ROUTE,
            route_long_name="Test Route 144",
            route_type=3,
            is_current=True,
        )
    )
    db.add(
        Stop(stop_id=STOP_ID, stop_name="Stop 144", stop_lat=38.9, stop_lon=-77.0, is_current=True)
    )
    db.add(Trip(trip_id=TRIP_ID, route_id=ROUTE, direction_id=DIRECTION, is_current=True))
    db.add(
        StopTime(
            trip_id=TRIP_ID,
            stop_id=STOP_ID,
            stop_sequence=1,
            arrival_time="12:00:00",
            departure_time="12:00:00",
            is_current=True,
        )
    )
    db.commit()

    # 1pm Eastern on a May (EDT, UTC-4) service date -> 17:00 naive UTC.
    sched_ts = datetime(2026, 5, 5, 17, 0, 0)
    events = []
    for i, dev in enumerate(DEVIATIONS):
        obs_ts = sched_ts + timedelta(seconds=dev)
        events.append(
            StopEvent(
                service_date=SERVICE_DATE_STR,
                trip_id=f"{TRIP_ID}_EVT{i}",
                route_id=ROUTE,
                direction_id=DIRECTION,
                stop_id=STOP_ID,
                stop_sequence=1,
                scheduled_arrival_ts=sched_ts,
                observed_arrival_ts=obs_ts,
                deviation_sec=dev,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
    db.add_all(events)
    db.commit()


class TestOtpWindowBounds:
    """`src.otp_constants.otp_window_bounds` — the name-to-bounds mapping."""

    def test_official(self):
        """`official` resolves to the WMATA scorecard constants."""
        assert otp_window_bounds("official") == (OTP_EARLY_SEC, OTP_LATE_SEC)

    def test_rider(self):
        """`rider` resolves to the stricter rider-experience constants."""
        assert otp_window_bounds("rider") == (RIDER_OTP_EARLY_SEC, RIDER_OTP_LATE_SEC)

    def test_invalid_name_raises(self):
        """Anything else raises ValueError."""
        with pytest.raises(ValueError):
            otp_window_bounds("bogus")


@pytest.mark.api
class TestTrendEndpointOtpWindow:
    """GET /api/routes/{route_id}/trend?metric=otp&otp_window=..."""

    def test_official_vs_rider_differ(self, client, db_session):
        """Official and rider OTP differ on the seeded straddling deviations."""
        _seed_route_with_stop_events(db_session)

        official = client.get(f"/api/routes/{ROUTE}/trend?metric=otp&otp_window=official")
        rider = client.get(f"/api/routes/{ROUTE}/trend?metric=otp&otp_window=rider")
        assert official.status_code == 200
        assert rider.status_code == 200

        official_point = next(
            p for p in official.json()["trend_data"] if p["date"] == SERVICE_DATE_STR
        )
        rider_point = next(p for p in rider.json()["trend_data"] if p["date"] == SERVICE_DATE_STR)

        assert official_point["otp_percentage"] == pytest.approx(100.0)
        assert rider_point["otp_percentage"] == pytest.approx(100.0 / 3.0)
        assert official.json()["otp_window"] == "official"
        assert rider.json()["otp_window"] == "rider"

    def test_default_is_official(self, client, db_session):
        """Omitting `otp_window` behaves like `official` (today's numbers unchanged)."""
        _seed_route_with_stop_events(db_session)

        default = client.get(f"/api/routes/{ROUTE}/trend?metric=otp")
        explicit_official = client.get(f"/api/routes/{ROUTE}/trend?metric=otp&otp_window=official")
        assert default.json()["otp_window"] == "official"
        default_point = next(
            p for p in default.json()["trend_data"] if p["date"] == SERVICE_DATE_STR
        )
        official_point = next(
            p for p in explicit_official.json()["trend_data"] if p["date"] == SERVICE_DATE_STR
        )
        assert default_point["otp_percentage"] == official_point["otp_percentage"]

    def test_invalid_otp_window_422(self, client, db_session):
        """An `otp_window` outside official|rider is rejected by FastAPI validation."""
        _seed_route_with_stop_events(db_session)
        response = client.get(f"/api/routes/{ROUTE}/trend?metric=otp&otp_window=bogus")
        assert response.status_code == 422


@pytest.mark.api
class TestStopsEndpointOtpWindow:
    """GET /api/routes/{route_id}/stops?otp_window=..."""

    def test_official_vs_rider_differ(self, client, db_session):
        """Official and rider `otp_pct` differ at the seeded stop."""
        _seed_route_with_stop_events(db_session)

        official = client.get(f"/api/routes/{ROUTE}/stops?otp_window=official")
        rider = client.get(f"/api/routes/{ROUTE}/stops?otp_window=rider")
        assert official.status_code == 200
        assert rider.status_code == 200

        official_row = next(s for s in official.json()["stops"] if s["stop_id"] == STOP_ID)
        rider_row = next(s for s in rider.json()["stops"] if s["stop_id"] == STOP_ID)

        assert official_row["otp_pct"] == 1.0  # 3/3
        assert rider_row["otp_pct"] == pytest.approx(1.0 / 3.0, abs=1e-4)  # 1/3
        # Non-OTP fields are unaffected by the window.
        assert official_row["median_deviation_sec"] == rider_row["median_deviation_sec"]
        assert official.json()["otp_window"] == "official"
        assert rider.json()["otp_window"] == "rider"

    def test_invalid_otp_window_422(self, client, db_session):
        """An `otp_window` outside official|rider is rejected by FastAPI validation."""
        _seed_route_with_stop_events(db_session)
        response = client.get(f"/api/routes/{ROUTE}/stops?otp_window=bogus")
        assert response.status_code == 422

    def test_cache_isolation_between_windows(self, client, db_session):
        """Official and rider requests never share a `_stop_diagnostics_cache` entry.

        Regression guard for NOTES-144: before `otp_window` was added to the
        cache key, a `rider` request following a warm `official` one would
        have been served the official (cached) result.
        """
        _seed_route_with_stop_events(db_session)

        official = client.get(f"/api/routes/{ROUTE}/stops?otp_window=official")
        rider = client.get(f"/api/routes/{ROUTE}/stops?otp_window=rider")

        # Two distinct cache entries were written (one per otp_window),
        # not one clobbering the other.
        assert len(agg._stop_diagnostics_cache) == 2
        cache_windows = {key[6] for key in agg._stop_diagnostics_cache}
        assert cache_windows == {"official", "rider"}

        # And a second official request still reads the official value back
        # from cache, not the rider value written afterward.
        official_again = client.get(f"/api/routes/{ROUTE}/stops?otp_window=official")
        official_row = next(s for s in official.json()["stops"] if s["stop_id"] == STOP_ID)
        official_again_row = next(
            s for s in official_again.json()["stops"] if s["stop_id"] == STOP_ID
        )
        rider_row = next(s for s in rider.json()["stops"] if s["stop_id"] == STOP_ID)
        assert official_again_row["otp_pct"] == official_row["otp_pct"]
        assert official_again_row["otp_pct"] != rider_row["otp_pct"]
