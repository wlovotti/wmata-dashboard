"""
Tests for the rider-experience OTP window (NOTES-144).

Covers the three request-time OTP endpoints wired for `otp_window`:
- GET /api/routes/{route_id} (headline `otp_all_pct` / grade)
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
`/stops` endpoint's `_stop_diagnostics_cache` and the `/api/routes/{id}`
cross-route `_live_metrics_cache`.
"""

from datetime import date, datetime, timedelta

import pytest

from api import aggregations as agg
from src.models import Calendar, Route, Run, Stop, StopEvent, StopTime, Trip
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
SERVICE_ID = "SVC144"
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
def _clear_module_caches():
    """Drop module-level caches this suite touches, before and after each test.

    `_stop_diagnostics_cache` (`/stops`) and `_live_metrics_cache` /
    `_window_metrics_cache` (`/api/routes/{id}`, shared with the
    `/api/routes` scorecard) are process-lifetime dicts keyed partly by
    `_db_identity(db)`, which resolves to the same value for every test in
    this session (all tests share one in-memory SQLite engine) — so a warm
    entry from an earlier test (or an earlier otp_window within the same
    test) could otherwise mask a cache-key bug.
    """
    agg._stop_diagnostics_cache.clear()
    agg._live_metrics_cache.clear()
    agg._window_metrics_cache.clear()
    yield
    agg._stop_diagnostics_cache.clear()
    agg._live_metrics_cache.clear()
    agg._window_metrics_cache.clear()


def _seed_route_with_stop_events(db) -> None:
    """Seed one route/trip/stop plus the three straddling deviations.

    Builds the minimal canonical stop sequence `/stops` needs (one trip,
    one stop, one direction) and three proximity `stop_events` rows at
    that stop with `DEVIATIONS`. `/trend` only needs the `stop_events`
    rows (it doesn't consult GTFS), but seeding both once keeps the
    endpoint tests sharing one fixture. `Trip.service_id` is set (but no
    `Calendar` row exists yet) so `TestDetailEndpointOtpWindow` can layer
    `_seed_service_delivered` on top without re-seeding the trip.
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
    db.add(
        Trip(
            trip_id=TRIP_ID,
            route_id=ROUTE,
            direction_id=DIRECTION,
            service_id=SERVICE_ID,
            is_current=True,
        )
    )
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


def _seed_service_delivered(db) -> None:
    """Layer a `Calendar` row + one delivered `Run` on top of `_seed_route_with_stop_events`.

    Gives `compute_service_delivered` a non-null `ratio` (1.0: one
    scheduled trip, one delivered) so `/api/routes/{id}`'s `grade` isn't
    `N/A` — `compute_route_grade` requires both `otp_pct` and
    `service_delivered_ratio` to score at all. `Calendar.tuesday=1`
    matches SERVICE_DATE (2026-05-05, a Tuesday); `stops_observable<=2`
    puts the delivered threshold at 1, so `stops_observed=2` clears it.
    """
    db.add(
        Calendar(
            service_id=SERVICE_ID,
            monday=0,
            tuesday=1,
            wednesday=0,
            thursday=0,
            friday=0,
            saturday=0,
            sunday=0,
            start_date="20260101",
            end_date="20261231",
            is_current=True,
        )
    )
    db.add(
        Run(
            service_date=SERVICE_DATE_STR,
            trip_id=TRIP_ID,
            route_id=ROUTE,
            direction_id=DIRECTION,
            source="proximity",
            stops_observable=2,
            stops_observed=2,
        )
    )
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
class TestDetailEndpointOtpWindow:
    """GET /api/routes/{route_id}?otp_window=... — the headline KPI card.

    Regression coverage for the NOTES-144 review finding: without this
    endpoint wired, the `otp_window` toggle would move the trend chart and
    stop heatmap but leave the headline `otp_all_pct` — and the letter
    grade, which scores on it — stuck on the official window.
    """

    def test_official_vs_rider_differ(self, client, db_session):
        """Headline `otp_all_pct` (and the grade it feeds) differ by window."""
        _seed_route_with_stop_events(db_session)
        _seed_service_delivered(db_session)

        official = client.get(f"/api/routes/{ROUTE}?otp_window=official")
        rider = client.get(f"/api/routes/{ROUTE}?otp_window=rider")
        assert official.status_code == 200
        assert rider.status_code == 200
        official_body = official.json()
        rider_body = rider.json()

        assert official_body["otp_all_pct"] == pytest.approx(100.0)
        # `otp_all_pct` is `on_time_pct` from `_aggregate_deviations`, rounded
        # to 2dp (33.33, not the exact repeating 33.333...) — abs tolerance
        # matches that rounding.
        assert rider_body["otp_all_pct"] == pytest.approx(100.0 / 3.0, abs=0.01)
        assert official_body["otp_window"] == "official"
        assert rider_body["otp_window"] == "rider"

        # `service_delivered_ratio` (from `_seed_service_delivered`) is 1.0
        # and identical in both bodies — grade differs solely because
        # `otp_all_pct` does, proving the window actually reaches
        # `compute_route_grade` and not just the raw OTP field.
        assert official_body["service_delivered_ratio"] == pytest.approx(1.0)
        assert official_body["service_delivered_ratio"] == rider_body["service_delivered_ratio"]
        assert official_body["grade"] != "N/A"
        assert rider_body["grade"] != "N/A"
        assert official_body["grade"] != rider_body["grade"]

        # `deltas` is sourced from the precomputed overlay (always official
        # — see get_route_detail_metrics docstring) — unaffected by the
        # request-time window either way. `deltas_otp_window` is the
        # machine-readable marker the frontend uses to detect that mismatch
        # in rider mode; it's always "official" regardless of `otp_window`.
        assert official_body["deltas"] == rider_body["deltas"]
        assert official_body["deltas_otp_window"] == "official"
        assert rider_body["deltas_otp_window"] == "official"

    def test_default_is_official(self, client, db_session):
        """Omitting `otp_window` behaves like `official` (today's numbers unchanged)."""
        _seed_route_with_stop_events(db_session)
        _seed_service_delivered(db_session)

        default = client.get(f"/api/routes/{ROUTE}")
        explicit_official = client.get(f"/api/routes/{ROUTE}?otp_window=official")
        assert default.json()["otp_window"] == "official"
        assert default.json()["otp_all_pct"] == explicit_official.json()["otp_all_pct"]
        assert default.json()["grade"] == explicit_official.json()["grade"]

    def test_invalid_otp_window_422(self, client, db_session):
        """An `otp_window` outside official|rider is rejected by FastAPI validation."""
        _seed_route_with_stop_events(db_session)
        response = client.get(f"/api/routes/{ROUTE}?otp_window=bogus")
        assert response.status_code == 422

    def test_rider_never_served_from_official_cache(self, client, db_session):
        """A warm scorecard cache never leaks into a rider detail request.

        Regression guard for the review finding: `get_live_metrics_for_route_today`
        must skip the cross-route `_live_metrics_cache` (official-only, shared
        with the `/api/routes` scorecard) whenever `otp_window != "official"`.
        The detail endpoint's own official-window request never writes that
        cache (`_compute_single_route_live_metrics` doesn't touch it) — only
        `GET /api/routes` (the scorecard, via `get_live_metrics_for_window` /
        `_compute_live_metrics_for_window_uncached`) does, keyed by
        `(db_identity, service_date)`, the same key
        `get_live_metrics_for_route_today`'s official branch reads. So the
        cache must be warmed via the scorecard endpoint, not a second detail
        call, or this test can't actually exercise the bypass: mutation-
        tested by removing the `otp_window == "official"` guard at
        `get_live_metrics_for_route_today` — this test FAILS without the
        guard (rider reads back the scorecard's official value, ~100.0) and
        PASSES with it (rider still computes ~33.33).
        """
        _seed_route_with_stop_events(db_session)
        _seed_service_delivered(db_session)

        # Warm `_live_metrics_cache[(db_identity, SERVICE_DATE)]` via the
        # scorecard — the only path that actually writes it.
        scorecard = client.get("/api/routes")
        assert scorecard.status_code == 200

        rider = client.get(f"/api/routes/{ROUTE}?otp_window=rider")
        official = client.get(f"/api/routes/{ROUTE}?otp_window=official")

        assert rider.json()["otp_all_pct"] == pytest.approx(100.0 / 3.0, abs=0.01)
        assert official.json()["otp_all_pct"] == pytest.approx(100.0)


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

        official_row = next(s for s in official.json()["stops"] if s["stop_id"] == STOP_ID)
        rider_row = next(s for s in rider.json()["stops"] if s["stop_id"] == STOP_ID)

        # Mutate the underlying data after both windows are warm: 5 more
        # events, all wildly late (2000s), that would pull *either* window's
        # otp_pct down noticeably on a fresh recompute. Committed directly —
        # a genuine recompute would see it immediately.
        sched_ts = datetime(2026, 5, 5, 17, 0, 0)
        db_session.add_all(
            [
                StopEvent(
                    service_date=SERVICE_DATE_STR,
                    trip_id=f"{TRIP_ID}_MUTATE{i}",
                    route_id=ROUTE,
                    direction_id=DIRECTION,
                    stop_id=STOP_ID,
                    stop_sequence=1,
                    scheduled_arrival_ts=sched_ts,
                    observed_arrival_ts=sched_ts + timedelta(seconds=2000),
                    deviation_sec=2000,
                    source="proximity",
                    schedule_relationship="SCHEDULED",
                )
                for i in range(5)
            ]
        )
        db_session.commit()

        # Both windows still read back their original (pre-mutation) value
        # from cache — if either had been keyed wrong (or not cached at
        # all), this would observe the new late events and the otp_pct
        # would drop.
        official_again = client.get(f"/api/routes/{ROUTE}/stops?otp_window=official")
        rider_again = client.get(f"/api/routes/{ROUTE}/stops?otp_window=rider")
        official_again_row = next(
            s for s in official_again.json()["stops"] if s["stop_id"] == STOP_ID
        )
        rider_again_row = next(s for s in rider_again.json()["stops"] if s["stop_id"] == STOP_ID)
        assert official_again_row["otp_pct"] == official_row["otp_pct"]
        assert official_again_row["n_observations"] == official_row["n_observations"]
        assert rider_again_row["otp_pct"] == rider_row["otp_pct"]
        assert rider_again_row["n_observations"] == rider_row["n_observations"]
