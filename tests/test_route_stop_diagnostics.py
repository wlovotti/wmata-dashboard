"""
Unit tests for `compute_route_stop_diagnostics` (NOTES-40).

Covers the load-bearing rules called out in the helper docstring:
- (route_id, direction_id, stop_id) grouping anti-double-count rule
- canonical sequence picker (longest trip per direction)
- skip-rate denominator semantics (TU rows only, NO_DATA included)
- day_type / period filter integration
- output ordering (direction_id ASC, stop_sequence ASC)
"""

from datetime import date, datetime, timedelta

import pytest

from api.aggregations import compute_route_stop_diagnostics
from src.models import Route, Stop, StopEvent, StopTime, Trip

ROUTE = "RT_TEST"
SERVICE_DATE = date(2026, 5, 5)  # Tuesday — weekday
SERVICE_DATE_STR = SERVICE_DATE.isoformat()


@pytest.fixture(autouse=True)
def _freeze_eastern_today(monkeypatch):
    """Pin `eastern_today()` to the test SERVICE_DATE so the 30-day window is stable.

    The diagnostic helper calls `eastern_today()` for the window's end_date.
    Tests build stop_events on SERVICE_DATE and need that date to fall inside
    the window — pinning makes the assertions deterministic and immune to
    real-clock drift around midnight.
    """
    import src.timezones as tz

    monkeypatch.setattr(tz, "eastern_today", lambda: SERVICE_DATE)


def _make_route(db, route_id: str = ROUTE) -> Route:
    """Create a Route with sensible defaults and commit."""
    route = Route(
        route_id=route_id,
        route_short_name=route_id,
        route_long_name=f"Test {route_id}",
        route_type=3,
        is_current=True,
    )
    db.add(route)
    db.commit()
    return route


def _make_stop(db, stop_id: str, name: str | None = None) -> Stop:
    """Create one current Stop and commit."""
    stop = Stop(
        stop_id=stop_id,
        stop_name=name or f"Stop {stop_id}",
        stop_lat=38.9,
        stop_lon=-77.0,
        is_current=True,
    )
    db.add(stop)
    db.commit()
    return stop


def _make_trip(db, trip_id: str, route_id: str, direction_id: int) -> Trip:
    """Create one current Trip and commit."""
    trip = Trip(
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        is_current=True,
    )
    db.add(trip)
    db.commit()
    return trip


def _make_stop_time(
    db, trip_id: str, stop_id: str, stop_sequence: int, arrival_time: str = "12:00:00"
) -> StopTime:
    """Create one current StopTime row and commit."""
    st = StopTime(
        trip_id=trip_id,
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        arrival_time=arrival_time,
        departure_time=arrival_time,
        is_current=True,
    )
    db.add(st)
    db.commit()
    return st


def _make_stop_event(
    trip_id: str,
    stop_id: str,
    stop_sequence: int,
    direction_id: int,
    *,
    deviation_sec: int | None = 0,
    observed_hour_eastern: int = 13,  # 1pm EDT — UTC = 17
    schedule_relationship: str = "SCHEDULED",
    source: str = "proximity",
    route_id: str = ROUTE,
    service_date: str = SERVICE_DATE_STR,
) -> StopEvent:
    """Build a StopEvent with naive-UTC timestamps derived from an Eastern hour.

    `observed_hour_eastern` controls which Eastern hour bucket the event
    lands in for the period filter — May 5 2026 is EDT (UTC-4), so the
    naive-UTC timestamp is `observed_hour_eastern + 4`.
    """
    # May is EDT, UTC-4. Use the same offset for scheduled and observed so
    # deviation_sec is meaningful as a parameter.
    utc_hour = observed_hour_eastern + 4
    sched_ts = datetime(2026, 5, 5, utc_hour, 0, 0)
    obs_ts = sched_ts + timedelta(seconds=deviation_sec) if deviation_sec is not None else None
    if schedule_relationship == "SKIPPED":
        obs_ts = None
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        scheduled_arrival_ts=sched_ts,
        observed_arrival_ts=obs_ts,
        deviation_sec=deviation_sec if obs_ts is not None else None,
        source=source,
        schedule_relationship=schedule_relationship,
    )


def _seed_minimal_route(db, route_id: str = ROUTE, n_stops: int = 3) -> dict[int, list[str]]:
    """Seed a route with one trip per direction × `n_stops` stops + GTFS rows.

    Returns `{direction_id: [stop_id, ...]}` so callers can produce
    stop_events keyed against the same sequence the canonical resolver
    will pick (the only trip in the direction is therefore the longest).
    """
    _make_route(db, route_id)
    by_direction: dict[int, list[str]] = {}
    for direction_id in (0, 1):
        trip_id = f"TRIP_{route_id}_D{direction_id}"
        _make_trip(db, trip_id, route_id, direction_id)
        stop_ids: list[str] = []
        for seq in range(1, n_stops + 1):
            stop_id = f"S_{route_id}_D{direction_id}_{seq}"
            _make_stop(db, stop_id)
            _make_stop_time(db, trip_id, stop_id, seq, arrival_time=f"12:{seq:02d}:00")
            stop_ids.append(stop_id)
        by_direction[direction_id] = stop_ids
    return by_direction


class TestCanonicalSequence:
    """Output rows match the longest trip per direction; ordering is stable."""

    def test_picks_longest_trip_per_direction(self, db_session):
        """A short variant on the same direction must not displace the long pattern."""
        _make_route(db_session)

        # Long trip: 5 stops on direction 0.
        _make_trip(db_session, "TRIP_LONG", ROUTE, direction_id=0)
        for seq in range(1, 6):
            sid = f"S_LONG_{seq}"
            _make_stop(db_session, sid)
            _make_stop_time(db_session, "TRIP_LONG", sid, seq, arrival_time=f"12:{seq:02d}:00")

        # Short trip on the same direction — only 2 stops, including a unique one.
        _make_trip(db_session, "TRIP_SHORT", ROUTE, direction_id=0)
        _make_stop(db_session, "S_SHORT_X")
        _make_stop_time(db_session, "TRIP_SHORT", "S_LONG_1", 1, arrival_time="12:01:00")
        _make_stop_time(db_session, "TRIP_SHORT", "S_SHORT_X", 2, arrival_time="12:02:00")

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        stop_ids = [s["stop_id"] for s in result["stops"]]
        # The long pattern wins — 5 stops in sequence; the short variant's
        # unique stop "S_SHORT_X" is dropped (not in the canonical sequence).
        assert stop_ids == ["S_LONG_1", "S_LONG_2", "S_LONG_3", "S_LONG_4", "S_LONG_5"]

    def test_orders_by_direction_then_sequence(self, db_session):
        """Output is direction_id ASC then stop_sequence ASC."""
        _seed_minimal_route(db_session, n_stops=3)

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        # Two directions × 3 stops each = 6 rows.
        assert len(result["stops"]) == 6
        # First three rows are direction 0 in sequence; next three are direction 1.
        assert [s["direction_id"] for s in result["stops"]] == [0, 0, 0, 1, 1, 1]
        assert [s["stop_sequence"] for s in result["stops"]] == [1, 2, 3, 1, 2, 3]

    def test_returns_empty_when_no_trips(self, db_session):
        """Routes with no current trips emit `stops: []` (not an error)."""
        _make_route(db_session)
        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        assert result["stops"] == []
        assert result["route_id"] == ROUTE


class TestDirectionGrouping:
    """The (route_id, direction_id, stop_id) anti-double-count rule."""

    def test_shared_stop_id_does_not_double_count(self, db_session):
        """Same stop_id served in both directions (terminus) appears twice, once per direction."""
        _make_route(db_session)

        # Direction 0 trip: stops A, SHARED_TERMINUS
        _make_trip(db_session, "TRIP_D0", ROUTE, direction_id=0)
        _make_stop(db_session, "S_A")
        _make_stop(db_session, "SHARED_TERMINUS")
        _make_stop_time(db_session, "TRIP_D0", "S_A", 1, arrival_time="12:00:00")
        _make_stop_time(db_session, "TRIP_D0", "SHARED_TERMINUS", 2, arrival_time="12:05:00")

        # Direction 1 trip: stops B, SHARED_TERMINUS (same stop_id)
        _make_trip(db_session, "TRIP_D1", ROUTE, direction_id=1)
        _make_stop(db_session, "S_B")
        _make_stop_time(db_session, "TRIP_D1", "S_B", 1, arrival_time="12:00:00")
        _make_stop_time(db_session, "TRIP_D1", "SHARED_TERMINUS", 2, arrival_time="12:05:00")

        # 10 events at SHARED_TERMINUS in direction 0, all 60s late.
        # 10 events at SHARED_TERMINUS in direction 1, all 60s early.
        events = []
        for i in range(10):
            events.append(
                _make_stop_event(
                    f"D0_TRIP_{i}",
                    "SHARED_TERMINUS",
                    2,
                    direction_id=0,
                    deviation_sec=60,
                )
            )
            events.append(
                _make_stop_event(
                    f"D1_TRIP_{i}",
                    "SHARED_TERMINUS",
                    2,
                    direction_id=1,
                    deviation_sec=-60,
                )
            )
        db_session.add_all(events)
        db_session.commit()

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        terminus_rows = [s for s in result["stops"] if s["stop_id"] == "SHARED_TERMINUS"]
        # CRITICAL: two rows, one per direction. If the helper grouped by
        # (route, stop_id) alone, it would collapse to one row with 20
        # observations and median ~0 — the bug this rule is guarding.
        assert len(terminus_rows) == 2
        by_dir = {s["direction_id"]: s for s in terminus_rows}
        assert by_dir[0]["n_observations"] == 10
        assert by_dir[0]["median_deviation_sec"] == 60
        assert by_dir[1]["n_observations"] == 10
        assert by_dir[1]["median_deviation_sec"] == -60

    def test_direction_filter_restricts_output(self, db_session):
        """`direction_id=0` returns only direction-0 rows."""
        _seed_minimal_route(db_session, n_stops=2)

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30, direction_id=0)
        assert all(s["direction_id"] == 0 for s in result["stops"])
        assert len(result["stops"]) == 2


class TestSkipRateDenominator:
    """Skip rate is `count(SKIPPED) / count(trip_update rows)` per (direction, stop)."""

    def test_denominator_is_tu_rows_count(self, db_session):
        """3 SKIPPED out of 10 TU rows → 30%; proximity rows don't change the denominator."""
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        events = []
        # 7 TU SCHEDULED arrivals
        for i in range(7):
            events.append(
                _make_stop_event(
                    f"TRIP_OK_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    deviation_sec=0,
                    source="trip_update",
                    schedule_relationship="SCHEDULED",
                )
            )
        # 3 TU SKIPPED rows (no observed_arrival_ts)
        for i in range(3):
            events.append(
                _make_stop_event(
                    f"TRIP_SKIP_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    source="trip_update",
                    schedule_relationship="SKIPPED",
                )
            )
        # 5 proximity rows on the same stop — should not affect skip rate.
        for i in range(5):
            events.append(
                _make_stop_event(
                    f"TRIP_PROX_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    deviation_sec=0,
                    source="proximity",
                )
            )
        db_session.add_all(events)
        db_session.commit()

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        row = next(
            s for s in result["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row["n_scheduled"] == 10  # 10 TU rows
        assert row["skip_pct"] == 0.3

    def test_no_tu_rows_means_skip_pct_none(self, db_session):
        """Stops with only proximity events report skip_pct=None (no denominator)."""
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        events = [
            _make_stop_event(
                f"TRIP_{i}",
                target_stop,
                1,
                direction_id=0,
                deviation_sec=0,
                source="proximity",
            )
            for i in range(5)
        ]
        db_session.add_all(events)
        db_session.commit()

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        row = next(
            s for s in result["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row["n_observations"] == 5
        assert row["skip_pct"] is None


class TestDayTypePeriodFilters:
    """day_type / period filters re-slice the per-stop aggregations."""

    def test_day_type_filter_excludes_other_days(self, db_session):
        """`day_type=saturday` drops weekday events."""
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        # Tuesday (weekday) — 5 events
        weekday_events = [
            _make_stop_event(
                f"TRIP_WD_{i}",
                target_stop,
                1,
                direction_id=0,
                deviation_sec=0,
                service_date="2026-05-05",  # Tuesday
            )
            for i in range(5)
        ]
        # Saturday — 3 events. (Within the 30-day window from May 5, May 2 is Sat.)
        sat_events = [
            _make_stop_event(
                f"TRIP_SAT_{i}",
                target_stop,
                1,
                direction_id=0,
                deviation_sec=120,
                service_date="2026-05-02",
            )
            for i in range(3)
        ]
        db_session.add_all(weekday_events + sat_events)
        db_session.commit()

        # Default day_type=all → 8 events
        result_all = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        row_all = next(
            s for s in result_all["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row_all["n_observations"] == 8

        # day_type=saturday → only the 3 saturday events
        result_sat = compute_route_stop_diagnostics(db_session, ROUTE, days=30, day_type="saturday")
        row_sat = next(
            s for s in result_sat["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row_sat["n_observations"] == 3
        assert row_sat["median_deviation_sec"] == 120

    def test_period_filter_excludes_other_hours(self, db_session):
        """`period=am_peak` (6-10am) drops events with observed_arrival_ts at 1pm."""
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        # 5 events at 1pm (PM Peak / Midday-ish), 3 events at 8am (AM Peak)
        events = []
        for i in range(5):
            events.append(
                _make_stop_event(
                    f"TRIP_PM_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    deviation_sec=0,
                    observed_hour_eastern=13,
                )
            )
        for i in range(3):
            events.append(
                _make_stop_event(
                    f"TRIP_AM_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    deviation_sec=300,
                    observed_hour_eastern=8,
                )
            )
        db_session.add_all(events)
        db_session.commit()

        result_all = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        row_all = next(
            s for s in result_all["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row_all["n_observations"] == 8

        result_am = compute_route_stop_diagnostics(db_session, ROUTE, days=30, period="am_peak")
        row_am = next(
            s for s in result_am["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        # Only the 3 AM events survive — and their median deviation = 300.
        assert row_am["n_observations"] == 3
        assert row_am["median_deviation_sec"] == 300

    def test_period_filter_keeps_skipped_via_scheduled_ts(self, db_session):
        """SKIPPED rows survive the period filter via scheduled_arrival_ts fallback.

        Otherwise period-filtered skip rates would always read 0% because
        SKIPPED rows have null observed_arrival_ts.
        """
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        events = []
        # 7 TU SCHEDULED at 8am
        for i in range(7):
            events.append(
                _make_stop_event(
                    f"TRIP_OK_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    deviation_sec=0,
                    observed_hour_eastern=8,
                    source="trip_update",
                    schedule_relationship="SCHEDULED",
                )
            )
        # 3 TU SKIPPED at 8am (only scheduled_arrival_ts is set)
        for i in range(3):
            events.append(
                _make_stop_event(
                    f"TRIP_SKIP_{i}",
                    target_stop,
                    1,
                    direction_id=0,
                    observed_hour_eastern=8,
                    source="trip_update",
                    schedule_relationship="SKIPPED",
                )
            )
        db_session.add_all(events)
        db_session.commit()

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30, period="am_peak")
        row = next(
            s for s in result["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        # All 10 TU rows survive the AM filter; skip rate is 3/10 = 0.3.
        assert row["n_scheduled"] == 10
        assert row["skip_pct"] == 0.3


class TestOTPAndPercentiles:
    """OTP and percentile computations against the deviation list."""

    def test_otp_pct_uses_constants(self, db_session):
        """OTP counts events with -120 <= dev <= 420 (per src/otp_constants.py)."""
        stops_by_dir = _seed_minimal_route(db_session, n_stops=2)
        target_stop = stops_by_dir[0][0]

        # Devs: 4 on-time (0s), 1 early (-180s), 5 late (600s)
        deviations = [0, 0, 0, 0, -180, 600, 600, 600, 600, 600]
        events = [
            _make_stop_event(
                f"TRIP_{i}",
                target_stop,
                1,
                direction_id=0,
                deviation_sec=dev,
                source="proximity",
            )
            for i, dev in enumerate(deviations)
        ]
        db_session.add_all(events)
        db_session.commit()

        result = compute_route_stop_diagnostics(db_session, ROUTE, days=30)
        row = next(
            s for s in result["stops"] if s["stop_id"] == target_stop and s["direction_id"] == 0
        )
        assert row["n_observations"] == 10
        # 4/10 on-time
        assert row["otp_pct"] == 0.4
