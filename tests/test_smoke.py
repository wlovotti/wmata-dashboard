"""
Smoke tests for WMATA Dashboard

Quick tests that verify critical paths are working.
These should run fast (<10s) and fail fast if something is fundamentally broken.

Run with: pytest -m smoke
"""

from datetime import datetime

import pytest
from sqlalchemy import text

from src.models import (
    Calendar,
    Route,
    RouteServiceProfile,
    Run,
    StopEvent,
    StopTime,
    Trip,
    TripUpdateSnapshot,
)
from src.service_profile import compute_route_service_profile
from src.wmata_collector import WMATADataCollector


@pytest.mark.smoke
def test_database_connection(db_session):
    """Test that database connection works"""
    # Should be able to execute a simple query
    result = db_session.execute(text("SELECT 1")).scalar()
    assert result == 1


@pytest.mark.smoke
def test_database_can_create_and_query_route(db_session):
    """Test basic database insert and query"""
    # Create a route
    route = Route(
        route_id="SMOKE1",
        route_short_name="SM1",
        route_long_name="Smoke Test Route",
        route_type=3,
        is_current=True,
    )
    db_session.add(route)
    db_session.commit()

    # Query it back
    queried_route = db_session.query(Route).filter_by(route_id="SMOKE1").first()
    assert queried_route is not None
    assert queried_route.route_short_name == "SM1"


@pytest.mark.smoke
def test_api_server_responds(client):
    """Test that API server starts and responds"""
    # Just check that we get any response
    response = client.get("/api/routes")
    # Should get a response (might be 200 with empty list or other valid response)
    assert response.status_code in [200, 404, 500]  # Any response means server is running


@pytest.mark.smoke
def test_api_routes_endpoint_structure(client, sample_route, sample_route_metrics_summary):
    """Test that /api/routes returns expected JSON structure"""
    response = client.get("/api/routes")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    if len(data) > 0:
        # Check first route has expected keys
        route = data[0]
        expected_keys = [
            "route_id",
            "route_name",
            "otp_percentage",
            "avg_headway_minutes",
            "avg_speed_mph",
            "grade",
        ]
        for key in expected_keys:
            assert key in route, f"Missing key: {key}"


@pytest.mark.smoke
def test_critical_modules_import():
    """Test that critical modules can be imported"""
    # Database
    from src.database import get_session

    assert get_session is not None

    # Analytics
    from src.analytics import calculate_headways

    assert calculate_headways is not None

    # API
    from api.aggregations import get_all_routes_scorecard

    assert get_all_routes_scorecard is not None

    # Models
    from src.models import Route, Stop, Trip

    assert Route is not None
    assert Stop is not None
    assert Trip is not None


@pytest.mark.smoke
def test_trip_update_snapshot_persists(db_session):
    """TripUpdateSnapshot rows insert and read back with a shared snapshot_ts."""
    snapshot_ts = datetime(2026, 5, 3, 14, 30, 0)
    rows = [
        TripUpdateSnapshot(
            snapshot_ts=snapshot_ts,
            trip_id="TRIP_A",
            route_id="R1",
            vehicle_id="V1",
            stop_id="S1",
            stop_sequence=1,
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 31, 0),
            schedule_relationship="SCHEDULED",
        ),
        TripUpdateSnapshot(
            snapshot_ts=snapshot_ts,
            trip_id="TRIP_A",
            route_id="R1",
            vehicle_id="V1",
            stop_id="S2",
            stop_sequence=2,
            predicted_arrival_ts=None,
            schedule_relationship="SKIPPED",
        ),
    ]
    db_session.add_all(rows)
    db_session.commit()

    persisted = (
        db_session.query(TripUpdateSnapshot)
        .filter_by(trip_id="TRIP_A")
        .order_by(TripUpdateSnapshot.stop_sequence)
        .all()
    )
    assert len(persisted) == 2
    assert persisted[0].stop_id == "S1"
    assert persisted[1].schedule_relationship == "SKIPPED"
    assert persisted[1].predicted_arrival_ts is None
    assert persisted[0].snapshot_ts == persisted[1].snapshot_ts


@pytest.mark.smoke
def test_stop_event_persists_with_both_sources(db_session):
    """A real-world arrival can have two stop_events — one per derivation source."""
    common = {
        "service_date": "2026-05-03",
        "trip_id": "TRIP_X",
        "route_id": "D80",
        "direction_id": 0,
        "stop_id": "STOP_5",
        "stop_sequence": 5,
        "scheduled_arrival_ts": datetime(2026, 5, 3, 14, 30, 0),
        "scheduled_departure_ts": datetime(2026, 5, 3, 14, 30, 30),
        "observed_arrival_ts": datetime(2026, 5, 3, 14, 31, 15),
        "deviation_sec": 75,
    }
    db_session.add_all(
        [
            StopEvent(**common, source="trip_update", schedule_relationship="SCHEDULED"),
            StopEvent(
                **common,
                source="proximity",
                schedule_relationship="SCHEDULED",
                match_distance_m=18.4,
            ),
        ]
    )
    db_session.commit()

    rows = (
        db_session.query(StopEvent)
        .filter_by(trip_id="TRIP_X", stop_sequence=5)
        .order_by(StopEvent.source)
        .all()
    )
    assert [r.source for r in rows] == ["proximity", "trip_update"]
    assert rows[0].match_distance_m == 18.4
    assert rows[1].match_distance_m is None


@pytest.mark.smoke
def test_stop_event_unique_constraint_rejects_duplicate(db_session):
    """The (service_date, trip_id, stop_sequence, source) unique constraint holds."""
    from sqlalchemy.exc import IntegrityError

    base = {
        "service_date": "2026-05-03",
        "trip_id": "TRIP_Y",
        "route_id": "D80",
        "direction_id": 1,
        "stop_id": "STOP_3",
        "stop_sequence": 3,
        "source": "trip_update",
        "schedule_relationship": "SCHEDULED",
        "observed_arrival_ts": datetime(2026, 5, 3, 15, 0, 0),
    }
    db_session.add(StopEvent(**base))
    db_session.commit()

    db_session.add(StopEvent(**base))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


@pytest.mark.smoke
def test_stop_event_skipped_has_null_observed(db_session):
    """SKIPPED stop_events are valid with no observed_arrival_ts."""
    db_session.add(
        StopEvent(
            service_date="2026-05-03",
            trip_id="TRIP_Z",
            route_id="D80",
            direction_id=0,
            stop_id="STOP_7",
            stop_sequence=7,
            source="trip_update",
            schedule_relationship="SKIPPED",
            scheduled_arrival_ts=datetime(2026, 5, 3, 16, 0, 0),
            observed_arrival_ts=None,
        )
    )
    db_session.commit()

    row = db_session.query(StopEvent).filter_by(trip_id="TRIP_Z").one()
    assert row.schedule_relationship == "SKIPPED"
    assert row.observed_arrival_ts is None


@pytest.mark.smoke
def test_parse_gtfs_time_handles_post_midnight_hours():
    """GTFS HH:MM:SS with HH ≥ 24 parses into the next calendar day in UTC."""
    from datetime import date

    from pipelines.stop_events_common import parse_gtfs_time_to_dt

    # 24:21:00 anchored at 2026-05-02 service date = 2026-05-03 00:21 Eastern = 04:21 UTC (EDT)
    crossover = parse_gtfs_time_to_dt("24:21:00", date(2026, 5, 2))
    assert crossover == datetime(2026, 5, 3, 4, 21, 0)

    # Normal daytime: 14:30:00 anchored at 2026-05-03 = 18:30 UTC (EDT, UTC-4)
    daytime = parse_gtfs_time_to_dt("14:30:00", date(2026, 5, 3))
    assert daytime == datetime(2026, 5, 3, 18, 30, 0)

    # Garbage input returns None
    assert parse_gtfs_time_to_dt("not-a-time", date(2026, 5, 3)) is None


@pytest.mark.smoke
def test_parse_trip_start_date_round_trip():
    """trip_start_date YYYYMMDD parses to a date; bad inputs return None."""
    from datetime import date

    from pipelines.stop_events_common import parse_trip_start_date

    assert parse_trip_start_date("20260503") == date(2026, 5, 3)
    assert parse_trip_start_date(None) is None
    assert parse_trip_start_date("") is None
    assert parse_trip_start_date("2026-05-03") is None  # wrong format
    assert parse_trip_start_date("20260230") is None  # invalid date


# Column order for `_last_snapshots_per_stop` tuples — must match the
# `db.query(...)` projection in `derive_trip_update_stop_events`.
def _tu_tuple(
    trip_id,
    stop_id,
    stop_sequence,
    vehicle_id,
    snapshot_ts,
    predicted_arrival_ts,
    predicted_departure_ts=None,
    schedule_relationship="SCHEDULED",
):
    """Build a snapshot tuple in the column order `_last_snapshots_per_stop` consumes."""
    return (
        trip_id,
        stop_id,
        stop_sequence,
        vehicle_id,
        snapshot_ts,
        predicted_arrival_ts,
        predicted_departure_ts,
        schedule_relationship,
    )


@pytest.mark.smoke
def test_last_snapshots_per_stop_picks_final_state_and_last_prediction():
    """Final state = absolute-last snapshot; observed = last snapshot with non-null pred_arr."""
    from pipelines.derive_stop_events_trip_updates import _last_snapshots_per_stop

    # One (trip, stop_seq): three snapshots. Last has null pred_arr (bus passed,
    # WMATA cleared the prediction). Middle has the most recent non-null pred_arr.
    snaps = [
        _tu_tuple(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            vehicle_id="V1",
            snapshot_ts=datetime(2026, 5, 3, 14, 30, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 35, 0),
        ),
        _tu_tuple(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            vehicle_id="V1",
            snapshot_ts=datetime(2026, 5, 3, 14, 33, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 34, 30),
        ),
        _tu_tuple(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            vehicle_id="V1",
            snapshot_ts=datetime(2026, 5, 3, 14, 35, 30),
            predicted_arrival_ts=None,
        ),
    ]
    out = _last_snapshots_per_stop(snaps)
    entry = out[("T1", 5)]
    assert entry["final_snapshot_ts"] == datetime(2026, 5, 3, 14, 35, 30)
    assert entry["last_predicted_arrival_ts"] == datetime(2026, 5, 3, 14, 34, 30)
    assert entry["final_schedule_relationship"] == "SCHEDULED"


@pytest.mark.smoke
def test_last_snapshots_per_stop_marks_skipped():
    """A SKIPPED final state propagates regardless of earlier SCHEDULED snapshots."""
    from pipelines.derive_stop_events_trip_updates import _last_snapshots_per_stop

    snaps = [
        _tu_tuple(
            trip_id="T2",
            stop_id="S3",
            stop_sequence=3,
            vehicle_id=None,
            snapshot_ts=datetime(2026, 5, 3, 14, 0, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
            schedule_relationship="SCHEDULED",
        ),
        _tu_tuple(
            trip_id="T2",
            stop_id="S3",
            stop_sequence=3,
            vehicle_id=None,
            snapshot_ts=datetime(2026, 5, 3, 14, 4, 30),
            predicted_arrival_ts=None,
            schedule_relationship="SKIPPED",
        ),
    ]
    out = _last_snapshots_per_stop(snaps)
    assert out[("T2", 3)]["final_schedule_relationship"] == "SKIPPED"


@pytest.mark.smoke
def test_last_snapshots_per_stop_skips_null_sequence():
    """Snapshots without stop_sequence (rare WMATA quirk) are dropped from output."""
    from pipelines.derive_stop_events_trip_updates import _last_snapshots_per_stop

    snaps = [
        _tu_tuple(
            trip_id="T3",
            stop_id="S1",
            stop_sequence=None,
            vehicle_id=None,
            snapshot_ts=datetime(2026, 5, 3, 14, 0, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
        ),
    ]
    assert _last_snapshots_per_stop(snaps) == {}


@pytest.mark.smoke
def test_eastern_window_utc_brackets_service_day():
    """The window starts before service_date and extends past midnight into the next day."""
    from datetime import date

    from pipelines.derive_stop_events_trip_updates import _eastern_window_utc

    start, end = _eastern_window_utc(date(2026, 5, 3))
    # 2026-05-03 03:00 ET (EDT, UTC-4) = 07:00 UTC; 2026-05-04 04:00 ET = 08:00 UTC
    assert start == datetime(2026, 5, 3, 7, 0, 0)
    assert end == datetime(2026, 5, 4, 8, 0, 0)
    # Window is wider than 24h
    assert (end - start).total_seconds() > 24 * 3600


@pytest.mark.smoke
def test_timezones_helpers_round_trip():
    """eastern_day_bounds_utc converts an Eastern date to a 24h UTC window."""
    from datetime import date, timedelta

    from src.timezones import eastern_day_bounds_utc, eastern_today

    today = eastern_today()
    assert isinstance(today, date)

    start, end = eastern_day_bounds_utc(today)
    # On non-DST-transition days the window is exactly 24h.
    span = end - start
    assert span == timedelta(hours=24)
    # Eastern midnight = UTC 04:00 (EDT) or 05:00 (EST). Both have minute=0.
    assert start.minute == 0
    assert start.tzinfo is None  # Helper returns naive UTC


@pytest.mark.smoke
def test_compute_route_service_profile_classifies_frequent(db_session):
    """compute_route_service_profile flags is_frequent for a 10-min headway weekday route."""
    db_session.add(
        Calendar(
            service_id="WK",
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            friday=1,
            saturday=0,
            sunday=0,
            start_date="20260101",
            end_date="20261231",
            is_current=True,
        )
    )
    # Six trips on route FREQ at 08:00, 08:10, ..., 08:50 — 10-min headway, frequent.
    # Two trips on route SPARSE at 08:00, 08:30 — 30-min headway, not frequent.
    for i, mins in enumerate(range(0, 60, 10)):
        trip_id = f"FREQ-{i}"
        db_session.add(
            Trip(
                trip_id=trip_id,
                route_id="FREQ",
                service_id="WK",
                is_current=True,
            )
        )
        db_session.add(
            StopTime(
                trip_id=trip_id,
                stop_id="S1",
                arrival_time=f"08:{mins:02d}:00",
                departure_time=f"08:{mins:02d}:00",
                stop_sequence=1,
                is_current=True,
            )
        )
    for i, mins in enumerate([0, 30]):
        trip_id = f"SPARSE-{i}"
        db_session.add(
            Trip(
                trip_id=trip_id,
                route_id="SPARSE",
                service_id="WK",
                is_current=True,
            )
        )
        db_session.add(
            StopTime(
                trip_id=trip_id,
                stop_id="S1",
                arrival_time=f"08:{mins:02d}:00",
                departure_time=f"08:{mins:02d}:00",
                stop_sequence=1,
                is_current=True,
            )
        )
    db_session.commit()

    rows = compute_route_service_profile(db_session)
    by_key = {(r["route_id"], r["day_type"], r["hour"]): r for r in rows}

    freq = by_key[("FREQ", "weekday", 8)]
    assert freq["scheduled_trips"] == 6
    assert abs(freq["mean_headway_min"] - 10.0) < 0.01
    assert freq["is_frequent"] is True

    sparse = by_key[("SPARSE", "weekday", 8)]
    assert sparse["scheduled_trips"] == 2
    assert abs(sparse["mean_headway_min"] - 30.0) < 0.01
    assert sparse["is_frequent"] is False


@pytest.mark.smoke
def test_compare_stop_event_sources_classifies_pairings(db_session):
    """Comparison correctly partitions events into both / TU-only / proximity-only."""
    from datetime import date

    from pipelines.compare_stop_event_sources import compare_stop_event_sources

    sd = "2026-05-03"
    base = {"service_date": sd, "route_id": "R1", "direction_id": 0, "stop_id": "S1"}

    # Pair 1: both sources, TU is 30s after prox
    db_session.add_all(
        [
            StopEvent(
                **base,
                trip_id="T1",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 30),
            ),
            StopEvent(
                **base,
                trip_id="T1",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
                match_distance_m=22.0,
            ),
        ]
    )
    # Pair 2: both sources, TU is 10s before prox
    db_session.add_all(
        [
            StopEvent(
                **base,
                trip_id="T2",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 15, 0, 50),
            ),
            StopEvent(
                **base,
                trip_id="T2",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 15, 1, 0),
                match_distance_m=15.0,
            ),
        ]
    )
    # TU only — proximity missed it
    db_session.add(
        StopEvent(
            **base,
            trip_id="T3",
            stop_sequence=1,
            source="trip_update",
            schedule_relationship="SCHEDULED",
            observed_arrival_ts=datetime(2026, 5, 3, 16, 0, 0),
        )
    )
    # Proximity only — TU has no prediction
    db_session.add(
        StopEvent(
            **base,
            trip_id="T4",
            stop_sequence=1,
            source="proximity",
            schedule_relationship="SCHEDULED",
            observed_arrival_ts=datetime(2026, 5, 3, 17, 0, 0),
            match_distance_m=42.0,
        )
    )
    # SKIPPED in TU — must be excluded by the observed_arrival_ts IS NOT NULL filter,
    # NOT counted as "TU only"
    db_session.add(
        StopEvent(
            **base,
            trip_id="T5",
            stop_sequence=1,
            source="trip_update",
            schedule_relationship="SKIPPED",
            observed_arrival_ts=None,
        )
    )
    db_session.commit()

    report = compare_stop_event_sources(db_session, date(2026, 5, 3))

    assert report["both_count"] == 2
    assert report["tu_only_count"] == 1
    assert report["proximity_only_count"] == 1
    assert report["tu_total"] == 3  # 2 paired + 1 TU-only
    assert report["proximity_total"] == 3  # 2 paired + 1 prox-only
    # |Δ| = {30, 10}; median = 20. Signed Δ (TU − prox) = {+30, −10}; median = 10.
    d = report["delta_stats"]
    assert d["n"] == 2
    assert d["abs_median_sec"] == 20.0
    assert d["signed_median_sec"] == 10.0
    # Coverage: 2 BOTH out of 3 prox total → 2/3
    assert abs(report["coverage_of_proximity"] - (2 / 3)) < 1e-9


@pytest.mark.smoke
def test_compare_stop_event_sources_per_route_breakdown(db_session):
    """Per-route output groups counts and Δ stats by route_id, sorted by activity."""
    from datetime import date

    from pipelines.compare_stop_event_sources import compare_stop_event_sources

    sd = "2026-05-03"
    # R_BIG: 2 paired events. R_SMALL: 1 TU-only.
    db_session.add_all(
        [
            StopEvent(
                service_date=sd,
                trip_id="A",
                route_id="R_BIG",
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 5),
            ),
            StopEvent(
                service_date=sd,
                trip_id="A",
                route_id="R_BIG",
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
                match_distance_m=10.0,
            ),
            StopEvent(
                service_date=sd,
                trip_id="A",
                route_id="R_BIG",
                direction_id=0,
                stop_id="S2",
                stop_sequence=2,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
            ),
            StopEvent(
                service_date=sd,
                trip_id="A",
                route_id="R_BIG",
                direction_id=0,
                stop_id="S2",
                stop_sequence=2,
                source="proximity",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
                match_distance_m=18.0,
            ),
            StopEvent(
                service_date=sd,
                trip_id="B",
                route_id="R_SMALL",
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 18, 0, 0),
            ),
        ]
    )
    db_session.commit()

    report = compare_stop_event_sources(db_session, date(2026, 5, 3))
    per_route = {r["route_id"]: r for r in report["per_route"]}
    assert per_route["R_BIG"]["both_count"] == 2
    assert per_route["R_BIG"]["tu_only_count"] == 0
    assert per_route["R_SMALL"]["tu_only_count"] == 1
    assert per_route["R_SMALL"]["both_count"] == 0
    # Sort order: R_BIG (4 events) before R_SMALL (1 event).
    assert report["per_route"][0]["route_id"] == "R_BIG"


@pytest.mark.smoke
def test_compare_stop_event_sources_route_filter(db_session):
    """`route_id` arg restricts the slice and suppresses the per_route field."""
    from datetime import date

    from pipelines.compare_stop_event_sources import compare_stop_event_sources

    sd = "2026-05-03"
    db_session.add_all(
        [
            StopEvent(
                service_date=sd,
                trip_id="X",
                route_id="KEEP",
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
            ),
            StopEvent(
                service_date=sd,
                trip_id="Y",
                route_id="DROP",
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
            ),
        ]
    )
    db_session.commit()

    report = compare_stop_event_sources(db_session, date(2026, 5, 3), route_id="KEEP")
    assert report["tu_total"] == 1
    assert report["proximity_total"] == 0
    assert "per_route" not in report
    assert report["route_id"] == "KEEP"


@pytest.mark.smoke
def test_save_trip_updates_bulk_inserts(db_session):
    """_save_trip_updates persists a list of dicts produced by the collector."""
    collector = WMATADataCollector(api_key="unused", db_session=db_session)
    snapshot_ts = datetime(2026, 5, 3, 15, 0, 0)
    rows = [
        {
            "snapshot_ts": snapshot_ts,
            "trip_id": "TRIP_B",
            "route_id": "R2",
            "vehicle_id": None,
            "stop_id": "S10",
            "stop_sequence": 5,
            "predicted_arrival_ts": datetime(2026, 5, 3, 15, 5, 0),
            "predicted_departure_ts": None,
            "schedule_relationship": "SCHEDULED",
        }
    ]

    saved = collector._save_trip_updates(rows)
    assert saved == 1

    row = db_session.query(TripUpdateSnapshot).filter_by(trip_id="TRIP_B").one()
    assert row.stop_sequence == 5
    assert row.vehicle_id is None
    assert row.snapshot_ts == snapshot_ts


def _se(**overrides) -> StopEvent:
    """Build a StopEvent with sensible defaults for run-aggregation tests."""
    defaults = {
        "service_date": "2026-05-03",
        "trip_id": "TRIP_A",
        "route_id": "R1",
        "direction_id": 0,
        "vehicle_id": "V1",
        "stop_id": "S1",
        "stop_sequence": 1,
        "scheduled_arrival_ts": None,
        "scheduled_departure_ts": None,
        "observed_arrival_ts": None,
        "deviation_sec": None,
        "source": "trip_update",
        "schedule_relationship": "SCHEDULED",
    }
    defaults.update(overrides)
    return StopEvent(**defaults)


@pytest.mark.smoke
def test_aggregate_run_rows_basic_observed_run():
    """One trip's stop_events collapse to one run with first/last seq, ts, gap, deviations."""
    from pipelines.aggregate_runs import aggregate_run_rows

    derived_at = datetime(2026, 5, 3, 20, 0, 0)
    events = [
        _se(
            stop_sequence=1,
            stop_id="S1",
            scheduled_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
            observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 30),
            deviation_sec=30,
        ),
        _se(
            stop_sequence=2,
            stop_id="S2",
            scheduled_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
            observed_arrival_ts=datetime(2026, 5, 3, 14, 6, 0),
            deviation_sec=60,
        ),
        # 4-minute gap (no S3 observation between S2 and S4) — drives max_gap_sec
        _se(
            stop_sequence=4,
            stop_id="S4",
            scheduled_arrival_ts=datetime(2026, 5, 3, 14, 12, 0),
            observed_arrival_ts=datetime(2026, 5, 3, 14, 10, 0),
            deviation_sec=-120,
        ),
    ]
    sched_index = {"TRIP_A": {"count": 5, "first_seq": 1, "last_seq": 5}}
    rows = aggregate_run_rows(
        events, sched_index=sched_index, service_date_str="2026-05-03", derived_at=derived_at
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["trip_id"] == "TRIP_A"
    assert r["source"] == "trip_update"
    assert r["stops_observed"] == 3
    assert r["stops_skipped"] == 0
    assert r["stops_scheduled"] == 5
    assert r["sched_first_seq"] == 1
    assert r["sched_last_seq"] == 5
    assert r["first_obs_seq"] == 1
    assert r["last_obs_seq"] == 4
    assert r["first_obs_ts"] == datetime(2026, 5, 3, 14, 0, 30)
    assert r["last_obs_ts"] == datetime(2026, 5, 3, 14, 10, 0)
    # Gaps in observed-arrival order: 14:00:30→14:06:00 = 330s; 14:06:00→14:10:00 = 240s.
    assert r["max_gap_sec"] == 330
    # Deviations: -120, 30, 60. p50 = 30, p95 = ~58 (numpy linear interp).
    assert r["dev_p50_sec"] == 30
    assert r["dev_p95_sec"] in (57, 58)  # numpy interp tolerance
    # Schedule bounds lifted from the observed rows
    assert r["sched_first_arrival_ts"] == datetime(2026, 5, 3, 14, 0, 0)
    assert r["sched_last_arrival_ts"] == datetime(2026, 5, 3, 14, 12, 0)
    # Origin observed (stop_sequence == sched_first_seq == 1), but destination
    # (sched_last_seq == 5) was not observed in this group.
    assert r["origin_dev_sec"] == 30
    assert r["destination_dev_sec"] is None


@pytest.mark.smoke
def test_aggregate_run_rows_splits_by_source():
    """TU and proximity stop_events for the same trip produce two run rows."""
    from pipelines.aggregate_runs import aggregate_run_rows

    events = [
        _se(
            stop_sequence=1,
            source="trip_update",
            observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
        ),
        _se(
            stop_sequence=1, source="proximity", observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 5)
        ),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    sources = sorted(r["source"] for r in rows)
    assert sources == ["proximity", "trip_update"]
    assert all(r["stops_observed"] == 1 for r in rows)


@pytest.mark.smoke
def test_aggregate_run_rows_skipped_separate_from_observed():
    """SKIPPED events count against stops_skipped, not stops_observed."""
    from pipelines.aggregate_runs import aggregate_run_rows

    events = [
        _se(stop_sequence=1, observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0)),
        _se(stop_sequence=2, observed_arrival_ts=None, schedule_relationship="SKIPPED"),
        _se(stop_sequence=3, observed_arrival_ts=datetime(2026, 5, 3, 14, 5, 0)),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={"TRIP_A": {"count": 3, "first_seq": 1, "last_seq": 3}},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["stops_observed"] == 2
    assert r["stops_skipped"] == 1
    assert r["first_obs_seq"] == 1
    assert r["last_obs_seq"] == 3


@pytest.mark.smoke
def test_aggregate_run_rows_post_midnight_run_keeps_service_date():
    """A run that crosses midnight reports service_date from stop_events, not calendar day."""
    from pipelines.aggregate_runs import aggregate_run_rows

    # Trip starts 23:50 service_date 2026-05-03 (Eastern), runs past midnight.
    # All stop_events carry service_date='2026-05-03'; last observation lands 2026-05-04 03:55 UTC.
    events = [
        _se(
            stop_sequence=1,
            scheduled_arrival_ts=datetime(2026, 5, 4, 3, 50, 0),
            observed_arrival_ts=datetime(2026, 5, 4, 3, 51, 0),
            deviation_sec=60,
        ),
        _se(
            stop_sequence=10,
            scheduled_arrival_ts=datetime(2026, 5, 4, 4, 30, 0),
            observed_arrival_ts=datetime(2026, 5, 4, 4, 31, 0),
            deviation_sec=60,
        ),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={"TRIP_A": {"count": 10, "first_seq": 1, "last_seq": 10}},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 4, 5, 0, 0),
    )
    assert rows[0]["service_date"] == "2026-05-03"
    assert rows[0]["sched_first_arrival_ts"] == datetime(2026, 5, 4, 3, 50, 0)
    assert rows[0]["sched_last_arrival_ts"] == datetime(2026, 5, 4, 4, 30, 0)


@pytest.mark.smoke
def test_aggregate_run_rows_latest_non_null_vehicle_wins():
    """vehicle_id resolves to the latest non-null value across the group's events."""
    from pipelines.aggregate_runs import aggregate_run_rows

    events = [
        _se(stop_sequence=1, vehicle_id=None),
        _se(stop_sequence=2, vehicle_id="V_FIRST"),
        _se(stop_sequence=3, vehicle_id=None),
        _se(stop_sequence=4, vehicle_id="V_LAST"),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    assert rows[0]["vehicle_id"] == "V_LAST"


@pytest.mark.smoke
def test_aggregate_run_rows_no_observed_yields_zero_stats():
    """A trip where every stop is SKIPPED still emits a row, with null obs/dev fields."""
    from pipelines.aggregate_runs import aggregate_run_rows

    events = [
        _se(
            stop_sequence=1,
            observed_arrival_ts=None,
            schedule_relationship="SKIPPED",
            scheduled_arrival_ts=datetime(2026, 5, 3, 14, 0, 0),
        ),
        _se(
            stop_sequence=2,
            observed_arrival_ts=None,
            schedule_relationship="SKIPPED",
            scheduled_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
        ),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={"TRIP_A": {"count": 2, "first_seq": 1, "last_seq": 2}},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["stops_observed"] == 0
    assert r["stops_skipped"] == 2
    assert r["first_obs_seq"] is None
    assert r["last_obs_seq"] is None
    assert r["max_gap_sec"] is None
    assert r["dev_p50_sec"] is None


@pytest.mark.smoke
def test_run_unique_constraint_rejects_duplicate(db_session):
    """The (service_date, trip_id, source) unique constraint holds."""
    from sqlalchemy.exc import IntegrityError

    base = {
        "service_date": "2026-05-03",
        "trip_id": "TRIP_DUP",
        "route_id": "R1",
        "direction_id": 0,
        "source": "proximity",
        "stops_observed": 5,
    }
    db_session.add(Run(**base))
    db_session.commit()

    db_session.add(Run(**base))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


@pytest.mark.smoke
def test_run_persists_with_both_sources(db_session):
    """The same trip can have one run per source — proximity and trip_update coexist."""
    common = {
        "service_date": "2026-05-03",
        "trip_id": "TRIP_BOTH",
        "route_id": "R1",
        "direction_id": 0,
        "stops_observed": 10,
    }
    db_session.add_all([Run(**common, source="trip_update"), Run(**common, source="proximity")])
    db_session.commit()

    rows = db_session.query(Run).filter_by(trip_id="TRIP_BOTH").order_by(Run.source).all()
    assert [r.source for r in rows] == ["proximity", "trip_update"]


@pytest.mark.smoke
def test_aggregate_run_rows_endpoint_dev_uses_literal_sched_seq():
    """origin/destination_dev_sec read from stop_events at sched_first_seq / sched_last_seq.

    Critically, NOT from min/max(observed stop_sequence) — WMATA's GTFS uses
    non-contiguous sequences (~99.9% of trips start at seq 2 with arbitrary
    gaps), so first observed != literal scheduled origin in general.
    """
    from pipelines.aggregate_runs import aggregate_run_rows

    # Realistic WMATA-shaped trip: scheduled stops at seqs [2, 4, 5, 8].
    # The bus only got observed at seqs 4, 5, 8 — origin (seq 2) was missed.
    events = [
        _se(stop_sequence=4, observed_arrival_ts=datetime(2026, 5, 3, 14, 5, 0), deviation_sec=20),
        _se(stop_sequence=5, observed_arrival_ts=datetime(2026, 5, 3, 14, 7, 0), deviation_sec=40),
        _se(stop_sequence=8, observed_arrival_ts=datetime(2026, 5, 3, 14, 12, 0), deviation_sec=80),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={"TRIP_A": {"count": 4, "first_seq": 2, "last_seq": 8}},
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    r = rows[0]
    # Origin is seq 2 — never observed — even though first observed seq is 4.
    assert r["sched_first_seq"] == 2
    assert r["sched_last_seq"] == 8
    assert r["first_obs_seq"] == 4  # bus first seen mid-route
    assert r["origin_dev_sec"] is None  # no event at seq 2
    assert r["destination_dev_sec"] == 80  # event at seq 8 → its deviation


@pytest.mark.smoke
def test_aggregate_run_rows_endpoint_dev_null_when_no_sched_index():
    """When the trip is missing from sched_index, endpoint devs default to null."""
    from pipelines.aggregate_runs import aggregate_run_rows

    events = [
        _se(stop_sequence=2, observed_arrival_ts=datetime(2026, 5, 3, 14, 0, 0), deviation_sec=10),
    ]
    rows = aggregate_run_rows(
        events,
        sched_index={},  # no entry for TRIP_A
        service_date_str="2026-05-03",
        derived_at=datetime(2026, 5, 3, 20, 0, 0),
    )
    r = rows[0]
    assert r["sched_first_seq"] is None
    assert r["sched_last_seq"] is None
    assert r["origin_dev_sec"] is None
    assert r["destination_dev_sec"] is None


@pytest.mark.smoke
def test_compute_otp_split_picks_proximity_for_origin_tu_for_destination(db_session):
    """The split aggregator pulls origin from proximity runs, destination from TU runs."""
    from datetime import date

    from src.otp_metrics import compute_otp_split

    sd = "2026-05-03"
    common = {
        "service_date": sd,
        "route_id": "R1",
        "direction_id": 0,
        "stops_observed": 10,
    }
    # Proximity runs supply origin; their destination_dev_sec is null (matches reality).
    db_session.add_all(
        [
            Run(
                **common,
                trip_id="T1",
                source="proximity",
                origin_dev_sec=-30,
                destination_dev_sec=None,
            ),  # on-time
            Run(
                **common,
                trip_id="T2",
                source="proximity",
                origin_dev_sec=600,
                destination_dev_sec=None,
            ),  # late (>420s)
            Run(
                **common,
                trip_id="T3",
                source="proximity",
                origin_dev_sec=None,
                destination_dev_sec=None,
            ),  # origin not observed — excluded
        ]
    )
    # TU runs supply destination; their origin_dev_sec is null (matches reality).
    db_session.add_all(
        [
            Run(
                **common,
                trip_id="T1",
                source="trip_update",
                origin_dev_sec=None,
                destination_dev_sec=-180,
            ),  # early (< -120s)
            Run(
                **common,
                trip_id="T2",
                source="trip_update",
                origin_dev_sec=None,
                destination_dev_sec=60,
            ),  # on-time
        ]
    )
    db_session.commit()

    out = compute_otp_split(db_session, "R1", date(2026, 5, 3))
    # Origin from proximity: T1 on-time, T2 late, T3 excluded (null) → n=2, 50% on-time, 50% late
    o = out["origin"]
    assert o["source"] == "proximity"
    assert o["n"] == 2
    assert o["on_time"] == 1
    assert o["late"] == 1
    assert o["early"] == 0
    assert o["on_time_pct"] == 50.0
    # Destination from TU: T1 early, T2 on-time → n=2, 50% early, 50% on-time
    d = out["destination"]
    assert d["source"] == "trip_update"
    assert d["n"] == 2
    assert d["early"] == 1
    assert d["on_time"] == 1
    assert d["late"] == 0
    # All-timepoints reads from stop_events; we didn't add any → n=0
    assert out["all_timepoints"]["n"] == 0


@pytest.mark.smoke
def test_compute_otp_split_returns_n_zero_when_no_data(db_session):
    """No matching runs/stop_events → all three sub-blocks return {'n': 0} with no rates."""
    from datetime import date

    from src.otp_metrics import compute_otp_split

    out = compute_otp_split(db_session, "GHOST", date(2026, 5, 3))
    assert out["origin"]["n"] == 0
    assert "on_time_pct" not in out["origin"]
    assert out["destination"]["n"] == 0
    assert out["all_timepoints"]["n"] == 0


@pytest.mark.smoke
def test_compute_otp_split_all_timepoints_uses_proximity_stop_events(db_session):
    """All-timepoints OTP reads from proximity stop_events — TU stop_events are ignored."""
    from datetime import date

    from src.otp_metrics import compute_otp_split

    base = {
        "service_date": "2026-05-03",
        "route_id": "R2",
        "direction_id": 0,
        "trip_id": "T1",
        "stop_id": "S1",
        "schedule_relationship": "SCHEDULED",
        "observed_arrival_ts": datetime(2026, 5, 3, 14, 0, 0),
    }
    # Three proximity events: one early, one on-time, one late
    db_session.add_all(
        [
            StopEvent(**base, stop_sequence=2, source="proximity", deviation_sec=-300),
            StopEvent(**base, stop_sequence=3, source="proximity", deviation_sec=0),
            StopEvent(**base, stop_sequence=4, source="proximity", deviation_sec=500),
            # TU events should be excluded from all-timepoints
            StopEvent(**base, stop_sequence=2, source="trip_update", deviation_sec=999),
        ]
    )
    db_session.commit()

    out = compute_otp_split(db_session, "R2", date(2026, 5, 3))
    a = out["all_timepoints"]
    assert a["source"] == "proximity"
    assert a["n"] == 3  # TU event excluded
    assert a["early"] == 1
    assert a["on_time"] == 1
    assert a["late"] == 1


# --- service-delivered ratio (PR #47) ---


def _profile(route_id: str, day_type: str, hour: int, scheduled_trips: int) -> RouteServiceProfile:
    """Test factory for RouteServiceProfile rows; only the fields the metric reads."""
    return RouteServiceProfile(
        route_id=route_id,
        day_type=day_type,
        hour=hour,
        scheduled_trips=scheduled_trips,
        is_frequent=False,
    )


def _run(trip_id: str, source: str, stops_observed: int, **overrides) -> Run:
    """Test factory for Run rows; defaults to Sunday 2026-05-03 / route R1 / dir 0."""
    base = {
        "service_date": "2026-05-03",
        "route_id": "R1",
        "direction_id": 0,
        "trip_id": trip_id,
        "source": source,
        "stops_observed": stops_observed,
    }
    base.update(overrides)
    return Run(**base)


def _gtfs_trip(trip_id: str, route_id: str = "R1", service_id: str = "SUN") -> Trip:
    """Test factory for GTFS Trip rows; minimal fields the service-delivered query reads."""
    return Trip(
        trip_id=trip_id,
        route_id=route_id,
        direction_id=0,
        service_id=service_id,
        is_current=True,
    )


def _gtfs_calendar(service_id: str = "SUN", **day_flags) -> Calendar:
    """Test factory for Calendar rows. Pass day flags as kwargs (e.g. sunday=1)."""
    base = {
        "monday": 0,
        "tuesday": 0,
        "wednesday": 0,
        "thursday": 0,
        "friday": 0,
        "saturday": 0,
        "sunday": 0,
        "start_date": "20260101",
        "end_date": "20261231",
    }
    base.update(day_flags)
    return Calendar(service_id=service_id, is_current=True, **base)


@pytest.mark.smoke
def test_compute_service_delivered_basic_ratio(db_session):
    """Sunday with 5 scheduled, 4 distinct trips delivered → ratio 0.80."""
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add_all(
        [
            _gtfs_calendar(service_id="SUN", sunday=1),
            _gtfs_trip("T1"),
            _gtfs_trip("T2"),
            _gtfs_trip("T3"),
            _gtfs_trip("T4"),
            _gtfs_trip("T5"),  # 5 scheduled trips for R1 on Sunday
            _run("T1", "proximity", 10),
            _run("T2", "proximity", 5),
            _run("T3", "trip_update", 7),
            _run("T4", "proximity", 3),  # boundary: == 3 counts
        ]
    )
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 3))
    assert out["day_type"] == "sunday"
    assert out["scheduled_trips"] == 5
    assert out["delivered_trips"] == 4
    assert out["ratio"] == 0.80


@pytest.mark.smoke
def test_compute_service_delivered_dedups_per_source(db_session):
    """Same trip_id appearing in both proximity and trip_update counts once."""
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add_all(
        [
            _gtfs_calendar(service_id="SUN", sunday=1),
            _gtfs_trip("T1"),
            _gtfs_trip("T2"),
            _gtfs_trip("T3"),
            _gtfs_trip("T4"),
            _run("T1", "proximity", 10),
            _run("T1", "trip_update", 12),  # same trip — dedupe
            _run("T2", "trip_update", 8),
        ]
    )
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 3))
    assert out["delivered_trips"] == 2  # T1, T2
    assert out["scheduled_trips"] == 4
    assert out["ratio"] == 0.5


@pytest.mark.smoke
def test_compute_service_delivered_filters_below_three_stops(db_session):
    """Runs with stops_observed < 3 don't count as delivered (RUN_EXISTED filter)."""
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add_all(
        [
            _gtfs_calendar(service_id="SUN", sunday=1),
            _gtfs_trip("T1"),
            _gtfs_trip("T2"),
            _gtfs_trip("T3"),
            _run("T1", "proximity", 2),  # below threshold — excluded
            _run("T2", "proximity", 3),  # exactly threshold — included
            _run("T3", "trip_update", 0),  # zero — excluded
        ]
    )
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 3))
    assert out["delivered_trips"] == 1
    assert out["ratio"] == round(1 / 3, 4)


@pytest.mark.smoke
def test_compute_service_delivered_no_schedule_returns_none_ratio(db_session):
    """Route with run but no GTFS schedule → ratio None and delivered=0.

    The numerator is "scheduled trips that ran"; an unscheduled run can't
    contribute. Distinguishes "ran nothing scheduled" from "wasn't supposed
    to run anything."
    """
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add(_run("T1", "proximity", 10))
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 3))
    assert out["scheduled_trips"] == 0
    assert out["delivered_trips"] == 0
    assert out["ratio"] is None


@pytest.mark.smoke
def test_compute_service_delivered_zero_delivered_returns_zero_ratio(db_session):
    """Scheduled but nothing delivered → ratio 0.0 (distinct from None)."""
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add_all(
        [
            _gtfs_calendar(service_id="SUN", sunday=1),
            *[_gtfs_trip(f"T{i}") for i in range(5)],
        ]
    )
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 3))
    assert out["scheduled_trips"] == 5
    assert out["delivered_trips"] == 0
    assert out["ratio"] == 0.0


@pytest.mark.smoke
def test_compute_service_delivered_uses_correct_day_type(db_session):
    """Weekday date pulls Tuesday-Calendar trips, ignoring saturday/sunday."""
    from datetime import date

    from src.service_delivered import compute_service_delivered

    db_session.add_all(
        [
            _gtfs_calendar(service_id="WKD", tuesday=1),
            *[_gtfs_trip(f"W{i}", service_id="WKD") for i in range(10)],
            _gtfs_calendar(service_id="SAT", saturday=1),
            *[_gtfs_trip(f"S{i}", service_id="SAT") for i in range(5)],
            _gtfs_calendar(service_id="SUN", sunday=1),
            *[_gtfs_trip(f"U{i}", service_id="SUN") for i in range(3)],
            # 2026-05-04 is a Monday — should match WKD (Tuesday-flagged) only
            _run("W0", "proximity", 10, service_date="2026-05-04"),
        ]
    )
    db_session.commit()

    out = compute_service_delivered(db_session, "R1", date(2026, 5, 4))
    assert out["day_type"] == "weekday"
    assert out["scheduled_trips"] == 10
    assert out["delivered_trips"] == 1


@pytest.mark.smoke
def test_compute_service_delivered_for_routes_unions_gtfs_and_runs(db_session):
    """Fan-out includes a route from GTFS only AND a route from runs only."""
    from datetime import date

    from src.service_delivered import compute_service_delivered_for_routes

    db_session.add_all(
        [
            _gtfs_calendar(service_id="SUN", sunday=1),
            _gtfs_trip("T_PROF_1", route_id="R_PROF_ONLY"),
            _gtfs_trip("T_PROF_2", route_id="R_PROF_ONLY"),
            _gtfs_trip("T_PROF_3", route_id="R_PROF_ONLY"),
            _gtfs_trip("T_PROF_4", route_id="R_PROF_ONLY"),
            _run("T1", "proximity", 10, route_id="R_RUNS_ONLY"),  # delivered, unscheduled
        ]
    )
    db_session.commit()

    out = compute_service_delivered_for_routes(db_session, date(2026, 5, 3))
    by_route = {r["route_id"]: r for r in out}
    assert "R_PROF_ONLY" in by_route and "R_RUNS_ONLY" in by_route
    assert by_route["R_PROF_ONLY"]["scheduled_trips"] == 4
    assert by_route["R_PROF_ONLY"]["delivered_trips"] == 0
    assert by_route["R_PROF_ONLY"]["ratio"] == 0.0
    assert by_route["R_RUNS_ONLY"]["scheduled_trips"] == 0
    assert by_route["R_RUNS_ONLY"]["ratio"] is None


# --- GTFS reload regression (PR #48) ---


def _minimal_gtfs_fixture() -> dict[str, list[dict]]:
    """
    A minimum-viable GTFS dataset that exercises every code path in
    `apply_gtfs_to_db`: one row per table, FKs consistent.
    """
    return {
        "agency": [
            {
                "agency_id": "1",
                "agency_name": "WMATA",
                "agency_url": "https://wmata.com",
                "agency_timezone": "America/New_York",
                "agency_lang": "en",
                "agency_phone": "202-637-7000",
                "agency_fare_url": "",
                "agency_email": "",
            }
        ],
        "feed_info": [
            {
                "feed_publisher_name": "WMATA",
                "feed_publisher_url": "https://wmata.com",
                "feed_lang": "en",
                "feed_start_date": "20260101",
                "feed_end_date": "20261231",
                "feed_version": "v1",
                "feed_contact_email": "",
                "feed_contact_url": "",
            }
        ],
        "routes": [
            {
                "route_id": "R1",
                "agency_id": "1",
                "route_short_name": "R1",
                "route_long_name": "Route 1",
                "route_desc": "",
                "route_type": "3",
                "route_url": "",
                "route_color": "",
                "route_text_color": "",
            }
        ],
        "stops": [
            {
                "stop_id": "S1",
                "stop_code": "",
                "stop_name": "Stop 1",
                "stop_desc": "",
                "stop_lat": "38.9072",
                "stop_lon": "-77.0369",
                "zone_id": "",
                "stop_url": "",
            },
            {
                "stop_id": "S2",
                "stop_code": "",
                "stop_name": "Stop 2",
                "stop_desc": "",
                "stop_lat": "38.9100",
                "stop_lon": "-77.0400",
                "zone_id": "",
                "stop_url": "",
            },
        ],
        "trips": [
            {
                "trip_id": "T1",
                "route_id": "R1",
                "service_id": "WK",
                "trip_headsign": "Downtown",
                "direction_id": "0",
                "block_id": "",
                "shape_id": "",
            }
        ],
        "stop_times": [
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_sequence": "1",
                "stop_headsign": "",
                "pickup_type": "",
                "drop_off_type": "",
                "shape_dist_traveled": "",
                "timepoint": "1",
            },
            {
                "trip_id": "T1",
                "stop_id": "S2",
                "arrival_time": "08:10:00",
                "departure_time": "08:10:00",
                "stop_sequence": "2",
                "stop_headsign": "",
                "pickup_type": "",
                "drop_off_type": "",
                "shape_dist_traveled": "",
                "timepoint": "1",
            },
        ],
        "shapes": [],
        "calendar": [
            {
                "service_id": "WK",
                "monday": "1",
                "tuesday": "1",
                "wednesday": "1",
                "thursday": "1",
                "friday": "1",
                "saturday": "0",
                "sunday": "0",
                "start_date": "20260101",
                "end_date": "20261231",
            }
        ],
        "calendar_dates": [
            {
                "service_id": "WK",
                "date": "20260704",
                "exception_type": "2",
            }
        ],
        "timepoints": [
            {
                "stop_id": "S1",
                "stop_code": "",
                "stop_name": "Stop 1",
                "stop_desc": "",
                "stop_lat": "38.9072",
                "stop_lon": "-77.0369",
                "zone_id": "",
                "stop_url": "",
            }
        ],
        "timepoint_times": [
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_sequence": "1",
                "stop_headsign": "",
                "pickup_type": "",
                "drop_off_type": "",
                "shape_dist_traveled": "",
                "timepoint": "1",
            }
        ],
    }


@pytest.mark.smoke
def test_apply_gtfs_to_db_succeeds_on_empty_db(db_session):
    """First reload against a fresh DB inserts one snapshot and current rows."""
    from scripts.reload_gtfs_complete import apply_gtfs_to_db
    from src.models import Agency, GTFSSnapshot

    db_session.execute(text("PRAGMA foreign_keys = ON"))
    snapshot_id = apply_gtfs_to_db(db_session, _minimal_gtfs_fixture())

    assert snapshot_id is not None
    assert db_session.query(GTFSSnapshot).count() == 1

    current_routes = db_session.query(Route).filter(Route.is_current).all()
    assert len(current_routes) == 1
    assert current_routes[0].route_id == "R1"
    assert current_routes[0].snapshot_id == snapshot_id

    agency = db_session.query(Agency).filter_by(agency_id="1").one()
    assert agency.agency_name == "WMATA"


@pytest.mark.smoke
def test_apply_gtfs_to_db_succeeds_on_populated_db_under_fk_enforcement(db_session):
    """
    Second reload against a populated DB succeeds with FKs enforced.

    This is the regression test for the FK violation in the previous
    reload script (fixed in PR #48): it did `DELETE FROM agencies`
    while routes still referenced agency_id, which FK-violates.
    Verifies the fix (upsert agencies, never delete) holds.
    """
    from scripts.reload_gtfs_complete import apply_gtfs_to_db
    from src.models import Agency, GTFSSnapshot

    db_session.execute(text("PRAGMA foreign_keys = ON"))

    # First pass populates the DB.
    first_id = apply_gtfs_to_db(db_session, _minimal_gtfs_fixture())
    db_session.flush()

    # Second pass — same agency_id, slightly different agency name to
    # confirm upsert refreshes fields.
    fixture = _minimal_gtfs_fixture()
    fixture["agency"][0]["agency_name"] = "WMATA (refreshed)"
    second_id = apply_gtfs_to_db(db_session, fixture)

    assert second_id != first_id
    assert db_session.query(GTFSSnapshot).count() == 2

    # Old versioned rows are no longer current; new ones are.
    old_routes = (
        db_session.query(Route).filter(Route.snapshot_id == first_id, ~Route.is_current).all()
    )
    new_routes = db_session.query(Route).filter(Route.is_current).all()
    assert len(old_routes) == 1
    assert len(new_routes) == 1
    assert new_routes[0].snapshot_id == second_id

    # Agency row was UPDATEd, not deleted/reinserted — same row, refreshed name.
    agencies = db_session.query(Agency).filter_by(agency_id="1").all()
    assert len(agencies) == 1
    assert agencies[0].agency_name == "WMATA (refreshed)"


@pytest.mark.smoke
def test_apply_gtfs_to_db_rolls_back_on_failure(db_session):
    """
    A failure mid-load leaves the DB unchanged when the caller rolls back.

    Drives the new transactional contract: `apply_gtfs_to_db` does not
    commit; one bad row → caller's `rollback()` discards the partial work.
    Uses a savepoint so this assertion is independent of the conftest's
    outer test-fixture transaction.
    """
    from scripts.reload_gtfs_complete import apply_gtfs_to_db
    from src.models import GTFSSnapshot

    db_session.execute(text("PRAGMA foreign_keys = ON"))

    # Populate first so we have a non-empty baseline to compare against.
    apply_gtfs_to_db(db_session, _minimal_gtfs_fixture())
    db_session.flush()

    baseline_snapshots = db_session.query(GTFSSnapshot).count()
    baseline_current_routes = db_session.query(Route).filter(Route.is_current).count()
    assert baseline_snapshots == 1
    assert baseline_current_routes == 1

    # A malformed fixture: stop_times with a non-integer stop_sequence.
    # The stop_times loop raises ValueError on int() conversion after
    # already inserting the new snapshot, agency upsert, and is_current
    # flips for the prior snapshot — exactly the partial-migration
    # scenario the previous reload script's partial-migration bug was filed against.
    bad_fixture = _minimal_gtfs_fixture()
    bad_fixture["stop_times"][0]["stop_sequence"] = "not-a-number"

    sp = db_session.begin_nested()
    with pytest.raises(ValueError):
        apply_gtfs_to_db(db_session, bad_fixture)
    sp.rollback()

    # Baseline is intact: no extra snapshot, prior current-route still current.
    assert db_session.query(GTFSSnapshot).count() == baseline_snapshots
    assert db_session.query(Route).filter(Route.is_current).count() == baseline_current_routes


def _seed_run_with_stop_events(db_session, *, with_partial_events=True):
    """Seed a route, trip, 4 stops, scheduled stop_times, a Run, and 3/4 stop_events.

    Returns (run_id, route_id) for the seeded run. Used by the per-run and
    recent-runs smoke tests; the fourth stop is intentionally left without a
    stop_event so the chart-rendering "gap" path is covered.
    """
    from src.models import Route, Run, Stop, StopEvent, StopTime, Trip

    route = Route(
        route_id="DEV1",
        route_short_name="DEV1",
        route_long_name="Deviation Test Route",
        route_type=3,
        is_current=True,
    )
    db_session.add(route)

    trip = Trip(
        trip_id="DEV_TRIP_1",
        route_id="DEV1",
        service_id="WEEKDAY",
        direction_id=0,
        trip_headsign="Test Headsign",
        is_current=True,
    )
    db_session.add(trip)

    stops = [
        Stop(stop_id=f"DEV_STOP_{i}", stop_name=f"Stop {i}", stop_lat=38.9, stop_lon=-77.0, is_current=True)
        for i in range(1, 5)
    ]
    db_session.add_all(stops)

    stop_times = [
        StopTime(
            trip_id="DEV_TRIP_1",
            stop_id=f"DEV_STOP_{i}",
            arrival_time=f"14:{i:02d}:00",
            departure_time=f"14:{i:02d}:30",
            stop_sequence=i,
            is_current=True,
        )
        for i in range(1, 5)
    ]
    db_session.add_all(stop_times)

    db_session.commit()

    sd = "2025-10-20"
    if with_partial_events:
        # Three of four stops observed; one (sequence=3) intentionally missing.
        observations = [
            (1, datetime(2025, 10, 20, 18, 1, 30), 90),  # 1:30 late
            (2, datetime(2025, 10, 20, 18, 2, 0), 60),  # 1:00 late
            (4, datetime(2025, 10, 20, 18, 4, 45), 165),  # 2:45 late
        ]
        for seq, observed, dev in observations:
            db_session.add(
                StopEvent(
                    service_date=sd,
                    trip_id="DEV_TRIP_1",
                    route_id="DEV1",
                    direction_id=0,
                    stop_id=f"DEV_STOP_{seq}",
                    stop_sequence=seq,
                    source="trip_update",
                    schedule_relationship="SCHEDULED",
                    scheduled_arrival_ts=datetime(2025, 10, 20, 18, seq, 0),
                    observed_arrival_ts=observed,
                    deviation_sec=dev,
                )
            )

    run = Run(
        service_date=sd,
        trip_id="DEV_TRIP_1",
        route_id="DEV1",
        direction_id=0,
        source="trip_update",
        vehicle_id="V_DEV_1",
        stops_scheduled=4,
        sched_first_seq=1,
        sched_last_seq=4,
        stops_observed=3 if with_partial_events else 0,
        stops_skipped=0,
        first_obs_seq=1 if with_partial_events else None,
        last_obs_seq=4 if with_partial_events else None,
        first_obs_ts=datetime(2025, 10, 20, 18, 1, 30) if with_partial_events else None,
        last_obs_ts=datetime(2025, 10, 20, 18, 4, 45) if with_partial_events else None,
        dev_p50_sec=90,
        dev_p95_sec=165,
        origin_dev_sec=None,
        destination_dev_sec=165,
    )
    db_session.add(run)
    db_session.commit()
    return run.id, "DEV1"


@pytest.mark.smoke
def test_run_deviations_endpoint_shape_and_ordering(client, db_session):
    """GET /api/runs/{run_id}/deviations returns one row per scheduled stop, ordered."""
    run_id, _ = _seed_run_with_stop_events(db_session)

    response = client.get(f"/api/runs/{run_id}/deviations")
    assert response.status_code == 200

    body = response.json()
    assert body["run_id"] == run_id
    assert body["route_id"] == "DEV1"
    assert body["trip_id"] == "DEV_TRIP_1"
    assert body["trip_headsign"] == "Test Headsign"
    assert body["stops_scheduled"] == 4
    assert body["stops_observed"] == 3

    deviations = body["deviations"]
    assert len(deviations) == 4
    assert [d["stop_sequence"] for d in deviations] == [1, 2, 3, 4]
    assert [d["stop_name"] for d in deviations] == [f"Stop {i}" for i in range(1, 5)]

    # Stops 1, 2, 4 have stop_events.
    assert deviations[0]["deviation_sec"] == 90
    assert deviations[0]["actual"] is not None
    assert deviations[0]["scheduled"] is not None
    # Stop 3 has no stop_event — actual/deviation must be null so the chart
    # renders a gap rather than collapsing the line.
    assert deviations[2]["deviation_sec"] is None
    assert deviations[2]["actual"] is None
    assert deviations[2]["scheduled"] is None

    # Last stop carries the worst deviation.
    assert deviations[3]["deviation_sec"] == 165


@pytest.mark.smoke
def test_run_deviations_endpoint_404_for_unknown_run(client):
    """Unknown run_id returns 404, not a 500."""
    response = client.get("/api/runs/999999/deviations")
    assert response.status_code == 404


@pytest.mark.smoke
def test_route_recent_runs_endpoint_returns_most_recent(client, db_session):
    """GET /api/routes/{route_id}/recent-runs returns the seeded run on the fallback date."""
    run_id, route_id = _seed_run_with_stop_events(db_session)

    response = client.get(f"/api/routes/{route_id}/recent-runs")
    assert response.status_code == 200

    body = response.json()
    assert body["route_id"] == route_id
    # No runs for "today" (Eastern) in this fixture, so the fallback fires.
    assert body["service_date"] == "2025-10-20"

    runs = body["runs"]
    assert len(runs) == 1
    row = runs[0]
    assert row["run_id"] == run_id
    assert row["trip_id"] == "DEV_TRIP_1"
    assert row["headsign"] == "Test Headsign"
    assert row["stops_scheduled"] == 4
    assert row["stops_observed"] == 3
    # Eastern-formatted HH:MM (storage is naive UTC; conversion is applied).
    assert row["start_time"] is not None
    assert row["end_time"] is not None


@pytest.mark.smoke
def test_route_recent_runs_endpoint_404_for_unknown_route(client):
    """Unknown route returns 404."""
    response = client.get("/api/routes/NOPE/recent-runs")
    assert response.status_code == 404
