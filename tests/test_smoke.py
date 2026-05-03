"""
Smoke tests for WMATA Dashboard

Quick tests that verify critical paths are working.
These should run fast (<10s) and fail fast if something is fundamentally broken.

Run with: pytest -m smoke
"""

from datetime import datetime

import pytest
from sqlalchemy import text

from src.models import Calendar, Route, StopEvent, StopTime, Trip, TripUpdateSnapshot
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


@pytest.mark.smoke
def test_last_snapshots_per_stop_picks_final_state_and_last_prediction():
    """Final state = absolute-last snapshot; observed = last snapshot with non-null pred_arr."""
    from pipelines.derive_stop_events_trip_updates import _last_snapshots_per_stop
    from src.models import TripUpdateSnapshot

    # One (trip, stop_seq): three snapshots. Last has null pred_arr (bus passed,
    # WMATA cleared the prediction). Middle has the most recent non-null pred_arr.
    snaps = [
        TripUpdateSnapshot(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            snapshot_ts=datetime(2026, 5, 3, 14, 30, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 35, 0),
            schedule_relationship="SCHEDULED",
            vehicle_id="V1",
        ),
        TripUpdateSnapshot(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            snapshot_ts=datetime(2026, 5, 3, 14, 33, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 34, 30),
            schedule_relationship="SCHEDULED",
            vehicle_id="V1",
        ),
        TripUpdateSnapshot(
            trip_id="T1",
            stop_id="S5",
            stop_sequence=5,
            snapshot_ts=datetime(2026, 5, 3, 14, 35, 30),
            predicted_arrival_ts=None,
            schedule_relationship="SCHEDULED",
            vehicle_id="V1",
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
    from src.models import TripUpdateSnapshot

    snaps = [
        TripUpdateSnapshot(
            trip_id="T2",
            stop_id="S3",
            stop_sequence=3,
            snapshot_ts=datetime(2026, 5, 3, 14, 0, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
            schedule_relationship="SCHEDULED",
            vehicle_id=None,
        ),
        TripUpdateSnapshot(
            trip_id="T2",
            stop_id="S3",
            stop_sequence=3,
            snapshot_ts=datetime(2026, 5, 3, 14, 4, 30),
            predicted_arrival_ts=None,
            schedule_relationship="SKIPPED",
            vehicle_id=None,
        ),
    ]
    out = _last_snapshots_per_stop(snaps)
    assert out[("T2", 3)]["final_schedule_relationship"] == "SKIPPED"


@pytest.mark.smoke
def test_last_snapshots_per_stop_skips_null_sequence():
    """Snapshots without stop_sequence (rare WMATA quirk) are dropped from output."""
    from pipelines.derive_stop_events_trip_updates import _last_snapshots_per_stop
    from src.models import TripUpdateSnapshot

    snaps = [
        TripUpdateSnapshot(
            trip_id="T3",
            stop_id="S1",
            stop_sequence=None,
            snapshot_ts=datetime(2026, 5, 3, 14, 0, 0),
            predicted_arrival_ts=datetime(2026, 5, 3, 14, 5, 0),
            schedule_relationship="SCHEDULED",
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
