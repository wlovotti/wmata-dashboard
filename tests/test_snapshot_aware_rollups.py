"""
Snapshot-aware scheduled-side reads for the rollup pipelines (NOTES-92).

The two rollup pipelines (`upsert_system_metrics_daily`,
`upsert_route_metrics_overlay`) accept `--gtfs-snapshot-id` so backfilled
dates read the GTFS snapshot that was in force on the date, not the live
`is_current` one. These tests exercise the two underlying reads that flag
threads into: `compute_service_delivered`'s scheduled-trip denominator and
`fetch_scheduled_cell_hours_for_routes`'s SWT pools.

Fixture shape: one route with TWO schedule versions — snapshot 1
(superseded, `is_current=False`) has one weekday trip; snapshot 2 (live,
`is_current=True`) has two weekday trips at different times. A correct
snapshot pin returns the old schedule; the default returns the live one.
"""

from datetime import date, datetime

import pytest

from src.ewt import fetch_scheduled_cell_hours_for_routes
from src.models import Calendar, GTFSSnapshot, Route, StopTime, Trip
from src.service_delivered import compute_service_delivered

ROUTE = "T99"
SERVICE_DATE = date(2026, 6, 16)  # a Tuesday inside both calendars' windows
OLD_SNAP = 1
NEW_SNAP = 2


def _seed_calendar(db_session, service_id: str, snapshot_id: int, is_current: bool) -> None:
    """Insert an all-weekday calendar row for one schedule version."""
    db_session.add(
        Calendar(
            service_id=service_id,
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            friday=1,
            saturday=0,
            sunday=0,
            start_date="20260101",
            end_date="20261231",
            snapshot_id=snapshot_id,
            is_current=is_current,
        )
    )


def _seed_trip(
    db_session, trip_id: str, service_id: str, snapshot_id: int, is_current: bool
) -> None:
    """Insert one Trip row for one schedule version."""
    db_session.add(
        Trip(
            trip_id=trip_id,
            route_id=ROUTE,
            service_id=service_id,
            direction_id=0,
            trip_headsign="Downtown",
            snapshot_id=snapshot_id,
            is_current=is_current,
        )
    )


def _seed_stop_time(
    db_session,
    trip_id: str,
    arrival_time: str,
    snapshot_id: int,
    is_current: bool,
    stop_id: str = "S1",
) -> None:
    """Insert one StopTime row for one schedule version."""
    db_session.add(
        StopTime(
            trip_id=trip_id,
            stop_id=stop_id,
            arrival_time=arrival_time,
            departure_time=arrival_time,
            stop_sequence=1,
            snapshot_id=snapshot_id,
            is_current=is_current,
        )
    )


@pytest.fixture
def two_snapshot_schedule(db_session):
    """Route T99 with a superseded snapshot-1 schedule and a live snapshot-2 one.

    Snapshot 1: one trip, arrivals 7:00 / 7:10 at S1 (one 600s headway in
    hour 7). Snapshot 2: two trips, arrivals 8:00 / 8:30 at S1 (one 1800s
    headway in hour 8).
    """
    # FK targets for the versioned tables' snapshot_id columns. Flushed
    # before the dependent rows: no relationship() ties GTFSSnapshot to the
    # versioned models, so the unit of work won't order these inserts first
    # on its own.
    for snap_id in (OLD_SNAP, NEW_SNAP):
        db_session.add(GTFSSnapshot(snapshot_id=snap_id, snapshot_date=datetime(2026, 5, snap_id)))
    db_session.flush()
    db_session.add(
        Route(
            route_id=ROUTE,
            route_short_name=ROUTE,
            route_long_name="Snapshot Test Route",
            route_type=3,
            is_current=True,
        )
    )
    _seed_calendar(db_session, "SVC_OLD", OLD_SNAP, is_current=False)
    _seed_calendar(db_session, "SVC_NEW", NEW_SNAP, is_current=True)

    _seed_trip(db_session, "T_OLD_1", "SVC_OLD", OLD_SNAP, is_current=False)
    _seed_stop_time(db_session, "T_OLD_1", "7:00:00", OLD_SNAP, is_current=False)
    _seed_stop_time(db_session, "T_OLD_1", "7:10:00", OLD_SNAP, is_current=False)

    for i, arrival in enumerate(("8:00:00", "8:30:00"), start=1):
        _seed_trip(db_session, f"T_NEW_{i}", "SVC_NEW", NEW_SNAP, is_current=True)
        _seed_stop_time(db_session, f"T_NEW_{i}", arrival, NEW_SNAP, is_current=True)
    db_session.commit()
    return db_session


@pytest.mark.smoke
class TestServiceDeliveredSnapshotPin:
    """Scheduled-trip denominators follow the pinned snapshot."""

    def test_default_reads_live_snapshot(self, two_snapshot_schedule):
        """Default (no pin) counts the two is_current trips."""
        result = compute_service_delivered(two_snapshot_schedule, ROUTE, SERVICE_DATE)
        assert result["scheduled_trips"] == 2

    def test_pinned_snapshot_reads_superseded_schedule(self, two_snapshot_schedule):
        """Pinning snapshot 1 counts its single trip despite is_current=False."""
        result = compute_service_delivered(
            two_snapshot_schedule, ROUTE, SERVICE_DATE, gtfs_snapshot_id=OLD_SNAP
        )
        assert result["scheduled_trips"] == 1


@pytest.mark.smoke
class TestScheduledCellHoursSnapshotPin:
    """EWT SWT pools follow the pinned snapshot.

    `route_ids` is passed explicitly so the module-level schedule cache
    (which only engages on the unfiltered path) can't leak state between
    tests.
    """

    def test_default_reads_live_snapshot(self, two_snapshot_schedule):
        """Default returns the live schedule's hour-8 headway only."""
        sched = fetch_scheduled_cell_hours_for_routes(
            two_snapshot_schedule, "weekday", route_ids=[ROUTE]
        )
        cells = sched.get(ROUTE, {})
        assert (0, "S1", 8) in cells
        assert cells[(0, "S1", 8)] == [1800.0]
        assert (0, "S1", 7) not in cells

    def test_pinned_snapshot_reads_superseded_schedule(self, two_snapshot_schedule):
        """Pinning snapshot 1 returns its hour-7 headway despite is_current=False."""
        sched = fetch_scheduled_cell_hours_for_routes(
            two_snapshot_schedule,
            "weekday",
            route_ids=[ROUTE],
            gtfs_snapshot_id=OLD_SNAP,
        )
        cells = sched.get(ROUTE, {})
        assert (0, "S1", 7) in cells
        assert cells[(0, "S1", 7)] == [600.0]
        assert (0, "S1", 8) not in cells
