"""Tests for pipelines.aggregate_runs — snapshot-aware schedule joins.

`aggregate_runs` joins per-trip schedule counts from GTFS `stop_times` to
compute `stops_scheduled` / `stops_observable`. For backfilled service
dates whose schedule has been superseded by a reload, the join must be
able to target the historical snapshot — otherwise old-schedule trips get
`stops_scheduled=None` and service-delivered metrics degrade.
"""

from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.models import GTFSSnapshot, Run, StopEvent, StopTime


def _seed_events_and_old_schedule(pg_session):
    """Seed stop_events for T_HIST plus its schedule in old snapshot 1 only."""
    derived_at = datetime(2026, 6, 12, 8, 0, 0)
    pg_session.add(GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 3, 18, 30, 0)))
    pg_session.add_all(
        [
            StopTime(
                trip_id="T_HIST",
                stop_sequence=seq,
                stop_id=f"S{seq}",
                arrival_time=f"14:0{seq}:00",
                departure_time=f"14:0{seq}:30",
                snapshot_id=1,
                is_current=False,
            )
            for seq in (1, 2)
        ]
        + [
            StopEvent(
                service_date="2026-06-11",
                trip_id="T_HIST",
                route_id="R1",
                direction_id=0,
                vehicle_id="V1",
                stop_id=f"S{seq}",
                stop_sequence=seq,
                observed_arrival_ts=datetime(2026, 6, 11, 14, seq, 30),
                source="trip_update",
                schedule_relationship="SCHEDULED",
                derived_at=derived_at,
            )
            for seq in (1, 2)
        ]
    )
    pg_session.commit()


@pytest.mark.integration
def test_aggregate_with_gtfs_snapshot_id_joins_historical_schedule(pg_session):
    """With gtfs_snapshot_id, stops_scheduled comes from the old snapshot's
    stop_times; without it, the old-schedule trip has no GTFS match."""
    from pipelines.aggregate_runs import aggregate_runs_for_route_date

    _seed_events_and_old_schedule(pg_session)

    aggregate_runs_for_route_date(pg_session, route_id="R1", service_date=date(2026, 6, 11))
    pg_session.commit()
    run = pg_session.execute(select(Run).where(Run.trip_id == "T_HIST")).scalar_one()
    assert run.stops_scheduled is None  # schedule invisible without the flag

    aggregate_runs_for_route_date(
        pg_session, route_id="R1", service_date=date(2026, 6, 11), gtfs_snapshot_id=1
    )
    pg_session.commit()
    run = pg_session.execute(select(Run).where(Run.trip_id == "T_HIST")).scalar_one()
    assert run.stops_scheduled == 2
