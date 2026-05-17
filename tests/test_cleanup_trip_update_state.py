"""Tests for pipelines.cleanup_trip_update_state."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState
from src.timezones import utcnow_naive


def _make_state_row(
    trip_id: str,
    stop_sequence: int,
    *,
    final_snapshot_ts: datetime,
    derived_at: datetime | None = None,
) -> TripUpdateState:
    """Build a TripUpdateState row for cleanup tests.

    Args:
        trip_id: GTFS trip identifier.
        stop_sequence: Stop sequence number within the trip.
        final_snapshot_ts: Timestamp of the final collected snapshot.
        derived_at: When stop_events were materialized; ``None`` means
            the row has not yet been derived.

    Returns:
        An unsaved :class:`~src.models.TripUpdateState` instance.
    """
    return TripUpdateState(
        trip_id=trip_id,
        stop_sequence=stop_sequence,
        stop_id="S1",
        vehicle_id="V1",
        final_snapshot_ts=final_snapshot_ts,
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=final_snapshot_ts,
        last_predicted_arrival_ts=final_snapshot_ts + timedelta(minutes=5),
        derived_at=derived_at,
    )


@pytest.mark.integration
def test_cleanup_deletes_derived_rows_older_than_two_days(pg_session):
    """Derived rows with derived_at older than 2 days are deleted."""
    from pipelines.cleanup_trip_update_state import run_cleanup

    now = utcnow_naive()
    pg_session.add_all(
        [
            _make_state_row(
                "T_old",
                1,
                final_snapshot_ts=now - timedelta(days=4),
                derived_at=now - timedelta(days=3),
            ),
            _make_state_row(
                "T_recent",
                1,
                final_snapshot_ts=now - timedelta(hours=12),
                derived_at=now - timedelta(hours=1),
            ),
            _make_state_row("T_unfinished", 1, final_snapshot_ts=now - timedelta(hours=1)),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    assert counts == {"derived_deleted": 1, "safety_deleted": 0}
    remaining = {r.trip_id for r in pg_session.execute(select(TripUpdateState)).scalars()}
    assert remaining == {"T_recent", "T_unfinished"}  # T_old gone


@pytest.mark.integration
def test_cleanup_safety_net_deletes_underived_rows_older_than_seven_days(pg_session):
    """Un-derived rows older than 7 days are deleted as safety net."""
    from pipelines.cleanup_trip_update_state import run_cleanup

    now = utcnow_naive()
    pg_session.add_all(
        [
            _make_state_row("T_stale", 1, final_snapshot_ts=now - timedelta(days=8)),
            _make_state_row("T_fresh", 1, final_snapshot_ts=now - timedelta(days=1)),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    assert counts == {"derived_deleted": 0, "safety_deleted": 1}
    remaining = {r.trip_id for r in pg_session.execute(select(TripUpdateState)).scalars()}
    assert remaining == {"T_fresh"}
