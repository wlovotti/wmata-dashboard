"""Tests for src.gtfs_versioning — selecting a GTFS version in queries.

`reload_gtfs_complete.py` retains every snapshot's full row set (old rows
get `is_current=False`, never deleted), so historical service dates can be
derived against the schedule that was actually in force. These tests pin
the helper that turns "which snapshot?" into a SQLAlchemy criterion.
"""

from src.gtfs_versioning import gtfs_version_filter
from src.models import Trip


def _seed_two_snapshots(db_session):
    """Seed the same trip_id in two GTFS snapshots: 1 (old) and 2 (current)."""
    db_session.add_all(
        [
            Trip(
                trip_id="T1",
                route_id="R_OLD",
                direction_id=0,
                snapshot_id=1,
                is_current=False,
            ),
            Trip(
                trip_id="T1",
                route_id="R_NEW",
                direction_id=0,
                snapshot_id=2,
                is_current=True,
            ),
        ]
    )
    db_session.commit()


def test_default_selects_current_snapshot(db_session):
    """With no snapshot_id, the filter matches only is_current rows."""
    _seed_two_snapshots(db_session)
    rows = db_session.query(Trip).filter(gtfs_version_filter(Trip)).all()
    assert [t.route_id for t in rows] == ["R_NEW"]


def test_snapshot_id_selects_historical_snapshot(db_session):
    """With a snapshot_id, the filter matches that snapshot's rows even
    though they are no longer current."""
    _seed_two_snapshots(db_session)
    rows = db_session.query(Trip).filter(gtfs_version_filter(Trip, 1)).all()
    assert [t.route_id for t in rows] == ["R_OLD"]
