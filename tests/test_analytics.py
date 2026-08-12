"""
Unit tests for src/analytics.py.

Currently covers `get_route_stops` / `_route_stops_cache` (NOTES-114): the
cache must be keyed with a db-identity and GTFS-snapshot component (mirroring
`src/ewt.py`'s `_db_identity` + `_schedule_cache` pattern, NOTES-108) so it
cannot collide across databases or survive a GTFS reload, and must not hand
back ORM objects bound to whichever session first populated the cache.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.analytics as analytics_module
from src.analytics import get_route_stops
from src.models import Base, GTFSSnapshot, Route, Stop, StopTime, Trip

ROUTE = "R1"
SERVICE_ID = "SVC1"


@pytest.fixture(autouse=True)
def _clear_route_stops_cache():
    """The cache is module-level and must not leak state between tests."""
    analytics_module._route_stops_cache.clear()
    yield
    analytics_module._route_stops_cache.clear()


@pytest.fixture
def other_agency_db_session(tmp_path):
    """A second, physically distinct database — simulates a different
    agency's database (e.g. `sfmta_dashboard`) whose `gtfs_snapshots` table
    has its own independent `snapshot_id` sequence that can reach the same
    integer value as the primary `db_session` fixture's, purely by
    coincidence (NOTES-108). File-backed (not `:memory:`) so its bind URL
    is guaranteed to differ from `db_session`'s.
    """
    db_path = tmp_path / "other_agency.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    engine.dispose()


def _seed_route_with_stop(db_session, route_id: str, stop_id: str, snapshot_id: int = 1) -> None:
    """Insert one current Route/Trip/StopTime/Stop chain, plus the
    GTFSSnapshot row `get_route_stops` uses to resolve the cache's
    snapshot component."""
    db_session.add(GTFSSnapshot(snapshot_id=snapshot_id, snapshot_date=datetime(2026, 5, 1)))
    db_session.add(
        Route(
            route_id=route_id,
            route_short_name=route_id,
            route_long_name=f"Test Route {route_id}",
            route_type=3,
            is_current=True,
        )
    )
    db_session.add(
        Stop(
            stop_id=stop_id,
            stop_name=stop_id,
            stop_lat=38.9,
            stop_lon=-77.0,
            is_current=True,
        )
    )
    db_session.add(
        Trip(
            trip_id=f"T-{route_id}-{stop_id}",
            route_id=route_id,
            service_id=SERVICE_ID,
            direction_id=0,
            is_current=True,
        )
    )
    db_session.add(
        StopTime(
            trip_id=f"T-{route_id}-{stop_id}",
            stop_id=stop_id,
            arrival_time="7:00:00",
            departure_time="7:00:00",
            stop_sequence=1,
            is_current=True,
        )
    )
    db_session.commit()


class TestRouteStopsCacheAgencyIsolation:
    """NOTES-114: `_route_stops_cache` used to be keyed on `route_id` alone.
    Two different databases that both define the same `route_id` (very
    plausible — WMATA and SFMTA both have short alphanumeric route ids)
    must never share a cache entry.
    """

    def test_does_not_leak_stops_across_databases(self, db_session, other_agency_db_session):
        _seed_route_with_stop(db_session, ROUTE, "WMATA_STOP")
        _seed_route_with_stop(other_agency_db_session, ROUTE, "SFMTA_STOP")

        wmata_stops = get_route_stops(db_session, ROUTE)
        assert {s.stop_id for s in wmata_stops} == {"WMATA_STOP"}

        # Pre-fix, this collides on cache key `ROUTE` and would incorrectly
        # return db_session's cached WMATA_STOP list.
        sfmta_stops = get_route_stops(other_agency_db_session, ROUTE)
        assert {s.stop_id for s in sfmta_stops} == {"SFMTA_STOP"}

        # Re-querying the first database must still return its own result —
        # not evicted or clobbered by the second database's store.
        wmata_stops_again = get_route_stops(db_session, ROUTE)
        assert {s.stop_id for s in wmata_stops_again} == {"WMATA_STOP"}


class TestRouteStopsCacheSnapshotInvalidation:
    """NOTES-114: entries must not survive a GTFS reload
    (`scripts/reload_gtfs_complete.py`), which bumps `GTFSSnapshot.snapshot_id`."""

    def test_new_snapshot_evicts_stale_entry(self, db_session):
        _seed_route_with_stop(db_session, ROUTE, "OLD_STOP", snapshot_id=1)

        first = get_route_stops(db_session, ROUTE)
        assert {s.stop_id for s in first} == {"OLD_STOP"}

        # Simulate a GTFS reload: old Stop marked stale, new Stop + higher
        # snapshot_id inserted (mirrors reload_gtfs_complete.py's UPDATE +
        # INSERT pattern for versioned tables).
        db_session.query(Stop).filter(Stop.stop_id == "OLD_STOP").update({"is_current": False})
        db_session.add(GTFSSnapshot(snapshot_id=2, snapshot_date=datetime(2026, 6, 1)))
        db_session.add(
            Stop(
                stop_id="NEW_STOP",
                stop_name="NEW_STOP",
                stop_lat=38.9,
                stop_lon=-77.0,
                is_current=True,
            )
        )
        db_session.add(
            Trip(
                trip_id="T-reload",
                route_id=ROUTE,
                service_id=SERVICE_ID,
                direction_id=0,
                is_current=True,
            )
        )
        db_session.add(
            StopTime(
                trip_id="T-reload",
                stop_id="NEW_STOP",
                arrival_time="7:00:00",
                departure_time="7:00:00",
                stop_sequence=1,
                is_current=True,
            )
        )
        db_session.commit()

        second = get_route_stops(db_session, ROUTE)
        assert {s.stop_id for s in second} == {"NEW_STOP"}, (
            "stale pre-reload entry must be evicted once the snapshot bumps, "
            "not served indefinitely"
        )


class TestRouteStopsCacheDetachedObjects:
    """NOTES-114: cached `Stop` objects must not be bound to whichever
    session first populated the cache — a later caller on a *different*
    session must be able to read attributes without a
    `DetachedInstanceError` or a query against the wrong session."""

    def test_cached_stops_are_usable_after_originating_session_closes(self, db_session, tmp_path):
        _seed_route_with_stop(db_session, ROUTE, "S1")

        stops = get_route_stops(db_session, ROUTE)
        db_session.close()

        # Attribute access on already-loaded columns must not require the
        # originating session to still be open/attached.
        assert {s.stop_id for s in stops} == {"S1"}
        assert all(s.stop_lat == 38.9 for s in stops)
