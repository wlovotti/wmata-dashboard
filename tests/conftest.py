"""
Shared pytest fixtures for WMATA Dashboard tests

Provides fixtures for:
- Database setup/teardown with in-memory SQLite
- Postgres-backed fixtures for integration tests that need Postgres-specific SQL
- FastAPI test client
- Mock data generators
- Environment variable mocking
"""

import os
from collections.abc import Generator
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import api.main
from api.main import app
from src.models import (
    Base,
    Route,
    Shape,
    Stop,
    StopEvent,
    StopTime,
    Trip,
    VehiclePosition,
)
from src.timezones import utcnow_naive


@pytest.fixture(scope="session")
def test_engine():
    """
    Create an in-memory SQLite engine for testing

    Session-scoped so it's created once for all tests. StaticPool +
    check_same_thread=False keeps the in-memory DB visible across threads
    (TestClient runs requests on a worker thread).
    """
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(test_engine) -> Generator[Session, None, None]:
    """
    Create a new database session for a test with transaction rollback

    Function-scoped so each test gets a clean database state
    """
    connection = test_engine.connect()
    transaction = connection.begin()
    SessionLocal = sessionmaker(bind=connection)
    session = SessionLocal()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="session")
def pg_engine():
    """Engine pointing to a real Postgres DB for integration tests that
    need Postgres-specific SQL (pg_insert / ON CONFLICT / DDL features
    SQLite can't represent).

    Reads ``PG_TEST_DATABASE_URL`` if set, otherwise falls back to
    the local dev DB ``postgresql:///wmata_dashboard``. The dev DB is
    safe because per-test transactions roll back.
    """
    url = os.environ.get("PG_TEST_DATABASE_URL", "postgresql:///wmata_dashboard")
    engine = create_engine(url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def pg_session(pg_engine):
    """Function-scoped Postgres session with transaction rollback.

    The dev DB schema is assumed to already exist (created via migrations
    on the developer's machine). Each test runs inside an outer transaction
    that rolls back on teardown, so tests don't pollute the dev DB.

    ``join_transaction_mode="create_savepoint"`` (SQLAlchemy 2.0+) makes
    the session open a SAVEPOINT when it auto-begins inside the already-open
    connection-level transaction. ``session.commit()`` in test bodies then
    releases the SAVEPOINT rather than committing the outer transaction, so
    ``transaction.rollback()`` in teardown still rolls back all test writes.
    Without this, ``session.commit()`` would promote and commit the outer
    transaction, turning ``transaction.rollback()`` into a no-op and leaking
    rows into the dev DB on every test run.
    """
    connection = pg_engine.connect()
    transaction = connection.begin()
    SessionLocal = sessionmaker(bind=connection, join_transaction_mode="create_savepoint")
    session = SessionLocal()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def client(db_session, monkeypatch):
    """
    FastAPI TestClient that routes API DB calls through the test session.

    api.main calls get_session() directly (not via fastapi.Depends), so
    app.dependency_overrides has no effect. Monkeypatch the bound name in
    api.main, and shim .close() to a no-op so the per-request close in the
    handlers doesn't break the surrounding test transaction.
    """

    class _SessionProxy:
        def __init__(self, session):
            self._session = session

        def __getattr__(self, name):
            return getattr(self._session, name)

        def close(self):
            return None

    monkeypatch.setattr(api.main, "get_session", lambda: _SessionProxy(db_session))
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample_route(db_session) -> Route:
    """Create and return a sample Route"""
    route = Route(
        route_id="TEST1",
        route_short_name="T1",
        route_long_name="Test Route 1",
        route_type=3,  # Bus
        is_current=True,
    )
    db_session.add(route)
    db_session.commit()
    db_session.refresh(route)
    return route


@pytest.fixture
def sample_routes(db_session) -> list[Route]:
    """Create and return multiple sample Routes"""
    routes = [
        Route(
            route_id=f"TEST{i}",
            route_short_name=f"T{i}",
            route_long_name=f"Test Route {i}",
            route_type=3,
            is_current=True,
        )
        for i in range(1, 4)
    ]
    db_session.add_all(routes)
    db_session.commit()
    for route in routes:
        db_session.refresh(route)
    return routes


@pytest.fixture
def sample_stop(db_session) -> Stop:
    """Create and return a sample Stop"""
    stop = Stop(
        stop_id="STOP_TEST1",
        stop_name="Test Stop 1",
        stop_lat=38.9072,
        stop_lon=-77.0369,
    )
    db_session.add(stop)
    db_session.commit()
    db_session.refresh(stop)
    return stop


@pytest.fixture
def sample_trip(db_session, sample_route) -> Trip:
    """Create and return a sample Trip"""
    trip = Trip(
        trip_id="TRIP_TEST1",
        route_id=sample_route.route_id,
        service_id="WEEKDAY",
        direction_id=0,
        trip_headsign="Downtown",
    )
    db_session.add(trip)
    db_session.commit()
    db_session.refresh(trip)
    return trip


@pytest.fixture
def sample_vehicle_positions(db_session, sample_route, sample_trip) -> list[VehiclePosition]:
    """Create and return multiple sample VehiclePositions"""
    base_time = utcnow_naive() - timedelta(hours=1)
    positions = []

    for i in range(5):
        position = VehiclePosition(
            vehicle_id=f"VEHICLE_{i}",
            route_id=sample_route.route_id,
            trip_id=sample_trip.trip_id,
            latitude=38.9072 + (i * 0.001),
            longitude=-77.0369 + (i * 0.001),
            speed=25.0 + (i * 2),
            timestamp=base_time + timedelta(minutes=i * 5),
            current_status=2,  # IN_TRANSIT_TO
        )
        positions.append(position)

    db_session.add_all(positions)
    db_session.commit()
    for pos in positions:
        db_session.refresh(pos)
    return positions


@pytest.fixture
def sample_route_otp_stop_events(db_session, sample_route) -> list[StopEvent]:
    """Proximity stop_events for `sample_route` on yesterday, 80% on-time.

    Five rows on `today - 1` with deviation_sec values that bucket to four
    on-time + one late under the WMATA -2/+7 OTP window — the new
    canonical OTP source after NOTES-19. Seeded as `proximity` to match
    the source filter the OTP path uses (matches legacy
    `route_metrics_daily.otp_percentage` semantics, position-derived).
    """
    yesterday = utcnow_naive() - timedelta(days=1)
    base_ts = yesterday.replace(hour=14, minute=0, second=0, microsecond=0)
    deviations = [0, 30, 60, -30, 600]  # 4 on-time, 1 late (>420s) → 80%
    events = []
    for i, dev in enumerate(deviations):
        events.append(
            StopEvent(
                service_date=yesterday.date().isoformat(),
                trip_id=f"TRIP_OTP_{i}",
                route_id=sample_route.route_id,
                direction_id=0,
                stop_id="STOP_OTP_TEST",
                stop_sequence=1,
                observed_arrival_ts=base_ts + timedelta(minutes=i * 5),
                deviation_sec=dev,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
    db_session.add_all(events)
    db_session.commit()
    for ev in events:
        db_session.refresh(ev)
    return events


@pytest.fixture
def populate_fixture_gtfs():
    """Return a callable that seeds a synthetic GTFS scenario into a session.

    Usage in a test:
        populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")

    Scenarios:
      - ``two_routes_one_corridor``: FX1 + FX2 share 10 stops along a
        synthetic east-west street ("East St") in both directions; FX3
        is a 3-stop perpendicular route with no overlap. Used by the
        corridor-pipeline integration tests (NOTES-62).

    Schema notes:
      - ``Shape`` has no ``snapshot_id``/``is_current`` columns in this
        codebase — shapes are unversioned. Versioned tables
        (Route/Stop/Trip/StopTime) get ``is_current=True`` but leave
        ``snapshot_id`` unset (column is nullable) so the seed doesn't
        need a ``gtfs_snapshots`` row to satisfy the FK.
      - ``Route.route_type`` is a ``String`` column; pass ``"3"`` (bus),
        not the integer ``3``.
    """

    def _seed(session, scenario: str) -> None:
        if scenario != "two_routes_one_corridor":
            raise ValueError(f"Unknown fixture scenario: {scenario}")

        # Routes (3 of them: FX1, FX2, FX3).
        for route_id in ("FX1", "FX2", "FX3"):
            session.add(
                Route(
                    route_id=route_id,
                    agency_id=None,
                    route_short_name=route_id,
                    route_long_name=f"{route_id} fixture",
                    route_type="3",
                    is_current=True,
                )
            )

        # Ten stops along East St — eastward stepping in longitude
        # (lat constant), ~85m spacing per 0.001 deg lon at this latitude.
        # Total length ~770m, comfortably above MIN_CORRIDOR_LENGTH_M=500.
        for i in range(10):
            session.add(
                Stop(
                    stop_id=f"east_{i}",
                    stop_name=f"East St & {i}th",
                    stop_lat=38.94,
                    stop_lon=-77.07 + 0.0010 * i,
                    is_current=True,
                )
            )

        # Three perpendicular stops for FX3 — north-south.
        for i in range(3):
            session.add(
                Stop(
                    stop_id=f"north_{i}",
                    stop_name=f"North St & {i}",
                    stop_lat=38.95 + 0.001 * i,
                    stop_lon=-77.08,
                    is_current=True,
                )
            )

        # Shapes for FX1/FX2 in both directions. Direction 0 = eastbound
        # (sequence 1..10 stepping east), direction 1 = westbound (reversed).
        for shape_id_prefix in ("FX1", "FX2"):
            for direction, suffix in ((0, "51"), (1, "03")):
                shape_id = f"{shape_id_prefix}:{suffix}"
                indices = list(range(10))
                if direction == 1:
                    indices = list(reversed(indices))
                for seq, i in enumerate(indices, start=1):
                    session.add(
                        Shape(
                            shape_id=shape_id,
                            shape_pt_lat=38.94,
                            shape_pt_lon=-77.07 + 0.0010 * i,
                            shape_pt_sequence=seq,
                        )
                    )

        # FX3 north-south shape (no overlap with FX1/FX2).
        for seq, i in enumerate(range(3), start=1):
            session.add(
                Shape(
                    shape_id="FX3:01",
                    shape_pt_lat=38.95 + 0.001 * i,
                    shape_pt_lon=-77.08,
                    shape_pt_sequence=seq,
                )
            )

        # Trips: one per (route, direction) for FX1/FX2; one for FX3.
        for route_id in ("FX1", "FX2"):
            for direction in (0, 1):
                shape_id = f"{route_id}:{'51' if direction == 0 else '03'}"
                trip_id = f"{route_id}_dir{direction}_T1"
                session.add(
                    Trip(
                        trip_id=trip_id,
                        route_id=route_id,
                        direction_id=direction,
                        shape_id=shape_id,
                        service_id="WEEKDAY",
                        is_current=True,
                    )
                )
                stop_indices = list(range(10))
                if direction == 1:
                    stop_indices = list(reversed(stop_indices))
                for stop_sequence, i in enumerate(stop_indices, start=1):
                    session.add(
                        StopTime(
                            trip_id=trip_id,
                            stop_id=f"east_{i}",
                            stop_sequence=stop_sequence,
                            arrival_time=f"6:{stop_sequence:02d}:00",
                            departure_time=f"6:{stop_sequence:02d}:00",
                            is_current=True,
                        )
                    )

        # FX3: single direction, 3 stops north-south.
        session.add(
            Trip(
                trip_id="FX3_T1",
                route_id="FX3",
                direction_id=0,
                shape_id="FX3:01",
                service_id="WEEKDAY",
                is_current=True,
            )
        )
        for i in range(3):
            session.add(
                StopTime(
                    trip_id="FX3_T1",
                    stop_id=f"north_{i}",
                    stop_sequence=i + 1,
                    arrival_time=f"6:{i:02d}:00",
                    departure_time=f"6:{i:02d}:00",
                    is_current=True,
                )
            )

        session.flush()

    return _seed


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Mock environment variables for tests

    autouse=True means this runs for every test automatically
    """
    # Use in-memory SQLite for tests (overridden by db_session fixture)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    # Mock API key to prevent accidental real API calls
    monkeypatch.setenv("WMATA_API_KEY", "test_api_key_do_not_use")
