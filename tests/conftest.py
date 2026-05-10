"""
Shared pytest fixtures for WMATA Dashboard tests

Provides fixtures for:
- Database setup/teardown with in-memory SQLite
- FastAPI test client
- Mock data generators
- Environment variable mocking
"""

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
    Stop,
    StopEvent,
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
            bearing=180.0,
            speed=25.0 + (i * 2),
            timestamp=base_time + timedelta(minutes=i * 5),
            current_status=2,  # IN_TRANSIT_TO
            occupancy_status=3,  # MANY_SEATS_AVAILABLE
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
