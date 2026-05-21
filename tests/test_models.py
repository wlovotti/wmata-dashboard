"""
Model tests for WMATA Dashboard

Tests database model creation, relationships, and constraints.

Run with: pytest tests/test_models.py
"""

from src.models import (
    Route,
    Stop,
    StopTime,
    Trip,
    VehiclePosition,
)
from src.timezones import utcnow_naive


def test_route_creation(db_session):
    """Test creating a Route model"""
    route = Route(
        route_id="TEST_ROUTE",
        route_short_name="TR",
        route_long_name="Test Route",
        route_type=3,
        is_current=True,
    )
    db_session.add(route)
    db_session.commit()

    queried = db_session.query(Route).filter_by(route_id="TEST_ROUTE").first()
    assert queried is not None
    assert queried.route_short_name == "TR"
    assert queried.is_current is True


def test_stop_creation(db_session):
    """Test creating a Stop model"""
    stop = Stop(
        stop_id="STOP_TEST",
        stop_name="Test Stop",
        stop_lat=38.9072,
        stop_lon=-77.0369,
    )
    db_session.add(stop)
    db_session.commit()

    queried = db_session.query(Stop).filter_by(stop_id="STOP_TEST").first()
    assert queried is not None
    assert queried.stop_name == "Test Stop"
    assert queried.stop_lat == 38.9072


def test_trip_route_relationship(db_session, sample_route):
    """Test Trip → Route relationship"""
    trip = Trip(
        trip_id="TRIP_TEST",
        route_id=sample_route.route_id,
        service_id="WEEKDAY",
        direction_id=0,
    )
    db_session.add(trip)
    db_session.commit()

    queried_trip = db_session.query(Trip).filter_by(trip_id="TRIP_TEST").first()
    assert queried_trip.route_id == sample_route.route_id


def test_stop_time_relationships(db_session, sample_route, sample_stop):
    """Test StopTime relationships with Trip and Stop"""
    trip = Trip(
        trip_id="TRIP_TEST",
        route_id=sample_route.route_id,
        service_id="WEEKDAY",
        direction_id=0,
    )
    db_session.add(trip)
    db_session.commit()

    stop_time = StopTime(
        trip_id=trip.trip_id,
        stop_id=sample_stop.stop_id,
        stop_sequence=1,
        arrival_time="08:00:00",
        departure_time="08:00:00",
    )
    db_session.add(stop_time)
    db_session.commit()

    queried = db_session.query(StopTime).filter_by(trip_id=trip.trip_id).first()
    assert queried.stop_id == sample_stop.stop_id
    assert queried.arrival_time == "08:00:00"


def test_vehicle_position_creation(db_session, sample_route, sample_trip):
    """Test creating VehiclePosition with all fields"""
    timestamp = utcnow_naive()
    position = VehiclePosition(
        vehicle_id="BUS_123",
        route_id=sample_route.route_id,
        trip_id=sample_trip.trip_id,
        latitude=38.9072,
        longitude=-77.0369,
        bearing=180.0,
        speed=25.5,
        timestamp=timestamp,
        current_status=2,
        occupancy_status=3,
    )
    db_session.add(position)
    db_session.commit()

    queried = db_session.query(VehiclePosition).filter_by(vehicle_id="BUS_123").first()
    assert queried.route_id == sample_route.route_id
    assert queried.latitude == 38.9072
    assert queried.speed == 25.5


def test_query_multiple_routes(db_session, sample_routes):
    """Test querying multiple routes"""
    routes = db_session.query(Route).filter(Route.is_current).all()
    assert len(routes) == 3
    assert all(r.is_current for r in routes)


def test_query_vehicle_positions_by_route(db_session, sample_route, sample_vehicle_positions):
    """Test filtering vehicle positions by route"""
    positions = (
        db_session.query(VehiclePosition)
        .filter(VehiclePosition.route_id == sample_route.route_id)
        .all()
    )

    assert len(positions) == 5
    assert all(p.route_id == sample_route.route_id for p in positions)


def test_trip_update_state_schema(db_session):
    """TripUpdateState has the columns the refactor design requires."""
    from src.models import TripUpdateState

    columns = {c.name for c in TripUpdateState.__table__.columns}
    expected = {
        "trip_id",
        "stop_sequence",
        "service_date",
        "stop_id",
        "vehicle_id",
        "final_snapshot_ts",
        "final_schedule_relationship",
        "last_pred_snapshot_ts",
        "last_predicted_arrival_ts",
        "derived_at",
    }
    assert columns == expected, f"unexpected columns: {columns ^ expected}"

    # Composite PK on (trip_id, stop_sequence, service_date) — see
    # 2026-05-20 spec addendum. Without service_date in the PK, WMATA's
    # day-over-day repeating trip_ids would overwrite themselves.
    pk_cols = {c.name for c in TripUpdateState.__table__.primary_key.columns}
    assert pk_cols == {"trip_id", "stop_sequence", "service_date"}
