"""
Model tests for WMATA Dashboard

Tests database model creation, relationships, and constraints.

Run with: pytest tests/test_models.py
"""

from datetime import timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from src.models import (
    Route,
    RouteMetricsDaily,
    RouteMetricsSummary,
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


def test_route_metrics_summary_creation(db_session, sample_route):
    """Test creating RouteMetricsSummary"""
    summary = RouteMetricsSummary(
        route_id=sample_route.route_id,
        otp_percentage=85.5,
        avg_headway_minutes=12.0,
        avg_speed_mph=18.5,
        total_observations=200,
        computed_at=utcnow_naive(),
    )
    db_session.add(summary)
    db_session.commit()

    queried = (
        db_session.query(RouteMetricsSummary).filter_by(route_id=sample_route.route_id).first()
    )
    assert queried.otp_percentage == 85.5
    assert queried.avg_headway_minutes == 12.0


def test_route_metrics_daily_creation(db_session, sample_route):
    """Test creating RouteMetricsDaily"""
    yesterday = (utcnow_naive() - timedelta(days=1)).date()
    daily = RouteMetricsDaily(
        route_id=sample_route.route_id,
        date=yesterday.isoformat(),
        otp_percentage=82.0,
        avg_headway_minutes=11.5,
        avg_speed_mph=19.0,
        total_arrivals=50,
        computed_at=utcnow_naive(),
    )
    db_session.add(daily)
    db_session.commit()

    queried = db_session.query(RouteMetricsDaily).filter_by(route_id=sample_route.route_id).first()
    assert queried.otp_percentage == 82.0
    assert queried.date == yesterday.isoformat()


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


def test_route_metrics_unique_constraint(db_session, sample_route):
    """Test that route_id is unique in RouteMetricsSummary"""
    route_id = sample_route.route_id

    db_session.add(
        RouteMetricsSummary(
            route_id=route_id,
            otp_percentage=85.0,
            computed_at=utcnow_naive(),
        )
    )
    db_session.flush()

    # Inside a SAVEPOINT so the failing flush doesn't unwind the outer
    # test transaction managed by the db_session fixture.
    savepoint = db_session.begin_nested()
    db_session.add(
        RouteMetricsSummary(
            route_id=route_id,
            otp_percentage=90.0,
            computed_at=utcnow_naive(),
        )
    )
    with pytest.raises(IntegrityError):
        db_session.flush()
    savepoint.rollback()

    count = db_session.query(RouteMetricsSummary).filter_by(route_id=route_id).count()
    assert count == 1
