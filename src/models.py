from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Route(Base):
    """GTFS static route data"""
    __tablename__ = 'routes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, unique=True, nullable=False, index=True)
    route_short_name = Column(String, nullable=False)
    route_long_name = Column(String)
    route_type = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    trips = relationship("Trip", back_populates="route")
    vehicle_positions = relationship("VehiclePosition", back_populates="route")


class Stop(Base):
    """GTFS static stop data"""
    __tablename__ = 'stops'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stop_id = Column(String, unique=True, nullable=False, index=True)
    stop_name = Column(String, nullable=False)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    stop_times = relationship("StopTime", back_populates="stop")


class Trip(Base):
    """GTFS static trip data"""
    __tablename__ = 'trips'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, unique=True, nullable=False, index=True)
    route_id = Column(String, ForeignKey('routes.route_id'), nullable=False, index=True)
    service_id = Column(String)
    trip_headsign = Column(String)
    direction_id = Column(Integer)
    block_id = Column(String, index=True)  # Links trips that use the same vehicle
    shape_id = Column(String, index=True)  # Links to Shape table
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    route = relationship("Route", back_populates="trips")
    stop_times = relationship("StopTime", back_populates="trip")
    vehicle_positions = relationship("VehiclePosition", back_populates="trip")


class StopTime(Base):
    """GTFS static stop_times data (scheduled stops)"""
    __tablename__ = 'stop_times'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, ForeignKey('trips.trip_id'), nullable=False, index=True)
    stop_id = Column(String, ForeignKey('stops.stop_id'), nullable=False, index=True)
    arrival_time = Column(String, nullable=False)
    departure_time = Column(String, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    trip = relationship("Trip", back_populates="stop_times")
    stop = relationship("Stop", back_populates="stop_times")

    # Composite index for efficient queries
    __table_args__ = (
        Index('idx_trip_stop_sequence', 'trip_id', 'stop_sequence'),
    )


class Shape(Base):
    """
    GTFS static shapes data - defines the actual path that vehicles follow.

    Shapes define the physical path that a vehicle travels along a route.
    Each shape is composed of multiple points that, when connected, show the
    actual street-level route. This is essential for accurate distance and
    speed calculations.
    """
    __tablename__ = 'shapes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_id = Column(String, nullable=False, index=True)
    shape_pt_lat = Column(Float, nullable=False)
    shape_pt_lon = Column(Float, nullable=False)
    shape_pt_sequence = Column(Integer, nullable=False)
    shape_dist_traveled = Column(Float)  # Optional: cumulative distance in GTFS
    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries by shape and sequence
    __table_args__ = (
        Index('idx_shape_sequence', 'shape_id', 'shape_pt_sequence'),
    )


class VehiclePosition(Base):
    """Real-time vehicle position data from GTFS-RT (collected every 30-60 seconds)"""
    __tablename__ = 'vehicle_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id = Column(String, nullable=False, index=True)
    route_id = Column(String, ForeignKey('routes.route_id'), index=True)
    trip_id = Column(String, ForeignKey('trips.trip_id'), index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    current_stop_sequence = Column(Integer)
    timestamp = Column(DateTime, nullable=False, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    route = relationship("Route", back_populates="vehicle_positions")
    trip = relationship("Trip", back_populates="vehicle_positions")

    # Composite indexes for efficient queries
    __table_args__ = (
        Index('idx_vehicle_timestamp', 'vehicle_id', 'timestamp'),
        Index('idx_route_timestamp', 'route_id', 'timestamp'),
        Index('idx_trip_timestamp', 'trip_id', 'timestamp'),
    )


class BusPosition(Base):
    """
    Real-time bus position data from WMATA's BusPositions API.

    This is WMATA's proprietary API that provides richer data than GTFS-RT,
    including schedule deviation which is crucial for accurate OTP metrics.
    """
    __tablename__ = 'bus_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id = Column(String, nullable=False, index=True)
    route_id = Column(String, nullable=False, index=True)
    trip_id = Column(String, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    # Schedule adherence in minutes (negative = early, positive = late)
    deviation = Column(Float)

    # Timestamp from API (when vehicle position was recorded)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Direction info
    direction_num = Column(Integer)
    direction_text = Column(String)

    # Trip details
    trip_headsign = Column(String)
    trip_start_time = Column(DateTime)
    trip_end_time = Column(DateTime)
    block_number = Column(String, index=True)

    # When we collected this data
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Composite indexes for efficient queries
    __table_args__ = (
        Index('idx_bus_vehicle_timestamp', 'vehicle_id', 'timestamp'),
        Index('idx_bus_route_timestamp', 'route_id', 'timestamp'),
        Index('idx_bus_trip_timestamp', 'trip_id', 'timestamp'),
        Index('idx_bus_block_timestamp', 'block_number', 'timestamp'),
    )
