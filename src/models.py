from datetime import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Agency(Base):
    """GTFS agency data (transit agency information)"""

    __tablename__ = "agencies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    agency_id = Column(String, unique=True, nullable=False, index=True)
    agency_name = Column(String, nullable=False)
    agency_url = Column(String)
    agency_timezone = Column(String)
    agency_lang = Column(String)
    agency_phone = Column(String)
    agency_fare_url = Column(String)
    agency_email = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    routes = relationship("Route", back_populates="agency")


class Calendar(Base):
    """GTFS calendar data (service schedules by day of week)"""

    __tablename__ = "calendar"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(String, unique=True, nullable=False, index=True)
    monday = Column(Integer, nullable=False)  # 0 or 1
    tuesday = Column(Integer, nullable=False)
    wednesday = Column(Integer, nullable=False)
    thursday = Column(Integer, nullable=False)
    friday = Column(Integer, nullable=False)
    saturday = Column(Integer, nullable=False)
    sunday = Column(Integer, nullable=False)
    start_date = Column(String, nullable=False)  # YYYYMMDD format
    end_date = Column(String, nullable=False)  # YYYYMMDD format
    created_at = Column(DateTime, default=datetime.utcnow)


class CalendarDate(Base):
    """GTFS calendar_dates data (service exceptions)"""

    __tablename__ = "calendar_dates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False, index=True)  # YYYYMMDD format
    exception_type = Column(Integer, nullable=False)  # 1=added, 2=removed
    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries
    __table_args__ = (Index("idx_service_date", "service_id", "date"),)


class FeedInfo(Base):
    """GTFS feed_info data (feed metadata)"""

    __tablename__ = "feed_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feed_publisher_name = Column(String, nullable=False)
    feed_publisher_url = Column(String)
    feed_lang = Column(String)
    feed_start_date = Column(String)  # YYYYMMDD format
    feed_end_date = Column(String)  # YYYYMMDD format
    feed_version = Column(String)
    feed_contact_email = Column(String)
    feed_contact_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class Timepoint(Base):
    """WMATA-specific timepoint data (subset of stops used for schedule adherence)"""

    __tablename__ = "timepoints"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stop_id = Column(String, unique=True, nullable=False, index=True)
    stop_code = Column(String)
    stop_name = Column(String, nullable=False)
    stop_desc = Column(String)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    zone_id = Column(String)
    stop_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class TimepointTime(Base):
    """WMATA-specific timepoint schedule data"""

    __tablename__ = "timepoint_times"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, nullable=False, index=True)
    stop_id = Column(String, nullable=False, index=True)
    arrival_time = Column(String, nullable=False)
    departure_time = Column(String, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    shape_dist_traveled = Column(Float)
    timepoint = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries
    __table_args__ = (Index("idx_timepoint_trip_sequence", "trip_id", "stop_sequence"),)


class Route(Base):
    """GTFS static route data"""

    __tablename__ = "routes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, unique=True, nullable=False, index=True)
    agency_id = Column(String, ForeignKey("agencies.agency_id"))
    route_short_name = Column(String, nullable=False)
    route_long_name = Column(String)
    route_desc = Column(String)
    route_type = Column(String)
    route_url = Column(String)
    route_color = Column(String)
    route_text_color = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    agency = relationship("Agency", back_populates="routes")
    trips = relationship("Trip", back_populates="route")
    vehicle_positions = relationship("VehiclePosition", back_populates="route")


class Stop(Base):
    """GTFS static stop data"""

    __tablename__ = "stops"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stop_id = Column(String, unique=True, nullable=False, index=True)
    stop_code = Column(String)
    stop_name = Column(String, nullable=False)
    stop_desc = Column(String)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    zone_id = Column(String)
    stop_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    stop_times = relationship("StopTime", back_populates="stop")


class Trip(Base):
    """GTFS static trip data"""

    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, unique=True, nullable=False, index=True)
    route_id = Column(String, ForeignKey("routes.route_id"), nullable=False, index=True)
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

    __tablename__ = "stop_times"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, ForeignKey("trips.trip_id"), nullable=False, index=True)
    stop_id = Column(String, ForeignKey("stops.stop_id"), nullable=False, index=True)
    arrival_time = Column(String, nullable=False)
    departure_time = Column(String, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    shape_dist_traveled = Column(Float)
    timepoint = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    trip = relationship("Trip", back_populates="stop_times")
    stop = relationship("Stop", back_populates="stop_times")

    # Composite index for efficient queries
    __table_args__ = (Index("idx_trip_stop_sequence", "trip_id", "stop_sequence"),)


class Shape(Base):
    """
    GTFS static shapes data - defines the actual path that vehicles follow.

    Shapes define the physical path that a vehicle travels along a route.
    Each shape is composed of multiple points that, when connected, show the
    actual street-level route. This is essential for accurate distance and
    speed calculations.
    """

    __tablename__ = "shapes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_id = Column(String, nullable=False, index=True)
    shape_pt_lat = Column(Float, nullable=False)
    shape_pt_lon = Column(Float, nullable=False)
    shape_pt_sequence = Column(Integer, nullable=False)
    shape_dist_traveled = Column(Float)  # Optional: cumulative distance in GTFS
    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries by shape and sequence
    __table_args__ = (Index("idx_shape_sequence", "shape_id", "shape_pt_sequence"),)


class VehiclePosition(Base):
    """Real-time vehicle position data from GTFS-RT (collected every 30-60 seconds)"""

    __tablename__ = "vehicle_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id = Column(String, nullable=False, index=True)
    vehicle_label = Column(String)  # Vehicle display label
    route_id = Column(String, ForeignKey("routes.route_id"), index=True)
    trip_id = Column(String, ForeignKey("trips.trip_id"), index=True)

    # Position data
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    bearing = Column(Float)  # Direction vehicle is facing (0-360 degrees)
    speed = Column(Float)  # Speed in meters/second

    # Stop information
    current_stop_sequence = Column(Integer)
    stop_id = Column(String)  # Current or next stop
    current_status = Column(Integer)  # 0=incoming, 1=stopped, 2=in_transit

    # Trip details
    direction_id = Column(Integer)  # 0 or 1 for trip direction
    trip_start_time = Column(String)  # HH:MM:SS format
    trip_start_date = Column(String)  # YYYYMMDD format
    schedule_relationship = Column(Integer)  # 0=scheduled, 1=added, 2=unscheduled, 3=canceled

    # Additional data
    occupancy_status = Column(Integer)  # Passenger load (0-7 scale)

    # Timestamps
    timestamp = Column(DateTime, nullable=False, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    route = relationship("Route", back_populates="vehicle_positions")
    trip = relationship("Trip", back_populates="vehicle_positions")

    # Composite indexes for efficient queries
    __table_args__ = (
        Index("idx_vehicle_timestamp", "vehicle_id", "timestamp"),
        Index("idx_route_timestamp", "route_id", "timestamp"),
        Index("idx_trip_timestamp", "trip_id", "timestamp"),
    )


class BusPosition(Base):
    """
    Real-time bus position data from WMATA's BusPositions API.

    This is WMATA's proprietary API that provides richer data than GTFS-RT,
    including schedule deviation which is crucial for accurate OTP metrics.
    """

    __tablename__ = "bus_positions"

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
        Index("idx_bus_vehicle_timestamp", "vehicle_id", "timestamp"),
        Index("idx_bus_route_timestamp", "route_id", "timestamp"),
        Index("idx_bus_trip_timestamp", "trip_id", "timestamp"),
        Index("idx_bus_block_timestamp", "block_number", "timestamp"),
    )


class RouteMetricsDaily(Base):
    """
    Pre-computed daily performance metrics for routes.

    This table stores calculated metrics for each route for each day,
    enabling fast API responses without recalculating from raw vehicle positions.
    Populated by nightly batch job (pipelines/compute_daily_metrics.py).
    """

    __tablename__ = "route_metrics_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False, index=True)  # YYYY-MM-DD format

    # On-time performance metrics
    otp_percentage = Column(Float)
    early_percentage = Column(Float)
    late_percentage = Column(Float)

    # Headway metrics
    avg_headway_minutes = Column(Float)
    min_headway_minutes = Column(Float)
    max_headway_minutes = Column(Float)

    # Speed metrics
    avg_speed_mph = Column(Float)
    median_speed_mph = Column(Float)

    # Data quality metrics
    total_arrivals = Column(Integer)
    unique_vehicles = Column(Integer)
    unique_trips = Column(Integer)

    # Metadata
    computed_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries
    __table_args__ = (Index("idx_route_date", "route_id", "date", unique=True),)


class RouteMetricsSummary(Base):
    """
    Pre-computed rolling summary metrics for routes (typically 7 days).

    This table stores aggregated metrics over a recent time period
    for quick scorecard/summary displays. Updated by nightly batch job.
    """

    __tablename__ = "route_metrics_summary"

    route_id = Column(String, primary_key=True)

    # Time period analyzed
    days_analyzed = Column(Integer, default=7)
    date_start = Column(String)  # YYYY-MM-DD
    date_end = Column(String)  # YYYY-MM-DD

    # Performance metrics
    otp_percentage = Column(Float)
    early_percentage = Column(Float)
    late_percentage = Column(Float)
    avg_headway_minutes = Column(Float)
    avg_speed_mph = Column(Float)

    # Data quality
    total_observations = Column(Integer)
    unique_vehicles = Column(Integer)
    last_data_timestamp = Column(DateTime)

    # Metadata
    computed_at = Column(DateTime, default=datetime.utcnow)
