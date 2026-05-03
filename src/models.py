from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
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
    """GTFS calendar data (service schedules by day of week) with versioning support"""

    __tablename__ = "calendar"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(String, nullable=False, index=True)
    monday = Column(Integer, nullable=False)  # 0 or 1
    tuesday = Column(Integer, nullable=False)
    wednesday = Column(Integer, nullable=False)
    thursday = Column(Integer, nullable=False)
    friday = Column(Integer, nullable=False)
    saturday = Column(Integer, nullable=False)
    sunday = Column(Integer, nullable=False)
    start_date = Column(String, nullable=False)  # YYYYMMDD format
    end_date = Column(String, nullable=False)  # YYYYMMDD format

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries on current calendars
    __table_args__ = (Index("idx_calendar_current", "service_id", "is_current"),)


class CalendarDate(Base):
    """GTFS calendar_dates data (service exceptions) with versioning support"""

    __tablename__ = "calendar_dates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False, index=True)  # YYYYMMDD format
    exception_type = Column(Integer, nullable=False)  # 1=added, 2=removed

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite indexes for efficient queries
    __table_args__ = (
        Index("idx_service_date", "service_id", "date"),
        Index("idx_calendardate_current", "date", "is_current"),
    )


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


class GTFSSnapshot(Base):
    """
    Metadata for GTFS static data snapshots.

    Tracks each time GTFS data is reloaded, allowing version control over
    routes, stops, and other GTFS data. When a new snapshot is loaded,
    previous records are marked as inactive instead of deleted, preserving
    historical data and all associated vehicle position data.
    """

    __tablename__ = "gtfs_snapshots"

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date = Column(DateTime, nullable=False, index=True)  # When this snapshot was created
    feed_version = Column(String)  # From GTFS feed_info.feed_version
    routes_count = Column(Integer)  # Number of routes in this snapshot
    stops_count = Column(Integer)  # Number of stops in this snapshot
    trips_count = Column(Integer)  # Number of trips in this snapshot
    stop_times_count = Column(Integer)  # Number of stop_times in this snapshot
    shapes_count = Column(Integer)  # Number of shapes in this snapshot
    calendar_entries = Column(Integer)  # Number of calendar entries
    calendar_exceptions = Column(Integer)  # Number of calendar_dates entries
    notes = Column(String)  # Optional notes about this snapshot
    created_at = Column(DateTime, default=datetime.utcnow)  # When we created this record


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
    """GTFS static route data with versioning support"""

    __tablename__ = "routes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False, index=True)
    agency_id = Column(String, ForeignKey("agencies.agency_id"))
    route_short_name = Column(String, nullable=False)
    route_long_name = Column(String)
    route_desc = Column(String)
    route_type = Column(String)
    route_url = Column(String)
    route_color = Column(String)
    route_text_color = Column(String)

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(
        Boolean, nullable=False, default=True, index=True
    )  # Fast lookup for current

    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries on current routes
    __table_args__ = (Index("idx_route_current", "route_id", "is_current"),)

    # Relationships
    agency = relationship("Agency", back_populates="routes")
    # Note: trips and vehicle_positions relationships removed due to versioning complexity
    # Query using: session.query(Trip).filter(Trip.route_id == route.route_id, Trip.is_current == True)


class Stop(Base):
    """GTFS static stop data with versioning support"""

    __tablename__ = "stops"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stop_id = Column(String, nullable=False, index=True)
    stop_code = Column(String)
    stop_name = Column(String, nullable=False)
    stop_desc = Column(String)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    zone_id = Column(String)
    stop_url = Column(String)

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries on current stops
    __table_args__ = (Index("idx_stop_current", "stop_id", "is_current"),)

    # Note: stop_times relationship removed due to versioning complexity
    # Query using: session.query(StopTime).filter(StopTime.stop_id == stop.stop_id, StopTime.is_current == True)


class Trip(Base):
    """GTFS static trip data with versioning support"""

    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, nullable=False, index=True)
    route_id = Column(
        String, nullable=False, index=True
    )  # References routes.route_id (not FK due to versioning)
    service_id = Column(String, index=True)
    trip_headsign = Column(String)
    direction_id = Column(Integer)
    block_id = Column(String, index=True)  # Links trips that use the same vehicle
    shape_id = Column(String, index=True)  # Links to Shape table

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite index for efficient queries on current trips
    __table_args__ = (Index("idx_trip_current", "trip_id", "is_current"),)

    # Note: Relationships removed due to versioning complexity
    # Query using explicit filters on route_id/trip_id with is_current=True


class StopTime(Base):
    """GTFS static stop_times data (scheduled stops) with versioning support"""

    __tablename__ = "stop_times"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(
        String, nullable=False, index=True
    )  # References trips.trip_id (not FK due to versioning)
    stop_id = Column(
        String, nullable=False, index=True
    )  # References stops.stop_id (not FK due to versioning)
    arrival_time = Column(String, nullable=False)
    departure_time = Column(String, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    shape_dist_traveled = Column(Float)
    timepoint = Column(Integer)

    # GTFS Snapshot versioning
    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    valid_from = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Note: Relationships removed due to versioning complexity
    # Query using explicit filters with is_current=True

    # Composite indexes for efficient queries
    __table_args__ = (
        Index("idx_trip_stop_sequence", "trip_id", "stop_sequence"),
        Index("idx_stoptime_current", "trip_id", "is_current"),
    )


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
    route_id = Column(String, index=True)  # References routes.route_id (not FK due to versioning)
    trip_id = Column(String, index=True)  # References trips.trip_id (not FK due to versioning)

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

    # Note: Relationships removed due to versioning complexity
    # Query using explicit filters on route_id/trip_id with is_current=True

    # Composite indexes for efficient queries
    __table_args__ = (
        Index("idx_vehicle_timestamp", "vehicle_id", "timestamp"),
        Index("idx_route_timestamp", "route_id", "timestamp"),
        Index("idx_trip_timestamp", "trip_id", "timestamp"),
    )


class TripUpdateSnapshot(Base):
    """
    Append-only raw rows from the WMATA GTFS-RT TripUpdates feed.

    One row per (trip_id, stop_id) entry in a single feed snapshot. The same
    snapshot_ts is shared across every row materialized from one feed pull,
    so per-pair time series can be reconstructed by ORDER BY snapshot_ts.
    Stop entries drop out of the feed once the bus passes them — the last
    predicted_arrival_ts before disappearance is WMATA's effective claimed
    actual arrival, the basis for derived stop_events.
    """

    __tablename__ = "trip_update_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Snapshot context — same value across all rows from one feed pull.
    snapshot_ts = Column(DateTime, nullable=False, index=True)

    # Trip identification
    trip_id = Column(String, nullable=False, index=True)
    route_id = Column(String, index=True)
    vehicle_id = Column(
        String
    )  # ~40% of trip_updates carry vehicle.id; rest are pure schedule predictions

    # Stop within the trip
    stop_id = Column(String, nullable=False)
    stop_sequence = Column(Integer)

    # Predictions from the StopTimeUpdate
    predicted_arrival_ts = Column(DateTime)
    predicted_departure_ts = Column(DateTime)
    schedule_relationship = Column(
        String
    )  # 'SCHEDULED' | 'SKIPPED' | 'NO_DATA' | 'UNSCHEDULED' | 'UNSET'

    collected_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Indexes target the two main access patterns:
    #   - per-pair time series: WHERE trip_id=? AND stop_id=? ORDER BY snapshot_ts
    #   - route-level slices over a time window
    __table_args__ = (
        Index("idx_tu_trip_stop_snap", "trip_id", "stop_id", "snapshot_ts"),
        Index("idx_tu_route_snap", "route_id", "snapshot_ts"),
    )


class StopEvent(Base):
    """
    Per-(trip, stop) observed arrival or skip — the foundational unit of the
    metrics redesign, replacing the daily-batch recompute-from-positions model
    (PRs #42, #43, #44).

    One row per (service_date, trip_id, stop_sequence, source). Two rows per
    real-world event when both sources observe it (one source='trip_update',
    one source='proximity'). The duplication is intentional — keeping each
    source's evidence separate is what makes the agreement comparison
    (`pipelines/compare_stop_event_sources.py`) possible. Downstream consumers
    should pick a source explicitly or aggregate with a deliberate tie-break.

    Direction is denormalized as a column (not in the unique key) because
    direction_id is fully determined by trip_id. The denorm exists to make
    per-direction stop aggregations fast and to enforce the CLAUDE.md rule
    that any per-route, per-stop aggregation must group by
    (route_id, direction_id, stop_id) — never (route_id, stop_id) alone.

    service_date is in the unique key because GTFS-RT's TripDescriptor
    requires (trip_id, start_date) to disambiguate trip instances — the same
    trip_id runs every weekday. Without service_date the same trip on
    consecutive days collides.

    Scheduled times are snapshotted at write time, not joined live. GTFS gets
    re-versioned (is_current); historical rows must keep what was scheduled
    when the bus actually ran, not what the latest GTFS snapshot now says.
    """

    __tablename__ = "stop_events"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Trip / run identification — see class docstring on uniqueness key.
    service_date = Column(String, nullable=False)  # YYYY-MM-DD, Eastern operational day
    trip_id = Column(String, nullable=False)
    route_id = Column(String, nullable=False)
    direction_id = Column(Integer, nullable=False)
    vehicle_id = Column(String)  # nullable: ~40% of trip_updates lack vehicle.id

    # Stop within the trip
    stop_id = Column(String, nullable=False)
    stop_sequence = Column(Integer, nullable=False)

    # Schedule snapshot (naive UTC, captured at derivation time)
    scheduled_arrival_ts = Column(DateTime)
    scheduled_departure_ts = Column(DateTime)

    # Observation
    observed_arrival_ts = Column(DateTime)  # nullable for SKIPPED / NO_DATA
    deviation_sec = Column(Integer)  # observed - scheduled; nullable when no schedule match

    # Provenance
    source = Column(String, nullable=False)  # 'trip_update' | 'proximity'
    schedule_relationship = Column(
        String, nullable=False, default="SCHEDULED"
    )  # 'SCHEDULED' | 'SKIPPED' | 'NO_DATA' | 'ADDED'
    match_distance_m = Column(Float)  # proximity source only — diagnostic for matcher quality
    derived_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "service_date",
            "trip_id",
            "stop_sequence",
            "source",
            name="uq_stop_events_run_stop_source",
        ),
        # Per-route per-day aggregations (most common access pattern)
        Index("idx_stop_events_route_date", "route_id", "service_date"),
        # Per-direction per-stop time series — headways, EWT, bunching
        Index(
            "idx_stop_events_route_dir_stop_obs",
            "route_id",
            "direction_id",
            "stop_id",
            "observed_arrival_ts",
        ),
        # Bare stop time-series queries (e.g., NOTES-14 stop-skip rate)
        Index("idx_stop_events_stop_obs", "stop_id", "observed_arrival_ts"),
    )


class RouteServiceProfile(Base):
    """
    Per-(route, day_type, hour) scheduled service profile derived from GTFS.

    Reference data for downstream metrics: scheduled_trips is the denominator
    for service-delivered ratio (NOTES.md NOTES-11), and is_frequent is the
    gate for EWT (NOTES.md NOTES-15). Derived fresh on every GTFS reload — no
    versioning, the table is rewritten in place to match the current GTFS
    snapshot.

    `is_frequent` follows the standard rider-experience definition: mean
    scheduled headway ≤ 15 minutes for that hour-of-day. We deliberately
    avoid WMATA's published "headway-based" route list because that's
    operational policy, not encoded in GTFS, and we want the schedule to
    classify routes itself.
    """

    __tablename__ = "route_service_profile"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False, index=True)
    day_type = Column(String, nullable=False)  # 'weekday' | 'saturday' | 'sunday'
    hour = Column(Integer, nullable=False)  # 0..23, trip start hour at origin

    scheduled_trips = Column(Integer, nullable=False)
    mean_headway_min = Column(Float)  # NULL when scheduled_trips < 2
    is_frequent = Column(Boolean, nullable=False, default=False)

    snapshot_id = Column(
        Integer, ForeignKey("gtfs_snapshots.snapshot_id"), nullable=True, index=True
    )
    computed_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index(
            "idx_route_service_profile_unique",
            "route_id",
            "day_type",
            "hour",
            unique=True,
        ),
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
    headway_std_dev_minutes = Column(Float)  # Standard deviation for bunching detection
    headway_cv = Column(Float)  # Coefficient of variation (std_dev / mean)

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
    headway_std_dev_minutes = Column(Float)  # Standard deviation for bunching detection
    headway_cv = Column(Float)  # Coefficient of variation (std_dev / mean)
    avg_speed_mph = Column(Float)

    # Data quality
    total_observations = Column(Integer)
    unique_vehicles = Column(Integer)
    last_data_timestamp = Column(DateTime)

    # Pre-computed position statistics (7-day window)
    total_positions_7d = Column(Integer)  # Total position records
    unique_vehicles_7d = Column(Integer)  # Unique vehicles seen
    unique_trips_7d = Column(Integer)  # Unique trips tracked
    last_position_timestamp = Column(DateTime)  # Most recent position timestamp

    # Metadata
    computed_at = Column(DateTime, default=datetime.utcnow)
