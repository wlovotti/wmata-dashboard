from sqlalchemy import (
    Boolean,
    Column,
    Date,
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

from src.timezones import utcnow_naive

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
    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=utcnow_naive)

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
    created_at = Column(DateTime, default=utcnow_naive)


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
    created_at = Column(DateTime, default=utcnow_naive)  # When we created this record


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
    created_at = Column(DateTime, default=utcnow_naive)


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
    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(
        Boolean, nullable=False, default=True, index=True
    )  # Fast lookup for current

    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=utcnow_naive)

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
    valid_from = Column(DateTime, nullable=False, default=utcnow_naive, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)  # NULL = currently active
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime, default=utcnow_naive)

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
    created_at = Column(DateTime, default=utcnow_naive)

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
    collected_at = Column(DateTime, default=utcnow_naive, index=True)

    # Note: Relationships removed due to versioning complexity
    # Query using explicit filters on route_id/trip_id with is_current=True

    # Composite indexes for efficient queries
    __table_args__ = (
        Index("idx_vehicle_timestamp", "vehicle_id", "timestamp"),
        Index("idx_route_timestamp", "route_id", "timestamp"),
        Index("idx_trip_timestamp", "trip_id", "timestamp"),
    )


class TripUpdateState(Base):
    """Final-state-only mirror of WMATA TripUpdate predictions per (trip, stop).

    Unlike the append-only ``trip_update_snapshots``, this table holds
    exactly one row per ``(trip_id, stop_sequence)``: the final state
    observed before the (trip, stop) drops out of WMATA's feed. The
    collector UPSERTs into this table on every poll. The derivation
    pipeline reads it directly, avoiding the ~21M-row/day snapshot scan.

    Lifecycle:
        1. Trip starts on service_date D -> rows inserted for upcoming stops.
        2. Bus moves -> rows update as predictions refine.
        3. Bus passes -> row's final state captured.
        4. End of service day -> ``derive_stop_events_from_state.py``
           materializes the corresponding ``stop_event`` and sets
           ``derived_at``.
        5. Cleanup cron deletes rows with ``service_date < CURRENT_DATE -
           INTERVAL '7 days'`` so the table can't grow unbounded.

    ``service_date`` is part of the PK because WMATA's GTFS-RT trip_ids
    repeat day-over-day on scheduled routes. Without it, the same
    (trip_id, stop_sequence) pair would overwrite itself across days,
    making historical re-derivation impossible. The collector computes
    ``service_date`` from ``tripDescriptor.start_date`` when present,
    falling back to the Eastern calendar day of ``snapshot_ts``. See
    ``docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md``.
    """

    __tablename__ = "trip_update_state"

    trip_id = Column(String, primary_key=True)
    stop_sequence = Column(Integer, primary_key=True)
    service_date = Column(Date, primary_key=True)

    stop_id = Column(String, nullable=False)
    vehicle_id = Column(String, nullable=True)

    final_snapshot_ts = Column(DateTime, nullable=False)
    final_schedule_relationship = Column(String, nullable=True)

    last_pred_snapshot_ts = Column(DateTime, nullable=True)
    last_predicted_arrival_ts = Column(DateTime, nullable=True)

    derived_at = Column(DateTime, nullable=True)


class TripUpdateSnapshot(Base):
    """
    Append-only rows from the WMATA GTFS-RT TripUpdates feed.

    Stored as **value-transitions, not every-tick observations.** The
    collector (``src/wmata_collector.py:_save_trip_updates``) keeps an
    in-memory ``(trip_id, stop_id) → (predicted_arrival_ts,
    predicted_departure_ts, schedule_relationship, vehicle_id)`` cache
    and only persists a row when that tuple differs from the last row
    stored for the same pair. The feed republishes every future stop on
    every poll, but predictions for far-future stops are often stable
    across many ticks; storing only transitions cuts row volume by ~50%
    in steady state with no loss for the derivation pipeline.

    Implications:
      * ``snapshot_ts`` on a row is the time the value FIRST appeared,
        not the most recent time it was observed in the feed. Pairs with
        a stable prediction will have ``snapshot_ts`` lag behind real
        time until the value changes or the stop drops out of the feed.
      * Per-pair "how often did WMATA re-emit this prediction" counts
        are a count of TRANSITIONS, not observations. The probe scripts
        (``scripts/probe_trip_updates*.py``) read this distinction at
        face value — adapt them if you need raw-observation counts.
      * The downstream derivation
        (``pipelines/derive_stop_events_trip_updates.py``) reduces all
        snapshots per pair to the last tuple-distinct value anyway, so
        dedup is provably lossless for ``stop_events`` and every metric
        derived from it (parity-tested across 6 routes × 2 days, 53k
        reduced keys, 0 mismatches).

    Stop entries drop out of the feed once the bus passes them — the last
    ``predicted_arrival_ts`` before disappearance is WMATA's effective
    claimed actual arrival, the basis for derived stop_events.
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

    collected_at = Column(DateTime, default=utcnow_naive, index=True)

    # Indexes target the two main access patterns:
    #   - per-pair time series: WHERE trip_id=? AND stop_id=? ORDER BY snapshot_ts
    #   - route-level slices over a time window
    #   - per-trip snapshots over a time window: the lazy-derivation pipeline
    #     filters by `trip_id IN (...)` + `snapshot_ts BETWEEN x AND y` (no
    #     stop_id), and `idx_tu_trip_stop_snap` doesn't help because stop_id
    #     sits between the two filtered columns.
    __table_args__ = (
        Index("idx_tu_trip_stop_snap", "trip_id", "stop_id", "snapshot_ts"),
        Index("idx_tu_route_snap", "route_id", "snapshot_ts"),
        Index("idx_tu_trip_snap", "trip_id", "snapshot_ts"),
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
    derived_at = Column(DateTime, nullable=False, default=utcnow_naive)

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
        # Bare stop time-series queries (e.g., stop-skip rate)
        Index("idx_stop_events_stop_obs", "stop_id", "observed_arrival_ts"),
    )


class Run(Base):
    """
    Per-(service_date, trip_id, source) aggregation over `stop_events`.

    A "run" is one bus's pass through one trip on one service date — the natural
    unit for trip-level metrics like end-to-end excess time (PR #49), per-run
    deviation charts (NOTES-5), and the delivered-runs numerator for
    service-delivered ratio (PR #47). One row per (service_date, trip_id,
    source); the source dimension propagates from stop_events so the
    proximity/trip_update agreement story extends to the run level.

    `vehicle_id` is informational, not in the unique key. The rare same-day
    vehicle reassignment shows up as a single run with the latest non-null
    vehicle_id seen across the run's stop_events; if vehicle-swap-aware
    accounting becomes load-bearing it can graduate to its own column.

    No materialized `is_complete` flag — each downstream metric applies its
    own filter at query time. Useful per-row filters:
      - RUN_EXISTED: stops_observed >= 3
      - RUN_DECENT_COVERAGE: stops_observed * 1.0 / stops_observable >= 0.7
                             AND (max_gap_sec IS NULL OR max_gap_sec < 300)
        (`stops_observable` rather than `stops_scheduled` — see column doc.)

    A literal "both endpoints observed in this row" filter is intentionally
    NOT defined — the data won't support it as a per-source predicate (see
    "Source asymmetry" below). Cross-source endpoint completeness, if ever
    needed, requires joining a trip's TU and proximity rows.

    --- Source asymmetry (load-bearing for endpoint metrics) ---
    The two derivation sources have nearly inverse blind spots at the
    literal scheduled endpoints, measured 2026-05-03 across 6 routes
    (~1.3k runs):
      - TripUpdate observes the literal origin (sched_first_seq) in
        ~0% of runs. WMATA's TU feed only contains a trip after the
        AVL system marks it "active" (typically operator-log-in at /
        after origin departure), and past stops are pruned. By the time
        a trip first appears in the feed, origin's StopTimeUpdate is
        already gone. The first published prediction is for the second
        stop in 87-100% of runs.
      - Proximity observes the literal destination (sched_last_seq) in
        only 0-5% of runs. Layover bays are typically off-route (>50m
        from the published last-stop point), so the bus parks outside
        the proximity radius; ~60s position polling also lets buses
        pass and dwell at the last stop without an in-window ping.

    Consequence: pick the source per endpoint.
      - origin_dev_sec is populated for proximity runs (78-93% literal
        coverage), null for trip_update runs.
      - destination_dev_sec is populated for trip_update runs (87-97%
        literal coverage), rarely populated for proximity runs.
    The OTP origin/destination split (PR #46) reads origin_dev_sec from
    proximity rows and destination_dev_sec from trip_update rows.

    Schedule snapshot fields (`sched_first_arrival_ts`, `sched_last_arrival_ts`,
    `sched_first_seq`, `sched_last_seq`, `stops_scheduled`) are denormalized at
    derivation time. The post-midnight anchor problem is already solved
    upstream — `stop_events.scheduled_arrival_ts` is parsed against the
    stop_event's own service_date — so this aggregation just lifts min/max
    from observed rows. Trips where every stop_event lacks a scheduled match
    still get a row with these fields null.

    `sched_first_seq` / `sched_last_seq` are the actual GTFS endpoint
    stop_sequence values for the trip, queried from `stop_times` at
    derivation time. They cannot be inferred from `stops_scheduled` because
    WMATA's GTFS uses non-contiguous stop_sequence values (almost every
    trip starts at sequence 2, not 1, with arbitrary gaps thereafter).
    """

    __tablename__ = "runs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity — see class docstring on uniqueness key.
    service_date = Column(String, nullable=False)  # YYYY-MM-DD, Eastern operational day
    trip_id = Column(String, nullable=False)
    route_id = Column(String, nullable=False)
    direction_id = Column(Integer, nullable=False)
    source = Column(String, nullable=False)  # 'trip_update' | 'proximity'

    # Vehicle — informational; latest non-null seen across the run's stop_events.
    vehicle_id = Column(String)

    # Schedule context (snapshotted from the underlying stop_events)
    stops_scheduled = Column(Integer)  # count from current GTFS stop_times for this trip
    # stops_observable is the count of stops the source can structurally see
    # for this trip — equal to stops_scheduled for proximity, and
    # stops_scheduled - 1 for trip_update (the GTFS-RT TripUpdates feed only
    # publishes upcoming stops, so the origin row is never present in any
    # snapshot we observe — see "Source asymmetry" below). Use this as the
    # honest denominator for completeness checks; using stops_scheduled
    # bakes in a guaranteed 1-stop miss on every TU run.
    stops_observable = Column(Integer)
    sched_first_seq = Column(Integer)  # min(stop_sequence) in current GTFS for this trip
    sched_last_seq = Column(Integer)  # max(stop_sequence) in current GTFS for this trip
    sched_first_arrival_ts = Column(DateTime)
    sched_last_arrival_ts = Column(DateTime)

    # Observation aggregates
    stops_observed = Column(Integer, nullable=False, default=0)  # rows with observed_arrival_ts
    stops_skipped = Column(Integer, nullable=False, default=0)  # SKIPPED rows for stop-skip rate
    first_obs_seq = Column(Integer)
    last_obs_seq = Column(Integer)
    first_obs_ts = Column(DateTime)  # earliest observed_arrival_ts
    last_obs_ts = Column(DateTime)  # latest observed_arrival_ts
    max_gap_sec = Column(Integer)  # largest gap between consecutive observed arrivals (by ts)

    # Per-stop deviation distribution across observed stops
    dev_p50_sec = Column(Integer)
    dev_p95_sec = Column(Integer)

    # Endpoint deviations — see "Source asymmetry" in class docstring.
    # origin_dev_sec is meaningful from proximity runs; destination_dev_sec
    # from trip_update runs. Both null when the literal endpoint wasn't observed.
    origin_dev_sec = Column(Integer)
    destination_dev_sec = Column(Integer)

    derived_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "service_date",
            "trip_id",
            "source",
            name="uq_runs_service_trip_source",
        ),
        Index("idx_runs_route_date", "route_id", "service_date"),
        Index("idx_runs_trip_date", "trip_id", "service_date"),
    )


class RouteServiceProfile(Base):
    """
    Per-(route, day_type, hour) scheduled service profile derived from GTFS.

    Reference data for downstream metrics: scheduled_trips is the denominator
    for service-delivered ratio (PR #47), and is_frequent flags route-level
    frequent service for UI/filtering. (EWT — see `src/ewt.py` — uses a
    per-(direction, stop, hour) cell-level frequent classification computed
    inline from the schedule, not this route-level flag, because pooling all
    stops on a frequent route pulls in branch-stop sparse-coverage cells.)
    Derived fresh on every GTFS reload — no versioning, the table is
    rewritten in place to match the current GTFS snapshot.

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
    computed_at = Column(DateTime, default=utcnow_naive)

    __table_args__ = (
        Index(
            "idx_route_service_profile_unique",
            "route_id",
            "day_type",
            "hour",
            unique=True,
        ),
    )


class SystemMetricsDaily(Base):
    """
    Pre-computed daily system-wide rollup metrics for the home-page trend
    strip (NOTES-36, materialized in NOTES-48).

    One row per service_date holding the system-level OTP, service-delivered
    ratio, EWT, and bunching rate. Populated by
    `pipelines/upsert_system_metrics_daily.py`, dispatched per-date from
    `pipelines/run_daily_batch.py` after the derivation pipelines commit
    their stop_events / runs rows.

    Why this table exists: the live system-trend rollup over a 60-day window
    (visible 30 + prior 30) costs ~30s on cold cache because EWT and bunching
    require pooling every observed/scheduled cell-hour across every route per
    day. Materializing the rollup turns the trend endpoint into a SELECT plus
    a single-day live compute for "today" — sub-50ms warm, sub-second cold.

    `service_date` is the primary key (`YYYY-MM-DD`, Eastern operational day).
    """

    __tablename__ = "system_metrics_daily"

    service_date = Column(String, primary_key=True)  # YYYY-MM-DD, Eastern service day

    otp_percentage = Column(Float)
    service_delivered_ratio = Column(Float)
    ewt_seconds = Column(Float)
    bunching_rate = Column(Float)

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)


class RouteHeadwayMetrics(Base):
    """
    Per-(route, service_date, time_period) bunching rate, materialized from
    `stop_events` (PR #53).

    Bunching is the rider-experience tail that headway CV averages away: a
    pair of buses arriving abnormally close implies a long gap behind them.
    A pair counts as bunched when the observed headway is below
    max(0.25 × cell-hour mean scheduled headway, 120s) — see `src/bunching.py`
    for threshold rationale (CTA's 0.25× ratio + SFMTA/TransitMatters' 2-min
    floor).

    All routes get rows; non-frequent routes still produce meaningful counts
    (a 5-min observed gap on a 30-min route is a long-gap signal even if
    operational holding doesn't apply). Use `total_headways` to gauge
    sample size — `bunching_rate` is NULL for cells with no eligible
    observed/scheduled pairs.

    Keyed by (route_id, date, time_period). Idempotent re-derivation upserts
    via the unique constraint.
    """

    __tablename__ = "route_headway_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False)
    date = Column(String, nullable=False)  # YYYY-MM-DD, Eastern service day
    time_period = Column(String, nullable=False)  # one of EWT_TIME_PERIODS labels

    day_type = Column(String, nullable=False)  # weekday | saturday | sunday

    # Bunching counts: the metric and its denominator. bunching_rate is NULL
    # iff total_headways == 0 (no eligible pairs in the period).
    bunching_count = Column(Integer, nullable=False, default=0)
    total_headways = Column(Integer, nullable=False, default=0)
    bunching_rate = Column(Float)

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint("route_id", "date", "time_period", name="uq_route_headway_metrics_key"),
        Index("idx_route_headway_metrics_route_date", "route_id", "date"),
    )


class RouteMetricsDailyOverlay(Base):
    """
    Per-(route, service_date) sufficient statistics for the four scorecard
    metrics. Materialized by `pipelines/upsert_route_metrics_overlay.py`,
    read by the windowed-scorecard endpoint.

    Why this table exists: the live windowed compute pulls ~3.27M
    stop_events rows for a 7-day window and runs Python pairing — ~35s
    cold. Materializing per-(route, date) sufficient statistics turns the
    endpoint into 126 × 7 = 882 row reads plus the cross-route aggregator
    pass — sub-100ms cold, no warm-up needed.

    What it stores: **sufficient statistics, not finalized metrics.** The
    EWT formula (AWT = Σh² / 2Σh), the OTP window (±X seconds), and the
    bunching threshold (0.25× scheduled, 120s floor) are all applied in
    code by the API aggregator at read time. If any formula changes,
    Python changes and not this table — that quarantines the brittleness
    that retired the original `route_metrics_daily` (NOTES-19).

    What it does NOT store:
      - day_type or period decomposition — the scorecard's window pools
        across day_types, and the per-route detail page recomputes live.
      - `route_headway_metrics` (per-(route, date, period) bunching) is a
        separate materialization used by the route-detail page; this one
        is the scorecard rollup.

    Re-derivation: `upsert_route_metrics_for_date` is idempotent — re-runs
    against the same (route, service_date) replace the prior row.
    """

    __tablename__ = "route_metrics_daily_overlay"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False)
    service_date = Column(String, nullable=False)  # YYYY-MM-DD, Eastern service day
    day_type = Column(String, nullable=False)  # weekday | saturday | sunday

    # OTP sufficient statistics — one (early, on_time, late) triple per
    # sub-block, mirroring `compute_otp_split`'s output structure. The
    # aggregator sums counts across rows and finalizes pcts at read time.
    # `n` is implicit as early + on_time + late.
    otp_origin_early = Column(Integer, nullable=False, default=0)
    otp_origin_on_time = Column(Integer, nullable=False, default=0)
    otp_origin_late = Column(Integer, nullable=False, default=0)
    otp_destination_early = Column(Integer, nullable=False, default=0)
    otp_destination_on_time = Column(Integer, nullable=False, default=0)
    otp_destination_late = Column(Integer, nullable=False, default=0)
    otp_all_early = Column(Integer, nullable=False, default=0)
    otp_all_on_time = Column(Integer, nullable=False, default=0)
    otp_all_late = Column(Integer, nullable=False, default=0)

    # Service-delivered numerator/denominator.
    scheduled_trips = Column(Integer, nullable=False, default=0)
    delivered_trips = Column(Integer, nullable=False, default=0)

    # EWT sufficient statistics. `AWT = sum_h_sq / (2 · sum_h)` is exact
    # under sums, so windowed AWT/SWT are computed from these directly.
    # `n_*` are convenient denominators for coverage_ratio.
    ewt_obs_sum_h = Column(Float, nullable=False, default=0.0)
    ewt_obs_sum_h_sq = Column(Float, nullable=False, default=0.0)
    ewt_n_observed_headways = Column(Integer, nullable=False, default=0)
    ewt_sched_sum_h = Column(Float, nullable=False, default=0.0)
    ewt_sched_sum_h_sq = Column(Float, nullable=False, default=0.0)
    ewt_n_scheduled_headways = Column(Integer, nullable=False, default=0)

    # Bunching counts.
    bunching_count = Column(Integer, nullable=False, default=0)
    bunching_total_headways = Column(Integer, nullable=False, default=0)

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint("route_id", "service_date", name="uq_route_metrics_overlay_route_date"),
        # Window reads filter by `service_date IN (...)`, then group by
        # route. A date-first index keeps the per-window read narrow.
        Index("idx_route_metrics_overlay_date", "service_date"),
    )


# ---------------------------------------------------------------------------
# Route diagnostic profile (NOTES-57)
# ---------------------------------------------------------------------------
#
# Three sibling tables hold the diagnostic surfaces materialized by
# `src/route_diagnostics.py` and refreshed nightly by
# `pipelines/refresh_route_diagnostic_profile.py`. The split is deliberate —
# each downstream panel reads exactly one shape:
#
#   route_diagnostic_segment   — per-segment slip + cumulative slip
#                                drives slip-trajectory charts (RouteDetail
#                                diagnosis panel, PR #124), stop-pair /
#                                corridor diagnostic (NOTES-59/62),
#                                schedule audit (NOTES-60)
#
#   route_diagnostic_timepoint — per-timepoint behavior classification
#                                drives timepoint behavior table (PR #124),
#                                hold-down candidates (NOTES-61)
#
#   route_diagnostic_direction — per-direction early%/late%/signature
#                                drives direction-asymmetry summary (PR #124)
#
# One denormalized table was considered; rejected because per-segment rows
# are per-edge (~50 per direction per route), per-timepoint rows are
# per-node (~5-10 per direction per route), and per-direction rows are 1
# per direction. Stuffing the three shapes into one table would either
# leave most columns null per row or require a discriminator and downstream
# filters at every read. Three narrow tables keep each panel's query trivial.


class RouteDiagnosticSegment(Base):
    """
    Per-(route_id, direction_id, period, from_seq, to_seq) mean slip and
    cumulative slip. Materialized nightly from `stop_events.source =
    'proximity'` over the last 30 days. NOTES-57.

    Slip = observed segment travel time − scheduled segment travel time,
    averaged across all observed trips in the period. The origin-departure
    segment is excluded (dominated by layover artifact, not real slip);
    see `src/route_diagnostics.py:compute_segment_slip`.

    The `period` dimension carries one of am_peak / midday / pm_peak /
    evening / late / all. The `all` row pools every hour and is the most
    common rendering target; the named-period rows enable time-of-day
    slicing.

    `is_timepoint` is a denormalized flag derived from the 50m haversine
    match between `timepoints` (GTFS-Plus internal stop_ids) and `stops`
    (public GTFS). Stored on the to-stop so panels can mark timepoint
    arrivals on the slip trajectory chart in one pass.

    `cum_slip_sec` carries dual semantics depending on the edge type
    (NOTES-57 fast-follow). On sparse-proximity routes the same
    `from_seq` can produce both a consecutive edge (min `to_seq`) and
    skip-N edges (larger `to_seq`, where intermediate stops didn't ping
    on every trip). Only consecutive edges advance the cumulative walk,
    so:

      - **Consecutive edges** (min `to_seq` per `from_seq`):
        `cum_slip_sec` is the cumulative slip measured at this edge's
        *to-stop*, walking only consecutive edges from origin. This is
        the trajectory-line value the slip charts render.
      - **Skip-N edges** (non-min `to_seq` for a given `from_seq`):
        `cum_slip_sec` is the cumulative slip measured at this edge's
        *from-stop* (the cumsum value just before this edge's origin
        stop in the consecutive walk). The column stays non-nullable;
        skip-N rows' per-edge `mean_slip_sec` remains the meaningful
        quantity for cross-route segment ranking (NOTES-59).

    Origin-departure segment is excluded; see
    `src/route_diagnostics.py:_assemble_segment_slip_output`.
    """

    __tablename__ = "route_diagnostic_segment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False)
    direction_id = Column(Integer, nullable=False)
    period = Column(String, nullable=False)  # am_peak | midday | pm_peak | evening | late | all

    from_seq = Column(Integer, nullable=False)
    from_stop_id = Column(String, nullable=False)
    to_seq = Column(Integer, nullable=False)
    to_stop_id = Column(String, nullable=False)

    mean_slip_sec = Column(Float, nullable=False)
    cum_slip_sec = Column(Float, nullable=False)
    n_observations = Column(Integer, nullable=False)
    is_timepoint = Column(Boolean, nullable=False, default=False)  # to-stop matches a timepoint

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "route_id",
            "direction_id",
            "period",
            "from_seq",
            "to_seq",
            name="uq_route_diag_segment_key",
        ),
        # Most-common read: one route, one period, all directions in order.
        Index(
            "idx_route_diag_segment_route_period",
            "route_id",
            "period",
            "direction_id",
            "from_seq",
        ),
        # Cross-route segment ranking (NOTES-59/62): scan by from_stop/to_stop.
        Index("idx_route_diag_segment_pair", "from_stop_id", "to_stop_id", "period"),
    )


class RouteDiagnosticTimepoint(Base):
    """
    Per-(route_id, direction_id, period, timepoint_stop_id) behavior
    classification with the entering/leaving distribution summaries that
    justify the label. Materialized nightly. NOTES-57.

    Classification values (from `src/route_diagnostics.py:classify_timepoint`):
      - `recovery`     — median deviation drops ≥ 120s across the timepoint
      - `leaky`        — p10 drops ≥ 180s downstream (early-departure bleed)
      - `underpowered` — median entering ≥ 120s, no material compression
      - `neutral`      — median in ±60s entering, no notable shift

    Insufficient-sample timepoints (< 30 observations on either side) are
    suppressed at the source — they don't get rows here. The renderer
    treats missing rows as "no data" rather than emitting a row with a
    null classification.

    The hold-down candidates page (NOTES-61) reads
    `classification = 'leaky'` over all routes and ranks by p10 drop;
    the timepoint behavior table on RouteDetail (PR #124) reads one
    `route_id` at a time.
    """

    __tablename__ = "route_diagnostic_timepoint"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False)
    direction_id = Column(Integer, nullable=False)
    period = Column(String, nullable=False)
    timepoint_stop_id = Column(String, nullable=False)

    classification = Column(String, nullable=False)  # recovery | leaky | underpowered | neutral

    # Distribution summaries that justify the classification — surfaced in
    # the RouteDetail timepoint table so the badge isn't a black box.
    median_dev_entering = Column(Float)
    median_dev_leaving = Column(Float)
    p10_dev_entering = Column(Float)
    p10_dev_leaving = Column(Float)

    n_observations = Column(Integer, nullable=False)

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "route_id",
            "direction_id",
            "period",
            "timepoint_stop_id",
            name="uq_route_diag_timepoint_key",
        ),
        # RouteDetail diagnosis panel (PR #124) read: one route, one period.
        Index(
            "idx_route_diag_timepoint_route_period",
            "route_id",
            "period",
            "direction_id",
        ),
        # NOTES-61 hold-down candidates read: scan by classification.
        Index(
            "idx_route_diag_timepoint_classification",
            "classification",
            "period",
        ),
    )


class RouteDiagnosticDirection(Base):
    """
    Per-(route_id, direction_id, period) early% / late% / signature.
    Materialized nightly. NOTES-57.

    The signature is one of:
      - `early_dominant` — early% > late% + 5pp
      - `late_dominant`  — late% > early% + 5pp
      - `balanced`       — within the 5pp margin

    Reads OTP-style buckets (−2 / +7 minute window, mirroring
    `src/otp_constants.py`) but does not store on_time% — it's
    `100 − early% − late%` if a panel wants it.

    Sample size guard lives upstream — rows with no observations at all in
    the period are suppressed; the renderer can scale visual emphasis by
    `n_observations` if it wants.
    """

    __tablename__ = "route_diagnostic_direction"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_id = Column(String, nullable=False)
    direction_id = Column(Integer, nullable=False)
    period = Column(String, nullable=False)

    early_pct = Column(Float, nullable=False)
    late_pct = Column(Float, nullable=False)
    signature = Column(String, nullable=False)  # early_dominant | late_dominant | balanced
    n_observations = Column(Integer, nullable=False)

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "route_id",
            "direction_id",
            "period",
            name="uq_route_diag_direction_key",
        ),
        Index("idx_route_diag_direction_route_period", "route_id", "period"),
    )


# ---------------------------------------------------------------------------
# Cross-route segment rollup (NOTES-59)
# ---------------------------------------------------------------------------
#
# Materialized nightly by `pipelines/refresh_cross_route_segments.py`.
# Aggregates per-segment slip from `route_diagnostic_segment` across all
# routes that traverse the same (from_stop_id, to_stop_id) stop-pair.
#
# V1 uses stop-pair identity only — two routes that share the same stop_ids
# for a given from→to segment are counted as traversing the same segment.
# Shape-aware corridor rollup (NOTES-62) is deferred so V1 ships without
# geometric matching infrastructure.
#
# One row per (from_stop_id, to_stop_id, period).  Route-level breakdown
# is stored as a JSON array in `contributing_routes_json` to avoid a
# separate join table.


class CrossRouteSegmentRollup(Base):
    """
    Per-(from_stop_id, to_stop_id, period) aggregated slip across all routes
    that traverse the stop-pair. Materialized nightly by
    ``pipelines/refresh_cross_route_segments.py``. NOTES-59.

    Only stop-pairs traversed by at least 2 distinct routes are included;
    single-route stop-pairs carry no cross-route signal and clutter the
    infrastructure-investment ranked list.

    ``total_weighted_slip_sec`` is the sum of ``mean_slip_sec *
    n_observations`` across all contributing (route, direction) rows for
    the pair — a trip-volume-weighted total so busier corridors rank
    higher when they also have higher slip.

    ``n_routes`` is the count of distinct route_ids contributing to the
    pair; ``n_route_directions`` is the count of distinct
    (route_id, direction_id) rows, which can exceed ``n_routes`` when
    the same stop-pair appears in both directions of a route.

    ``contributing_routes_json`` is a JSON array of
    ``{"route_id": ..., "route_short_name": ..., "direction_id": ...,
    "mean_slip_sec": ..., "n_observations": ...}`` sorted by
    ``n_observations`` descending — the per-route drilldown without a
    second round-trip.

    ``peak_period`` records which named period (am_peak / midday /
    pm_peak / evening / late) has the highest total_weighted_slip_sec
    for this pair — the "peak hour" column in the ranked list. Null
    when ``period != 'all'`` (already filtered to a named period).
    """

    __tablename__ = "cross_route_segment_rollup"

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_stop_id = Column(String, nullable=False)
    to_stop_id = Column(String, nullable=False)
    period = Column(String, nullable=False)  # am_peak | midday | pm_peak | evening | late | all

    total_weighted_slip_sec = Column(Float, nullable=False)
    n_routes = Column(Integer, nullable=False)
    n_route_directions = Column(Integer, nullable=False)
    n_total_observations = Column(Integer, nullable=False)
    contributing_routes_json = Column(String, nullable=False)  # JSON array, see docstring
    peak_period = Column(String, nullable=True)  # populated only on period='all' rows

    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "from_stop_id",
            "to_stop_id",
            "period",
            name="uq_cross_route_segment_key",
        ),
        # Primary read: ranked list scan for a given period.
        Index(
            "idx_cross_route_segment_period",
            "period",
            "total_weighted_slip_sec",
        ),
        # Drilldown: look up one stop-pair across periods.
        Index(
            "idx_cross_route_segment_pair",
            "from_stop_id",
            "to_stop_id",
        ),
    )


# ---------------------------------------------------------------------------
# LLM-generated route diagnosis narrative (PR #141)
# ---------------------------------------------------------------------------
#
# Narrative text is generated offline by `scripts/generate_route_diagnosis.py`
# (calls Claude; requires ANTHROPIC_API_KEY) and cached here. The API serves
# the cache read-only — Claude is never called at request time. Staleness is
# detected by comparing `profile_snapshot_hash` (SHA256 of the current
# route_diagnostic_* rows) against the hash stored at generation time.


class RouteDiagnosisNarrative(Base):
    """
    Cached LLM-generated narrative for one (route_id, period). PR #141.

    Written by ``scripts/generate_route_diagnosis.py`` (offline; requires
    ANTHROPIC_API_KEY). Read by ``GET /api/routes/{id}/diagnosis?period=``.
    The API never calls Claude — it only serves rows already here.

    Staleness detection: ``profile_snapshot_hash`` is a SHA-256 hex digest
    of the canonicalized JSON of the ``route_diagnostic_segment`` +
    ``route_diagnostic_timepoint`` rows for the same ``(route_id, period)``
    at generation time. The API endpoint recomputes the current hash and
    sets ``is_stale=True`` in the response when the two hashes differ —
    indicating the diagnostic profile changed since the narrative was
    generated. Regeneration is always manual (re-run the CLI script).

    ``prompt_version`` is a short string (e.g. ``"v1"``) baked into the
    CLI so cache invalidation can be forced by bumping the version even
    when the profile data hasn't changed.
    """

    __tablename__ = "route_diagnosis_narrative"

    route_id = Column(String, primary_key=True)
    period = Column(String, primary_key=True)  # all | am_peak | midday | pm_peak | evening | late

    narrative = Column(String, nullable=False)
    generated_at = Column(DateTime, nullable=False)  # naive UTC
    model_id = Column(String, nullable=False)
    prompt_version = Column(String, nullable=False)
    profile_snapshot_hash = Column(String, nullable=False)  # SHA-256 hex

    __table_args__ = (Index("idx_route_diagnosis_narrative_route", "route_id"),)
