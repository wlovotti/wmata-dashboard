"""
FastAPI application for WMATA Performance Dashboard API

This API serves pre-computed transit performance metrics for the web dashboard.
Endpoints provide route-level OTP, headway, and speed data.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.aggregations import (
    _latest_service_date_with_stop_events,
    compute_block_timeline,
    get_all_routes_scorecard,
    get_live_metrics_for_window,
    get_route_blocks,
    get_route_bunching_causes,
    get_route_contributors,
    get_route_detail_metrics,
    get_route_period_drilldown,
    get_route_recent_runs,
    get_route_stop_diagnostics,
    get_route_time_period_summary,
    get_route_trend_data,
    get_run_deviations,
    get_system_trend_data,
)
from src.database import get_session
from src.models import GTFSSnapshot, VehiclePosition
from src.time_periods import (
    ALL_DAY_TYPES,
    ALL_HOURS,
    VALID_DAY_TYPES,
    VALID_PERIOD_KEYS,
)
from src.timezones import eastern_today, utcnow_naive

logger = logging.getLogger(__name__)


def _warm_scorecard_cache_sync():
    """Compute the windowed scorecard once so the first user request finds a warm cache.

    Runs in a background thread at app startup. The cold compute is ~30-40s
    (7 days × per-date EWT/bunching/SD/OTP); paying it once at boot beats
    making the first user wait. Subsequent requests within the 1-hour
    per-date TTL hit dict-lookup speed.
    """
    try:
        db = get_session()
        try:
            end_date = _latest_service_date_with_stop_events(db)
            if end_date is None:
                logger.info("Scorecard warm-up skipped: no derived stop_events yet")
                return
            get_live_metrics_for_window(db, end_date, 7)
            logger.info("Scorecard cache warmed for window ending %s", end_date)
        finally:
            db.close()
    except Exception:
        # Warming is best-effort — never let it crash startup. A failure
        # here just means the first user request pays the cold cost.
        logger.exception("Scorecard cache warm-up failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Kick off background cache warming at startup, no-op on shutdown."""
    asyncio.create_task(asyncio.to_thread(_warm_scorecard_cache_sync))
    yield


# Create FastAPI app
app = FastAPI(
    title="WMATA Performance API",
    description="REST API for WMATA transit performance metrics",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """API root - health check"""
    return {"status": "ok", "name": "WMATA Performance API", "version": "1.0.0", "docs": "/docs"}


@app.get("/health")
async def health_check():
    """
    Comprehensive health check endpoint for monitoring

    Checks:
    - API service status
    - Database connectivity
    - Recent data collection activity

    Returns:
        Health status with component details and timestamps
    """
    health_status = {
        "status": "healthy",
        "timestamp": utcnow_naive().isoformat(),
        "service": "wmata-dashboard-api",
        "version": "1.0.0",
        "checks": {},
    }

    # Check database connectivity
    try:
        db = get_session()
        try:
            # Test database connection with a simple query
            db.execute("SELECT 1").scalar()
            health_status["checks"]["database"] = {
                "status": "healthy",
                "message": "Database connection successful",
            }

            # Check for recent data collection (last 5 minutes)
            five_minutes_ago = utcnow_naive() - timedelta(minutes=5)
            recent_data_count = (
                db.query(VehiclePosition)
                .filter(VehiclePosition.timestamp >= five_minutes_ago)
                .count()
            )

            if recent_data_count > 0:
                health_status["checks"]["data_collection"] = {
                    "status": "healthy",
                    "message": f"Recent data: {recent_data_count} vehicle positions in last 5 min",
                }
            else:
                health_status["checks"]["data_collection"] = {
                    "status": "warning",
                    "message": "No recent data collected in last 5 minutes",
                }
                health_status["status"] = "degraded"
        finally:
            db.close()
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "message": f"Database connection failed: {str(e)}",
        }

    return health_status


@app.get("/api/gtfs/freshness")
async def get_gtfs_freshness():
    """
    Return metadata for the most recent GTFS snapshot.

    Thin observability endpoint so the dashboard can surface a "schedule
    current as of …" indicator and a stale schedule is visible instead of
    silent. Returns the newest row from `gtfs_snapshots` (by `snapshot_date`,
    falling back to `created_at` for tie-breaks). When the table is empty
    (fresh DB, no reload yet) every field is null.

    Returns:
        Dict with `snapshot_date`, `created_at`, `feed_version`, and the
        per-table counts at load time. Datetimes are ISO8601 strings (naive
        UTC, matching storage convention); the frontend converts to Eastern
        for display.
    """
    db = get_session()
    try:
        snapshot = (
            db.query(GTFSSnapshot)
            .order_by(GTFSSnapshot.snapshot_date.desc(), GTFSSnapshot.created_at.desc())
            .first()
        )
        if snapshot is None:
            return {
                "snapshot_date": None,
                "created_at": None,
                "feed_version": None,
                "routes_count": None,
                "stops_count": None,
                "trips_count": None,
            }
        return {
            "snapshot_date": snapshot.snapshot_date.isoformat() if snapshot.snapshot_date else None,
            "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
            "feed_version": snapshot.feed_version,
            "routes_count": snapshot.routes_count,
            "stops_count": snapshot.stops_count,
            "trips_count": snapshot.trips_count,
        }
    finally:
        db.close()


@app.get("/api/routes")
async def get_routes(days: int = 7):
    """
    Get performance scorecard for all routes, pooled over a rolling window.

    Each metric is pooled across `[end_date - days + 1, end_date]` where
    `end_date` is the latest service_date with derived stop_events. Returns
    `{window: {start, end, days}, routes: [...]}` so the frontend can label
    the visible date range. Used by the dashboard landing page.

    Args:
        days: Window length in days (default: 7, capped at 30 to bound the
            cold-cache cost)

    Returns:
        Dict with `window` and `routes` (route summaries sorted by OTP desc).
    """
    if days < 1:
        days = 1
    if days > 30:
        days = 30
    db = get_session()
    try:
        return get_all_routes_scorecard(db, days=days)
    finally:
        db.close()


@app.get("/api/routes/contributors")
async def get_routes_contributors(metric: str = "otp", days: int = 30):
    """
    Routes ranked by their contribution to system underperformance (NOTES-39).

    For each route, contribution is `(baseline - route_value) * scheduled_trips`
    for higher-is-better metrics (OTP, service-delivered) — sign-flipped for
    lower-is-better metrics (EWT, bunching) so a positive score always means
    "this route is dragging the system down." Baseline is the system's
    window-mean from `system_metrics_daily` (no per-route targets exist yet;
    NOTES-47 will swap `baseline` for `target` once they do). Scheduled-trip
    count over the window is the only volume proxy in the data — ridership
    is not available — so the score answers "where would moving the needle
    move the system most?" rather than just "which route looks worst."

    For OTP, `route_value` is the window mean computed live from
    `stop_events`. For service-delivered, EWT, and bunching, `route_value`
    is the latest single-day snapshot from the live-metrics cache (those
    metrics aren't materialized per-route per-day; a window mean would
    require N× per-day computes per route). The baseline is always a
    window mean from `system_metrics_daily`.

    Args:
        metric: One of `otp`, `service_delivered`, `ewt`, `bunching`
        days: Window length in days (default: 30, capped at 90)

    Returns:
        Dict with `metric`, `days`, `baseline_value`, `higher_is_better`,
        and `contributors` (list ranked by `contribution_score` desc).
    """
    valid_metrics = ["otp", "service_delivered", "ewt", "bunching"]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400, detail=f"Invalid metric. Must be one of: {', '.join(valid_metrics)}"
        )
    if days < 1:
        days = 1
    if days > 90:
        days = 90

    db = get_session()
    try:
        return get_route_contributors(db, metric=metric, days=days)
    finally:
        db.close()


@app.get("/api/routes/{route_id}")
async def get_route(
    route_id: str,
    days: int = 7,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
):
    """
    Get detailed metrics for a specific route

    Returns current performance metrics and metadata for a route.
    Used by the route detail page header.

    `day_type` and `period` (NOTES-41) re-slice the live KPIs (OTP split,
    EWT, bunching) by day-of-week and time-of-day Eastern hour.
    Excess-trip-time and service-delivered are trip-level so `period`
    doesn't change their values; `day_type` does, by anchoring on the
    latest matching service_date.

    Args:
        route_id: Route identifier (e.g., 'C51')
        days: Window for the excess-trip-time freshest-day lookup
        day_type: One of `all` (default), `weekday`, `saturday`, `sunday`
        period: One of `all` (default), `am_peak`, `midday`, `pm_peak`,
            `evening`, `late`

    Returns:
        Detailed route metrics — live OTP/EWT/bunching, service-delivered,
        excess-trip-time, frequency class. Echoes `day_type_filter` and
        `period_key` so the UI can render the active-filter chip without
        holding extra state.
    """
    if day_type not in VALID_DAY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day_type. Must be one of: {', '.join(VALID_DAY_TYPES)}",
        )
    if period not in VALID_PERIOD_KEYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(VALID_PERIOD_KEYS)}",
        )

    db = get_session()
    try:
        result = get_route_detail_metrics(
            db,
            route_id,
            days=days,
            day_type_filter=day_type,
            period_key=period,
        )
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/routes/{route_id}/trend")
async def get_route_trend(
    route_id: str,
    metric: str = "otp",
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
):
    """
    Get time-series trend data for a specific route metric

    Returns daily values for a metric over time, used for trend charts.

    `day_type` (NOTES-41) emits null on dates whose day-of-week doesn't
    match (sparkline draws gaps cleanly rather than collapsing the time
    axis). `period` is meaningful only for `metric=otp` — when set, OTP
    is recomputed per-day from `stop_events` with the hour filter applied.
    `service_delivered` and `excess_trip_time` are trip-level and ignore
    `period`.

    Args:
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'service_delivered',
            'excess_trip_time')
        days: Number of days to analyze (default: 30)
        day_type: One of `all` (default), `weekday`, `saturday`, `sunday`
        period: One of `all` (default), `am_peak`, `midday`, `pm_peak`,
            `evening`, `late`. Only applies to `metric=otp`.

    Returns:
        Time-series data with daily values for the specified metric
    """
    valid_metrics = [
        "otp",
        "service_delivered",
        "excess_trip_time",
    ]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400, detail=f"Invalid metric. Must be one of: {', '.join(valid_metrics)}"
        )
    if day_type not in VALID_DAY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day_type. Must be one of: {', '.join(VALID_DAY_TYPES)}",
        )
    if period not in VALID_PERIOD_KEYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(VALID_PERIOD_KEYS)}",
        )

    db = get_session()
    try:
        result = get_route_trend_data(
            db,
            route_id,
            metric=metric,
            days=days,
            day_type_filter=day_type,
            period_key=period,
        )
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/system/trend")
async def get_system_trend(metric: str = "otp", days: int = 30):
    """
    System-level trend rollup for the home-page trend strip (NOTES-36).

    Returns 30 days (or `days`) of one system metric — OTP / service-delivered
    / EWT / bunching — plus a `prior_window_value` summarizing the immediately
    prior `days` window so the frontend can render a 30-vs-prior-30 delta.

    Args:
        metric: One of `otp`, `service_delivered`, `ewt`, `bunching`
        days: Length of the visible window in days (default: 30, capped at 90
            to bound the EWT/bunching cold-cache cost)

    Returns:
        Dict with `metric`, `days`, `trend_data` (list of `{date, <metric_key>}`),
        and `prior_window_value` (float or null).
    """
    valid_metrics = ["otp", "service_delivered", "ewt", "bunching"]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400, detail=f"Invalid metric. Must be one of: {', '.join(valid_metrics)}"
        )
    if days < 1:
        days = 1
    if days > 90:
        days = 90

    db = get_session()
    try:
        return get_system_trend_data(db, metric=metric, days=days)
    finally:
        db.close()


@app.get("/api/routes/{route_id}/time-periods")
async def get_route_time_periods(route_id: str, days: int = 7):
    """
    Get performance metrics by time of day

    Returns OTP and headway broken down by time periods
    (AM Peak, Midday, PM Peak, Evening, Night).

    Args:
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Performance metrics grouped by time period
    """
    db = get_session()
    try:
        result = get_route_time_period_summary(db, route_id, days=days)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/routes/{route_id}/period-drilldown")
async def get_route_period_drilldown_endpoint(route_id: str):
    """
    Per-time-period EWT and bunching for one route on the latest service_date.

    Returns the AM peak / midday / PM peak / evening / night breakdown that
    the headline scorecard fields (`ewt_seconds`, `bunching_rate`) collapse.
    Anchors on the same service_date as the headline so the rows reconcile.

    Args:
        route_id: Route identifier (e.g., 'C51')

    Returns:
        EWT and bunching rows keyed by time_period, plus the anchor service_date.
    """
    db = get_session()
    try:
        result = get_route_period_drilldown(db, route_id)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/routes/{route_id}/stops")
async def get_route_stop_diagnostics_endpoint(
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
    direction_id: int | None = None,
):
    """
    Stop-level diagnostic metrics for one route over a window (NOTES-40).

    Returns per-(direction_id, stop_id) median/p95 deviation, OTP%, skip%,
    and observation counts along the route's canonical stop sequence —
    the answer to "where on the route do trips slip?" Output is ordered
    by direction_id ASC then stop_sequence ASC, surfaceable as a strip
    chart from origin to destination per direction.

    Per the CLAUDE.md `stop_id` direction rule, aggregation groups
    strictly by (route_id, direction_id, stop_id) — termini and shared
    bays serve both directions under one stop_id and double-count
    without it. The canonical sequence is the longest trip per direction
    (handles partial / express variants by surfacing the longest superset).

    `day_type` and `period` (NOTES-41) re-slice the per-stop aggregations:
    `day_type` filters by service-day-of-week, `period` filters by Eastern
    hour of `observed_arrival_ts` (or `scheduled_arrival_ts` for SKIPPED
    rows that have no observed timestamp). Skip-rate denominator is the
    count of trip_update rows for the stop — proximity never emits
    SKIPPED, so it doesn't contribute to either numerator or denominator.

    Args:
        route_id: Route identifier (e.g., 'C51').
        days: Window length in days (default: 30).
        day_type: One of `all` (default), `weekday`, `saturday`, `sunday`.
        period: One of `all` (default), `am_peak`, `midday`, `pm_peak`,
            `evening`, `late`.
        direction_id: Optional — restrict output to one direction (0 or 1).

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`, and `stops`
        (list ordered by direction_id ASC then stop_sequence ASC).
    """
    if day_type not in VALID_DAY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day_type. Must be one of: {', '.join(VALID_DAY_TYPES)}",
        )
    if period not in VALID_PERIOD_KEYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(VALID_PERIOD_KEYS)}",
        )
    if days < 1:
        days = 1
    if days > 90:
        days = 90
    if direction_id is not None and direction_id not in (0, 1):
        raise HTTPException(
            status_code=400,
            detail="Invalid direction_id. Must be 0 or 1 if provided.",
        )

    db = get_session()
    try:
        return get_route_stop_diagnostics(
            db,
            route_id,
            days=days,
            day_type=day_type,
            period=period,
            direction_id=direction_id,
        )
    finally:
        db.close()


@app.get("/api/routes/{route_id}/bunching-causes")
async def get_route_bunching_causes_endpoint(
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
):
    """
    Bunching cause decomposition for one route over a window (NOTES-42).

    For every bunched pair on the route, classify by leader/trailer schedule
    deviation against the WMATA OTP window (-2/+7 min):

    - `leader_late_only`: running-time / recovery problem (leader fell
      behind, trailer wasn't early — gap closed because the leader
      couldn't recover)
    - `trailer_early_only`: dispatch / departure-discipline problem
      (leader on schedule, trailer rolled out early and caught up)
    - `both_off`: compounding (both interventions apply)
    - `neither_off`: both within the OTP window — the trailer compressed
      running time without crossing the early threshold; documented but
      not operationally featured
    - `unknown`: at least one side has no schedule match

    The mechanism is textbook bus-bunching theory (late leaders pick up
    more passengers, extending dwell; trailers run light and catch up;
    Wikipedia "Bus bunching"; Tandfonline 2024 review of bunching control
    strategies). The five-bucket decomposition surfaced here is internal
    to this dashboard — useful diagnostic, not an industry-standard or
    transit-agency-published metric.

    `day_type` and `period` (NOTES-41) re-slice the same way as the rest
    of the per-route surface: `day_type` filters by service-day-of-week,
    `period` filters by Eastern hour of the leader's observed arrival.

    Args:
        route_id: Route identifier (e.g., 'C51').
        days: Window length in days (default: 30).
        day_type: One of `all` (default), `weekday`, `saturday`, `sunday`.
        period: One of `all` (default), `am_peak`, `midday`, `pm_peak`,
            `evening`, `late`.

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`,
        `n_bunched_pairs`, and `breakdown` (per-category count + pct).
    """
    if day_type not in VALID_DAY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day_type. Must be one of: {', '.join(VALID_DAY_TYPES)}",
        )
    if period not in VALID_PERIOD_KEYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(VALID_PERIOD_KEYS)}",
        )
    if days < 1:
        days = 1
    if days > 90:
        days = 90

    db = get_session()
    try:
        return get_route_bunching_causes(
            db,
            route_id,
            days=days,
            day_type=day_type,
            period=period,
        )
    finally:
        db.close()


@app.get("/api/routes/{route_id}/shapes")
async def get_route_shapes(route_id: str):
    """
    Get GTFS shapes data for a route

    Returns geographic coordinates for drawing the route on a map.

    Args:
        route_id: Route identifier (e.g., 'C51')

    Returns:
        List of shape variants with coordinate arrays
    """
    from src.models import Shape, Trip

    db = get_session()
    try:
        # Get distinct shape_ids for this route
        shape_ids = db.query(Trip.shape_id).filter(Trip.route_id == route_id).distinct().all()

        if not shape_ids:
            raise HTTPException(status_code=404, detail=f"No shapes found for route {route_id}")

        shapes_data = []
        for (shape_id,) in shape_ids:
            if not shape_id:
                continue

            # Get all points for this shape, ordered by sequence
            points = (
                db.query(Shape)
                .filter(Shape.shape_id == shape_id)
                .order_by(Shape.shape_pt_sequence)
                .all()
            )

            if points:
                shapes_data.append(
                    {
                        "shape_id": shape_id,
                        "points": [{"lat": p.shape_pt_lat, "lon": p.shape_pt_lon} for p in points],
                    }
                )

        return {"route_id": route_id, "shapes": shapes_data}
    finally:
        db.close()


@app.get("/api/routes/{route_id}/recent-runs")
async def get_route_recent_runs_endpoint(route_id: str, limit: int = 25):
    """
    Recent runs for a route — populates the RouteDetail "Recent runs" list.

    Returns up to `limit` runs from the latest service_date that has runs for
    this route (today if available, otherwise the most recent date with any
    runs). Each row carries the run-summary fields needed for the list view
    plus the headsign and start/end times in Eastern HH:MM. The chart is
    fetched on-demand from `/api/runs/{run_id}/deviations` when a row is
    clicked.

    Args:
        route_id: Route identifier (e.g., 'C51')
        limit: Max number of runs to return (default: 25, capped here too)

    Returns:
        Recent-runs envelope with the chosen service_date and the run list.
    """
    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    db = get_session()
    try:
        result = get_route_recent_runs(db, route_id, limit=limit)
        if isinstance(result, dict) and result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/runs/{run_id}/deviations")
async def get_run_deviations_endpoint(run_id: int):
    """
    Per-stop schedule deviations for one run — feeds the per-run drift chart.

    The list is one row per scheduled stop on the run's trip ordered by
    stop_sequence; rows for stops without an observed `stop_event` carry null
    `actual` / `deviation_sec` so the chart renders gaps cleanly. Times are
    serialized as ISO8601 strings already converted to Eastern (the storage
    convention is naive UTC; conversion happens at the API boundary).

    Args:
        run_id: Internal `runs.id` (autoincrement int from the runs table)

    Returns:
        Run summary plus a `deviations` list with stop_sequence, stop_id,
        stop_name, scheduled, actual, and deviation_sec per stop.
    """
    db = get_session()
    try:
        result = get_run_deviations(db, run_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        return result
    finally:
        db.close()


def _parse_service_date_param(service_date: str | None):
    """Parse a YYYY-MM-DD query string to a `date`, defaulting to Eastern today.

    Centralized so the block endpoints share one parsing path. Raises
    HTTPException(400) for malformed input — pulling this into the
    endpoints keeps them slim.
    """
    from datetime import date as date_type

    if service_date is None or service_date == "":
        return eastern_today()
    try:
        return date_type.fromisoformat(service_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid service_date {service_date!r}; expected YYYY-MM-DD",
        ) from exc


@app.get("/api/routes/{route_id}/blocks")
async def get_route_blocks_endpoint(route_id: str, service_date: str | None = None):
    """
    List blocks that touch one route on one service date (NOTES-45).

    Populates the "Blocks" tab on RouteDetail. Each row carries the block_id,
    the trip count chained on the block (across all routes the block serves),
    how many of those trips are on this route, the scheduled origin time in
    Eastern, and the worst per-trip absolute deviation observed.

    Args:
        route_id: Route identifier (e.g., 'C51').
        service_date: YYYY-MM-DD Eastern date. Defaults to today (Eastern).

    Returns:
        Dict with `route_id`, `service_date`, and `blocks` (ordered by the
        block's earliest scheduled start).
    """
    sd = _parse_service_date_param(service_date)
    db = get_session()
    try:
        result = get_route_blocks(db, route_id, sd)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/blocks/{block_id}")
async def get_block_timeline_endpoint(block_id: str, service_date: str | None = None):
    """
    Block timeline for one block on one service date (NOTES-45).

    Returns the scheduled chain of trips for `(block_id, service_date)`,
    each annotated with origin/destination deviation (per the
    `src/otp_metrics.py` source-asymmetry rule — origin from proximity,
    destination from trip_update), the observed vehicle_id, and a coarse
    status. Trips with no observations on the day still appear in the
    chain so the planning context is visible; the frontend renders them
    greyed out.

    Args:
        block_id: GTFS block_id (e.g., '3').
        service_date: YYYY-MM-DD Eastern date. Defaults to today (Eastern).

    Returns:
        Dict with `block_id`, `service_date`, and `trips` (ordered by
        scheduled start).
    """
    sd = _parse_service_date_param(service_date)
    db = get_session()
    try:
        result = compute_block_timeline(db, block_id, sd)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Block {block_id} not found in current GTFS",
            )
        return result
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
