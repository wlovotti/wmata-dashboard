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
    get_active_blocks,
    get_all_routes_scorecard,
    get_corridor_constituent_segments,
    get_corridor_rollup,
    get_cross_route_segments,
    get_live_metrics_for_window,
    get_route_blocks,
    get_route_bunching_causes,
    get_route_contributors,
    get_route_detail_metrics,
    get_route_diagnosis,
    get_route_diagnostic_profile,
    get_route_period_drilldown,
    get_route_recent_runs,
    get_route_stop_diagnostics,
    get_route_time_period_summary,
    get_route_trend_data,
    get_run_deviations,
    get_schedule_audit,
    get_system_trend_data,
)
from api.config import settings
from src.database import get_session
from src.models import Corridor, GTFSSnapshot, VehiclePosition
from src.route_diagnostics import ALL_PERIODS as DIAGNOSTIC_PERIODS
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
    allow_origins=settings.cors_allow_origins,  # dev: ["*"]; prod: set CORS_ALLOW_ORIGINS
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

    For each route, contribution is `(reference - route_value) * scheduled_trips`
    for higher-is-better metrics (OTP, service-delivered) — sign-flipped for
    lower-is-better metrics (EWT, bunching) so a positive score always means
    "this route is dragging the system down." The reference is the route's
    configured target from `config/route_targets.yaml` when set (PR #99),
    otherwise the system 30-day window mean from `system_metrics_daily`
    (`baseline_value`). Each row reports `reference_source` so the frontend
    can disclose which was used. Scheduled-trip count over the window is
    the only volume proxy in the data — ridership is not available — so
    the score answers "where would moving the needle move the system
    most?" rather than just "which route looks worst."

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


@app.get("/api/blocks/active")
async def get_active_blocks_endpoint(service_date: str | None = None, limit: int = 100):
    """
    System-level list of active blocks for one service date (PR #105).

    Powers the `/blocks` index page in the dashboard. Returns every block
    whose GTFS trips run on `service_date`, ranked by trip count desc
    then worst observed origin/destination deviation desc — the longest,
    most cascade-prone dispatch chains land at the top. Each row carries
    the block_id, the trips chained on it, the routes the block touches,
    its scheduled origin time in Eastern, and the worst absolute
    deviation observed across the chain.

    Args:
        service_date: YYYY-MM-DD Eastern date. Defaults to today (Eastern).
        limit: Cap on the number of returned blocks (default 100, max 500).

    Returns:
        Dict with `service_date` and `blocks` (ranked list).
    """
    sd = _parse_service_date_param(service_date)
    db = get_session()
    try:
        return get_active_blocks(db, sd, limit=limit)
    finally:
        db.close()


@app.get("/api/targets")
async def get_targets_endpoint():
    """
    Return the full per-route + system-default targets payload (PR #105).

    Reads `config/route_targets.yaml` via `src/route_targets.py` (which
    reloads lazily on file mtime change). Powers the `/targets` page in
    the dashboard, which renders the system defaults plus per-route
    overrides read-only — editing stays git-only.

    Returns:
        Dict with `system_default` (mapping of metric -> canonical-units
        value), `routes` (mapping of route_id -> per-metric overrides),
        and `metrics` (the list of canonical metric keys the schema knows
        about). Canonical units are OTP percent, service_delivered
        fraction, EWT seconds, bunching fraction. Missing per-route
        metrics inherit the system default — the frontend can compute
        the effective value with `route_value ?? system_default`.
    """
    from src.route_targets import get_all_targets

    return get_all_targets()


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


@app.get("/api/routes/{route_id}/diagnostic_profile")
async def get_route_diagnostic_profile_endpoint(
    route_id: str,
    period: str = "all",
):
    """
    Pre-materialized diagnostic profile for one route and time-of-day period (PR #124).

    Reads the three ``route_diagnostic_*`` tables materialized nightly by
    ``pipelines/refresh_route_diagnostic_profile.py`` (PR #107) and returns
    them in one response so the RouteDetail diagnosis panel can populate the
    slip-trajectory chart and timepoint-behavior table in a single fetch.

    Returns three parallel lists:

    - ``segments`` — per-segment mean slip and cumulative slip ordered by
      ``(direction_id, from_seq)``. Each row carries ``from_stop_name`` and
      ``to_stop_name`` (joined from current GTFS) and ``is_timepoint`` so
      the chart can mark schedule-checkpoints on the trajectory line without
      a separate fetch. Slip sign convention: positive = bus ran slower than
      scheduled (under-padded); negative = faster (over-padded / recovery).
    - ``timepoints`` — per-timepoint behavior classification ordered by
      ``(direction_id, timepoint_stop_id)``. Includes ``stop_name`` and the
      four distribution summaries that justify the classification badge
      (``median_dev_entering``, ``median_dev_leaving``, ``p10_dev_entering``,
      ``p10_dev_leaving``).
    - ``direction_asymmetry`` — per-direction early%/late%/signature ordered
      by ``direction_id``.

    Returns empty lists for each surface when no materialized data exists
    for the route+period. This is the normal state before the pipeline has
    run or for routes with insufficient stop_events coverage.

    Args:
        route_id: Route identifier (e.g., ``'D80'``).
        period: One of ``all`` (default), ``am_peak``, ``midday``,
            ``pm_peak``, ``evening``, ``late``.

    Returns:
        Dict with ``route_id``, ``period``, ``segments``, ``timepoints``,
        and ``direction_asymmetry``.
    """
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )

    db = get_session()
    try:
        return get_route_diagnostic_profile(db, route_id, period=period)
    finally:
        db.close()


@app.get("/api/routes/{route_id}/diagnosis")
async def get_route_diagnosis_endpoint(
    route_id: str,
    period: str = "all",
):
    """
    Cached LLM narrative for one route's diagnostic profile (PR #141).

    Reads from ``route_diagnosis_narrative`` — a cache table written offline
    by ``scripts/generate_route_diagnosis.py``. This endpoint NEVER calls
    Claude; it only serves rows already in the cache. The ``is_stale`` field
    signals when the underlying diagnostic profile has changed since the
    narrative was generated so the frontend can show a regeneration prompt.

    Returns 404 when no narrative has been generated yet for this
    ``(route_id, period)`` combination.

    Args:
        route_id: Route identifier (e.g., ``'D80'``).
        period: One of ``all`` (default), ``am_peak``, ``midday``,
            ``pm_peak``, ``evening``, ``late``.

    Returns:
        Dict with ``narrative`` (string), ``generated_at`` (ISO-8601 UTC),
        ``model_id`` (string), ``prompt_version`` (string), and ``is_stale``
        (bool — ``True`` when the diagnostic profile changed after the
        narrative was generated).
    """
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )

    db = get_session()
    try:
        result = get_route_diagnosis(db, route_id, period=period)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No diagnosis narrative cached for route {route_id!r}, "
                    f"period {period!r}. "
                    f"Run: scripts/generate_route_diagnosis.py --route {route_id}"
                ),
            )
        return result
    finally:
        db.close()


_SCHEDULE_AUDIT_SIGNS: tuple[str, ...] = ("all", "under", "over")


@app.get("/api/schedule-audit")
async def get_schedule_audit_endpoint(
    route_id: str | None = None,
    direction_id: int | None = None,
    period: str = "all",
    sign: str = "all",
    limit: int = 100,
):
    """
    System-wide under-/over-padded segment audit (NOTES-60).

    Reads `route_diagnostic_segment` rows materialized nightly by
    `pipelines/refresh_route_diagnostic_profile.py` (PR #107) and returns
    one row per (route, direction, period, from-stop, to-stop) segment
    ranked by absolute slip magnitude × daily trip volume — the schedule
    planner's "biggest leverage first" cut. Filter by route, direction,
    time-of-day period, and slip sign (under-padded / over-padded / all).

    Sign convention (mirrors `src/route_diagnostics.py`):

    - `mean_slip_sec > 0`: observed segment travel time exceeds the
      schedule's segment travel time on average → the schedule is
      UNDER-padded for the segment (bus arrives late at the next stop).
    - `mean_slip_sec < 0`: observed time is faster than scheduled →
      OVER-padded (recoverable service-hours).

    `minutes_per_day` is `mean_slip_sec × n_observations / lookback_days
    / 60` — the per-day minutes that would be saved (or recovered) if
    the segment's mean slip were eliminated. Signed: positive = delay
    that revisions could shave, negative = excess padding revisions
    could trim. Lookback days mirror the materialization default (30).

    Args:
        route_id: If set, restrict to one route_id.
        direction_id: If set (0 or 1), restrict to one direction.
        period: One of `all` (default), `am_peak`, `midday`, `pm_peak`,
            `evening`, `late` — must match the materialized period set.
        sign: One of `all` (default), `under`, `over`.
        limit: Max rows to return (default 100, max 500).

    Returns:
        Dict with `period`, `sign`, `lookback_days`, `n_rows`, and
        `segments` (ranked list of per-segment audit rows).
    """
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )
    if sign not in _SCHEDULE_AUDIT_SIGNS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sign. Must be one of: {', '.join(_SCHEDULE_AUDIT_SIGNS)}",
        )
    if direction_id is not None and direction_id not in (0, 1):
        raise HTTPException(
            status_code=400,
            detail="Invalid direction_id. Must be 0 or 1 if provided.",
        )

    db = get_session()
    try:
        return get_schedule_audit(
            db,
            route_id=route_id,
            direction_id=direction_id,
            period=period,
            sign=sign,
            limit=limit,
        )
    finally:
        db.close()


@app.get("/api/segments")
async def get_segments(
    level: str = "segment",
    period: str = "all",
    limit: int = 100,
):
    """
    Ranked cross-route diagnostic — either stop-pair (NOTES-59) or
    shape-anchored corridor (NOTES-62) view.

    ``level='segment'`` (default; PR #140 contract) reads
    ``cross_route_segment_rollup`` and returns a ranked list of stop-pairs
    that appear on ≥2 routes, ordered by total trip-volume-weighted slip
    descending. Two routes count as sharing a segment only when they
    traverse the same ``(from_stop_id, to_stop_id)`` pair.

    ``level='corridor'`` reads ``corridor_slip_rollup`` joined with
    ``corridors`` and ``corridor_route_membership`` and returns a ranked
    list of shape-anchored corridors — contiguous stretches of street
    where ≥2 routes' canonical shapes run within 15m and 30° of each
    other. Catches the cross-route slip that the stop-pair view misses
    when routes share infrastructure with different stop_ids (typical at
    near-side / far-side stops on the same intersection).

    ``slip_min_per_trip`` (segment mode) is the average per-trip slip in
    minutes across contributing routes/directions. ``peak_period``
    (populated on ``period='all'`` rows only) names the time-of-day band
    with the highest shared slip.

    ``contributing_routes`` is the per-route breakdown for the drill-down
    panel: trip volume in segment mode; per-(route, direction) stop-range
    membership in corridor mode. Corridor mode also carries
    ``geometry_wkt`` so the frontend can render the corridor's LineString
    on Leaflet without a follow-up request.

    Args:
        level: ``segment`` (default) or ``corridor``.
        period: One of ``all`` (default), ``am_peak``, ``midday``,
            ``pm_peak``, ``evening``, ``late``.
        limit: Max rows to return (default 100, max 500).

    Returns:
        ``level='segment'`` → dict with ``period``, ``lookback_days``,
        ``n_rows``, ``segments``.
        ``level='corridor'`` → dict with ``level``, ``period``, ``n_rows``,
        ``corridors``.
    """
    if level not in ("segment", "corridor"):
        raise HTTPException(
            status_code=400,
            detail="level must be 'segment' or 'corridor'",
        )
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    db = get_session()
    try:
        if level == "corridor":
            return get_corridor_rollup(db, period=period, limit=limit)
        return get_cross_route_segments(db, period=period, limit=limit)
    finally:
        db.close()


@app.get("/api/corridors/{corridor_id}/segments")
async def get_corridor_segments(corridor_id: int, period: str = "all"):
    """Drill-down: per-route stop-pair segments inside a single corridor (NOTES-62).

    Companion to ``GET /api/segments?level=corridor``. For each
    contributing route in the corridor's membership, returns the
    ``route_diagnostic_segment`` rows whose stop_sequence range falls
    inside the corridor — matching the same join the slip rollup
    pipeline uses so the row-level breakdown reconciles with the
    headline ``total_weighted_slip_sec``.

    Args:
        corridor_id: Path parameter from ``corridors.corridor_id``.
        period: One of ``all`` (default), ``am_peak``, ``midday``,
            ``pm_peak``, ``evening``, ``late``.

    Returns:
        Dict with ``corridor_id``, ``period``, and ``segments``
        (ordered by mean_slip_sec descending; each row carries route +
        direction + from/to stop ids and names + mean_slip_sec +
        n_observations).
    """
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )

    db = get_session()
    try:
        if not db.query(Corridor).filter_by(corridor_id=corridor_id).first():
            raise HTTPException(status_code=404, detail="Corridor not found")
        return get_corridor_constituent_segments(db, corridor_id=corridor_id, period=period)
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
