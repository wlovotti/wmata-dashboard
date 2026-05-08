"""
FastAPI application for WMATA Performance Dashboard API

This API serves pre-computed transit performance metrics for the web dashboard.
Endpoints provide route-level OTP, headway, and speed data.
"""

from datetime import timedelta

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.aggregations import (
    get_all_routes_scorecard,
    get_route_contributors,
    get_route_detail_metrics,
    get_route_period_drilldown,
    get_route_recent_runs,
    get_route_time_period_summary,
    get_route_trend_data,
    get_run_deviations,
    get_system_trend_data,
)
from src.database import get_session
from src.models import GTFSSnapshot, VehiclePosition
from src.timezones import utcnow_naive

# Create FastAPI app
app = FastAPI(
    title="WMATA Performance API",
    description="REST API for WMATA transit performance metrics",
    version="1.0.0",
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
    Get performance scorecard for all routes

    Returns summary metrics for all routes including OTP, headway, and speed.
    Used by the dashboard landing page.

    Args:
        days: Number of days to analyze (default: 7)

    Returns:
        List of route summaries with performance metrics
    """
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

    For OTP, `route_value` is the window mean from the materialized
    `route_metrics_daily` table. For service-delivered, EWT, and bunching,
    `route_value` is the latest single-day snapshot from the live-metrics
    cache (those metrics aren't materialized per-route per-day; a window
    mean would require N× per-day computes per route). The baseline is
    always a window mean from `system_metrics_daily`.

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
async def get_route(route_id: str, days: int = 7):
    """
    Get detailed metrics for a specific route

    Returns current performance metrics and metadata for a route.
    Used by the route detail page header.

    Args:
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Detailed route metrics including OTP, headway, speed, and trip counts
    """
    db = get_session()
    try:
        result = get_route_detail_metrics(db, route_id, days=days)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/routes/{route_id}/trend")
async def get_route_trend(route_id: str, metric: str = "otp", days: int = 30):
    """
    Get time-series trend data for a specific route metric

    Returns daily values for a metric over time, used for trend charts.

    Args:
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'early', 'late', 'headway',
            'headway_std_dev', 'speed', 'service_delivered',
            'excess_trip_time')
        days: Number of days to analyze (default: 30)

    Returns:
        Time-series data with daily values for the specified metric
    """
    valid_metrics = [
        "otp",
        "early",
        "late",
        "headway",
        "headway_std_dev",
        "speed",
        "service_delivered",
        "excess_trip_time",
    ]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400, detail=f"Invalid metric. Must be one of: {', '.join(valid_metrics)}"
        )

    db = get_session()
    try:
        result = get_route_trend_data(db, route_id, metric=metric, days=days)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
