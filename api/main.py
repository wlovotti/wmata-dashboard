"""
FastAPI application for WMATA Performance Dashboard API

This API serves pre-computed transit performance metrics for the web dashboard.
Endpoints provide route-level OTP, headway, and speed data.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.aggregations import (
    get_all_routes_scorecard,
    get_route_detail_metrics,
    get_route_speed_segments,
    get_route_time_period_summary,
    get_route_trend_data,
)
from src.database import get_session

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
        metric: Metric to analyze ('otp', 'headway', 'speed')
        days: Number of days to analyze (default: 30)

    Returns:
        Time-series data with daily values for the specified metric
    """
    if metric not in ["otp", "headway", "speed"]:
        raise HTTPException(
            status_code=400, detail="Invalid metric. Must be 'otp', 'headway', or 'speed'"
        )

    db = get_session()
    try:
        result = get_route_trend_data(db, route_id, metric=metric, days=days)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    finally:
        db.close()


@app.get("/api/routes/{route_id}/segments")
async def get_route_segments_endpoint(route_id: str, days: int = 7):
    """
    Get average speed by route segment for map visualization

    Returns speed data for each stop-to-stop segment along the route,
    including geographic coordinates for map display.

    Args:
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        List of route segments with coordinates and average speeds
    """
    db = get_session()
    try:
        result = get_route_speed_segments(db, route_id, days=days)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
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
        shape_ids = (
            db.query(Trip.shape_id)
            .filter(Trip.route_id == route_id)
            .distinct()
            .all()
        )

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
                shapes_data.append({
                    "shape_id": shape_id,
                    "points": [
                        {"lat": p.shape_pt_lat, "lon": p.shape_pt_lon}
                        for p in points
                    ]
                })

        return {"route_id": route_id, "shapes": shapes_data}
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
