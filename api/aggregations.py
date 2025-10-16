"""
Aggregation functions for dashboard API

These functions compute high-level metrics from raw vehicle position data,
optimized for fast API responses and dashboard visualization.
"""

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.analytics import (
    calculate_average_speed,
    calculate_headways,
    calculate_line_level_otp,
    calculate_time_period_otp,
)
from src.models import Route, RouteMetricsSummary, VehiclePosition


def calculate_performance_grade(otp_percentage: Optional[float]) -> str:
    """
    Calculate letter grade from OTP percentage

    Args:
        otp_percentage: On-time performance percentage (0-100)

    Returns:
        Letter grade: A (>80%), B (60-80%), C (40-60%), D (20-40%), F (<20%)
    """
    if otp_percentage is None:
        return "N/A"
    if otp_percentage >= 80:
        return "A"
    elif otp_percentage >= 60:
        return "B"
    elif otp_percentage >= 40:
        return "C"
    elif otp_percentage >= 20:
        return "D"
    else:
        return "F"


def get_all_routes_scorecard(db: Session, days: int = 7) -> list[dict]:
    """
    Get performance scorecard for all routes from pre-computed summary table

    Returns pre-computed metrics for all routes. Much faster than live calculation.
    Data is updated by nightly batch job (pipelines/compute_daily_metrics.py).

    Args:
        db: Database session
        days: Number of days to analyze (ignored, uses pre-computed summaries)

    Returns:
        List of route summaries with performance metrics
    """
    # Get all routes
    routes = db.query(Route).all()
    route_map = {r.route_id: r for r in routes}

    # Get pre-computed summaries
    summaries = db.query(RouteMetricsSummary).all()

    scorecard = []

    for summary in summaries:
        route = route_map.get(summary.route_id)
        if not route:
            continue

        scorecard.append(
            {
                "route_id": summary.route_id,
                "route_name": route.route_short_name,
                "route_long_name": route.route_long_name,
                "otp_percentage": summary.otp_percentage,
                "avg_headway_minutes": summary.avg_headway_minutes,
                "avg_speed_mph": summary.avg_speed_mph,
                "grade": calculate_performance_grade(summary.otp_percentage),
                "total_observations": summary.total_observations,
                "data_updated_at": summary.last_data_timestamp.isoformat()
                if summary.last_data_timestamp
                else None,
                "computed_at": summary.computed_at.isoformat() if summary.computed_at else None,
            }
        )

    # Add routes without computed metrics
    summary_route_ids = {s.route_id for s in summaries}
    for route in routes:
        if route.route_id not in summary_route_ids:
            scorecard.append(
                {
                    "route_id": route.route_id,
                    "route_name": route.route_short_name,
                    "route_long_name": route.route_long_name,
                    "otp_percentage": None,
                    "avg_headway_minutes": None,
                    "avg_speed_mph": None,
                    "grade": "N/A",
                    "total_observations": 0,
                    "data_updated_at": None,
                    "computed_at": None,
                }
            )

    # Sort by OTP descending (best routes first), None values last
    scorecard.sort(key=lambda x: (x["otp_percentage"] is None, -(x["otp_percentage"] or 0)))

    return scorecard


def get_route_detail_metrics(db: Session, route_id: str, days: int = 7) -> dict:
    """
    Get detailed performance metrics for a specific route

    Returns current metrics and metadata for display on route detail page header.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Dictionary with detailed route metrics
    """
    # Get route info
    route = db.query(Route).filter(Route.route_id == route_id).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    # Calculate date range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    # Get OTP
    otp_result = calculate_line_level_otp(
        db,
        route_id,
        start_time=start_time,
        end_time=end_time,
        sample_rate=1,  # More accurate for detail view
    )

    # Get headway
    headway_result = calculate_headways(db, route_id, start_time=start_time, end_time=end_time)

    # Get speed
    speed_result = calculate_average_speed(db, route_id, start_time=start_time, end_time=end_time)

    # Count unique vehicles and trips
    position_stats = (
        db.query(
            func.count(VehiclePosition.id).label("total_positions"),
            func.count(func.distinct(VehiclePosition.vehicle_id)).label("unique_vehicles"),
            func.count(func.distinct(VehiclePosition.trip_id)).label("unique_trips"),
        )
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.timestamp >= start_time,
            VehiclePosition.timestamp <= end_time,
        )
        .first()
    )

    return {
        "route_id": route.route_id,
        "route_name": route.route_short_name,
        "route_long_name": route.route_long_name,
        "time_period_days": days,
        "otp_percentage": otp_result.get("on_time_pct"),
        "early_percentage": otp_result.get("early_pct"),
        "late_percentage": otp_result.get("late_pct"),
        "avg_headway_minutes": headway_result.get("avg_headway_minutes"),
        "min_headway_minutes": headway_result.get("min_headway_minutes"),
        "max_headway_minutes": headway_result.get("max_headway_minutes"),
        "avg_speed_mph": speed_result.get("avg_speed_mph"),
        "total_arrivals_analyzed": otp_result.get("matched_observations", 0),
        "total_positions": position_stats.total_positions if position_stats else 0,
        "unique_vehicles": position_stats.unique_vehicles if position_stats else 0,
        "unique_trips": position_stats.unique_trips if position_stats else 0,
        "grade": calculate_performance_grade(otp_result.get("on_time_pct")),
    }


def get_route_trend_data(db: Session, route_id: str, metric: str = "otp", days: int = 30) -> dict:
    """
    Get time-series trend data for a specific route metric

    Computes daily values for OTP, headway, or speed over the specified time period.
    Used for trend charts on the route detail page.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'headway', 'speed')
        days: Number of days to analyze (default: 30)

    Returns:
        Time-series data for the specified metric
    """
    # TODO: Implement daily metric calculation
    # For now, return placeholder
    return {
        "route_id": route_id,
        "metric": metric,
        "days": days,
        "time_series": [],
        "avg": None,
        "trend": "unknown",
        "note": "Time-series aggregation not yet implemented",
    }


def get_route_speed_segments(db: Session, route_id: str, days: int = 7) -> dict:
    """
    Get average speed by route segment for map visualization

    Computes stop-to-stop average speeds and includes geographic coordinates
    for displaying color-coded route segments on an interactive map.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Route segments with coordinates and speed data
    """
    # TODO: Implement segment-level speed calculation
    # For now, return placeholder
    return {
        "route_id": route_id,
        "days": days,
        "segments": [],
        "note": "Segment-level speed analysis not yet implemented",
    }


def get_route_time_period_summary(db: Session, route_id: str, days: int = 7) -> dict:
    """
    Get performance metrics by time of day

    Returns OTP and headway broken down by time periods (AM Peak, Midday, PM Peak, etc.)
    for display on the route detail page.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Performance metrics grouped by time period
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    # Use existing time period OTP function
    result = calculate_time_period_otp(db, route_id, start_time=start_time, end_time=end_time)

    return {
        "route_id": route_id,
        "days": days,
        "periods": result.get("periods", {}),
        "thresholds": result.get("thresholds", {}),
    }
