"""
Aggregation functions for dashboard API

These functions compute high-level metrics from raw vehicle position data,
optimized for fast API responses and dashboard visualization.
"""

import math
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.analytics import (
    calculate_time_period_otp,
)
from src.models import Route, RouteMetricsDaily, RouteMetricsSummary, VehiclePosition


def sanitize_float(value):
    """
    Convert float value to None if it's NaN or Infinity

    Args:
        value: Float value to sanitize

    Returns:
        None if value is NaN/Infinity, otherwise the float value
    """
    if value is None:
        return None
    try:
        float_value = float(value)
        if math.isnan(float_value) or math.isinf(float_value):
            return None
        return float_value
    except (ValueError, TypeError):
        return None


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
    # Get all routes (current version only)
    routes = db.query(Route).filter(Route.is_current).all()
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
                "otp_percentage": sanitize_float(summary.otp_percentage),
                "avg_headway_minutes": sanitize_float(summary.avg_headway_minutes),
                "headway_std_dev_minutes": sanitize_float(
                    getattr(summary, "headway_std_dev_minutes", None)
                ),
                "avg_speed_mph": sanitize_float(summary.avg_speed_mph),
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
                    "headway_std_dev_minutes": None,
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
    Uses pre-computed summary metrics for fast response.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Dictionary with detailed route metrics
    """
    # Get route info (current version only)
    route = db.query(Route).filter(Route.route_id == route_id, Route.is_current).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    # Get pre-computed summary metrics (includes position stats)
    summary = db.query(RouteMetricsSummary).filter(RouteMetricsSummary.route_id == route_id).first()

    # Return summary metrics if available
    if summary:
        return {
            "route_id": route.route_id,
            "route_name": route.route_short_name,
            "route_long_name": route.route_long_name,
            "time_period_days": days,
            "date_range_start": summary.date_start if hasattr(summary, "date_start") else None,
            "date_range_end": summary.date_end if hasattr(summary, "date_end") else None,
            "otp_percentage": sanitize_float(summary.otp_percentage),
            "early_percentage": sanitize_float(getattr(summary, "early_percentage", None)),
            "late_percentage": sanitize_float(getattr(summary, "late_percentage", None)),
            "avg_headway_minutes": sanitize_float(summary.avg_headway_minutes),
            "headway_std_dev_minutes": sanitize_float(getattr(summary, "headway_std_dev_minutes", None)),
            "headway_cv": sanitize_float(getattr(summary, "headway_cv", None)),
            "min_headway_minutes": None,  # Not in summary table
            "max_headway_minutes": None,  # Not in summary table
            "avg_speed_mph": sanitize_float(summary.avg_speed_mph),
            "total_arrivals_analyzed": getattr(summary, "total_arrivals_analyzed", 0) or 0,
            "total_positions": getattr(summary, "total_positions_7d", 0) or 0,
            "unique_vehicles": getattr(summary, "unique_vehicles_7d", 0) or 0,
            "unique_trips": getattr(summary, "unique_trips_7d", 0) or 0,
            "grade": calculate_performance_grade(summary.otp_percentage),
        }
    else:
        # No pre-computed metrics available
        return {
            "route_id": route.route_id,
            "route_name": route.route_short_name,
            "route_long_name": route.route_long_name,
            "time_period_days": days,
            "otp_percentage": None,
            "early_percentage": None,
            "late_percentage": None,
            "avg_headway_minutes": None,
            "headway_std_dev_minutes": None,
            "headway_cv": None,
            "min_headway_minutes": None,
            "max_headway_minutes": None,
            "avg_speed_mph": None,
            "total_arrivals_analyzed": 0,
            "total_positions": 0,
            "unique_vehicles": 0,
            "unique_trips": 0,
            "grade": "N/A",
        }


def get_route_trend_data(db: Session, route_id: str, metric: str = "otp", days: int = 30) -> dict:
    """
    Get time-series trend data for a specific route metric

    Computes daily values for OTP, headway, speed, or other metrics over the specified time period.
    Used for trend charts on the route detail page.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'early', 'late', 'headway', 'headway_std_dev', 'speed')
        days: Number of days to analyze (default: 30)

    Returns:
        Time-series data for the specified metric
    """
    from datetime import datetime, timedelta

    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    # Get daily metrics from database
    daily_metrics = (
        db.query(RouteMetricsDaily)
        .filter(
            RouteMetricsDaily.route_id == route_id,
            RouteMetricsDaily.date >= start_date.isoformat(),
            RouteMetricsDaily.date <= end_date.isoformat(),
        )
        .order_by(RouteMetricsDaily.date)
        .all()
    )

    if not daily_metrics:
        return {
            "route_id": route_id,
            "metric": metric,
            "days": days,
            "trend_data": [],
        }

    # Map metric name to field and response key
    metric_config = {
        "otp": {"field": "otp_percentage", "key": "otp_percentage"},
        "early": {"field": "early_percentage", "key": "early_percentage"},
        "late": {"field": "late_percentage", "key": "late_percentage"},
        "headway": {"field": "avg_headway_minutes", "key": "avg_headway_minutes"},
        "headway_std_dev": {"field": "headway_std_dev_minutes", "key": "headway_std_dev_minutes"},
        "speed": {"field": "avg_speed_mph", "key": "avg_speed_mph"},
    }

    config = metric_config.get(metric, metric_config["otp"])
    field_name = config["field"]
    response_key = config["key"]

    # Build time series data
    trend_data = []
    for day_metric in daily_metrics:
        value = getattr(day_metric, field_name, None)
        if value is not None:
            trend_data.append({"date": day_metric.date, response_key: value})

    return {"route_id": route_id, "metric": metric, "days": days, "trend_data": trend_data}


def get_route_speed_segments(db: Session, route_id: str, days: int = 7) -> dict:
    """
    Get average speed by route segment for map visualization

    Computes average speeds along the route path by dividing the shape into segments
    and calculating speed from vehicle position data.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Number of days to analyze (default: 7)

    Returns:
        Route segments with coordinates and speed data
    """
    from datetime import datetime, timedelta

    import numpy as np

    from src.models import Shape, Trip, VehiclePosition

    # Get shape data for this route
    shape_ids = db.query(Trip.shape_id).filter(Trip.route_id == route_id).distinct().limit(1).all()

    if not shape_ids or not shape_ids[0][0]:
        return {"route_id": route_id, "days": days, "segments": []}

    shape_id = shape_ids[0][0]

    # Get all shape points ordered by sequence
    shape_points = (
        db.query(Shape).filter(Shape.shape_id == shape_id).order_by(Shape.shape_pt_sequence).all()
    )

    if len(shape_points) < 2:
        return {"route_id": route_id, "days": days, "segments": []}

    # Calculate date range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    # Get vehicle positions with speed data for this route
    positions = (
        db.query(VehiclePosition)
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.timestamp >= start_time,
            VehiclePosition.timestamp <= end_time,
            VehiclePosition.speed.isnot(None),
        )
        .all()
    )

    if not positions:
        # Return segments with no speed data
        segments = []
        segment_size = max(1, len(shape_points) // 20)  # Divide into ~20 segments
        for i in range(0, len(shape_points) - segment_size, segment_size):
            segment_points = shape_points[i : i + segment_size + 1]
            segments.append(
                {
                    "points": [
                        {"lat": p.shape_pt_lat, "lon": p.shape_pt_lon} for p in segment_points
                    ],
                    "avg_speed_mph": None,
                }
            )
        return {"route_id": route_id, "days": days, "segments": segments}

    # Divide shape into segments and calculate average speed for each
    segment_size = max(1, len(shape_points) // 20)  # Divide into ~20 segments
    segments = []

    # Convert shape points and positions to NumPy arrays for vectorized operations
    shape_coords = np.array([[p.shape_pt_lat, p.shape_pt_lon] for p in shape_points])

    # Filter positions with valid coordinates and speed
    valid_positions = [
        (p.latitude, p.longitude, p.speed)
        for p in positions
        if p.latitude is not None and p.longitude is not None and p.speed is not None
    ]

    if not valid_positions:
        # Return segments with no speed data
        for i in range(0, len(shape_points) - segment_size, segment_size):
            segment_points = shape_points[i : i + segment_size + 1]
            segments.append(
                {
                    "points": [
                        {"lat": p.shape_pt_lat, "lon": p.shape_pt_lon} for p in segment_points
                    ],
                    "avg_speed_mph": None,
                }
            )
        return {"route_id": route_id, "days": days, "segments": segments}

    pos_coords = np.array([[lat, lon] for lat, lon, _ in valid_positions])
    pos_speeds = np.array([speed for _, _, speed in valid_positions])

    # Sample shape points every 10th point for faster nearest neighbor search
    sample_indices = np.arange(0, len(shape_coords), 10)
    sampled_coords = shape_coords[sample_indices]

    # Vectorized distance calculation: find nearest sampled shape point for each position
    # Shape: (n_positions, n_sampled_points)
    distances = np.sum(
        (pos_coords[:, np.newaxis, :] - sampled_coords[np.newaxis, :, :]) ** 2, axis=2
    )
    closest_sampled_indices = np.argmin(distances, axis=1)
    min_distances = np.min(distances, axis=1)

    # Map back to original indices
    closest_indices = sample_indices[closest_sampled_indices]

    # Filter positions that are reasonably close (~500m threshold)
    close_mask = min_distances < 0.005
    closest_indices = closest_indices[close_mask]
    pos_speeds_filtered = pos_speeds[close_mask]

    # Assign positions to segments
    segment_indices = closest_indices // segment_size

    # Group speeds by segment using dictionary
    position_segments = {}
    for seg_idx, speed in zip(segment_indices, pos_speeds_filtered):
        if seg_idx not in position_segments:
            position_segments[seg_idx] = []
        position_segments[seg_idx].append(speed)

    # Build segments with computed speeds
    for i in range(0, len(shape_points) - segment_size, segment_size):
        segment_idx = i // segment_size
        segment_points = shape_points[i : i + segment_size + 1]

        segment_speeds = position_segments.get(segment_idx, [])
        avg_speed = np.mean(segment_speeds) if segment_speeds else None

        segments.append(
            {
                "points": [{"lat": p.shape_pt_lat, "lon": p.shape_pt_lon} for p in segment_points],
                "avg_speed_mph": sanitize_float(avg_speed),
            }
        )

    return {"route_id": route_id, "days": days, "segments": segments}


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
