"""
Aggregation functions for dashboard API

These functions compute high-level metrics from raw vehicle position data,
optimized for fast API responses and dashboard visualization.
"""

import math
import time
from datetime import datetime, timedelta
from threading import Lock

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.analytics import (
    calculate_time_period_otp,
)
from src.bunching import (
    compute_bunching_for_route_date,
    compute_bunching_headline_for_route,
    compute_bunching_headline_for_routes,
)
from src.ewt import (
    _day_type_for,
    compute_ewt_for_route_date,
    compute_ewt_headline_for_route,
    compute_ewt_headline_for_routes,
    fetch_scheduled_cell_hours_for_routes,
)
from src.models import Route, RouteMetricsDaily, RouteMetricsSummary, RouteServiceProfile, StopEvent
from src.otp_metrics import compute_otp_split, compute_otp_split_for_routes
from src.service_delivered import (
    compute_service_delivered,
    compute_service_delivered_for_routes,
)
from src.service_profile import (
    classify_route_frequency,
    compute_route_frequency_classes,
)

# Per-service-date cache of the new live-computed scorecard metrics. The
# scheduled stop_times fetch (~1.7s) dominates cost; observed stop_events are
# the only thing that changes minute-to-minute. A short TTL keeps the
# scorecard within ~60s of fresh while amortizing the scheduled fetch across
# every page load in the window.
_LIVE_METRICS_TTL_SEC = 60.0
_live_metrics_cache: dict[str, tuple[float, dict[str, dict]]] = {}
_live_metrics_lock = Lock()


def _latest_service_date_with_stop_events(db: Session):
    """Return the most recent service_date that has any stop_events, or None.

    The new metrics depend on `stop_events`, which is materialized by the
    derivation pipelines (`pipelines/derive_stop_events*.py`) — typically run
    after a day completes. Today's date may have no derived events yet, so
    the scorecard should anchor on whatever the latest derived date is, not
    `eastern_today()`. Cache key naturally advances when the pipeline runs.
    """
    row = db.query(func.max(StopEvent.service_date)).scalar()
    if not row:
        return None
    return datetime.strptime(row, "%Y-%m-%d").date()


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


def calculate_performance_grade(otp_percentage: float | None) -> str:
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


def _compute_live_metrics_uncached(db: Session, service_date) -> dict[str, dict]:
    """One-pass live compute of the four new scorecard metrics for every route.

    EWT and bunching share the scheduled-cell-hour fetch (the dominant cost,
    ~1.7s for ~422k stop_times rows). Service-delivered and OTP split are
    independent and cheap. Returns `{route_id: {service_delivered, otp_split,
    ewt, bunching}}` — each sub-key is the dict shape from the corresponding
    compute function, or None when the route has no entry in that source.
    """
    day_type = _day_type_for(service_date)
    sched = fetch_scheduled_cell_hours_for_routes(db, day_type)
    ewt_by_route = compute_ewt_headline_for_routes(db, service_date, sched_by_route_cell_hour=sched)
    bunching_by_route = compute_bunching_headline_for_routes(
        db, service_date, sched_by_route_cell_hour=sched
    )
    sd_by_route = {r["route_id"]: r for r in compute_service_delivered_for_routes(db, service_date)}
    otp_by_route = {r["route_id"]: r for r in compute_otp_split_for_routes(db, service_date)}

    all_routes = set(ewt_by_route) | set(bunching_by_route) | set(sd_by_route) | set(otp_by_route)
    return {
        route_id: {
            "service_delivered": sd_by_route.get(route_id),
            "otp_split": otp_by_route.get(route_id),
            "ewt": ewt_by_route.get(route_id),
            "bunching": bunching_by_route.get(route_id),
        }
        for route_id in all_routes
    }


def _compute_single_route_live_metrics(db: Session, route_id: str, service_date) -> dict:
    """Single-route equivalent of `_compute_live_metrics_uncached` for one route.

    Used on RouteDetail when the all-routes scorecard cache is cold — computing
    one route directly (~150ms) is much faster than triggering the full ~3s
    scorecard build just to pluck a single entry.
    """
    return {
        "service_delivered": compute_service_delivered(db, route_id, service_date),
        "otp_split": compute_otp_split(db, route_id, service_date),
        "ewt": compute_ewt_headline_for_route(db, route_id, service_date),
        "bunching": compute_bunching_headline_for_route(db, route_id, service_date),
    }


def get_live_metrics_for_route_today(db: Session, route_id: str) -> dict | None:
    """Latest derived service_date's live metrics for one route, cached when warm.

    On a warm cache (any /api/routes call within the TTL), returns the cached
    bundle for `route_id` instantly. On cold cache, computes single-route
    directly without warming the full scorecard cache — RouteDetail shouldn't
    pay the all-routes price.

    Returns `None` if there are no stop_events at all (DB freshly initialized).
    """
    service_date = _latest_service_date_with_stop_events(db)
    if service_date is None:
        return None
    cache_key = service_date.isoformat()

    with _live_metrics_lock:
        cached = _live_metrics_cache.get(cache_key)
    if cached is not None and (time.monotonic() - cached[0]) < _LIVE_METRICS_TTL_SEC:
        return cached[1].get(route_id) or _compute_single_route_live_metrics(
            db, route_id, service_date
        )

    return _compute_single_route_live_metrics(db, route_id, service_date)


def get_live_metrics_for_today(db: Session) -> dict[str, dict]:
    """Cached-by-service-date wrapper around `_compute_live_metrics_uncached`.

    Anchors on the latest service_date that has stop_events (today's data may
    not yet be derived — see `_latest_service_date_with_stop_events`). Reuses
    the cached result if computed within `_LIVE_METRICS_TTL_SEC` (default 60s).
    Cold-cache cost is the full ~3s; warm-cache cost is dict access.
    Concurrent callers may both compute on a cold miss — acceptable
    thundering-herd cost in single-process dev. Returns an empty dict if no
    stop_events exist at all.
    """
    service_date = _latest_service_date_with_stop_events(db)
    if service_date is None:
        return {}
    cache_key = service_date.isoformat()

    with _live_metrics_lock:
        cached = _live_metrics_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _LIVE_METRICS_TTL_SEC:
                return value

    result = _compute_live_metrics_uncached(db, service_date)

    with _live_metrics_lock:
        _live_metrics_cache[cache_key] = (time.monotonic(), result)
    return result


def _live_metric_fields(metrics: dict | None) -> dict:
    """Flatten the per-route live-metrics bundle into scorecard fields.

    Used by both scorecard and detail endpoints. Returns a dict with the new
    fields all set to None when `metrics` is None — i.e. the route had no
    entry in any of the four live sources for the day.
    """
    if metrics is None:
        return {
            "service_delivered_ratio": None,
            "service_delivered_scheduled": None,
            "service_delivered_delivered": None,
            "otp_origin_pct": None,
            "otp_destination_pct": None,
            "otp_all_pct": None,
            "ewt_seconds": None,
            "ewt_n_observed": None,
            "bunching_rate": None,
            "bunching_count": None,
            "bunching_total_headways": None,
        }
    sd = metrics.get("service_delivered") or {}
    otp = metrics.get("otp_split") or {}
    ewt = metrics.get("ewt") or {}
    bun = metrics.get("bunching") or {}
    return {
        "service_delivered_ratio": sd.get("ratio"),
        "service_delivered_scheduled": sd.get("scheduled_trips"),
        "service_delivered_delivered": sd.get("delivered_trips"),
        "otp_origin_pct": (otp.get("origin") or {}).get("on_time_pct"),
        "otp_destination_pct": (otp.get("destination") or {}).get("on_time_pct"),
        "otp_all_pct": (otp.get("all_timepoints") or {}).get("on_time_pct"),
        "ewt_seconds": ewt.get("ewt_seconds"),
        "ewt_n_observed": ewt.get("n_observed_headways"),
        "bunching_rate": bun.get("bunching_rate"),
        "bunching_count": bun.get("bunching_count"),
        "bunching_total_headways": bun.get("total_headways"),
    }


def get_all_routes_scorecard(db: Session, days: int = 7) -> list[dict]:
    """
    Get performance scorecard for all routes.

    Combines two data layers:
      * **Legacy fields** (otp_percentage, avg_headway_minutes, etc.) from the
        pre-computed `route_metrics_summary` table — populated by the nightly
        batch job. Will be retired in NOTES-19 once new metrics fully replace.
      * **New live metrics** (service-delivered, OTP origin/destination split,
        EWT headline, bunching headline) computed live from `runs` and
        `stop_events` for today's service date. Cached by service_date with a
        short TTL — see `get_live_metrics_for_today`.

    Args:
        db: Database session
        days: Number of days to analyze (ignored, uses pre-computed summaries)

    Returns:
        List of route summaries with both legacy and new performance metrics.
    """
    # Get all routes (current version only)
    routes = db.query(Route).filter(Route.is_current).all()
    route_map = {r.route_id: r for r in routes}

    # Get pre-computed summaries (legacy fields)
    summaries = db.query(RouteMetricsSummary).all()

    # Get today's live metrics (cached)
    live = get_live_metrics_for_today(db)

    # Frequency class per route (GTFS-derived, ~2ms — no caching needed).
    freq_classes = compute_route_frequency_classes(db)

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
                "frequency_class": freq_classes.get(summary.route_id),
                **_live_metric_fields(live.get(summary.route_id)),
            }
        )

    # Add routes without computed legacy metrics — but still surface live ones.
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
                    "frequency_class": freq_classes.get(route.route_id),
                    **_live_metric_fields(live.get(route.route_id)),
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

    # Get pre-computed summary metrics (legacy, includes position stats)
    summary = db.query(RouteMetricsSummary).filter(RouteMetricsSummary.route_id == route_id).first()

    # Live metrics for today (single-route compute on cache miss).
    live_fields = _live_metric_fields(get_live_metrics_for_route_today(db, route_id))

    # Frequency class — single-route lookup against route_service_profile.
    headways = [
        h
        for (h,) in db.query(RouteServiceProfile.mean_headway_min)
        .filter(
            RouteServiceProfile.route_id == route_id,
            RouteServiceProfile.day_type == "weekday",
            RouteServiceProfile.mean_headway_min.isnot(None),
        )
        .all()
    ]
    frequency_class = classify_route_frequency(headways, route_id)

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
            "headway_std_dev_minutes": sanitize_float(
                getattr(summary, "headway_std_dev_minutes", None)
            ),
            "headway_cv": sanitize_float(getattr(summary, "headway_cv", None)),
            "min_headway_minutes": None,  # Not in summary table
            "max_headway_minutes": None,  # Not in summary table
            "avg_speed_mph": sanitize_float(summary.avg_speed_mph),
            "total_positions": getattr(summary, "total_positions_7d", 0) or 0,
            "unique_vehicles": getattr(summary, "unique_vehicles_7d", 0) or 0,
            "unique_trips": getattr(summary, "unique_trips_7d", 0) or 0,
            "grade": calculate_performance_grade(summary.otp_percentage),
            "frequency_class": frequency_class,
            **live_fields,
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
            "total_positions": 0,
            "unique_vehicles": 0,
            "unique_trips": 0,
            "grade": "N/A",
            "frequency_class": frequency_class,
            **live_fields,
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
    from datetime import timedelta

    from src.timezones import eastern_today

    # Calculate date range in Eastern (the WMATA service date)
    end_date = eastern_today()
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


def get_route_period_drilldown(db: Session, route_id: str) -> dict:
    """Per-time-period EWT and bunching for one route on the latest service_date.

    Surfaces the AM peak vs evening variance the headline scorecard collapses.
    Anchors on the same `_latest_service_date_with_stop_events` as the headline
    so the drilldown numbers reconcile with the scorecard above them.

    Returns `{"error": ...}` if the route is missing. Returns empty `ewt` /
    `bunching` lists when no stop_events have been derived yet.
    """
    route = db.query(Route).filter(Route.route_id == route_id, Route.is_current).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    service_date = _latest_service_date_with_stop_events(db)
    if service_date is None:
        return {
            "route_id": route_id,
            "service_date": None,
            "day_type": None,
            "ewt": [],
            "bunching": [],
        }

    ewt_rows = compute_ewt_for_route_date(db, route_id, service_date)
    bunching_rows = compute_bunching_for_route_date(db, route_id, service_date)

    return {
        "route_id": route_id,
        "service_date": service_date.isoformat(),
        "day_type": _day_type_for(service_date),
        "ewt": [
            {
                "time_period": r["time_period"],
                "ewt_seconds": sanitize_float(r["ewt_seconds"]),
                "awt_seconds": sanitize_float(r["awt_seconds"]),
                "swt_seconds": sanitize_float(r["swt_seconds"]),
                "n_observed_headways": r["n_observed_headways"],
                "n_scheduled_headways": r["n_scheduled_headways"],
                "frequent_cell_hours": r["frequent_cell_hours"],
            }
            for r in ewt_rows
        ],
        "bunching": [
            {
                "time_period": r["time_period"],
                "bunching_rate": sanitize_float(r["bunching_rate"]),
                "bunching_count": r["bunching_count"],
                "total_headways": r["total_headways"],
            }
            for r in bunching_rows
        ],
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
