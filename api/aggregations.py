"""
Aggregation functions for dashboard API

These functions compute high-level metrics from raw vehicle position data,
optimized for fast API responses and dashboard visualization.
"""

import math
import time
from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta
from threading import Lock

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.analytics import (
    calculate_time_period_otp,
)
from src.bunching import (
    BUNCHING_ABSOLUTE_FLOOR_SEC,
    BUNCHING_RATIO,
    MAX_OBSERVED_HEADWAY_SEC,
    compute_bunching_for_route_date,
    compute_bunching_headline_for_route,
    compute_bunching_headline_for_routes,
)
from src.ewt import (
    _day_type_for,
    _eastern_hour,
    _is_cell_hour_frequent,
    compute_awt,
    compute_ewt_for_route_date,
    compute_ewt_headline_for_route,
    compute_ewt_headline_for_routes,
    fetch_scheduled_cell_hours_for_routes,
)
from src.models import (
    Calendar,
    Route,
    RouteMetricsDaily,
    RouteMetricsSummary,
    RouteServiceProfile,
    Run,
    Stop,
    StopEvent,
    StopTime,
    SystemMetricsDaily,
    Trip,
)
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC
from src.otp_metrics import compute_otp_split, compute_otp_split_for_routes
from src.service_delivered import (
    compute_service_delivered,
    compute_service_delivered_for_routes,
)
from src.service_profile import (
    classify_route_frequency,
    compute_route_frequency_classes,
)
from src.timezones import utcnow_naive

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
            "ewt_coverage_ratio": None,
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
        # Observed-to-scheduled headway coverage for the EWT pool. Below ~0.5
        # the trip_update derivation is missing arrivals badly enough that AWT
        # is biased low — the frontend renders a "data thin" badge so the EWT
        # clamp at 0 doesn't masquerade as a healthy metric.
        "ewt_coverage_ratio": ewt.get("coverage_ratio"),
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


def _excess_trip_time_fields(db: Session, route_id: str, days: int = 7) -> dict:
    """Most-recent non-null excess-trip-time values for one route.

    NOTES-43 columns live on `route_metrics_daily`, not on the rolling
    summary table. The KPI card and subline use the most recent day where
    the metric was populated within the last `days` window — gives a
    "freshest reasonable signal" rather than a smoothed average over days
    that may have been TU-blind. Returns all fields as None when no daily
    row in the window has the columns set.
    """
    from datetime import timedelta

    from src.timezones import eastern_today

    end_date = eastern_today()
    start_date = end_date - timedelta(days=days)
    row = (
        db.query(RouteMetricsDaily)
        .filter(
            RouteMetricsDaily.route_id == route_id,
            RouteMetricsDaily.date >= start_date.isoformat(),
            RouteMetricsDaily.date <= end_date.isoformat(),
            RouteMetricsDaily.excess_trip_time_pct.isnot(None),
        )
        .order_by(RouteMetricsDaily.date.desc())
        .first()
    )
    if row is None:
        return {
            "excess_trip_time_pct": None,
            "excess_trip_time_median_actual_sec": None,
            "excess_trip_time_median_scheduled_sec": None,
            "excess_trip_time_n_trips": None,
            "excess_trip_time_as_of_date": None,
        }
    return {
        "excess_trip_time_pct": sanitize_float(row.excess_trip_time_pct),
        "excess_trip_time_median_actual_sec": row.median_actual_trip_time_sec,
        "excess_trip_time_median_scheduled_sec": row.median_scheduled_trip_time_sec,
        "excess_trip_time_n_trips": row.excess_trip_time_n_trips,
        "excess_trip_time_as_of_date": row.date,
    }


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

    # Excess trip time (NOTES-43): freshest non-null value within the window.
    excess_fields = _excess_trip_time_fields(db, route_id, days=days)

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
            **excess_fields,
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
            **excess_fields,
        }


def get_route_trend_data(db: Session, route_id: str, metric: str = "otp", days: int = 30) -> dict:
    """
    Get time-series trend data for a specific route metric.

    Computes daily values for OTP, headway, speed, or service-delivered over the
    specified time period. Used for trend charts on the route detail page.

    `service_delivered` is computed live per service_date from `runs` + GTFS via
    `compute_service_delivered` (NOTES-37). It is not stored in
    `route_metrics_daily`, so the trend loop pays one pair of count queries per
    day in the window; acceptable on a per-route detail page (not iterated over
    a route list).

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'early', 'late', 'headway',
            'headway_std_dev', 'speed', 'service_delivered',
            'excess_trip_time')
        days: Number of days to analyze (default: 30)

    Returns:
        Time-series data for the specified metric
    """
    from datetime import timedelta

    from src.timezones import eastern_today

    # Calculate date range in Eastern (the WMATA service date)
    end_date = eastern_today()
    start_date = end_date - timedelta(days=days)

    # service_delivered isn't materialized in RouteMetricsDaily; compute per-day
    # from runs + GTFS. Same Eastern service-date window as the daily-table path.
    #
    # Emit one row per service date in the window; days with no observations
    # carry `service_delivered_ratio: null` so the frontend can distinguish
    # "no data" from "ran zero trips" and skip the point in both the
    # sparkline and the 7-vs-prior-7-day delta. The discriminator is
    # `Run` existence — if no runs exist for the route on that date, we
    # have no observations regardless of what the schedule says. If runs
    # exist but `delivered_trips == 0`, that's a real 0% (every trip was
    # too thin to count as delivered).
    if metric == "service_delivered":
        trend_data = []
        current = start_date
        while current <= end_date:
            sd = compute_service_delivered(db, route_id, current)
            ratio = sd.get("ratio")
            scheduled = sd.get("scheduled_trips") or 0
            delivered = sd.get("delivered_trips") or 0
            # No-data discriminator: if the schedule says 0 trips, ratio is
            # already None (route doesn't run that day_type). If scheduled > 0
            # but we observed nothing at all, treat as no data — otherwise
            # phantom 0% points dominate the chart and the delta. Cheap
            # existence check (LIMIT 1 effectively).
            if ratio is not None and delivered == 0:
                has_runs = (
                    db.query(Run.id)
                    .filter(
                        Run.route_id == route_id,
                        Run.service_date == current.isoformat(),
                    )
                    .first()
                    is not None
                )
                if not has_runs:
                    ratio = None
            trend_data.append(
                {
                    "date": current.isoformat(),
                    "service_delivered_ratio": ratio,
                    "scheduled_trips": scheduled,
                    "delivered_trips": delivered,
                }
            )
            current = current + timedelta(days=1)
        return {
            "route_id": route_id,
            "metric": metric,
            "days": days,
            "trend_data": trend_data,
        }

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

    # Map metric name to field and response key
    metric_config = {
        "otp": {"field": "otp_percentage", "key": "otp_percentage"},
        "early": {"field": "early_percentage", "key": "early_percentage"},
        "late": {"field": "late_percentage", "key": "late_percentage"},
        "headway": {"field": "avg_headway_minutes", "key": "avg_headway_minutes"},
        "headway_std_dev": {"field": "headway_std_dev_minutes", "key": "headway_std_dev_minutes"},
        "speed": {"field": "avg_speed_mph", "key": "avg_speed_mph"},
        # NOTES-43: share of trips on the day with actual end-to-end duration
        # > 110% of scheduled. Source: `src/excess_trip_time.py`, materialized
        # in `route_metrics_daily.excess_trip_time_pct` by the daily pipeline.
        "excess_trip_time": {
            "field": "excess_trip_time_pct",
            "key": "excess_trip_time_pct",
        },
    }

    config = metric_config.get(metric, metric_config["otp"])
    field_name = config["field"]
    response_key = config["key"]

    # Emit one row per service date in the window. Days with no row in
    # `route_metrics_daily` (or with a null value) carry `<key>: null` so
    # the frontend can distinguish "no data" from a real zero and skip
    # those points in both the sparkline and the 7-vs-prior-7-day delta.
    # Without this, sparse early days in production data look like a cliff
    # to zero and break every comparison.
    by_date = {m.date: m for m in daily_metrics}
    trend_data = []
    current = start_date
    while current <= end_date:
        date_str = current.isoformat()
        day_metric = by_date.get(date_str)
        value = getattr(day_metric, field_name, None) if day_metric else None
        trend_data.append({"date": date_str, response_key: value})
        current = current + timedelta(days=1)

    return {"route_id": route_id, "metric": metric, "days": days, "trend_data": trend_data}


# ---------------------------------------------------------------------------
# System trend (NOTES-36, materialized in NOTES-48): rollup across all
# routes for the home-page strip.
#
# The trend payload covers a 60-day span (current `days` + the immediately
# prior `days` window) so a 30-vs-prior-30 delta can be computed entirely
# server-side. Returning `prior_window_value` as a single per-metric scalar
# keeps the wire shape parallel to the per-route trend (one row per service
# date in the visible window, plus a small companion field) and avoids
# double-counting concerns on the frontend.
#
# Hybrid serve path: history (every date strictly before today's Eastern
# service date) is served from the materialized `system_metrics_daily`
# table populated by `pipelines/compute_daily_metrics.py`. Today is
# computed live via `compute_system_metrics_for_date` because the daily
# pipeline runs once and won't have written today's row yet — keeps the
# strip current without paying the 60-day cold-cache cost the original
# fully-live path incurred. The `_SYSTEM_TREND_TTL_SEC` cache now mostly
# absorbs today's single-day compute on rapid refreshes; the historical
# read is sub-50ms either way.
# ---------------------------------------------------------------------------

_SYSTEM_TREND_TTL_SEC = 60.0
_system_trend_cache: dict[tuple[str, int, str], tuple[float, dict]] = {}
_system_trend_lock = Lock()


def _system_otp_series(db: Session, dates: list[date_type]) -> dict[str, float | None]:
    """System-level OTP per service_date, derived directly from `stop_events`.

    Pools every proximity stop_event with a non-null `deviation_sec` across
    all routes for each date and returns `on_time_count / total_count * 100`,
    where on-time is `OTP_EARLY_SEC <= deviation_sec <= OTP_LATE_SEC` (the
    WMATA -2/+7 window). Pooling is mathematically equivalent to weighting
    each route's OTP by its observation count — the rider-weighted aggregate
    the prior `route_metrics_daily`-backed implementation produced.

    Source filter is `proximity` to match `compute_otp_split`'s
    `all_timepoints` block (position-derived, every observed stop) and the
    historical `RouteMetricsDaily.otp_percentage` semantics.

    Days with zero qualifying stop_events return `None` so the frontend
    plots a gap. Pivoting off `route_metrics_daily` decouples the system
    trend from the legacy daily-batch pipeline (NOTES-19, partial).
    """
    if not dates:
        return {}
    start_iso = min(dates).isoformat()
    end_iso = max(dates).isoformat()
    on_time_expr = case(
        (
            (StopEvent.deviation_sec >= OTP_EARLY_SEC) & (StopEvent.deviation_sec <= OTP_LATE_SEC),
            1,
        ),
        else_=0,
    )
    rows = (
        db.query(
            StopEvent.service_date,
            func.sum(on_time_expr).label("on_time"),
            func.count(StopEvent.id).label("total"),
        )
        .filter(
            StopEvent.service_date >= start_iso,
            StopEvent.service_date <= end_iso,
            StopEvent.source == "proximity",
            StopEvent.deviation_sec.isnot(None),
        )
        .group_by(StopEvent.service_date)
        .all()
    )
    by_date: dict[str, float | None] = {}
    for date_str, on_time, total in rows:
        if total and total > 0:
            by_date[date_str] = (float(on_time) / float(total)) * 100.0
        else:
            by_date[date_str] = None
    return {d.isoformat(): by_date.get(d.isoformat()) for d in dates}


def _system_service_delivered_series(
    db: Session, dates: list[date_type]
) -> dict[str, float | None]:
    """System-level service-delivered per service_date.

    Aggregated as `sum(delivered_trips) / sum(scheduled_trips)` across every
    route on the date — the natural rider/trip-weighted aggregate. Equivalent
    to "what fraction of all scheduled trips on the system were delivered."

    Run existence is the discriminator (mirrors the per-route rule from
    PR #77): if no `runs` rows exist on a date, return `None` rather than
    `0.0`. Without Run data we can't observe delivery at all, and a literal
    zero would falsely advertise "complete failure" on dates the collector
    simply wasn't recording. Days with runs but zero scheduled trips also
    return `None` (no signal). Computed live per-day via
    `compute_service_delivered_for_routes`.
    """
    if not dates:
        return {}
    date_strs = [d.isoformat() for d in dates]
    dates_with_runs = {
        s
        for (s,) in db.query(Run.service_date)
        .filter(Run.service_date.in_(date_strs))
        .distinct()
        .all()
    }

    out: dict[str, float | None] = {}
    for d in dates:
        d_iso = d.isoformat()
        if d_iso not in dates_with_runs:
            out[d_iso] = None
            continue
        rows = compute_service_delivered_for_routes(db, d)
        scheduled = 0
        delivered = 0
        for r in rows:
            sched = r.get("scheduled_trips") or 0
            deliv = r.get("delivered_trips") or 0
            scheduled += sched
            delivered += deliv
        out[d_iso] = (delivered / scheduled) if scheduled > 0 else None
    return out


def _system_ewt_and_bunching_for_date(
    db: Session,
    service_date: date_type,
    sched_by_day_type: dict[str, dict],
) -> tuple[float | None, float | None]:
    """Pooled EWT and bunching across all routes for one service date.

    EWT is computed over the union of every route's frequent (direction,
    stop, hour) cell-hours — pooling all observed and scheduled headways into
    a single rider-weighted AWT/SWT pair. Mathematically equivalent to "EWT
    across the whole system at every cell where service is actually frequent
    on this date." Average-of-route-EWTs is wrong (averaging AWTs is wrong);
    pooling is the only correct cross-route aggregation.

    Bunching uses the same pool of routes' (direction, stop, hour) cells;
    `bunched / total` is naturally pair-weighted, so the per-route headline
    formula extends directly to the system-level union.

    `sched_by_day_type` is a memoized fetch of per-route schedule data per
    day_type so the schedule cost is amortized across many days in the
    window. Returns `(ewt_seconds, bunching_rate)` — either may be `None`
    when the underlying pool is empty or when the date has no observed
    stop_events.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)
    if day_type not in sched_by_day_type:
        sched_by_day_type[day_type] = fetch_scheduled_cell_hours_for_routes(db, day_type)
    sched_by_route = sched_by_day_type[day_type]

    obs_q = (
        db.query(
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
            StopEvent.schedule_relationship,
        )
        .filter(
            StopEvent.service_date == service_date_str,
            StopEvent.source == "trip_update",
            StopEvent.observed_arrival_ts.isnot(None),
        )
        .order_by(
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
        )
    )

    # Two parallel observed pools per (route, direction, stop, hour):
    #   - ewt: every observed pair (matches src/ewt.py)
    #   - bun: only schedule_relationship='SCHEDULED' (matches src/bunching.py)
    obs_ewt: dict[tuple[str, int, str, int], list[float]] = defaultdict(list)
    obs_bun: dict[tuple[str, int, str, int], list[float]] = defaultdict(list)

    prev_key_ewt: tuple[str, int, str] | None = None
    prev_ts_ewt: datetime | None = None
    prev_key_bun: tuple[str, int, str] | None = None
    prev_ts_bun: datetime | None = None

    for route_id, direction_id, stop_id, ts, sched_rel in obs_q.all():
        key = (route_id, direction_id, stop_id)
        # EWT pool: no schedule_relationship filter.
        if prev_key_ewt == key and prev_ts_ewt is not None:
            delta = (ts - prev_ts_ewt).total_seconds()
            if delta > 0:
                obs_ewt[(route_id, direction_id, stop_id, _eastern_hour(prev_ts_ewt))].append(delta)
        prev_key_ewt = key
        prev_ts_ewt = ts
        # Bunching pool: SCHEDULED only — keeps its own consecutive-arrival walk so
        # an ADDED row between two SCHEDULED rows doesn't fabricate a phantom pair.
        if sched_rel == "SCHEDULED":
            if prev_key_bun == key and prev_ts_bun is not None:
                delta_b = (ts - prev_ts_bun).total_seconds()
                if delta_b > 0:
                    obs_bun[(route_id, direction_id, stop_id, _eastern_hour(prev_ts_bun))].append(
                        delta_b
                    )
            prev_key_bun = key
            prev_ts_bun = ts

    # System-level EWT: pool every frequent cell-hour across every route.
    obs_pool: list[float] = []
    sched_pool: list[float] = []
    for route_id, sched_cells in sched_by_route.items():
        for cell_hour, sched_headways in sched_cells.items():
            if not _is_cell_hour_frequent(sched_headways):
                continue
            sched_pool.extend(sched_headways)
            obs_key = (route_id, *cell_hour)
            obs_pool.extend(obs_ewt.get(obs_key, []))
    awt = compute_awt(obs_pool)
    swt = compute_awt(sched_pool)
    if awt is not None and swt is not None:
        ewt_seconds: float | None = max(0.0, awt - swt)
    else:
        ewt_seconds = None

    # System-level bunching: pool every cell-hour with a defined threshold.
    bunched = 0
    total = 0
    for route_id, sched_cells in sched_by_route.items():
        for cell_hour, sched_headways in sched_cells.items():
            if not sched_headways:
                continue
            mean_sched = sum(sched_headways) / len(sched_headways)
            threshold = max(BUNCHING_RATIO * mean_sched, BUNCHING_ABSOLUTE_FLOOR_SEC)
            obs_key = (route_id, *cell_hour)
            for headway in obs_bun.get(obs_key, []):
                if headway > MAX_OBSERVED_HEADWAY_SEC:
                    continue
                total += 1
                if headway < threshold:
                    bunched += 1
    bunching_rate = (bunched / total) if total > 0 else None

    return ewt_seconds, bunching_rate


def _mean_skip_null(values: list[float | None]) -> float | None:
    """Mean of a list, skipping null entries. Returns None if no valid entries."""
    valid = [v for v in values if v is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


_METRIC_TO_COLUMN: dict[str, str] = {
    "otp": "otp_percentage",
    "service_delivered": "service_delivered_ratio",
    "ewt": "ewt_seconds",
    "bunching": "bunching_rate",
}


def _read_system_metrics_history(
    db: Session, dates: list[date_type], metric_key: str
) -> dict[str, float | None]:
    """Read the materialized `system_metrics_daily` rows for `dates`.

    Returns a dict keyed by ISO date string; every requested date appears
    in the dict, even if the row doesn't exist (value is `None`). Tests
    that don't seed the table will get all-null history, which is exactly
    the right behavior for the empty-DB envelope assertions.
    """
    if not dates:
        return {}
    start_iso = min(dates).isoformat()
    end_iso = max(dates).isoformat()
    rows = (
        db.query(SystemMetricsDaily)
        .filter(
            SystemMetricsDaily.service_date >= start_iso,
            SystemMetricsDaily.service_date <= end_iso,
        )
        .all()
    )
    by_date: dict[str, float | None] = {row.service_date: getattr(row, metric_key) for row in rows}
    return {d.isoformat(): by_date.get(d.isoformat()) for d in dates}


def _system_trend_uncached(db: Session, metric: str, days: int) -> dict:
    """Compute a system-trend payload for one metric over `days + prior days`.

    Hybrid path: prior dates and visible-but-not-today dates come from the
    materialized `system_metrics_daily` table; today's row is computed
    live via `compute_system_metrics_for_date` since the daily pipeline
    has not yet written it. Falls back to `None` for any date without a
    materialized row, matching the empty-DB envelope tests rely on.

    Returns `{metric, days, trend_data, prior_window_value}` where
    `trend_data` is one row per service date in the *current* window (days+1
    inclusive points; `value: null` for days with no data) and
    `prior_window_value` is the simple mean of the prior-window values
    (skipping null days). The 30-vs-prior-30 delta is then
    `mean(trend_data) - prior_window_value` on the frontend.
    """
    # Local import: src.system_metrics imports back into api.aggregations
    # for the per-date helpers, so a top-level import would cycle.
    from src.system_metrics import compute_system_metrics_for_date
    from src.timezones import eastern_today

    if metric not in _METRIC_TO_COLUMN:
        raise ValueError(f"Unsupported system-trend metric: {metric}")
    response_key = _METRIC_TO_COLUMN[metric]

    today = eastern_today()
    end_date = today
    start_current = end_date - timedelta(days=days)
    # Prior window is the `days` days immediately before the current window
    # — exclusive of the current window's start so the two don't overlap.
    end_prior = start_current - timedelta(days=1)
    start_prior = end_prior - timedelta(days=days - 1)

    current_dates = [
        start_current + timedelta(days=i) for i in range((end_date - start_current).days + 1)
    ]
    prior_dates = [
        start_prior + timedelta(days=i) for i in range((end_prior - start_prior).days + 1)
    ]

    # Read every materialized row in one query. Today's row will usually be
    # absent (overwritten below by the live compute) but reading it is
    # cheap and lets the table act as a backstop if the live compute fails.
    history = _read_system_metrics_history(db, prior_dates + current_dates, response_key)

    # Compute today live — single-date cost, ~1-2s rather than 60×.
    try:
        today_metrics = compute_system_metrics_for_date(db, today)
        history[today.isoformat()] = today_metrics.get(response_key)
    except Exception:
        # Live compute failure should not blow up the endpoint; fall back
        # to whatever the table currently holds for today (likely None).
        pass

    trend_data = [
        {"date": d.isoformat(), response_key: history.get(d.isoformat())} for d in current_dates
    ]
    prior_window_value = _mean_skip_null([history.get(d.isoformat()) for d in prior_dates])

    return {
        "metric": metric,
        "days": days,
        "trend_data": trend_data,
        "prior_window_value": prior_window_value,
    }


def get_system_trend_data(db: Session, metric: str = "otp", days: int = 30) -> dict:
    """System-level trend rollup for the home-page trend strip (NOTES-36).

    Returns 30 days (or `days`) of system-level values for one of OTP /
    service-delivered / EWT / bunching, plus a single `prior_window_value`
    summarizing the immediately prior `days` window so the frontend can
    render a 30-vs-prior-30 delta without fetching twice.

    Cached per `(metric, days, today)` for `_SYSTEM_TREND_TTL_SEC` (60s) so
    the home page doesn't pay the full rollup cost on every poll. The cache
    key includes today's Eastern date so the cache rolls naturally at the
    service-day boundary.

    Args:
        db: Database session
        metric: One of `otp`, `service_delivered`, `ewt`, `bunching`
        days: Length of the visible window in days (default: 30)

    Returns:
        Dict with `metric`, `days`, `trend_data` (list of `{date, <metric_key>}`),
        and `prior_window_value` (float or null).
    """
    from src.timezones import eastern_today

    cache_key = (metric, days, eastern_today().isoformat())
    with _system_trend_lock:
        cached = _system_trend_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _SYSTEM_TREND_TTL_SEC:
                return value

    result = _system_trend_uncached(db, metric, days)

    with _system_trend_lock:
        _system_trend_cache[cache_key] = (time.monotonic(), result)
    return result


# ---------------------------------------------------------------------------
# Biggest contributors view (NOTES-39): rank routes by absolute impact on
# system underperformance, not by raw worst percentage.
#
# Contribution formula (from NOTES-39):
#     contribution_score = (baseline - actual) * scheduled_trips
# for higher-is-better metrics (OTP, service-delivered) — sign-flipped for
# lower-is-better metrics (EWT, bunching) so a positive score always means
# "this route is dragging the system down."
#
# Baseline is the system's window-mean from `system_metrics_daily`. Per-route
# targets do not yet exist; NOTES-47 is the open item that adds them. When
# they land, the same formula applies with `target` substituted for
# `baseline` — this module does not need to change shape, only swap the
# baseline source.
#
# `scheduled_trips_in_window` is computed from GTFS `trips` joined to
# `calendar` for the route's day_type, then weighted by the count of days of
# that day_type in the window. We deliberately do NOT use
# `route_service_profile.scheduled_trips`: that field stores trunk-stop
# arrivals at a single unidirectional stop, useful for headway/frequency
# classification but ~half the actual trip count on bidirectional routes
# (would inflate the volume proxy by ~2x). Same reasoning as
# `src/service_delivered.py`.
#
# Snapshot semantics for `route_value`:
#   - OTP: window-mean of `route_metrics_daily.otp_percentage` (cheap;
#     materialized).
#   - service_delivered / EWT / bunching: latest single-day value from the
#     live cache (`get_live_metrics_for_today`). These metrics are not
#     materialized per-route per-day, so a window mean would require N×
#     per-day computes per route — too expensive for an interactive
#     ranking endpoint. The single-day snapshot is the freshest reasonable
#     signal; a full window-mean implementation would land alongside
#     materializing those metrics in `route_metrics_daily`.
#
# `baseline_value` always uses the window-mean from `system_metrics_daily`
# regardless of metric, since that table holds all four metrics per day. The
# baseline is therefore a window value while the route value may be a
# single-day snapshot for SD/EWT/bunching — we surface the anchor date in
# the response so the frontend can disclose the asymmetry.
# ---------------------------------------------------------------------------

_CONTRIBUTORS_TTL_SEC = 60.0
_contributors_cache: dict[tuple[str, int, str], tuple[float, dict]] = {}
_contributors_lock = Lock()

# `metric → (column-on-system-table, higher_is_better)`. The frontend uses
# `metric` as the toggle value; the backend uses both fields here.
_CONTRIBUTORS_METRIC_CONFIG: dict[str, tuple[str, bool]] = {
    "otp": ("otp_percentage", True),
    "service_delivered": ("service_delivered_ratio", True),
    "ewt": ("ewt_seconds", False),
    "bunching": ("bunching_rate", False),
}


def _system_baseline_for_window(
    db: Session, metric_column: str, end_date: date_type, days: int
) -> float | None:
    """Mean of `system_metrics_daily.<metric_column>` over the past `days` days.

    Skips null rows (days with no data) so a single empty day doesn't
    poison the mean. Returns None when no rows in the window have a
    non-null value (fresh DB, or the materialization pipeline hasn't run).
    """
    start_iso = (end_date - timedelta(days=days - 1)).isoformat()
    end_iso = end_date.isoformat()
    rows = (
        db.query(getattr(SystemMetricsDaily, metric_column))
        .filter(
            SystemMetricsDaily.service_date >= start_iso,
            SystemMetricsDaily.service_date <= end_iso,
            getattr(SystemMetricsDaily, metric_column).isnot(None),
        )
        .all()
    )
    values = [row[0] for row in rows if row[0] is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _route_otp_window_mean(
    db: Session, route_id: str, end_date: date_type, days: int
) -> float | None:
    """Mean of `route_metrics_daily.otp_percentage` for one route over the window.

    Skips null entries. Returns None when no rows in the window have a
    non-null OTP — the route either didn't run or the daily pipeline
    didn't see enough data to score it.
    """
    start_iso = (end_date - timedelta(days=days - 1)).isoformat()
    end_iso = end_date.isoformat()
    rows = (
        db.query(RouteMetricsDaily.otp_percentage)
        .filter(
            RouteMetricsDaily.route_id == route_id,
            RouteMetricsDaily.date >= start_iso,
            RouteMetricsDaily.date <= end_iso,
            RouteMetricsDaily.otp_percentage.isnot(None),
        )
        .all()
    )
    values = [row[0] for row in rows if row[0] is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _scheduled_trips_per_day_type(db: Session) -> dict[tuple[str, str], int]:
    """Distinct GTFS trip_ids per (route_id, day_type) on the current snapshot.

    Returns a dict keyed by (route_id, day_type) where day_type is one of
    `weekday` / `saturday` / `sunday`. Same semantics as
    `src/service_delivered.py`: a representative weekday (`tuesday`) plus
    Saturday and Sunday flags from `Calendar` are the membership filter,
    so a trip running every weekday is counted once for `weekday`,
    not five times. Counts both directions — a delivered round-trip is
    two trips, and missing either direction is a delivery failure.

    `is_current` filters apply to both `Trip` and `Calendar` so the count
    matches the actual schedule the dashboard is reasoning about.
    """
    out: dict[tuple[str, str], int] = {}
    day_type_field_pairs = [
        ("weekday", Calendar.tuesday),
        ("saturday", Calendar.saturday),
        ("sunday", Calendar.sunday),
    ]
    for day_type, field in day_type_field_pairs:
        rows = (
            db.query(Trip.route_id, func.count(func.distinct(Trip.trip_id)))
            .join(Calendar, Calendar.service_id == Trip.service_id)
            .filter(Trip.is_current, Calendar.is_current, field == 1)
            .group_by(Trip.route_id)
            .all()
        )
        for route_id, count in rows:
            out[(route_id, day_type)] = int(count or 0)
    return out


def _day_type_counts_in_window(end_date: date_type, days: int) -> dict[str, int]:
    """Count how many days of each day_type fall in the past `days` days.

    Inclusive of `end_date`. Returns counts keyed by `weekday` / `saturday` /
    `sunday`. Used to weight per-day_type scheduled trips up to a window
    total.
    """
    counts = {"weekday": 0, "saturday": 0, "sunday": 0}
    start = end_date - timedelta(days=days - 1)
    cur = start
    while cur <= end_date:
        wd = cur.weekday()
        if wd == 5:
            counts["saturday"] += 1
        elif wd == 6:
            counts["sunday"] += 1
        else:
            counts["weekday"] += 1
        cur = cur + timedelta(days=1)
    return counts


def _scheduled_trips_in_window_by_route(
    db: Session, end_date: date_type, days: int
) -> dict[str, int]:
    """Total scheduled trips per route over the past `days` days.

    `per_day_type[route, dt] * day_type_counts[dt]`, summed across day_types.
    Single GTFS+Calendar query per day_type (3 total), Python aggregation
    across the window. Cheap relative to per-day live computes.
    """
    per_day_type = _scheduled_trips_per_day_type(db)
    day_counts = _day_type_counts_in_window(end_date, days)
    out: dict[str, int] = {}
    for (route_id, day_type), trip_count in per_day_type.items():
        out[route_id] = out.get(route_id, 0) + trip_count * day_counts.get(day_type, 0)
    return out


def _contributors_uncached(db: Session, metric: str, days: int) -> dict:
    """Compute the contributors payload for one metric / window.

    Anchors `route_value` and `baseline_value` together so they're directly
    comparable: OTP uses the window mean for both; service_delivered, EWT,
    and bunching use the latest single-day value the live cache observed
    (`_latest_service_date_with_stop_events`) for the route value, and the
    same single date's row from `system_metrics_daily` for the baseline
    when available — falling back to a window mean if today's row hasn't
    been materialized.

    Returns `{metric, days, anchor_date, baseline_value, contributors}`
    where `contributors` is a list ranked by `contribution_score` desc
    (most-dragging routes first). Routes without enough data to score
    (route_value or baseline missing) are dropped, not listed.
    """
    if metric not in _CONTRIBUTORS_METRIC_CONFIG:
        raise ValueError(f"Unsupported contributors metric: {metric}")
    metric_column, higher_is_better = _CONTRIBUTORS_METRIC_CONFIG[metric]

    from src.timezones import eastern_today

    end_date = eastern_today()

    # Baseline: window-mean over `days`. Used as the comparison target until
    # NOTES-47 (per-route targets) lands.
    baseline_value = _system_baseline_for_window(db, metric_column, end_date, days)

    # Volume proxy: total scheduled trips per route over the window.
    sched_trips_by_route = _scheduled_trips_in_window_by_route(db, end_date, days)

    # Per-route metric values. OTP comes from the materialized daily table;
    # the other three come from the live cache (latest service_date with
    # stop_events). The live cache is a single-day snapshot — see module
    # comment for the tradeoff.
    routes = db.query(Route).filter(Route.is_current).all()
    route_short_names = {r.route_id: r.route_short_name for r in routes}
    route_long_names = {r.route_id: r.route_long_name for r in routes}

    route_values: dict[str, float | None] = {}
    if metric == "otp":
        for r in routes:
            route_values[r.route_id] = _route_otp_window_mean(db, r.route_id, end_date, days)
    else:
        # service_delivered / EWT / bunching: snapshot from live cache.
        live = get_live_metrics_for_today(db)
        for r in routes:
            metrics_bundle = live.get(r.route_id)
            fields = _live_metric_fields(metrics_bundle)
            if metric == "service_delivered":
                route_values[r.route_id] = fields.get("service_delivered_ratio")
            elif metric == "ewt":
                route_values[r.route_id] = fields.get("ewt_seconds")
            elif metric == "bunching":
                route_values[r.route_id] = fields.get("bunching_rate")

    contributors: list[dict] = []
    if baseline_value is not None:
        for route_id, route_value in route_values.items():
            if route_value is None:
                continue
            scheduled_trips = sched_trips_by_route.get(route_id, 0)
            if scheduled_trips <= 0:
                # No GTFS schedule for the window → no volume to weight by.
                # Drop rather than list with a 0 score; surfacing it would
                # rank it dead-last for every metric and add visual noise.
                continue
            gap = baseline_value - route_value
            # Sign convention: positive `contribution_score` = "dragging the
            # system down." For higher-is-better metrics, that's when route
            # is below baseline (gap > 0). For lower-is-better metrics,
            # that's when route is above baseline (gap < 0), so we flip.
            score = gap * scheduled_trips if higher_is_better else (-gap) * scheduled_trips
            contributors.append(
                {
                    "route_id": route_id,
                    "route_short_name": route_short_names.get(route_id),
                    "route_long_name": route_long_names.get(route_id),
                    "metric": metric,
                    "baseline_value": baseline_value,
                    "route_value": route_value,
                    "scheduled_trips": scheduled_trips,
                    "contribution_score": score,
                }
            )

    # Sort by contribution_score desc — biggest draggers first. Negative
    # scores (route is *better* than baseline) sort below zero-volume drops.
    contributors.sort(key=lambda c: c["contribution_score"], reverse=True)

    return {
        "metric": metric,
        "days": days,
        "baseline_value": baseline_value,
        "higher_is_better": higher_is_better,
        "contributors": contributors,
    }


def get_route_contributors(db: Session, metric: str = "otp", days: int = 30) -> dict:
    """Cached-by-(metric, days, today) wrapper for the contributors view.

    See module comment above for the contribution formula and baseline
    semantics. Cache key includes today's Eastern date so the cache rolls
    naturally at the service-day boundary.

    Args:
        db: Database session.
        metric: One of `otp`, `service_delivered`, `ewt`, `bunching`.
        days: Length of the window in days (default: 30).

    Returns:
        Dict with `metric`, `days`, `baseline_value`, `higher_is_better`,
        and `contributors` (list ranked by `contribution_score` desc).
    """
    from src.timezones import eastern_today

    cache_key = (metric, days, eastern_today().isoformat())
    with _contributors_lock:
        cached = _contributors_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _CONTRIBUTORS_TTL_SEC:
                return value

    result = _contributors_uncached(db, metric, days)

    with _contributors_lock:
        _contributors_cache[cache_key] = (time.monotonic(), result)
    return result


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
                "coverage_ratio": sanitize_float(r["coverage_ratio"]),
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
    end_time = utcnow_naive()
    start_time = end_time - timedelta(days=days)

    # Use existing time period OTP function
    result = calculate_time_period_otp(db, route_id, start_time=start_time, end_time=end_time)

    return {
        "route_id": route_id,
        "days": days,
        "periods": result.get("periods", {}),
        "thresholds": result.get("thresholds", {}),
    }


def _utc_naive_to_eastern_iso(value):
    """Convert a naive-UTC datetime to an ISO8601 string in Eastern local time.

    Reads as a naive UTC value (the storage convention from `src/timezones.py`),
    reinterprets to the America/New_York zone, then drops the tzinfo so the
    serialized string is "YYYY-MM-DDTHH:MM:SS" in Eastern. Returns None if
    `value` is None.
    """
    if value is None:
        return None
    from src.timezones import EASTERN, UTC

    aware_utc = value.replace(tzinfo=UTC)
    eastern = aware_utc.astimezone(EASTERN).replace(tzinfo=None)
    return eastern.isoformat()


def _utc_naive_to_eastern_hhmm(value):
    """Convert a naive-UTC datetime to a HH:MM string in Eastern local time.

    Used for the recent-runs row summaries where seconds are noise. Returns
    None if `value` is None.
    """
    if value is None:
        return None
    from src.timezones import EASTERN, UTC

    aware_utc = value.replace(tzinfo=UTC)
    return aware_utc.astimezone(EASTERN).strftime("%H:%M")


def get_run_deviations(db: Session, run_id: int) -> dict | None:
    """Return one run's per-stop schedule deviations, joined with stop names.

    The list is one row per scheduled stop on the run's trip, ordered by
    stop_sequence. Actual / deviation are populated where a `stop_events` row
    exists for (service_date, trip_id, stop_sequence) on the run's source;
    otherwise they're null so the chart can render gaps cleanly.

    Reads from `stop_events` directly — this is a per-row read, not a metric
    computation, so the pre-aggregation rule in CLAUDE.md doesn't apply.

    Returns None if the run_id is not found.
    """
    run = db.query(Run).filter(Run.id == run_id).first()
    if run is None:
        return None

    # Pull the trip's scheduled stops from current GTFS as the spine. Trip
    # is direction-anchored to the run, so joining stops by stop_id is safe
    # (the stop_id-not-direction-unique gotcha doesn't bite here).
    sched_rows = (
        db.query(
            StopTime.stop_sequence,
            StopTime.stop_id,
            Stop.stop_name,
            StopTime.arrival_time,
        )
        .join(Stop, (Stop.stop_id == StopTime.stop_id) & Stop.is_current)
        .filter(StopTime.trip_id == run.trip_id, StopTime.is_current)
        .order_by(StopTime.stop_sequence)
        .all()
    )

    # Pull the run's observed stop_events keyed by stop_sequence. Filter by
    # source so we don't blend trip_update and proximity rows for the same
    # (run, stop) — Run rows are per-source, so the right source is the run's.
    event_rows = (
        db.query(StopEvent)
        .filter(
            StopEvent.service_date == run.service_date,
            StopEvent.trip_id == run.trip_id,
            StopEvent.source == run.source,
        )
        .all()
    )
    events_by_seq = {e.stop_sequence: e for e in event_rows}

    # Headsign for the header — pulled live from current GTFS Trip.
    trip = db.query(Trip).filter(Trip.trip_id == run.trip_id, Trip.is_current).first()

    deviations = []
    for stop_sequence, stop_id, stop_name, _arrival_time in sched_rows:
        event = events_by_seq.get(stop_sequence)
        scheduled_iso = (
            _utc_naive_to_eastern_iso(event.scheduled_arrival_ts) if event is not None else None
        )
        actual_iso = (
            _utc_naive_to_eastern_iso(event.observed_arrival_ts) if event is not None else None
        )
        deviation_sec = event.deviation_sec if event is not None else None
        deviations.append(
            {
                "stop_sequence": stop_sequence,
                "stop_id": stop_id,
                "stop_name": stop_name,
                "scheduled": scheduled_iso,
                "actual": actual_iso,
                "deviation_sec": deviation_sec,
            }
        )

    return {
        "run_id": run.id,
        "service_date": run.service_date,
        "trip_id": run.trip_id,
        "route_id": run.route_id,
        "direction_id": run.direction_id,
        "source": run.source,
        "vehicle_id": run.vehicle_id,
        "trip_headsign": trip.trip_headsign if trip else None,
        "stops_scheduled": run.stops_scheduled,
        # stops_observable is the per-source structural ceiling — see
        # `Run.stops_observable` doc. Surfaced alongside stops_scheduled so
        # UI completeness checks can avoid the trip_update 1-stop bias.
        "stops_observable": run.stops_observable,
        "stops_observed": run.stops_observed,
        "first_obs_ts": _utc_naive_to_eastern_iso(run.first_obs_ts),
        "last_obs_ts": _utc_naive_to_eastern_iso(run.last_obs_ts),
        "dev_p50_sec": run.dev_p50_sec,
        "dev_p95_sec": run.dev_p95_sec,
        "deviations": deviations,
    }


def _route_recent_runs_service_date(db: Session, route_id: str):
    """Pick the service_date for the recent-runs list.

    Today's runs if any exist (early-day or just-aggregated case), otherwise
    the latest service_date that has runs for the route. Returns None when
    the route has no aggregated runs at all.
    """
    from src.timezones import eastern_today

    today_iso = eastern_today().isoformat()
    today_count = (
        db.query(func.count(Run.id))
        .filter(Run.route_id == route_id, Run.service_date == today_iso)
        .scalar()
    )
    if today_count and today_count > 0:
        return today_iso

    latest = db.query(func.max(Run.service_date)).filter(Run.route_id == route_id).scalar()
    return latest


def get_route_recent_runs(db: Session, route_id: str, limit: int = 25) -> dict:
    """Return up to `limit` recent runs for a route on its latest run-bearing date.

    "Latest run-bearing date" is today if there are runs for today, else the
    most recent service_date with any runs for the route — so the list is
    populated from page-load on a fresh service day even when today's
    aggregation hasn't run yet. Each row carries the per-trip headsign (joined
    from current GTFS) and the run-summary fields stored on the `runs` table.

    Returns `{"error": ...}` if the route is not found.
    """
    route = db.query(Route).filter(Route.route_id == route_id, Route.is_current).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    service_date = _route_recent_runs_service_date(db, route_id)
    if service_date is None:
        return {
            "route_id": route_id,
            "service_date": None,
            "runs": [],
        }

    # When both proximity and trip_update runs exist for the same (date, trip),
    # collapse to one row per trip — the user wants "trips run today", not
    # "source-rows derived". Prefer trip_update because its destination
    # observation rate is materially higher (see Run docstring "Source
    # asymmetry") which means run summaries are more complete.
    rows = (
        db.query(Run)
        .filter(Run.route_id == route_id, Run.service_date == service_date)
        .order_by(Run.first_obs_ts.desc().nullslast(), Run.id.desc())
        .all()
    )

    by_trip: dict[str, Run] = {}
    for r in rows:
        existing = by_trip.get(r.trip_id)
        if existing is None:
            by_trip[r.trip_id] = r
            continue
        # Keep the trip_update one if both sources have a row.
        if r.source == "trip_update" and existing.source != "trip_update":
            by_trip[r.trip_id] = r

    chosen_runs = list(by_trip.values())
    chosen_runs.sort(
        key=lambda r: (r.first_obs_ts is None, r.first_obs_ts),
        reverse=True,
    )
    chosen_runs = chosen_runs[:limit]

    # Batch-fetch headsigns for the relevant trips.
    trip_ids = [r.trip_id for r in chosen_runs]
    headsigns: dict[str, str] = {}
    if trip_ids:
        for trip in (
            db.query(Trip.trip_id, Trip.trip_headsign)
            .filter(Trip.trip_id.in_(trip_ids), Trip.is_current)
            .all()
        ):
            headsigns[trip.trip_id] = trip.trip_headsign

    return {
        "route_id": route_id,
        "service_date": service_date,
        "runs": [
            {
                "run_id": r.id,
                "trip_id": r.trip_id,
                "direction_id": r.direction_id,
                "source": r.source,
                "vehicle_id": r.vehicle_id,
                "headsign": headsigns.get(r.trip_id),
                "start_time": _utc_naive_to_eastern_hhmm(r.first_obs_ts),
                "end_time": _utc_naive_to_eastern_hhmm(r.last_obs_ts),
                "stops_scheduled": r.stops_scheduled,
                "stops_observable": r.stops_observable,
                "stops_observed": r.stops_observed,
                "dev_p50_sec": r.dev_p50_sec,
                "dev_p95_sec": r.dev_p95_sec,
                "origin_dev_sec": r.origin_dev_sec,
                "destination_dev_sec": r.destination_dev_sec,
            }
            for r in chosen_runs
        ],
    }
