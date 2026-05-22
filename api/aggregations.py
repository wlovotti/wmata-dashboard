"""
Aggregation functions for dashboard API

These functions compute high-level metrics from raw vehicle position data,
optimized for fast API responses and dashboard visualization.
"""

import math
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import date as date_type
from datetime import datetime, timedelta
from threading import Event, Lock

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.analytics import (
    calculate_time_period_otp,
)
from src.bunching import (
    BUNCHING_ABSOLUTE_FLOOR_SEC,
    BUNCHING_RATIO,
    MAX_OBSERVED_HEADWAY_SEC,
    compute_bunching_cause_breakdown,
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
from src.excess_trip_time import compute_excess_trip_time
from src.frequent_routes import get_cell_hour_gate_sec, load_frequent_route_ids
from src.models import (
    Calendar,
    CrossRouteSegmentRollup,
    Route,
    RouteDiagnosticDirection,
    RouteDiagnosticSegment,
    RouteDiagnosticTimepoint,
    RouteMetricsDailyOverlay,
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
from src.route_targets import (
    get_system_targets,
    get_target,
    get_targets_for_route,
)
from src.service_delivered import (
    compute_service_delivered,
    compute_service_delivered_for_routes,
)
from src.service_profile import (
    classify_route_frequency,
    compute_route_frequency_classes,
)
from src.time_periods import (
    ALL_DAY_TYPES,
    ALL_HOURS,
    is_hour_in_period,
)
from src.timezones import utcnow_naive

# Per-service-date cache of the new live-computed scorecard metrics. Cold
# per-date compute costs ~6-7s (scheduled fetch + observed stop_events pull
# + Python pairing). Historical dates never change once the pipeline has
# derived them, so they're safe to cache for a long time. Today's date
# being up to an hour stale on a 7-day rollup dashboard is invisible (the
# window dwarfs any in-day delta). The 1-hour TTL is the right tradeoff
# given how expensive a recompute is.
_LIVE_METRICS_TTL_SEC = 3600.0
_live_metrics_cache: dict[str, tuple[float, dict[str, dict]]] = {}
_live_metrics_lock = Lock()

# Default scorecard window (in days). The scorecard pools each metric over
# this window so a route that only runs Mon-Fri doesn't appear empty just
# because the latest service_date is a Sunday. Comparable across rows: every
# route's metric is computed over the same calendar window.
_SCORECARD_WINDOW_DAYS = 7

# Windowed live-metrics cache keyed by `(end_service_date, days)`. Shorter
# TTL than the per-date cache because this layer also caches the
# cross-route aggregation pass; refreshing it lets newly-warmed per-date
# entries flow into the rollup without waiting an hour.
_WINDOW_METRICS_TTL_SEC = 300.0
_window_metrics_cache: dict[tuple[str, int], tuple[float, dict[str, dict]]] = {}
_window_metrics_lock = Lock()

# Singleflight registry for windowed-cache compute. Without this, two
# concurrent callers on a cold cache (e.g. the lifespan-startup warm task
# and the first user request) each run the full ~40s compute in parallel.
# Threads that find an in-flight Event for their cache key wait on it,
# then read the freshly-populated cache.
_window_metrics_inflight: dict[tuple[str, int], Event] = {}


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


def _latest_service_date_for_day_type(db: Session, day_type_filter: str):
    """Return the latest service_date with stop_events matching `day_type_filter`.

    `day_type_filter` is one of `all` / `weekday` / `saturday` / `sunday`.
    `all` falls through to `_latest_service_date_with_stop_events`. Otherwise
    we scan distinct service_dates desc and return the first whose day_type
    matches. Returns None when no matching date has stop_events. Used by
    the RouteDetail filter (NOTES-41) so the headline picks a date the
    user's day-type filter would actually contribute to — anchoring on
    today's literal date but filtering live can produce empty cards.
    """
    if day_type_filter == ALL_DAY_TYPES:
        return _latest_service_date_with_stop_events(db)

    # Distinct dates with stop_events, newest-first. Scan in Python to filter
    # by day_type — the date count is small (≤ a few months in production)
    # so this is cheap; the alternative (a Calendar join) trips the SQLite
    # parity rule and isn't load-bearing for performance here.
    date_strs = [
        s
        for (s,) in db.query(StopEvent.service_date)
        .distinct()
        .order_by(StopEvent.service_date.desc())
        .all()
    ]
    for s in date_strs:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        if _day_type_for(d) == day_type_filter:
            return d
    return None


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


def _make_otp_block(
    early_count: int,
    on_time_count: int,
    late_count: int,
    total_count: int,
    source: str | None,
) -> dict:
    """Build one OTP sub-block (origin / destination / all_timepoints).

    Single source of truth for the on-the-wire OTP block shape used by
    both the live-compute path (`_compute_live_metrics_uncached` via
    `_aggregate_otp_split_window`) and the materialized-overlay hydration
    path (`_hydrate_overlay_row`). Extracting this collapses what used to
    be two inline builders and removes the drift surface that caused the
    PR #115 near-miss bug.

    Contract:
    - When `total_count == 0`: returns `{"source": source, "n": 0}` —
      the "no data" sentinel that consumers distinguish from a real 0%
      on-time. Caller must still pass `source` so downstream consumers
      can label the empty block correctly.
    - When `total_count > 0`: returns the full shape with the three
      counts, their percentages (rounded to 2 dp), `n`, and `source`.

    `total_count` is taken from the caller (matches the NOTES-66
    signature spec) and is the authority for the empty-vs-populated
    branch — callers always compute it as `early + on_time + late`,
    so the two are interchangeable in practice.
    """
    if total_count == 0:
        return {"source": source, "n": 0}
    return {
        "source": source,
        "n": total_count,
        "early": early_count,
        "on_time": on_time_count,
        "late": late_count,
        "early_pct": round(early_count * 100 / total_count, 2),
        "on_time_pct": round(on_time_count * 100 / total_count, 2),
        "late_pct": round(late_count * 100 / total_count, 2),
    }


def aggregate_metric_window(
    daily_values: list[dict | None],
    reducer_fn: Callable[[list[dict]], dict],
) -> dict | None:
    """Shared boilerplate for per-metric window aggregators.

    Handles the three steps common to every per-metric aggregator:
    1. Filter out `None` entries (days with no data for this route).
    2. Return `None` when every day in the window was null.
    3. Call `reducer_fn` with the filtered list and inject `route_id`
       from the first entry so reducers don't repeat that threading.

    `reducer_fn` receives the non-empty, non-null list and must return a
    `dict` containing all metric-specific fields.  The `route_id` key is
    injected by this function after the reducer returns — reducers must
    NOT include `route_id` in their output (it would be silently
    overwritten).

    Usage::

        def _reduce_my_metric(daily: list[dict]) -> dict:
            total = sum(d.get("count", 0) for d in daily)
            return {"count": total}

        result = aggregate_metric_window(daily_values, _reduce_my_metric)
    """
    daily_values = [d for d in daily_values if d is not None]
    if not daily_values:
        return None
    result = reducer_fn(daily_values)
    result["route_id"] = daily_values[0].get("route_id")
    return result


def _reduce_otp_split(daily_values: list[dict]) -> dict:
    """OTP split reducer: pool origin/destination/all_timepoints sub-blocks.

    Sums `early`/`on_time`/`late` counts for each sub-block key across
    all days and recomputes percentages via `_make_otp_block`.  Days where
    a sub-block has `n == 0` contribute nothing to that block's totals.
    The `window` metadata is carried forward from the first day's entry.
    """

    def _pool_block(block_key: str) -> dict:
        """Sum one OTP sub-block (origin / destination / all_timepoints) across days."""
        early = on_time = late = 0
        source = None
        for d in daily_values:
            block = d.get(block_key) or {}
            if source is None:
                source = block.get("source")
            n = block.get("n", 0)
            if n == 0:
                continue
            early += block.get("early", 0)
            on_time += block.get("on_time", 0)
            late += block.get("late", 0)
        return _make_otp_block(early, on_time, late, early + on_time + late, source)

    first = daily_values[0]
    return {
        "window": first.get("window"),
        "origin": _pool_block("origin"),
        "destination": _pool_block("destination"),
        "all_timepoints": _pool_block("all_timepoints"),
    }


def _reduce_service_delivered(daily_values: list[dict]) -> dict:
    """Service-delivered reducer: sum trip counts and recompute ratio.

    `ratio = delivered / scheduled`, or `None` when scheduled is zero
    across the entire window (route never ran vs. wasn't supposed to run).
    """
    scheduled = sum(d.get("scheduled_trips", 0) for d in daily_values)
    delivered = sum(d.get("delivered_trips", 0) for d in daily_values)
    ratio = round(delivered / scheduled, 4) if scheduled else None
    return {
        "scheduled_trips": int(scheduled),
        "delivered_trips": int(delivered),
        "ratio": ratio,
    }


def _reduce_ewt(daily_values: list[dict]) -> dict:
    """EWT reducer: pool sufficient statistics and recompute AWT/SWT/EWT.

    Uses the algebraic identity `AWT = Σh² / (2·Σh)` so the daily
    `obs_sum_h` / `obs_sum_h_sq` (and scheduled equivalents) sum cleanly
    across the window without re-pulling raw headways.  The result is
    mathematically identical to pooling every individual headway at once.
    """
    obs_sum_h = sum(d.get("obs_sum_h", 0.0) for d in daily_values)
    obs_sum_h_sq = sum(d.get("obs_sum_h_sq", 0.0) for d in daily_values)
    sched_sum_h = sum(d.get("sched_sum_h", 0.0) for d in daily_values)
    sched_sum_h_sq = sum(d.get("sched_sum_h_sq", 0.0) for d in daily_values)
    n_observed = sum(d.get("n_observed_headways", 0) for d in daily_values)
    n_scheduled = sum(d.get("n_scheduled_headways", 0) for d in daily_values)
    return {
        **_derive_ewt_metrics(
            obs_sum_h, obs_sum_h_sq, sched_sum_h, sched_sum_h_sq, n_observed, n_scheduled
        ),
        "n_observed_headways": int(n_observed),
        "n_scheduled_headways": int(n_scheduled),
    }


def _reduce_bunching(daily_values: list[dict]) -> dict:
    """Bunching reducer: sum counts and recompute the bunching rate.

    `rate = bunched / total`, or `None` when no observed headway pairs
    exist across the window.
    """
    bunched = sum(d.get("bunching_count", 0) for d in daily_values)
    total = sum(d.get("total_headways", 0) for d in daily_values)
    return {
        "bunching_count": int(bunched),
        "total_headways": int(total),
        **_derive_bunching_metrics(int(bunched), int(total)),
    }


def _aggregate_otp_split_window(daily_otp: list[dict]) -> dict | None:
    """Pool OTP split sub-blocks across days by summing raw deviation counts.

    Each per-day result has `origin`/`destination`/`all_timepoints` blocks with
    `early`/`on_time`/`late`/`n` counts (when `n > 0`) — sum them across days
    and recompute the percentages. Empty/missing days contribute nothing.
    Returns None if no day in the window has data for any sub-block.

    Delegates shared null-filtering and route-id threading to
    `aggregate_metric_window`; metric-specific logic lives in
    `_reduce_otp_split`.
    """
    return aggregate_metric_window(daily_otp, _reduce_otp_split)


def _aggregate_service_delivered_window(daily_sd: list[dict]) -> dict | None:
    """Pool service-delivered counts across days; recompute the ratio.

    Sums `scheduled_trips` and `delivered_trips`, then `ratio =
    delivered / scheduled` (None if scheduled is zero across the window —
    same "didn't run any" vs "wasn't supposed to" distinction as the per-day
    function). Returns None if no day in the window had a result.

    Delegates shared null-filtering and route-id threading to
    `aggregate_metric_window`; metric-specific logic lives in
    `_reduce_service_delivered`.
    """
    return aggregate_metric_window(daily_sd, _reduce_service_delivered)


def _derive_ewt_metrics(
    obs_sum_h: float,
    obs_sum_h_sq: float,
    sched_sum_h: float,
    sched_sum_h_sq: float,
    n_observed: int,
    n_scheduled: int,
) -> dict:
    """Compute AWT / SWT / EWT / coverage_ratio from EWT sufficient statistics.

    `AWT = Σh² / (2·Σh)` is exact under sums, so this works for both
    single-date hydration and windowed aggregation. Each field is `None`
    when its input sum is non-positive — matches the raw-pool convention
    in `src/ewt.py`. EWT is unclamped (raw `awt - swt`) to match the
    headline live-compute path; the per-period variant in `src/ewt.py`
    clamps at 0 and is not relevant here.
    """
    awt = obs_sum_h_sq / (2.0 * obs_sum_h) if obs_sum_h > 0 else None
    swt = sched_sum_h_sq / (2.0 * sched_sum_h) if sched_sum_h > 0 else None
    ewt = (awt - swt) if (awt is not None and swt is not None) else None
    coverage = (n_observed / n_scheduled) if n_scheduled > 0 else None
    return {
        "awt_seconds": round(awt, 2) if awt is not None else None,
        "swt_seconds": round(swt, 2) if swt is not None else None,
        "ewt_seconds": round(ewt, 2) if ewt is not None else None,
        "coverage_ratio": round(coverage, 4) if coverage is not None else None,
    }


def _derive_bunching_metrics(bunching_count: int, total_headways: int) -> dict:
    """Compute bunching_rate from bunching counts.

    `rate = bunched / total`, or `None` when no observed pairs exist.
    """
    rate = round(bunching_count / total_headways, 4) if total_headways else None
    return {"bunching_rate": rate}


def _aggregate_ewt_window(daily_ewt: list[dict]) -> dict | None:
    """Pool EWT sufficient statistics across days; recompute AWT/SWT/EWT.

    Uses the algebraic identity `AWT = Σh² / (2·Σh)` so daily `obs_sum_h` /
    `obs_sum_h_sq` (and the scheduled equivalents, added in this PR) sum
    cleanly across the window without re-pulling raw headways. The result
    is mathematically identical to pooling every individual headway in
    one shot.

    Delegates shared null-filtering and route-id threading to
    `aggregate_metric_window`; metric-specific logic lives in `_reduce_ewt`.
    """
    return aggregate_metric_window(daily_ewt, _reduce_ewt)


def _aggregate_bunching_window(daily_bun: list[dict]) -> dict | None:
    """Pool bunching counts across days; recompute the rate.

    Sums `bunching_count` and `total_headways`, then `rate = bunched / total`.
    Returns None if no day had any observed pairs.

    Delegates shared null-filtering and route-id threading to
    `aggregate_metric_window`; metric-specific logic lives in
    `_reduce_bunching`.
    """
    return aggregate_metric_window(daily_bun, _reduce_bunching)


def _aggregate_live_metrics_window(
    per_date_results: list[dict[str, dict]],
) -> dict[str, dict]:
    """Combine per-date `_compute_live_metrics_uncached` outputs into a windowed dict.

    Walks every route_id that appeared on any day in the window and applies
    the per-metric aggregator. Routes that didn't run on a given day simply
    contribute nothing on that day — but the window itself is identical
    across every row, so the resulting metrics are comparable.
    """
    all_routes: set[str] = set()
    for d_result in per_date_results:
        all_routes |= set(d_result.keys())

    aggregated: dict[str, dict] = {}
    for route_id in all_routes:
        daily_otp = [d.get(route_id, {}).get("otp_split") for d in per_date_results]
        daily_sd = [d.get(route_id, {}).get("service_delivered") for d in per_date_results]
        daily_ewt = [d.get(route_id, {}).get("ewt") for d in per_date_results]
        daily_bun = [d.get(route_id, {}).get("bunching") for d in per_date_results]
        aggregated[route_id] = {
            "service_delivered": _aggregate_service_delivered_window(daily_sd),
            "otp_split": _aggregate_otp_split_window(daily_otp),
            "ewt": _aggregate_ewt_window(daily_ewt),
            "bunching": _aggregate_bunching_window(daily_bun),
        }
    return aggregated


def _hydrate_overlay_row(row: RouteMetricsDailyOverlay) -> dict:
    """Reshape one `route_metrics_daily_overlay` row into a per-date bundle.

    Output shape matches what the live single-route compute path produces:
    sufficient statistics PLUS the derived headline fields (AWT/SWT/EWT,
    coverage_ratio, bunching_rate). The window aggregator pools the
    sufficient stats across days and ignores the derived fields — those
    are recomputed at the window level from the pooled sums. The detail
    endpoint, which reads a single date from the same shared cache,
    needs the derived fields directly.

    Empty OTP sub-blocks emit `{n: 0}` — same distinction the live path
    uses between "no data" and "0% on-time."
    """

    def _otp_block(early: int, on_time: int, late: int, source: str) -> dict:
        """Thin wrapper around the module-level factory; computes total locally."""
        return _make_otp_block(early, on_time, late, early + on_time + late, source)

    return {
        "service_delivered": {
            "scheduled_trips": row.scheduled_trips,
            "delivered_trips": row.delivered_trips,
            "ratio": (
                round(row.delivered_trips / row.scheduled_trips, 4) if row.scheduled_trips else None
            ),
        },
        "otp_split": {
            "origin": _otp_block(
                row.otp_origin_early, row.otp_origin_on_time, row.otp_origin_late, "proximity"
            ),
            "destination": _otp_block(
                row.otp_destination_early,
                row.otp_destination_on_time,
                row.otp_destination_late,
                "trip_update",
            ),
            "all_timepoints": _otp_block(
                row.otp_all_early, row.otp_all_on_time, row.otp_all_late, "proximity"
            ),
        },
        "ewt": {
            "obs_sum_h": row.ewt_obs_sum_h,
            "obs_sum_h_sq": row.ewt_obs_sum_h_sq,
            "n_observed_headways": row.ewt_n_observed_headways,
            "sched_sum_h": row.ewt_sched_sum_h,
            "sched_sum_h_sq": row.ewt_sched_sum_h_sq,
            "n_scheduled_headways": row.ewt_n_scheduled_headways,
            **_derive_ewt_metrics(
                row.ewt_obs_sum_h,
                row.ewt_obs_sum_h_sq,
                row.ewt_sched_sum_h,
                row.ewt_sched_sum_h_sq,
                row.ewt_n_observed_headways,
                row.ewt_n_scheduled_headways,
            ),
        },
        "bunching": {
            "bunching_count": row.bunching_count,
            "total_headways": row.bunching_total_headways,
            **_derive_bunching_metrics(row.bunching_count, row.bunching_total_headways),
        },
    }


def _read_overlay_for_dates(db: Session, dates: list[date_type]) -> dict[str, dict[str, dict]]:
    """Read materialized overlay rows for `dates`, hydrated into per-date bundles.

    Returns `{service_date_str: {route_id: bundle}}` covering only dates
    that have at least one overlay row. Dates absent from the result are
    not materialized yet (typically today, before the daily batch runs)
    and the caller should fall back to live compute for those.

    One SQL query for the whole window, then a Python pass to reshape —
    cost is dominated by ~126 routes × N days rows being materialized
    (well under 1k for a 7-day window). Sub-100ms on a warm Postgres.
    """
    if not dates:
        return {}
    date_strs = [d.isoformat() for d in dates]
    rows = (
        db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date.in_(date_strs))
        .all()
    )
    out: dict[str, dict[str, dict]] = {}
    for row in rows:
        out.setdefault(row.service_date, {})[row.route_id] = _hydrate_overlay_row(row)
    return out


def _compute_live_metrics_for_window_uncached(
    db: Session,
    end_date: date_type,
    days: int,
) -> dict[str, dict]:
    """Pool the four scorecard metrics over `[end_date - days + 1, end_date]`.

    Three-tier read path, fastest first:
      1. In-memory per-date cache (`_live_metrics_cache`, 1-hour TTL).
      2. Materialized `route_metrics_daily_overlay` rows (written by the
         daily batch — see `pipelines/upsert_route_metrics_overlay.py`).
         Sub-100ms for a 7-day window.
      3. Live compute via the multi-date helpers — used only for dates
         the daily batch hasn't materialized yet (typically today). Still
         ~35s for 7 days but normally only one date is cold at any time.

    Anything resolved from tiers 2 or 3 is stashed into the in-memory
    per-date cache, so subsequent window slides reuse it as tier 1.
    """
    from src.bunching import compute_bunching_headline_for_routes_multi_date
    from src.ewt import (
        compute_ewt_headline_for_routes_multi_date,
        fetch_observed_stop_events_for_window,
    )

    dates = [end_date - timedelta(days=i) for i in range(days)]
    cached_results: dict[str, dict[str, dict]] = {}
    uncached_dates: list[date_type] = []

    now = time.monotonic()
    with _live_metrics_lock:
        for d in dates:
            cache_key = d.isoformat()
            cached = _live_metrics_cache.get(cache_key)
            if cached is not None and (now - cached[0]) < _LIVE_METRICS_TTL_SEC:
                cached_results[cache_key] = cached[1]
            else:
                uncached_dates.append(d)

    # Tier 2: read the overlay for everything that wasn't in-memory cached.
    # Stash hits back in the per-date cache so the next window slide reads
    # tier 1.
    cold_dates: list[date_type] = []
    if uncached_dates:
        overlay = _read_overlay_for_dates(db, uncached_dates)
        for d in uncached_dates:
            ds = d.isoformat()
            if ds in overlay:
                cached_results[ds] = overlay[ds]
                with _live_metrics_lock:
                    _live_metrics_cache[ds] = (time.monotonic(), overlay[ds])
            else:
                cold_dates.append(d)

    if cold_dates:
        # Pre-fetch schedule once per distinct day_type across the cold dates;
        # the EWT and bunching multi-date computes will share the dict and
        # avoid re-fetching for every date.
        sched_by_day_type: dict[str, dict] = {}
        for d in cold_dates:
            dt = _day_type_for(d)
            if dt not in sched_by_day_type:
                sched_by_day_type[dt] = fetch_scheduled_cell_hours_for_routes(db, dt)

        # Pull observed stop_events ONCE for the window — EWT and bunching
        # share the same source filter (source='trip_update' + non-null
        # observed_arrival_ts), differing only in whether they pair across
        # non-SCHEDULED rows. Sharing the materialization saves ~9s on cold.
        observed_rows = fetch_observed_stop_events_for_window(db, cold_dates)

        ewt_by_date = compute_ewt_headline_for_routes_multi_date(
            db,
            cold_dates,
            sched_by_day_type=sched_by_day_type,
            observed_rows=observed_rows,
        )
        bunching_by_date = compute_bunching_headline_for_routes_multi_date(
            db,
            cold_dates,
            sched_by_day_type=sched_by_day_type,
            observed_rows=observed_rows,
        )

        # Service-delivered and OTP are still per-date — each does a small
        # per-route loop. Total cost across the window is modest compared to
        # the EWT/bunching pulls and not worth refactoring yet.
        sd_by_date: dict[str, dict[str, dict]] = {}
        otp_by_date: dict[str, dict[str, dict]] = {}
        for d in cold_dates:
            ds = d.isoformat()
            sd_by_date[ds] = {r["route_id"]: r for r in compute_service_delivered_for_routes(db, d)}
            otp_by_date[ds] = {r["route_id"]: r for r in compute_otp_split_for_routes(db, d)}

        # Stitch the four metrics into the per-date shape that
        # `_aggregate_live_metrics_window` consumes, then stash each cold
        # date in the per-date cache for future window slides to reuse.
        for d in cold_dates:
            ds = d.isoformat()
            ewt_d = ewt_by_date.get(ds, {})
            bun_d = bunching_by_date.get(ds, {})
            sd_d = sd_by_date.get(ds, {})
            otp_d = otp_by_date.get(ds, {})
            all_routes = set(ewt_d) | set(bun_d) | set(sd_d) | set(otp_d)
            per_date = {
                route_id: {
                    "service_delivered": sd_d.get(route_id),
                    "otp_split": otp_d.get(route_id),
                    "ewt": ewt_d.get(route_id),
                    "bunching": bun_d.get(route_id),
                }
                for route_id in all_routes
            }
            cached_results[ds] = per_date
            with _live_metrics_lock:
                _live_metrics_cache[ds] = (time.monotonic(), per_date)

    per_date_results = [cached_results[d.isoformat()] for d in dates]
    return _aggregate_live_metrics_window(per_date_results)


def _compute_live_metrics_for_date(db: Session, service_date) -> dict[str, dict]:
    """Cached wrapper around `_compute_live_metrics_uncached` for any service_date.

    The pre-existing `get_live_metrics_for_today` cache only covers the
    latest service_date. The windowed scorecard needs cached lookup for any
    date in the window, so this helper applies the same cache + TTL logic
    keyed by `service_date.isoformat()`.
    """
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


def get_live_metrics_for_window(db: Session, end_date: date_type, days: int) -> dict[str, dict]:
    """Cached-by-(end_date, days) wrapper around `_compute_live_metrics_for_window_uncached`.

    Singleflight: when one thread is computing a given (end_date, days),
    concurrent callers wait on the same Event and then read the populated
    cache — they don't each run the full ~40s compute. This is the
    difference between "first user after restart waits 40s once" vs
    "warm-up thread and first user each pay 40s in parallel."

    Cold-cache cost grows roughly linearly in `days`, but the per-date cache
    means a window-slide hits the slow path only on the newly-included date.
    Empty result dict if the window has no derived stop_events at all.
    """
    cache_key = (end_date.isoformat(), days)
    while True:
        with _window_metrics_lock:
            cached = _window_metrics_cache.get(cache_key)
            if cached is not None:
                ts, value = cached
                if (time.monotonic() - ts) < _WINDOW_METRICS_TTL_SEC:
                    return value
            inflight = _window_metrics_inflight.get(cache_key)
            if inflight is None:
                inflight = Event()
                _window_metrics_inflight[cache_key] = inflight
                we_compute = True
            else:
                we_compute = False
        if not we_compute:
            # Another thread is already computing this key — wait for it to
            # finish, then loop and read the freshly-populated cache.
            inflight.wait()
            continue
        try:
            result = _compute_live_metrics_for_window_uncached(db, end_date, days)
        except Exception:
            with _window_metrics_lock:
                _window_metrics_inflight.pop(cache_key, None)
            inflight.set()
            raise
        with _window_metrics_lock:
            _window_metrics_cache[cache_key] = (time.monotonic(), result)
            _window_metrics_inflight.pop(cache_key, None)
        inflight.set()
        return result


def _compute_single_route_live_metrics(
    db: Session,
    route_id: str,
    service_date,
    period_key: str = ALL_HOURS,
) -> dict:
    """Single-route equivalent of `_compute_live_metrics_uncached` for one route.

    Used on RouteDetail when the all-routes scorecard cache is cold — computing
    one route directly (~150ms) is much faster than triggering the full ~3s
    scorecard build just to pluck a single entry.

    `period_key` (NOTES-41) restricts EWT / bunching / OTP to the given
    Eastern-hour bucket. Service-delivered is trip-level, not stop-level,
    so the `period_key` doesn't change its value — the trip either ran or
    it didn't, regardless of which hour the rider would have been waiting.
    """
    return {
        "service_delivered": compute_service_delivered(db, route_id, service_date),
        "otp_split": compute_otp_split(db, route_id, service_date, period_key=period_key),
        "ewt": compute_ewt_headline_for_route(db, route_id, service_date, period_key=period_key),
        "bunching": compute_bunching_headline_for_route(
            db, route_id, service_date, period_key=period_key
        ),
    }


def get_live_metrics_for_route_today(
    db: Session,
    route_id: str,
    day_type_filter: str = ALL_DAY_TYPES,
    period_key: str = ALL_HOURS,
) -> dict | None:
    """Latest derived service_date's live metrics for one route, cached when warm.

    On a warm cache (any /api/routes call within the TTL), returns the cached
    bundle for `route_id` instantly. On cold cache, computes single-route
    directly without warming the full scorecard cache — RouteDetail shouldn't
    pay the all-routes price.

    `day_type_filter` (NOTES-41) anchors on the latest service_date matching
    the day_type — so picking "Saturday" on a Tuesday surfaces last
    Saturday's metrics, not Tuesday's. `period_key` restricts the live
    compute to the given Eastern-hour bucket. When either filter is set,
    the cross-route cache is bypassed (it stores unfiltered values keyed
    only by service_date). Single-route compute at ~150ms keeps the
    filter interactive without a full cache rebuild per filter combo.

    Returns `None` if no stop_events exist for the requested day_type
    (DB freshly initialized, or the filter has no matching date yet).
    """
    if day_type_filter == ALL_DAY_TYPES and period_key == ALL_HOURS:
        # Unfiltered fast path — preserves the existing cache behavior.
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

    # Filtered path — anchor on the day_type's latest matching date and skip
    # the cross-route cache (which holds unfiltered values).
    service_date = _latest_service_date_for_day_type(db, day_type_filter)
    if service_date is None:
        return None
    return _compute_single_route_live_metrics(db, route_id, service_date, period_key=period_key)


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


# --- NOTES-18 composite grade ---------------------------------------------
# Replaces the OTP-only letter grade dropped in the NOTES-19 cleanup. Inputs
# are mapped to a 0-100 scale, weighted, and bucketed into A-F. EWT is
# included only for routes where it's defined (i.e. EWT is computed at all).
#
# Weights (rationale: service_delivered is the most rider-felt failure mode;
# EWT captures unreliability for frequent service):
#   - frequent route (EWT available): OTP 30 / SD 50 / EWT 20
#   - non-frequent route (no EWT):    OTP 40 / SD 60 (renormalized)
#
# EWT-to-score mapping uses TfL's published EWT bands as the anchor:
# TfL targets EWT ≤ 60s for high-frequency routes ("good") and labels >120s
# as "poor." We need a 0 anchor too — pick 300s (5 min) as the "service
# broken" floor, linearly interpolated. Editorial choices, not strictly
# TfL-cited, but grounded in the same scale.
EWT_SCORE_TARGET_SEC = 60  # TfL "good" threshold for high-frequency service
EWT_SCORE_FLOOR_SEC = 300  # 5 min — editorial floor for "service is broken"


def _ewt_to_score(ewt_sec: float | None) -> float | None:
    """Map EWT seconds to a 0-100 score (higher is better) for grading.

    Linear interpolation between TfL's "good" target (60s = 100) and an
    editorial 5-min "broken" floor (300s = 0). Returns None when EWT is
    unavailable (caller falls back to non-frequent weights).
    """
    if ewt_sec is None:
        return None
    if ewt_sec <= EWT_SCORE_TARGET_SEC:
        return 100.0
    if ewt_sec >= EWT_SCORE_FLOOR_SEC:
        return 0.0
    return 100.0 * (EWT_SCORE_FLOOR_SEC - ewt_sec) / (EWT_SCORE_FLOOR_SEC - EWT_SCORE_TARGET_SEC)


def compute_route_grade(
    otp_pct: float | None,
    service_delivered_ratio: float | None,
    ewt_sec: float | None,
) -> str:
    """Composite letter grade (NOTES-18).

    Weighted composite of OTP, service-delivered, and EWT, bucketed:
      A ≥ 80, B ≥ 60, C ≥ 40, D ≥ 20, F otherwise.

    Returns "N/A" if either OTP or service-delivered is missing — those two
    define the non-frequent grade and are required. EWT is optional: when
    `ewt_sec` is None (route lacks frequent service the metric covers), the
    weights renormalize to OTP 40 / SD 60.

    Inputs:
      - otp_pct: 0-100 percentage (`otp_all_pct` on the live overlay)
      - service_delivered_ratio: 0-1 ratio (multiplied by 100 for scoring)
      - ewt_sec: seconds, or None for non-frequent routes
    """
    if otp_pct is None or service_delivered_ratio is None:
        return "N/A"

    sd_score = service_delivered_ratio * 100.0
    ewt_score = _ewt_to_score(ewt_sec)

    if ewt_score is not None:
        composite = otp_pct * 0.30 + sd_score * 0.50 + ewt_score * 0.20
    else:
        composite = otp_pct * 0.40 + sd_score * 0.60

    if composite >= 80:
        return "A"
    if composite >= 60:
        return "B"
    if composite >= 40:
        return "C"
    if composite >= 20:
        return "D"
    return "F"


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


# ---------------------------------------------------------------------------
# Period-over-period deltas (NOTES-38): every scorecard metric carries a
# 7-day-vs-prior-7-day delta so RouteList and RouteDetail can render
# direction-of-travel arrows server-side.
#
# Sign convention (deliberate): `delta = current_window_mean - prior_window_mean`
# without flipping for lower-is-better metrics. The frontend already knows
# which direction is "good" per metric (OTP up = good, EWT up = bad), so
# flipping on the server would mean every consumer has to *un*-flip to
# recover the raw mean diff. Keeping the raw signed delta keeps the wire
# value interpretable independently of any "is this good?" lookup.
#
# Thin-data rules (suppressed via `valid: false`):
#   1. Generic: either window has fewer than DELTA_MIN_VALID_DAYS valid days.
#      A delta computed from one or two daily samples is more about which
#      days happened to land in which window than any real change in
#      service quality, and an up/down arrow on it would mislead.
#   2. EWT-specific coverage floor: the sum of observed headways across
#      the valid days in the window must reach EWT_MIN_OBS_HEADWAYS.
#      EWT itself is only meaningful where a non-trivial pool of observed
#      headways exists; without it the AWT underlying each daily value is
#      statistically thin even when the daily count passes the generic rule.
#
# Data source: `route_metrics_daily_overlay` for OTP / SD / EWT / bunching
# (one SQL pass for the 14-day window). `excess_trip_time_pct` is computed
# live from `runs` per-route per-day (not in the overlay); the 14-day set
# is cached for the same TTL as the deltas block itself.
# ---------------------------------------------------------------------------

DELTA_WINDOW_DAYS = 7
DELTA_MIN_VALID_DAYS = 3
EWT_MIN_OBS_HEADWAYS = 20  # minimum pooled observed headways per window

_DELTAS_TTL_SEC = 60.0
_deltas_cache: dict[str, tuple[float, dict[str, dict]]] = {}
_deltas_lock = Lock()


def _mean(values: list[float]) -> float:
    """Mean of a non-empty list of floats."""
    return sum(values) / len(values)


def _build_metric_delta(
    current_values: list[float | None],
    prior_values: list[float | None],
) -> dict:
    """Compute one delta dict from a current-window list and a prior-window list.

    Both inputs may contain None for days with no observation; nulls are
    skipped when computing each window's mean. The shape is:

        {"value": float | None, "valid": bool, "current_n": int, "prior_n": int}

    `valid` is `False` whenever either window has fewer than
    `DELTA_MIN_VALID_DAYS` non-null entries — the delta would be too noisy
    to render an arrow. EWT-specific coverage gating happens in
    `_compute_ewt_delta`, not here.
    """
    current_valid = [v for v in current_values if v is not None]
    prior_valid = [v for v in prior_values if v is not None]
    current_n = len(current_valid)
    prior_n = len(prior_valid)
    if current_n < DELTA_MIN_VALID_DAYS or prior_n < DELTA_MIN_VALID_DAYS:
        return {
            "value": None,
            "valid": False,
            "current_n": current_n,
            "prior_n": prior_n,
        }
    return {
        "value": _mean(current_valid) - _mean(prior_valid),
        "valid": True,
        "current_n": current_n,
        "prior_n": prior_n,
    }


def _compute_ewt_delta(
    current_values: list[float | None],
    prior_values: list[float | None],
    current_obs_headways: list[int],
    prior_obs_headways: list[int],
) -> dict:
    """EWT delta with an additional observed-headway coverage floor.

    Layered on top of `_build_metric_delta`: even with three valid daily
    EWT values, if the pooled observed-headway count is below
    `EWT_MIN_OBS_HEADWAYS` in either window, the metric isn't operationally
    meaningful and the delta gets `valid=False`. Sums headways only across
    days that contributed an EWT value (so a day with zero observed headways
    doesn't quietly double its weight).
    """
    base = _build_metric_delta(current_values, prior_values)
    if not base["valid"]:
        return base
    current_obs = sum(
        h
        for value, h in zip(current_values, current_obs_headways, strict=False)
        if value is not None
    )
    prior_obs = sum(
        h for value, h in zip(prior_values, prior_obs_headways, strict=False) if value is not None
    )
    if current_obs < EWT_MIN_OBS_HEADWAYS or prior_obs < EWT_MIN_OBS_HEADWAYS:
        return {
            "value": None,
            "valid": False,
            "current_n": base["current_n"],
            "prior_n": base["prior_n"],
        }
    return base


def _overlay_per_route_per_day(
    db: Session, all_dates: list[date_type]
) -> tuple[
    dict[str, dict[str, float | None]],  # otp_all_pct
    dict[str, dict[str, float | None]],  # service_delivered ratio
    dict[str, dict[str, float | None]],  # ewt_seconds
    dict[str, dict[str, int]],  # ewt n_observed_headways
    dict[str, dict[str, float | None]],  # bunching_rate
]:
    """Per-route per-day metric values derived from `route_metrics_daily_overlay`.

    One SQL pass for the full 14-day window. Derives final metric values
    from each row's sufficient statistics using the same helpers used by the
    scorecard window aggregator, so results are consistent with what the
    scorecard shows.

    Returns five dicts (OTP pct, SD ratio, EWT seconds, EWT observed
    headways, bunching rate) each keyed `{route_id: {iso_date: value}}`.
    Only rows that exist in the overlay are included — dates not yet
    materialized by the daily pipeline are simply absent. `n_observed_headways`
    is the count of observed headway pairs, used downstream for the
    EWT-specific coverage floor on deltas.
    """
    if not all_dates:
        return {}, {}, {}, {}, {}
    date_strs = [d.isoformat() for d in all_dates]
    rows = (
        db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date.in_(date_strs))
        .all()
    )
    otp_by_route: dict[str, dict[str, float | None]] = defaultdict(dict)
    sd_by_route: dict[str, dict[str, float | None]] = defaultdict(dict)
    ewt_by_route: dict[str, dict[str, float | None]] = defaultdict(dict)
    ewt_obs_by_route: dict[str, dict[str, int]] = defaultdict(dict)
    bun_by_route: dict[str, dict[str, float | None]] = defaultdict(dict)

    for row in rows:
        ds = row.service_date
        rid = row.route_id

        # OTP: pooled all-timepoints on_time percentage
        all_n = row.otp_all_early + row.otp_all_on_time + row.otp_all_late
        otp_by_route[rid][ds] = round(row.otp_all_on_time * 100 / all_n, 4) if all_n else None

        # Service-delivered ratio
        sd_by_route[rid][ds] = (
            round(row.delivered_trips / row.scheduled_trips, 4) if row.scheduled_trips else None
        )

        # EWT seconds from sufficient statistics
        ewt_derived = _derive_ewt_metrics(
            row.ewt_obs_sum_h,
            row.ewt_obs_sum_h_sq,
            row.ewt_sched_sum_h,
            row.ewt_sched_sum_h_sq,
            row.ewt_n_observed_headways,
            row.ewt_n_scheduled_headways,
        )
        ewt_by_route[rid][ds] = ewt_derived.get("ewt_seconds")
        ewt_obs_by_route[rid][ds] = int(row.ewt_n_observed_headways)

        # Bunching rate
        bun_by_route[rid][ds] = _derive_bunching_metrics(
            row.bunching_count, row.bunching_total_headways
        ).get("bunching_rate")

    return otp_by_route, sd_by_route, ewt_by_route, ewt_obs_by_route, bun_by_route


def _excess_per_route_per_day(
    db: Session, all_dates: list[date_type], all_route_ids: set[str]
) -> dict[str, dict[str, float | None]]:
    """Per-route per-day excess-trip-time percentages, computed live from `runs`.

    `excess_trip_time_pct` is not materialized in `route_metrics_daily_overlay`
    (it's a trip-level metric computed directly from `runs`, post NOTES-19).
    For the delta window this is one `compute_excess_trip_time` call per
    (route, date) combination — modest cost for a 14-day × N-route window
    since each call is a single narrow SELECT against `runs`. Results are
    rolled into the same deltas cache (60s TTL) so the cost is only paid
    once per anchor date.

    Returns `{route_id: {iso_date: pct_or_None}}`.
    """
    out: dict[str, dict[str, float | None]] = defaultdict(dict)
    for service_date in all_dates:
        ds = service_date.isoformat()
        for route_id in all_route_ids:
            result = compute_excess_trip_time(db, route_id, service_date)
            pct = result.get("pct_over_110")
            out[route_id][ds] = sanitize_float(pct) if result.get("n_trips", 0) > 0 else None
    return out


def _compute_route_deltas_uncached(db: Session) -> dict[str, dict]:
    """Period-over-period deltas for every route, all five scorecard metrics.

    Window: the past `DELTA_WINDOW_DAYS` days (current) and the
    `DELTA_WINDOW_DAYS` days immediately preceding (prior). Today's Eastern
    date anchors the current window's right edge.

    Returns `{route_id: {metric_key: delta_dict}}`. Any route present in
    the overlay for the window will appear; routes not in any source for
    the window are absent (the scorecard caller defaults them to all-suppressed
    deltas via `_empty_deltas()`).
    """
    from src.timezones import eastern_today

    today = eastern_today()
    end_current = today
    start_current = end_current - timedelta(days=DELTA_WINDOW_DAYS - 1)
    end_prior = start_current - timedelta(days=1)
    start_prior = end_prior - timedelta(days=DELTA_WINDOW_DAYS - 1)

    current_dates = [start_current + timedelta(days=i) for i in range(DELTA_WINDOW_DAYS)]
    prior_dates = [start_prior + timedelta(days=i) for i in range(DELTA_WINDOW_DAYS)]
    all_dates = prior_dates + current_dates

    # One SQL pass for OTP / SD / EWT / bunching from the overlay.
    otp_by_route, sd_by_route, ewt_by_route, ewt_obs_by_route, bun_by_route = (
        _overlay_per_route_per_day(db, all_dates)
    )

    # Union of all route_ids that appear in any overlay row for the window.
    route_ids = set(otp_by_route) | set(sd_by_route) | set(ewt_by_route) | set(bun_by_route)

    # Excess trip time: live compute per (route, date). Only run for routes
    # that appear in the overlay so we don't iterate the full route catalog.
    excess_by_route = _excess_per_route_per_day(db, all_dates, route_ids)

    out: dict[str, dict] = {}
    for route_id in route_ids:
        current_iso = [d.isoformat() for d in current_dates]
        prior_iso = [d.isoformat() for d in prior_dates]

        otp_map = otp_by_route.get(route_id, {})
        sd_map = sd_by_route.get(route_id, {})
        ewt_map = ewt_by_route.get(route_id, {})
        ewt_obs_map = ewt_obs_by_route.get(route_id, {})
        bun_map = bun_by_route.get(route_id, {})
        excess_map = excess_by_route.get(route_id, {})

        # SD ratio is stored 0..1 — the wire delta is also 0..1; the
        # frontend already multiplies by 100 wherever it renders SD as a
        # percentage. OTP pct and excess_trip_time_pct are in 0..100 units.
        # Bunching rate is 0..1. Mixing scales is fine because each consumer
        # knows each metric's units.
        out[route_id] = {
            "otp": _build_metric_delta(
                [otp_map.get(d) for d in current_iso],
                [otp_map.get(d) for d in prior_iso],
            ),
            "service_delivered": _build_metric_delta(
                [sd_map.get(d) for d in current_iso],
                [sd_map.get(d) for d in prior_iso],
            ),
            "ewt": _compute_ewt_delta(
                [ewt_map.get(d) for d in current_iso],
                [ewt_map.get(d) for d in prior_iso],
                [ewt_obs_map.get(d, 0) for d in current_iso],
                [ewt_obs_map.get(d, 0) for d in prior_iso],
            ),
            "bunching": _build_metric_delta(
                [bun_map.get(d) for d in current_iso],
                [bun_map.get(d) for d in prior_iso],
            ),
            "excess_trip_time_pct": _build_metric_delta(
                [excess_map.get(d) for d in current_iso],
                [excess_map.get(d) for d in prior_iso],
            ),
        }
    return out


def get_route_deltas_all(db: Session) -> dict[str, dict]:
    """Cached-by-today wrapper for the all-routes deltas computation.

    Cache key is today's Eastern service date so the cache rolls naturally
    at the day boundary. TTL `_DELTAS_TTL_SEC` (60s) absorbs repeated
    scorecard polls within the same minute. Thread-safe via `_deltas_lock`.
    """
    from src.timezones import eastern_today

    cache_key = eastern_today().isoformat()
    with _deltas_lock:
        cached = _deltas_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _DELTAS_TTL_SEC:
                return value

    result = _compute_route_deltas_uncached(db)

    with _deltas_lock:
        _deltas_cache[cache_key] = (time.monotonic(), result)
    return result


def compute_route_deltas(db: Session, route_id: str) -> dict:
    """Period-over-period deltas for one route across all five scorecard metrics.

    Returns a dict shaped like::

        {
            "otp": {"value": 0.024, "valid": true, "current_n": 5, "prior_n": 6},
            "service_delivered": {...},
            "ewt": {...},
            "bunching": {...},
            "excess_trip_time_pct": {...},
        }

    Each per-metric block is `current_window_mean - prior_window_mean`
    where each window is `DELTA_WINDOW_DAYS` days. The sign is *not*
    flipped for lower-is-better metrics (EWT, bunching, excess_trip_time_pct);
    the consumer interprets direction-of-good per metric.

    `valid=False` means the delta should not be rendered as an arrow; both
    `current_n` and `prior_n` are still populated for tooltips/debugging.
    Suppression rules:
      * Either window has fewer than `DELTA_MIN_VALID_DAYS` (3) non-null
        daily samples.
      * EWT additionally suppresses when the pooled observed headways fall
        below `EWT_MIN_OBS_HEADWAYS` in either window.

    A route absent from the overlay for the window returns the same shape
    with all metrics suppressed (`valid=False`, `value=None`).
    """
    all_deltas = get_route_deltas_all(db)
    if route_id in all_deltas:
        return all_deltas[route_id]
    suppressed = {
        "value": None,
        "valid": False,
        "current_n": 0,
        "prior_n": 0,
    }
    return {
        "otp": dict(suppressed),
        "service_delivered": dict(suppressed),
        "ewt": dict(suppressed),
        "bunching": dict(suppressed),
        "excess_trip_time_pct": dict(suppressed),
    }


def _empty_deltas() -> dict:
    """All-suppressed delta block for routes with no overlay data in the window.

    Used as a default in the scorecard payload so every route row carries
    a `deltas` key even when no data was available for the 14-day window.
    """
    suppressed = {"value": None, "valid": False, "current_n": 0, "prior_n": 0}
    return {
        "otp": dict(suppressed),
        "service_delivered": dict(suppressed),
        "ewt": dict(suppressed),
        "bunching": dict(suppressed),
        "excess_trip_time_pct": dict(suppressed),
    }


def get_all_routes_scorecard(db: Session, days: int = _SCORECARD_WINDOW_DAYS) -> dict:
    """
    Get performance scorecard for all routes, pooled over a rolling window.

    Each metric is pooled across `[end_date - days + 1, end_date]` where
    `end_date` is the latest service_date with stop_events. Pooling uses
    sufficient statistics so the windowed values are mathematically identical
    to recomputing each metric over every raw observation in the window:
      - OTP: sum early/on_time/late counts, recompute pcts.
      - Service-delivered: sum scheduled and delivered, recompute ratio.
      - EWT: sum Σh and Σh² for observed and scheduled pools, recompute
        AWT/SWT/EWT (the formula `mean(h²)/(2·mean(h))` is exact under sums).
      - Bunching: sum bunched_count and total_headways, recompute rate.

    Why a window: prior behavior anchored on a single service_date, so any
    route that didn't run that day (Sunday-only short turns, school routes,
    layover-only routes) showed empty cells even when its weekday data was
    healthy. Every row now reflects the same calendar window — comparable
    across routes.

    Args:
        db: Database session
        days: Window length in days (default 7). The window ends on the
            latest service_date with stop_events.

    Returns:
        Dict with `window` (with `start`, `end`, `days`) and `routes` (list
        of route summaries sorted by `otp_all_pct` descending; routes
        without OTP data last). Returns an empty `routes` list when no
        derived stop_events exist yet.
    """
    if days < 1:
        days = 1

    # Get all routes (current version only)
    routes = db.query(Route).filter(Route.is_current).all()

    end_date = _latest_service_date_with_stop_events(db)
    if end_date is None:
        # No derived stop_events yet — every row gets all-None overlay; the
        # window block reports the requested length but null endpoints so
        # the frontend can still render the heading without a date range.
        live: dict[str, dict] = {}
        window_start_iso = None
        window_end_iso = None
    else:
        live = get_live_metrics_for_window(db, end_date, days)
        window_end_iso = end_date.isoformat()
        window_start_iso = (end_date - timedelta(days=days - 1)).isoformat()

    # Frequency class per route (GTFS-derived, ~2ms — no caching needed).
    freq_classes = compute_route_frequency_classes(db)

    # WMATA-designated frequent-service routes (NOTES-56). One mtime-cached
    # read of the YAML; the set membership check below is constant-time.
    frequent_route_ids = load_frequent_route_ids()

    # Period-over-period deltas (NOTES-38) for every metric. Cached separately
    # (60s TTL keyed by Eastern date) so cold-cache cost only hits once per
    # anchor date; subsequent scorecard builds within the TTL pull straight
    # from the cache.
    deltas = get_route_deltas_all(db)

    scorecard: list[dict] = []
    for route in routes:
        live_fields = _live_metric_fields(live.get(route.route_id))
        scorecard.append(
            {
                "route_id": route.route_id,
                "route_name": route.route_short_name,
                "route_long_name": route.route_long_name,
                "frequency_class": freq_classes.get(route.route_id),
                # NOTES-56: route-level WMATA designation (config/
                # frequent_routes.yaml). Drives headline-KPI choice on the
                # frontend — EWT becomes the headline for `True`, OTP for
                # `False`. Distinct from `frequency_class` (data-derived,
                # rough bin) and `src/ewt.py:FREQUENT_HEADWAY_MAX_SEC`
                # (per-cell-hour gate inside EWT computation).
                "is_frequent": route.route_id in frequent_route_ids,
                "grade": compute_route_grade(
                    live_fields["otp_all_pct"],
                    live_fields["service_delivered_ratio"],
                    live_fields["ewt_seconds"],
                ),
                # Per-route targets (NOTES-47). Keyed by canonical metric
                # name; values are in the same units as the corresponding
                # live fields (OTP %, service_delivered fraction, EWT
                # seconds, bunching fraction). `None` when no target is
                # configured in `config/route_targets.yaml`.
                "targets": get_targets_for_route(route.route_id),
                # Period-over-period deltas (NOTES-38). Shape per metric:
                # {value, valid, current_n, prior_n}. `valid=False` means
                # thin data — don't render an arrow. Sign is raw
                # (current_mean - prior_mean), not flipped for lower-is-better.
                "deltas": deltas.get(route.route_id) or _empty_deltas(),
                **live_fields,
            }
        )

    # Sort by live OTP descending (best first); routes without an OTP
    # number sink to the bottom so the table reads top-down by signal.
    scorecard.sort(key=lambda x: (x.get("otp_all_pct") is None, -(x.get("otp_all_pct") or 0)))

    return {
        "window": {
            "start": window_start_iso,
            "end": window_end_iso,
            "days": days,
        },
        "routes": scorecard,
    }


def _excess_trip_time_fields(db: Session, route_id: str, days: int = 7) -> dict:
    """Most-recent non-zero excess-trip-time values for one route.

    Walks back day-by-day through the last `days`-day window, calling
    `compute_excess_trip_time` (NOTES-19 migration: now sourced live from
    `runs`, not from materialized `route_metrics_daily.excess_trip_time_pct`).
    Returns the first day with `n_trips > 0` — the "freshest reasonable
    signal" the KPI card and subline display, rather than a smoothed
    average over days that may have been TU-blind. Returns all fields as
    None when no day in the window has any qualifying trips.

    Cost: typically one `compute_excess_trip_time` call (the most recent
    day usually has data); worst case `days` calls. Each call is one
    SELECT against `runs` for the route+date plus a small Python pass.
    """
    from datetime import timedelta

    from src.timezones import eastern_today

    end_date = eastern_today()
    for offset in range(days + 1):
        d = end_date - timedelta(days=offset)
        result = compute_excess_trip_time(db, route_id, d)
        if result["n_trips"] > 0:
            return {
                "excess_trip_time_pct": sanitize_float(result["pct_over_110"]),
                "excess_trip_time_median_actual_sec": result["median_actual_sec"],
                "excess_trip_time_median_scheduled_sec": result["median_scheduled_sec"],
                "excess_trip_time_n_trips": result["n_trips"],
                "excess_trip_time_as_of_date": d.isoformat(),
            }
    return {
        "excess_trip_time_pct": None,
        "excess_trip_time_median_actual_sec": None,
        "excess_trip_time_median_scheduled_sec": None,
        "excess_trip_time_n_trips": None,
        "excess_trip_time_as_of_date": None,
    }


def get_route_detail_metrics(
    db: Session,
    route_id: str,
    days: int = 7,
    day_type_filter: str = ALL_DAY_TYPES,
    period_key: str = ALL_HOURS,
) -> dict:
    """
    Get detailed performance metrics for a specific route.

    Returns the live overlay metrics (OTP origin/destination split,
    service-delivered, EWT, bunching) plus the freshest excess-trip-time
    snapshot, identity fields, and frequency class.

    `day_type_filter` and `period_key` (NOTES-41) re-slice the live
    metrics by day-of-week and time-of-day. Service-delivered and
    excess-trip-time are trip-level so the period filter has no effect on
    them; the day_type filter anchors live metrics on the latest matching
    service_date.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        days: Window for the excess-trip-time freshest-day lookup
        day_type_filter: One of `all` / `weekday` / `saturday` / `sunday`
        period_key: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`

    Returns:
        Dictionary with detailed route metrics; echoes the active filter
        values so the frontend can render the chip without a round-trip
        to its own state.
    """
    # Get route info (current version only)
    route = db.query(Route).filter(Route.route_id == route_id, Route.is_current).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    # Live metrics for today (single-route compute on cache miss).
    live_fields = _live_metric_fields(
        get_live_metrics_for_route_today(
            db, route_id, day_type_filter=day_type_filter, period_key=period_key
        )
    )

    # Excess trip time (NOTES-43): freshest non-zero value within the window,
    # computed live per-day from `runs` (NOTES-19 migration). Trip-level
    # metric — period filter doesn't decompose into it (the trip spans
    # hours). The day_type filter is also not threaded through here: the
    # freshest-non-zero lookup over a 7-day window already collapses across
    # day_types in practice.
    excess_fields = _excess_trip_time_fields(db, route_id, days=days)

    # Period-over-period deltas (NOTES-38). Server-computed so RouteList and
    # RouteDetail see the same values. RouteDetail's KPI cards consume these
    # directly; the trend block keeps its own client-side deltas because they
    # pair with the sparkline render (different code path, same 7-day window).
    route_deltas = compute_route_deltas(db, route_id)

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

    return {
        "route_id": route.route_id,
        "route_name": route.route_short_name,
        "route_long_name": route.route_long_name,
        "time_period_days": days,
        "day_type_filter": day_type_filter,
        "period_key": period_key,
        "frequency_class": frequency_class,
        # NOTES-56: WMATA-designated frequent-service routes get EWT as
        # the headline KPI on the frontend; standard routes keep OTP.
        # Same source-of-truth as the all-routes scorecard.
        "is_frequent": route_id in load_frequent_route_ids(),
        "grade": compute_route_grade(
            live_fields["otp_all_pct"],
            live_fields["service_delivered_ratio"],
            live_fields["ewt_seconds"],
        ),
        # Per-route targets (NOTES-47). See `get_all_routes_scorecard`
        # for the shape — keyed by canonical metric name in canonical
        # units. The frontend renders them next to each KPI card.
        "targets": get_targets_for_route(route_id),
        # Period-over-period deltas (NOTES-38). Shape per metric:
        # {value, valid, current_n, prior_n}. `valid=False` means
        # thin data — don't render an arrow. Sign is raw
        # (current_mean - prior_mean), not flipped for lower-is-better.
        "deltas": route_deltas,
        **live_fields,
        **excess_fields,
    }


def _compute_otp_per_day_with_filters(
    db: Session,
    route_id: str,
    start_date: date_type,
    end_date: date_type,
    period_key: str,
) -> dict[str, float | None]:
    """Per-service-date OTP for one route, computed live from stop_events.

    Mirrors `compute_otp_split`'s `all_timepoints` semantics: source =
    proximity (position-derived, every observed stop), bucketed by
    Eastern hour of `observed_arrival_ts` and filtered by `period_key`.
    Returns `{date_str: percentage_or_None}` for every date in the window
    — `None` when no qualifying stops exist on that date.

    Hour filter is applied in Python to keep test parity with SQLite
    (which can't `EXTRACT(HOUR FROM ... AT TIME ZONE ...)`).
    `is_hour_in_period(_, ALL_HOURS)` returns True for every hour, so
    passing `ALL_HOURS` produces the unfiltered daily aggregate.
    """
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    rows = (
        db.query(
            StopEvent.service_date,
            StopEvent.deviation_sec,
            StopEvent.observed_arrival_ts,
        )
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date >= start_iso,
            StopEvent.service_date <= end_iso,
            StopEvent.source == "proximity",
            StopEvent.deviation_sec.isnot(None),
        )
        .all()
    )

    counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [on_time, total]
    for service_date, dev, ts in rows:
        if ts is None:
            continue
        # Reuse the OTP eastern-hour helper: same naive-UTC → Eastern
        # convention used elsewhere, DST-aware via zoneinfo.
        from src.otp_metrics import _eastern_hour as _otp_eastern_hour

        h = _otp_eastern_hour(ts)
        if h is None or not is_hour_in_period(h, period_key):
            continue
        counts[service_date][1] += 1
        if OTP_EARLY_SEC <= dev <= OTP_LATE_SEC:
            counts[service_date][0] += 1

    out: dict[str, float | None] = {}
    current = start_date
    while current <= end_date:
        d_iso = current.isoformat()
        on_time, total = counts.get(d_iso, [0, 0])
        out[d_iso] = (on_time / total * 100.0) if total > 0 else None
        current = current + timedelta(days=1)
    return out


def get_route_trend_data(
    db: Session,
    route_id: str,
    metric: str = "otp",
    days: int = 30,
    day_type_filter: str = ALL_DAY_TYPES,
    period_key: str = ALL_HOURS,
) -> dict:
    """
    Get time-series trend data for a specific route metric.

    Computes daily values for OTP, headway, speed, or service-delivered over the
    specified time period. Used for trend charts on the route detail page.

    `service_delivered` is computed live per service_date from `runs` + GTFS
    via `compute_service_delivered` (NOTES-37). The trend loop pays one pair
    of count queries per day in the window; acceptable on a per-route detail
    page (not iterated over a route list).

    `day_type_filter` (NOTES-41) drops dates whose day-of-week doesn't match
    (weekday / saturday / sunday); the row's value is set to `None` so the
    sparkline draws gaps cleanly rather than collapsing the time axis.
    `period_key` is meaningful only for the `otp` metric — OTP is computed
    per-day from `stop_events` with the hour filter applied. For non-otp
    metrics, `period_key` is ignored: excess_trip_time and service_delivered
    are trip-level (the trip spans hours), and headway / speed legacy fields
    aren't grouped by hour. Document mismatch the frontend chip with a
    "filter applies to OTP only" note when both are set on a non-otp metric.

    Args:
        db: Database session
        route_id: Route identifier (e.g., 'C51')
        metric: Metric to analyze ('otp', 'early', 'late', 'headway',
            'headway_std_dev', 'speed', 'service_delivered',
            'excess_trip_time')
        days: Number of days to analyze (default: 30)
        day_type_filter: One of `all` / `weekday` / `saturday` / `sunday`
        period_key: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`. Only `otp` recomputes by hour; other
            metrics ignore it.

    Returns:
        Time-series data for the specified metric
    """
    from datetime import timedelta

    from src.timezones import eastern_today

    # Calculate date range in Eastern (the WMATA service date)
    end_date = eastern_today()
    start_date = end_date - timedelta(days=days)

    def _matches_day_type(d: date_type) -> bool:
        """True iff `d` matches the active day_type filter (or filter is `all`)."""
        if day_type_filter == ALL_DAY_TYPES:
            return True
        return _day_type_for(d) == day_type_filter

    # service_delivered: compute per-day from runs + GTFS. Emit one row per
    # service date in the window; days with no observations carry
    # `service_delivered_ratio: null` so the frontend can distinguish "no
    # data" from "ran zero trips" and skip the point in both the sparkline
    # and the 7-vs-prior-7-day delta. The discriminator is `Run` existence
    # — if no runs exist for the route on that date, we have no
    # observations regardless of what the schedule says. If runs exist but
    # `delivered_trips == 0`, that's a real 0% (every trip was too thin to
    # count as delivered).
    if metric == "service_delivered":
        trend_data = []
        current = start_date
        while current <= end_date:
            # Day-type filter: skip dates that don't match (emit null so the
            # sparkline draws a gap rather than a phantom 0%).
            if not _matches_day_type(current):
                trend_data.append(
                    {
                        "date": current.isoformat(),
                        "service_delivered_ratio": None,
                        "scheduled_trips": 0,
                        "delivered_trips": 0,
                    }
                )
                current = current + timedelta(days=1)
                continue
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
            "day_type_filter": day_type_filter,
            "period_key": period_key,
            "trend_data": trend_data,
        }

    # excess_trip_time series — computed live per-day from `runs`
    # (NOTES-19 migration). Trip-level metric so `period_key` doesn't
    # decompose into it. Days with no qualifying trips emit `null` so the
    # frontend can distinguish "no observations" from a real zero — same
    # null-gap convention OTP and service_delivered use.
    if metric == "excess_trip_time":
        trend_data = []
        current = start_date
        while current <= end_date:
            d_iso = current.isoformat()
            value: float | None
            if not _matches_day_type(current):
                value = None
            else:
                result = compute_excess_trip_time(db, route_id, current)
                value = sanitize_float(result["pct_over_110"]) if result["n_trips"] > 0 else None
            trend_data.append({"date": d_iso, "excess_trip_time_pct": value})
            current = current + timedelta(days=1)
        return {
            "route_id": route_id,
            "metric": metric,
            "days": days,
            "day_type_filter": day_type_filter,
            "period_key": period_key,
            "trend_data": trend_data,
        }

    # OTP series — proximity stop_events are the canonical source
    # (NOTES-19 migration). The same helper handles both the no-filter and
    # period-filtered cases — `is_hour_in_period(_, ALL_HOURS)` returns True
    # for every hour, so ALL_HOURS produces the unfiltered daily aggregate
    # the legacy `route_metrics_daily.otp_percentage` field used to carry.
    # OTP is the only remaining trend metric; the legacy `early/late/
    # headway/headway_std_dev/speed` metrics were dropped in NOTES-19
    # alongside the source table.
    otp_by_date = _compute_otp_per_day_with_filters(db, route_id, start_date, end_date, period_key)
    trend_data = []
    current = start_date
    while current <= end_date:
        d_iso = current.isoformat()
        value: float | None
        if not _matches_day_type(current):
            value = None
        else:
            value = otp_by_date.get(d_iso)
        trend_data.append({"date": d_iso, "otp_percentage": value})
        current = current + timedelta(days=1)
    return {
        "route_id": route_id,
        "metric": metric,
        "days": days,
        "day_type_filter": day_type_filter,
        "period_key": period_key,
        "trend_data": trend_data,
    }


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
# table populated by `pipelines/upsert_system_metrics_daily.py` (run once
# per service date by the daily batch). Today is computed live via
# `compute_system_metrics_for_date` because the batch runs once per day
# and won't have written today's row yet — keeps the strip current
# without paying the 60-day cold-cache cost the original fully-live path
# incurred. The `_SYSTEM_TREND_TTL_SEC` cache now mostly absorbs today's
# single-day compute on rapid refreshes; the historical read is
# sub-50ms either way.
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
    each route's OTP by its observation count — the rider-weighted aggregate.

    Source filter is `proximity` to match `compute_otp_split`'s
    `all_timepoints` block (position-derived, every observed stop).

    Days with zero qualifying stop_events return `None` so the frontend
    plots a gap.
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
    # Each route's cell-hour gate matches its tier (15-min for high-freq /
    # undesignated, 20-min for medium-freq) — see src/frequent_routes.py.
    obs_pool: list[float] = []
    sched_pool: list[float] = []
    for route_id, sched_cells in sched_by_route.items():
        gate_sec = get_cell_hour_gate_sec(route_id)
        for cell_hour, sched_headways in sched_cells.items():
            if not _is_cell_hour_frequent(sched_headways, gate_sec):
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

    # System-default target for this metric (NOTES-47). Surfaces as a
    # reference line / "vs target" badge on the system trend strip.
    # `None` when the YAML omits the metric — the frontend hides the
    # badge in that case rather than rendering a placeholder.
    target_value = get_system_targets().get(metric)

    return {
        "metric": metric,
        "days": days,
        "trend_data": trend_data,
        "prior_window_value": prior_window_value,
        "target_value": target_value,
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
# Reference value is the route's configured target from
# `config/route_targets.yaml` when set (PR #99), otherwise the system's
# window-mean from `system_metrics_daily` (`baseline_value`). Each row
# reports `reference_source` so the frontend can disclose which was used.
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
#   - OTP: window-mean computed live from `stop_events` via
#     `_route_otp_window_mean` (one per-day OTP per date in the window,
#     averaged with null-skip).
#   - service_delivered / EWT / bunching: latest single-day value from the
#     live cache (`get_live_metrics_for_today`). These metrics are not
#     materialized per-route per-day, so a window mean would require N×
#     per-day computes per route — too expensive for an interactive
#     ranking endpoint. The single-day snapshot is the freshest reasonable
#     signal.
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
    """Mean of per-day OTP for one route over the window.

    Skips dates with no observations so a single empty day doesn't
    poison the mean. Returns None when no day in the window has any
    qualifying proximity stop_events — the route either didn't run or
    the derivation pipeline hasn't materialized events yet.

    Source is proximity `stop_events.deviation_sec` bucketed via the WMATA
    OTP window — same path as `_compute_otp_per_day_with_filters` and
    `_system_otp_series`. Mean-of-daily-percentages (not pooled) so the
    route value stays comparable to the system baseline computed by
    `_system_baseline_for_window`, which also averages per-day rates.
    """
    start_date = end_date - timedelta(days=days - 1)
    by_date = _compute_otp_per_day_with_filters(db, route_id, start_date, end_date, ALL_HOURS)
    values = [v for v in by_date.values() if v is not None]
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

    # System window-mean baseline. Per-row reference comes from either
    # the route's configured target (PR #99) or this baseline as fallback.
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

    # NOTES-47: per-route target replaces the system baseline as the
    # comparison reference whenever one is configured. The `reference_value`
    # field on each row is the value actually used to compute the gap —
    # equal to the route's target if set, otherwise the system baseline.
    # `baseline_value` on the envelope stays the system window mean so the
    # frontend can still render the "system baseline" annotation.
    contributors: list[dict] = []
    # Score against a row's effective reference (target or baseline). If
    # neither is available we drop the row — there's nothing to compare to.
    for route_id, route_value in route_values.items():
        if route_value is None:
            continue
        scheduled_trips = sched_trips_by_route.get(route_id, 0)
        if scheduled_trips <= 0:
            # No GTFS schedule for the window → no volume to weight by.
            # Drop rather than list with a 0 score; surfacing it would
            # rank it dead-last for every metric and add visual noise.
            continue
        per_route_target = get_target(route_id, metric)
        if per_route_target is not None:
            reference_value = per_route_target
            reference_source = "target"
        elif baseline_value is not None:
            reference_value = baseline_value
            reference_source = "baseline"
        else:
            # Neither a target nor a baseline — skip rather than score
            # against nothing.
            continue
        gap = reference_value - route_value
        # Sign convention: positive `contribution_score` = "dragging the
        # system down." For higher-is-better metrics, that's when route
        # is below the reference (gap > 0). For lower-is-better metrics,
        # that's when route is above the reference (gap < 0), so we flip.
        score = gap * scheduled_trips if higher_is_better else (-gap) * scheduled_trips
        contributors.append(
            {
                "route_id": route_id,
                "route_short_name": route_short_names.get(route_id),
                "route_long_name": route_long_names.get(route_id),
                "metric": metric,
                "baseline_value": baseline_value,
                "target_value": per_route_target,
                "reference_value": reference_value,
                "reference_source": reference_source,
                "route_value": route_value,
                "scheduled_trips": scheduled_trips,
                "contribution_score": score,
            }
        )

    # Sort by contribution_score desc — biggest draggers first. Negative
    # scores (route is *better* than baseline) sort below zero-volume drops.
    contributors.sort(key=lambda c: c["contribution_score"], reverse=True)

    # System-default target (NOTES-47) for this metric — the frontend
    # surfaces it as the reference annotation alongside `baseline_value`.
    # Per-row `target_value` may differ if a route has an override.
    system_target_value = get_system_targets().get(metric)

    return {
        "metric": metric,
        "days": days,
        "baseline_value": baseline_value,
        "system_target_value": system_target_value,
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
        # block_id (NOTES-45) — surface so the per-run page can link to the
        # block timeline (`/blocks/:blockId`) for cascade-vs-incidental
        # context. Null when the trip predates the current GTFS snapshot or
        # has no block assigned.
        "block_id": trip.block_id if trip else None,
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

    # Batch-fetch headsigns + block_ids for the relevant trips. `block_id` is
    # surfaced (NOTES-45) so the recent-runs row can hyperlink to the block
    # timeline view (`/blocks/:blockId`), where a run's lateness is shown
    # alongside the chained sibling trips on the same vehicle.
    trip_ids = [r.trip_id for r in chosen_runs]
    headsigns: dict[str, str] = {}
    block_ids: dict[str, str | None] = {}
    if trip_ids:
        for trip in (
            db.query(Trip.trip_id, Trip.trip_headsign, Trip.block_id)
            .filter(Trip.trip_id.in_(trip_ids), Trip.is_current)
            .all()
        ):
            headsigns[trip.trip_id] = trip.trip_headsign
            block_ids[trip.trip_id] = trip.block_id

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
                "block_id": block_ids.get(r.trip_id),
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


# ---------------------------------------------------------------------------
# Stop-level diagnostic (NOTES-40)
#
# `compute_route_stop_diagnostics` answers the "where on the route do trips
# slip?" question by aggregating `stop_events` per (direction_id, stop_id)
# along the route's canonical stop sequence. Output is a list of stops
# ordered (direction_id ASC, stop_sequence ASC) — origin-to-destination per
# direction — with median/p95 deviation, OTP%, skip%, observation counts.
#
# --- (route_id, direction_id, stop_id) grouping rule (load-bearing) ---
# Per the CLAUDE.md gotcha: most WMATA stops are split by direction (NB and
# SB are different stop_ids on opposite sides of a street), but termini,
# layover bays, and some hubs serve both directions under one stop_id.
# Grouping by (route_id, stop_id) alone silently double-counts at those
# shared stops and produces metrics that look ~2x too tight. The query
# groups strictly by (direction_id, stop_id), and the canonical-sequence
# resolver below also keys on (direction_id, stop_sequence) so a shared
# terminus appears once per direction in the output, never collapsed.
#
# --- Canonical stop sequence picker ---
# Routes vary in trip length — express variants, short-turn patterns, and
# branches all show up as different `stop_times` sequences for the same
# (route_id, direction_id). For the strip chart we want one sequence per
# direction that the user can read as "origin → destination" without
# missing stops. Heuristic: pick the trip with the most stops in each
# direction (the longest superset). This surfaces every stop the longest
# variant serves and lets short-turn variants register their per-stop
# metrics against the long sequence's positions.
#
# --- Skip-rate denominator ---
# `schedule_relationship='SKIPPED'` rows count toward the numerator. The
# denominator is the count of stop_events rows for the (direction, stop)
# combination — i.e., the number of trips for which the trip_update feed
# evaluated this stop and emitted a SCHEDULED, SKIPPED, or NO_DATA row.
# Source is restricted to `trip_update` because proximity never emits
# SKIPPED (the bus is either close enough or it isn't). NO_DATA rows are
# included in the denominator because the trip *was* scheduled to serve
# the stop — the absence of a confirmed arrival is itself diagnostic.
#
# --- day_type / period filter (PR #83 / NOTES-41 integration) ---
# `day_type` filters service_dates by day-of-week (matches `_day_type_for`).
# `period` filters by Eastern hour of `observed_arrival_ts`. SKIPPED rows
# have null `observed_arrival_ts` and survive the period filter (we treat
# the SKIPPED event as belonging to its scheduled hour bucket via
# `scheduled_arrival_ts`); without that fallback, period-filtered skip
# rates would always read 0 because skipped rows have no observed time.
# ---------------------------------------------------------------------------

_STOP_DIAGNOSTICS_TTL_SEC = 60.0
_stop_diagnostics_cache: dict[tuple[str, int, str, str, int | None, str], tuple[float, dict]] = {}
_stop_diagnostics_lock = Lock()


def _canonical_stop_sequences_for_route(
    db: Session, route_id: str
) -> dict[int, list[tuple[str, int, str]]]:
    """Return the canonical (direction_id → [(stop_id, stop_sequence, stop_name), ...]) sequence.

    For each direction served by the route, picks the trip with the most
    stops as the canonical sequence (the longest superset). Reads
    `stops × stop_times × trips` filtered to `is_current=True` per the
    CLAUDE.md GTFS rule. The output preserves stop_sequence ordering so
    callers can render origin-to-destination directly.

    Returns an empty dict when the route has no current trips. Returns a
    direction-keyed dict where each value is a list of (stop_id,
    stop_sequence, stop_name) tuples ordered by stop_sequence.
    """
    # Step 1: pick the longest trip per direction. We count stop_times rows
    # per trip and pick the trip_id with the max count for each direction.
    # Note we don't tiebreak — any trip tied for max will work (they have
    # identical stop counts, which usually means they're the same pattern).
    trip_counts = (
        db.query(
            Trip.trip_id,
            Trip.direction_id,
            func.count(StopTime.id).label("n_stops"),
        )
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .filter(
            Trip.route_id == route_id,
            Trip.is_current,
            StopTime.is_current,
        )
        .group_by(Trip.trip_id, Trip.direction_id)
        .all()
    )

    longest_trip_by_dir: dict[int, tuple[str, int]] = {}
    for trip_id, direction_id, n_stops in trip_counts:
        if direction_id is None:
            continue
        existing = longest_trip_by_dir.get(direction_id)
        if existing is None or n_stops > existing[1]:
            longest_trip_by_dir[direction_id] = (trip_id, n_stops)

    if not longest_trip_by_dir:
        return {}

    # Step 2: load stop_times + stop names for each chosen trip, ordered.
    out: dict[int, list[tuple[str, int, str]]] = {}
    for direction_id, (trip_id, _n) in longest_trip_by_dir.items():
        rows = (
            db.query(StopTime.stop_id, StopTime.stop_sequence, Stop.stop_name)
            .join(Stop, Stop.stop_id == StopTime.stop_id)
            .filter(
                StopTime.trip_id == trip_id,
                StopTime.is_current,
                Stop.is_current,
            )
            .order_by(StopTime.stop_sequence.asc())
            .all()
        )
        # De-dup on (stop_id, stop_sequence) — the Stop join can produce
        # multiple rows per stop_id if `is_current` filtering misses a
        # stale duplicate (defensive; production data should have one
        # current row per stop_id).
        seen: set[tuple[str, int]] = set()
        seq: list[tuple[str, int, str]] = []
        for stop_id, stop_sequence, stop_name in rows:
            key = (stop_id, stop_sequence)
            if key in seen:
                continue
            seen.add(key)
            seq.append((stop_id, stop_sequence, stop_name))
        out[direction_id] = seq
    return out


def _percentile(values: list[int], pct: float) -> float | None:
    """Return the `pct`-th percentile (0..100) using linear interpolation.

    Uses the same NIST nearest-rank-with-interpolation semantics as
    `numpy.percentile(..., interpolation='linear')` so callers get stable
    values without pulling numpy as an API-layer dep. Returns None for an
    empty list. `values` is mutated by the sort — callers should pass a
    list they don't need preserved.
    """
    if not values:
        return None
    values.sort()
    if len(values) == 1:
        return float(values[0])
    rank = (pct / 100.0) * (len(values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] + (values[hi] - values[lo]) * frac


def compute_route_stop_diagnostics(
    db: Session,
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
    direction_id: int | None = None,
) -> dict:
    """Compute per-stop diagnostic metrics for one route over a time window.

    Returns one row per (direction_id, stop_id) along the route's canonical
    stop sequence (the longest trip per direction — see module-level docstring
    for the canonical-sequence rationale). Each row carries median/p95
    deviation, OTP%, skip%, and observation counts.

    --- Grouping (load-bearing) ---
    Aggregation groups strictly by (route_id, direction_id, stop_id) per
    the CLAUDE.md `stop_id` direction rule. Termini and shared bays serve
    both directions under one stop_id; grouping without direction silently
    double-counts and produces metrics that look ~2x too tight. The
    canonical sequence is keyed on direction too, so a shared terminus
    surfaces twice (once per direction) — never collapsed.

    --- Skip-rate denominator ---
    Numerator is `count(schedule_relationship='SKIPPED')`. Denominator is
    the total stop_events rows for the (direction, stop) — i.e., the
    number of trips for which the trip_update feed emitted a
    SCHEDULED/SKIPPED/NO_DATA row. Source restricted to `trip_update`
    because proximity never emits SKIPPED (the bus is either close
    enough or it isn't). NO_DATA stays in the denominator because the
    trip was scheduled — the absence of a confirmed arrival is itself
    diagnostic.

    --- day_type / period filter ---
    `day_type` filters by `_day_type_for(service_date)`. `period` filters
    by Eastern hour of `observed_arrival_ts` (or `scheduled_arrival_ts`
    for SKIPPED rows that have null observed timestamps). Filtering
    happens in Python after the row fetch — keeps test parity with
    SQLite, mirrors the same approach `compute_otp_split` uses.

    Args:
        db: SQLAlchemy session.
        route_id: Route identifier (e.g., 'C51').
        days: Window length in days (default 30).
        day_type: One of `all` / `weekday` / `saturday` / `sunday`.
        period: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`.
        direction_id: Optional — restrict output to one direction. When
            None, both directions are returned interleaved by
            (direction_id, stop_sequence).

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`, and `stops`
        (list ordered by direction_id ASC then stop_sequence ASC).
    """
    from src.timezones import eastern_today

    end_date = eastern_today()
    start_date = end_date - timedelta(days=days)
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    no_period_filter = period == ALL_HOURS
    no_day_type_filter = day_type == ALL_DAY_TYPES

    # Step 1: canonical sequence per direction. Doubles as the output shape —
    # any (direction, stop) not in the sequence is dropped from the response
    # so partial-route variants (express trips, short-turns) don't add
    # duplicate rows for stops the canonical longest pattern doesn't serve.
    seq_by_direction = _canonical_stop_sequences_for_route(db, route_id)
    if not seq_by_direction:
        return {
            "route_id": route_id,
            "days": days,
            "day_type": day_type,
            "period": period,
            "stops": [],
        }

    # Apply direction filter to the sequence keys early — no point fetching
    # stop_events for directions we'll filter out.
    if direction_id is not None:
        seq_by_direction = {d: stops for d, stops in seq_by_direction.items() if d == direction_id}
        if not seq_by_direction:
            return {
                "route_id": route_id,
                "days": days,
                "day_type": day_type,
                "period": period,
                "stops": [],
            }

    # Step 2: pull stop_events for the route over the window. We pull
    # trip_update rows for skip/observation counts and proximity rows for
    # OTP deviation (same source as the headline OTP, so the per-stop
    # rollup reconciles with the headline number). Both sources go
    # through the same direction/stop grouping.
    rows = (
        db.query(
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.deviation_sec,
            StopEvent.observed_arrival_ts,
            StopEvent.scheduled_arrival_ts,
            StopEvent.schedule_relationship,
            StopEvent.source,
            StopEvent.service_date,
        )
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date >= start_iso,
            StopEvent.service_date <= end_iso,
        )
        .all()
    )

    # Index aggregations by (direction_id, stop_id). Each cell holds:
    #   devs: list of proximity deviation_sec values (for OTP / median / p95)
    #   tu_total: count of trip_update rows (skip-rate denominator)
    #   tu_skipped: count of trip_update SKIPPED rows (skip-rate numerator)
    cells: dict[tuple[int, str], dict] = defaultdict(
        lambda: {"devs": [], "tu_total": 0, "tu_skipped": 0}
    )

    # Pre-compute valid (direction, stop_id) keys from the sequence — anything
    # outside this set is dropped (variant-only stops, off-route data drift).
    valid_keys: set[tuple[int, str]] = set()
    for d, stops in seq_by_direction.items():
        for stop_id, _seq, _name in stops:
            valid_keys.add((d, stop_id))

    for (
        d_id,
        s_id,
        dev,
        obs_ts,
        sched_ts,
        sched_rel,
        source,
        service_date,
    ) in rows:
        key = (d_id, s_id)
        if key not in valid_keys:
            continue

        # day_type filter
        if not no_day_type_filter:
            try:
                d = datetime.strptime(service_date, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if _day_type_for(d) != day_type:
                continue

        # period filter — apply to the row's effective hour. For observed
        # rows that's observed_arrival_ts; for SKIPPED rows (no observed
        # timestamp) we fall back to scheduled_arrival_ts so the row is
        # still attributable to its scheduled hour bucket. Without this
        # fallback, period-filtered skip rates would read 0% because
        # SKIPPED rows would all drop out of the denominator.
        if not no_period_filter:
            ref_ts = obs_ts if obs_ts is not None else sched_ts
            if ref_ts is None:
                continue
            h = _eastern_hour(ref_ts)
            if h is None or not is_hour_in_period(h, period):
                continue

        if source == "proximity":
            if dev is not None:
                cells[key]["devs"].append(int(dev))
        elif source == "trip_update":
            cells[key]["tu_total"] += 1
            if sched_rel == "SKIPPED":
                cells[key]["tu_skipped"] += 1

    # Step 3: emit one row per (direction, sequence-stop), ordered. This
    # preserves direction ASC then stop_sequence ASC — origin-to-destination
    # within each direction.
    out_stops: list[dict] = []
    for d in sorted(seq_by_direction.keys()):
        for stop_id, stop_sequence, stop_name in seq_by_direction[d]:
            cell = cells.get((d, stop_id))
            if cell is None:
                # No data for this stop in the window — surface as a row
                # with null metrics so the strip chart still shows the
                # stop's position (gaps are diagnostic in their own right).
                out_stops.append(
                    {
                        "direction_id": d,
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        "stop_sequence": stop_sequence,
                        "median_deviation_sec": None,
                        "p95_deviation_sec": None,
                        "otp_pct": None,
                        "skip_pct": None,
                        "n_observations": 0,
                        "n_scheduled": 0,
                    }
                )
                continue

            devs = cell["devs"]
            n_obs = len(devs)
            tu_total = cell["tu_total"]
            tu_skipped = cell["tu_skipped"]

            if n_obs == 0:
                median = None
                p95 = None
                otp_pct = None
            else:
                # Copy before _percentile mutates via sort (we need devs
                # twice — once for median, once for p95).
                median = _percentile(list(devs), 50.0)
                p95 = _percentile(list(devs), 95.0)
                on_time = sum(1 for x in devs if OTP_EARLY_SEC <= x <= OTP_LATE_SEC)
                otp_pct = round(on_time / n_obs, 4)

            skip_pct = round(tu_skipped / tu_total, 4) if tu_total > 0 else None

            out_stops.append(
                {
                    "direction_id": d,
                    "stop_id": stop_id,
                    "stop_name": stop_name,
                    "stop_sequence": stop_sequence,
                    "median_deviation_sec": int(round(median)) if median is not None else None,
                    "p95_deviation_sec": int(round(p95)) if p95 is not None else None,
                    "otp_pct": otp_pct,
                    "skip_pct": skip_pct,
                    "n_observations": n_obs,
                    "n_scheduled": tu_total,
                }
            )

    return {
        "route_id": route_id,
        "days": days,
        "day_type": day_type,
        "period": period,
        "stops": out_stops,
    }


def get_route_stop_diagnostics(
    db: Session,
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
    direction_id: int | None = None,
) -> dict:
    """Cached wrapper around `compute_route_stop_diagnostics`.

    60-second TTL keyed by (route_id, days, day_type, period, direction_id,
    today_iso). Cache key includes today's Eastern date so the cache
    rolls naturally at the service-day boundary (matches the system-trend
    and contributors caches above).

    Args:
        db: SQLAlchemy session.
        route_id: Route identifier.
        days: Window length in days (default 30).
        day_type: One of `all` / `weekday` / `saturday` / `sunday`.
        period: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`.
        direction_id: Optional direction filter.

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`, and `stops`
        (list ordered by direction_id ASC then stop_sequence ASC).
    """
    from src.timezones import eastern_today

    cache_key = (route_id, days, day_type, period, direction_id, eastern_today().isoformat())
    with _stop_diagnostics_lock:
        cached = _stop_diagnostics_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _STOP_DIAGNOSTICS_TTL_SEC:
                return value

    result = compute_route_stop_diagnostics(
        db,
        route_id,
        days=days,
        day_type=day_type,
        period=period,
        direction_id=direction_id,
    )

    with _stop_diagnostics_lock:
        _stop_diagnostics_cache[cache_key] = (time.monotonic(), result)
    return result


# ---------------------------------------------------------------------------
# Bunching cause breakdown (NOTES-42)
#
# Wraps `src.bunching.compute_bunching_cause_breakdown` with the standard
# 60-second TTL keyed by (route_id, days, day_type, period, today_iso) —
# matches the contributors / system-trend / stop-diagnostics endpoint
# pattern. The breakdown live-computes from `stop_events`; pulling all
# bunched pairs over a 30-day window is cheap once `compute_bunching_for_route_date`
# already runs in <1s on the headline path, but the cache keeps repeated
# page loads from re-computing within the polling window.
#
# Surfaced on `PeriodDrilldown` per the NOTES-42 spec. The frontend
# carries `dayType` / `period` from RouteDetail's filter (PR #83) so the
# breakdown re-slices the same way as the rest of the route surface.
# ---------------------------------------------------------------------------

_BUNCHING_CAUSES_TTL_SEC = 60.0
_bunching_causes_cache: dict[tuple[str, int, str, str, str], tuple[float, dict]] = {}
_bunching_causes_lock = Lock()


def get_route_bunching_causes(
    db: Session,
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
) -> dict:
    """Cached wrapper around `compute_bunching_cause_breakdown` (NOTES-42).

    60-second TTL keyed by (route_id, days, day_type, period, today_iso).
    Cache key includes today's Eastern date so the cache rolls naturally
    at the service-day boundary (matches the contributors / stop-diagnostics
    caches above).

    The mechanism (late leaders pick up more passengers, trailers run
    light) is well-established in the bus-bunching literature; the
    five-bucket presentation is internal to this dashboard. See
    `src.bunching.compute_bunching_cause_breakdown` and the section
    comment in that module for the framing rationale.

    Args:
        db: SQLAlchemy session.
        route_id: Route identifier (e.g., 'C51').
        days: Window length in days (default 30).
        day_type: One of `all` / `weekday` / `saturday` / `sunday`.
        period: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`.

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`,
        `n_bunched_pairs`, and `breakdown` (per-category count + pct).
    """
    from src.timezones import eastern_today

    cache_key = (route_id, days, day_type, period, eastern_today().isoformat())
    with _bunching_causes_lock:
        cached = _bunching_causes_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if (time.monotonic() - ts) < _BUNCHING_CAUSES_TTL_SEC:
                return value

    result = compute_bunching_cause_breakdown(
        db,
        route_id,
        days=days,
        day_type=day_type,
        period=period,
    )

    with _bunching_causes_lock:
        _bunching_causes_cache[cache_key] = (time.monotonic(), result)
    return result


# ---------------------------------------------------------------------------
# Block-level cascade view (NOTES-45)
#
# A `block_id` chains a vehicle's consecutive trips during a service day —
# when one trip falls behind, the next trip on the same block inherits the
# lateness unless a recovery layover absorbs it. The cascade view surfaces
# that chain so a GM can tell a single-root-cause four-bad-trips story
# apart from four independent misses.
#
# --- Scheduled chain, not observed chain ---
# A block timeline is the scheduled chain: all trips with the same
# (block_id, service_date) ordered by GTFS scheduled start time. Vehicle
# swaps mid-day (dispatcher pulls a bus for maintenance, drops another
# in) appear as `observed_vehicle_id` changing between adjacent cards;
# the timeline does NOT slice the chain by vehicle_id because the back
# half on a different bus is still riding the front half's schedule
# slip until a layover or holding adjusts it.
#
# --- Origin/destination deviation sourcing ---
# Same source-asymmetry rule as `src/otp_metrics.py`: origin_dev_sec
# comes from a trip's `proximity` Run row; destination_dev_sec comes
# from its `trip_update` Run row. A trip with only one source observed
# still yields a partial card (e.g., only origin known). Both null →
# "not_observed" status, scheduled chain still rendered for the planning
# context.
#
# --- GTFS time → wall-clock datetime ---
# GTFS `stop_times.arrival_time` is HH:MM:SS with hour possibly ≥ 24
# (post-midnight service on the same service day). The scheduled start
# of a trip is the MIN parsed arrival_time across its stop_times rows;
# scheduled end is the MAX. Translating those to Eastern wall-clock
# requires anchoring on the service_date's Eastern midnight then adding
# the seconds offset (DST-safe via `eastern_midnight_as_utc`).
#
# --- Calendar / service_id filtering ---
# A `block_id` is reused across calendar variants (weekday / saturday /
# sunday / federal holidays). Naively pulling every Trip with the block_id
# would surface 90+ rows for a block that only chains ~24 trips on any
# real-world day. We resolve which service_ids actually run on the
# given service_date via the GTFS calendar / calendar_dates rules
# (same logic `src/service_delivered.py` uses) and restrict the trip
# pull to those.
# ---------------------------------------------------------------------------


_WEEKDAY_TO_CALENDAR_FIELD_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def _active_service_ids_for_date(db: Session, service_date: date_type) -> set[str]:
    """Return the set of GTFS service_ids that actually run on `service_date`.

    Mirrors the calendar / calendar_dates resolution rule in
    `src/service_delivered.py:_scheduled_trip_ids_query`: base service_ids
    are picked by day-of-week + date-window in `calendar`, minus type-2
    removals on the date and union type-1 additions on the date.

    Returns a Python set so the caller can filter the Trip query in
    application code — keeps the SQL small and avoids the subquery
    plumbing the service_delivered version uses (the trip list per block
    is small enough that a Python intersect is fine).
    """
    service_date_str = service_date.strftime("%Y%m%d")
    weekday_field = getattr(Calendar, _WEEKDAY_TO_CALENDAR_FIELD_NAMES[service_date.weekday()])

    base_ids = {
        sid
        for (sid,) in db.query(Calendar.service_id).filter(
            Calendar.is_current,
            weekday_field == 1,
            Calendar.start_date <= service_date_str,
            Calendar.end_date >= service_date_str,
        )
    }
    from src.models import CalendarDate

    removed = {
        sid
        for (sid,) in db.query(CalendarDate.service_id).filter(
            CalendarDate.is_current,
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 2,
        )
    }
    added = {
        sid
        for (sid,) in db.query(CalendarDate.service_id).filter(
            CalendarDate.is_current,
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 1,
        )
    }
    return (base_ids - removed) | added


def _gtfs_seconds_to_eastern_iso(seconds: int | None, service_date: date_type) -> str | None:
    """Convert GTFS seconds-since-service-day-start to an Eastern ISO timestamp.

    Anchors on Eastern midnight for `service_date` and adds the seconds
    offset. `seconds` can exceed 86400 for post-midnight service that's still
    part of the prior service day (a 25:30:00 GTFS time on 2026-05-10 is
    1:30am Eastern on 2026-05-11). Returns None if `seconds` is None.

    The result is a naive ISO string in Eastern local time (no offset
    suffix) to match the rest of the API's serialization convention.
    """
    if seconds is None:
        return None
    from src.timezones import EASTERN, UTC, eastern_midnight_as_utc

    midnight_utc = eastern_midnight_as_utc(service_date)
    aware_utc = midnight_utc.replace(tzinfo=UTC)
    target = aware_utc + timedelta(seconds=seconds)
    eastern = target.astimezone(EASTERN).replace(tzinfo=None)
    return eastern.isoformat()


def _scheduled_endpoints_for_trips(
    db: Session, trip_ids: list[str]
) -> dict[str, tuple[int | None, int | None]]:
    """Return {trip_id: (start_seconds, end_seconds)} from current GTFS.

    Reads `stop_times` filtered to `is_current=True`, parses each
    arrival_time to integer seconds (GTFS strings are unpadded — see
    CLAUDE.md gotcha), takes the min/max per trip. Trips with no current
    stop_times rows map to (None, None).
    """
    from src.service_profile import _parse_gtfs_time_to_seconds

    if not trip_ids:
        return {}
    rows = (
        db.query(StopTime.trip_id, StopTime.arrival_time)
        .filter(StopTime.trip_id.in_(trip_ids), StopTime.is_current)
        .all()
    )
    by_trip: dict[str, list[int]] = defaultdict(list)
    for tid, arr in rows:
        if arr is None:
            continue
        try:
            by_trip[tid].append(_parse_gtfs_time_to_seconds(arr))
        except (ValueError, AttributeError):
            # Malformed GTFS time — skip rather than crash. The trip will
            # show with null scheduled times, which renders fine downstream.
            continue
    return {tid: (min(secs), max(secs)) if secs else (None, None) for tid, secs in by_trip.items()}


def _runs_by_trip_for_block(
    db: Session, trip_ids: list[str], service_date: str
) -> dict[str, dict[str, Run]]:
    """Return {trip_id: {source: Run}} for the given trips on one service date.

    A trip can have 0, 1, or 2 Run rows (one per source). Used by the
    block timeline to pull origin_dev_sec from `proximity` and
    destination_dev_sec from `trip_update` per the source-asymmetry rule.
    """
    if not trip_ids:
        return {}
    rows = db.query(Run).filter(Run.trip_id.in_(trip_ids), Run.service_date == service_date).all()
    out: dict[str, dict[str, Run]] = defaultdict(dict)
    for r in rows:
        out[r.trip_id][r.source] = r
    return out


def _observed_vehicle_for_trip(runs_for_trip: dict[str, Run]) -> str | None:
    """Pick the observed vehicle_id from a trip's Run rows.

    Prefer `trip_update` (its vehicle.id field is the AVL log-in record and
    is what the dispatcher considers authoritative); fall back to
    `proximity` (vehicle_id is derived from the matched VehiclePosition
    rows). Either may be null even when its Run exists — TU only carries
    vehicle.id ~40% of the time. Returns None if neither source has a
    vehicle_id.
    """
    for src in ("trip_update", "proximity"):
        run = runs_for_trip.get(src)
        if run is not None and run.vehicle_id:
            return run.vehicle_id
    return None


def _trip_status(
    runs_for_trip: dict[str, Run], origin_dev: int | None, destination_dev: int | None
) -> str:
    """Classify a trip's observation status for the timeline.

    - "complete" when both endpoint deviations are populated
    - "partial" when at least one Run row exists (any observation) but
      not both endpoint deviations
    - "not_observed" when no Run rows exist for this trip
    """
    if origin_dev is not None and destination_dev is not None:
        return "complete"
    if runs_for_trip:
        return "partial"
    return "not_observed"


def compute_block_timeline(db: Session, block_id: str, service_date: date_type) -> dict | None:
    """Build the timeline payload for one block on one service date.

    Returns the scheduled chain of trips for `(block_id, service_date)`
    in order, each annotated with the observed vehicle_id (if any),
    origin/destination deviation in seconds, and a coarse status. Returns
    None when no trips exist for the block_id in the current GTFS
    snapshot — the caller should 404. Returns the chain (with empty
    observations) when trips exist but no Runs do — the planning view
    is still useful when no buses have run yet.

    The chain is scheduled-block, not observed-block: dispatcher
    vehicle swaps mid-day appear as `observed_vehicle_id` changing
    between adjacent trips, and the frontend flags them as swap points.
    """
    # Pull trips for this block from the current GTFS snapshot, restricted
    # to service_ids that actually run on `service_date`. Without this
    # filter the same block_id pulls 90+ trips because the static feed
    # carries weekday / Saturday / Sunday / holiday variants under the
    # same block_id — a real-world block on any one day chains ~24.
    active_service_ids = _active_service_ids_for_date(db, service_date)
    base_q = db.query(Trip).filter(Trip.block_id == block_id, Trip.is_current)
    trips = (
        base_q.filter(Trip.service_id.in_(active_service_ids)).all() if active_service_ids else []
    )
    if not trips:
        # Fallback: surface every trip on the block_id when the calendar
        # resolver returns nothing (fresh DB without calendar / a service
        # date outside any calendar window). The chain may include
        # variants that don't actually run, but a 404 here would hide
        # legitimate block_ids — return the unfiltered chain so the
        # caller can still see the planned chain.
        trips = base_q.all()
        if not trips:
            return None

    trip_ids = [t.trip_id for t in trips]
    sched_endpoints = _scheduled_endpoints_for_trips(db, trip_ids)
    runs_by_trip = _runs_by_trip_for_block(db, trip_ids, service_date.isoformat())

    timeline = []
    for trip in trips:
        start_sec, end_sec = sched_endpoints.get(trip.trip_id, (None, None))
        runs_for_trip = runs_by_trip.get(trip.trip_id, {})
        proximity_run = runs_for_trip.get("proximity")
        tu_run = runs_for_trip.get("trip_update")
        origin_dev = proximity_run.origin_dev_sec if proximity_run else None
        destination_dev = tu_run.destination_dev_sec if tu_run else None
        # Surface the run_id for click-through to the per-run deviation
        # chart. Prefer trip_update (its destination observation rate is
        # materially higher, so the chart looks more complete); fall back
        # to proximity. Null when no Run exists.
        run_id = None
        if tu_run is not None:
            run_id = tu_run.id
        elif proximity_run is not None:
            run_id = proximity_run.id
        timeline.append(
            {
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "trip_headsign": trip.trip_headsign,
                "scheduled_start": _gtfs_seconds_to_eastern_iso(start_sec, service_date),
                "scheduled_end": _gtfs_seconds_to_eastern_iso(end_sec, service_date),
                "scheduled_start_sec": start_sec,
                "scheduled_end_sec": end_sec,
                "observed_vehicle_id": _observed_vehicle_for_trip(runs_for_trip),
                "origin_deviation_seconds": origin_dev,
                "destination_deviation_seconds": destination_dev,
                "trip_status": _trip_status(runs_for_trip, origin_dev, destination_dev),
                "run_id": run_id,
            }
        )

    # Order by scheduled start; trips with no parseable start sink to the
    # end so the chain still renders. Tie-break on trip_id for determinism.
    timeline.sort(
        key=lambda row: (
            row["scheduled_start_sec"] is None,
            row["scheduled_start_sec"] if row["scheduled_start_sec"] is not None else 0,
            row["trip_id"],
        )
    )

    return {
        "block_id": block_id,
        "service_date": service_date.isoformat(),
        "trips": timeline,
    }


def get_route_blocks(db: Session, route_id: str, service_date: date_type) -> dict:
    """List blocks that touch one route on one service date.

    Populates the "Blocks" tab on RouteDetail. For each distinct block_id
    among current trips on the route, returns the block's origin start
    time (Eastern), the number of trips chained on it, the worst
    per-trip absolute deviation observed (max(|origin_dev|,
    |destination_dev|) across the chain's Runs), and the count of trips
    on this route in the block (so blocks that mix multiple routes show
    their route share). Returns `{"error": ...}` for unknown routes.

    The list is ordered by the block's earliest scheduled start so the
    UI reads as "first block out → last block out."
    """
    route = db.query(Route).filter(Route.route_id == route_id, Route.is_current).first()
    if not route:
        return {"error": f"Route {route_id} not found"}

    # Pull every block_id that touches the route in current GTFS, along
    # with the trips on this route per block. Trips on other routes that
    # share the block still appear in the block timeline; this list just
    # answers "which blocks touch this route?" Restrict to service_ids
    # active on `service_date` so the list matches the day's real
    # dispatch (without this filter, weekday/saturday/sunday variants of
    # the same block all show up regardless of the picked date).
    active_service_ids = _active_service_ids_for_date(db, service_date)
    route_trips_q = db.query(Trip.trip_id, Trip.block_id).filter(
        Trip.route_id == route_id, Trip.is_current, Trip.block_id.isnot(None)
    )
    if active_service_ids:
        route_trips_q = route_trips_q.filter(Trip.service_id.in_(active_service_ids))
    route_trips = route_trips_q.all()
    if not route_trips:
        return {
            "route_id": route_id,
            "service_date": service_date.isoformat(),
            "blocks": [],
        }

    trips_by_block: dict[str, list[str]] = defaultdict(list)
    for tid, bid in route_trips:
        trips_by_block[bid].append(tid)

    block_ids = list(trips_by_block.keys())
    # Pull the full trip set per block (including trips on OTHER routes
    # that share the block) so the count + scheduled-start columns
    # reflect the real chain, not just this route's slice. Apply the same
    # service_id filter so the chain matches the day's running schedule.
    all_block_trips_q = db.query(Trip.trip_id, Trip.block_id).filter(
        Trip.block_id.in_(block_ids), Trip.is_current
    )
    if active_service_ids:
        all_block_trips_q = all_block_trips_q.filter(Trip.service_id.in_(active_service_ids))
    all_block_trips = all_block_trips_q.all()
    full_trips_by_block: dict[str, list[str]] = defaultdict(list)
    for tid, bid in all_block_trips:
        full_trips_by_block[bid].append(tid)

    all_trip_ids = [tid for tids in full_trips_by_block.values() for tid in tids]
    sched_endpoints = _scheduled_endpoints_for_trips(db, all_trip_ids)
    runs_by_trip = _runs_by_trip_for_block(db, all_trip_ids, service_date.isoformat())

    rows = []
    for bid, full_tids in full_trips_by_block.items():
        starts = [
            sched_endpoints.get(tid, (None, None))[0]
            for tid in full_tids
            if sched_endpoints.get(tid, (None, None))[0] is not None
        ]
        block_start_sec = min(starts) if starts else None

        worst_dev_abs = None
        for tid in full_tids:
            runs_for_trip = runs_by_trip.get(tid, {})
            prox = runs_for_trip.get("proximity")
            tu = runs_for_trip.get("trip_update")
            candidates = []
            if prox is not None and prox.origin_dev_sec is not None:
                candidates.append(abs(prox.origin_dev_sec))
            if tu is not None and tu.destination_dev_sec is not None:
                candidates.append(abs(tu.destination_dev_sec))
            if candidates:
                trip_worst = max(candidates)
                if worst_dev_abs is None or trip_worst > worst_dev_abs:
                    worst_dev_abs = trip_worst

        any_observed = any(tid in runs_by_trip and runs_by_trip[tid] for tid in full_tids)

        rows.append(
            {
                "block_id": bid,
                "trip_count": len(full_tids),
                "trips_on_route": len(trips_by_block[bid]),
                "scheduled_start": _gtfs_seconds_to_eastern_iso(block_start_sec, service_date),
                "scheduled_start_sec": block_start_sec,
                "worst_deviation_seconds": worst_dev_abs,
                "any_observed": any_observed,
            }
        )

    rows.sort(
        key=lambda r: (
            r["scheduled_start_sec"] is None,
            r["scheduled_start_sec"] if r["scheduled_start_sec"] is not None else 0,
            r["block_id"],
        )
    )

    return {
        "route_id": route_id,
        "service_date": service_date.isoformat(),
        "blocks": rows,
    }


def get_active_blocks(db: Session, service_date: date_type, limit: int = 100) -> dict:
    """List blocks active on `service_date`, ranked by trip count and worst observed deviation.

    Powers the system-level `/blocks` index page (PR #105). Mirrors
    `get_route_blocks` but unscoped to a single route — every block whose
    GTFS trips run on `service_date` is returned, with `routes` (the list
    of route_ids the block touches), `trip_count`, scheduled origin time,
    and the worst per-trip absolute origin/destination deviation observed
    across the chain.

    Ordering is `trip_count` descending then `worst_deviation_seconds`
    descending so the longest, most cascade-prone blocks land at the top
    — the operator question this view answers is "which dispatched
    chains are biggest or hurting most right now?" `limit` caps the
    response size; default 100 is enough headroom for WMATA's ~600
    weekday active blocks while keeping the table scannable.

    Routes with no current trips on `service_date` (off-day variants)
    are filtered out by the active-service-id intersection, matching the
    per-route version's behavior.
    """
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    active_service_ids = _active_service_ids_for_date(db, service_date)
    trips_q = db.query(Trip.trip_id, Trip.block_id, Trip.route_id).filter(
        Trip.is_current, Trip.block_id.isnot(None)
    )
    if active_service_ids:
        trips_q = trips_q.filter(Trip.service_id.in_(active_service_ids))
    trips = trips_q.all()

    if not trips:
        return {
            "service_date": service_date.isoformat(),
            "blocks": [],
        }

    trips_by_block: dict[str, list[str]] = defaultdict(list)
    routes_by_block: dict[str, set[str]] = defaultdict(set)
    for tid, bid, rid in trips:
        trips_by_block[bid].append(tid)
        routes_by_block[bid].add(rid)

    all_trip_ids = [tid for tids in trips_by_block.values() for tid in tids]
    sched_endpoints = _scheduled_endpoints_for_trips(db, all_trip_ids)
    runs_by_trip = _runs_by_trip_for_block(db, all_trip_ids, service_date.isoformat())

    rows = []
    for bid, tids in trips_by_block.items():
        starts = [
            sched_endpoints.get(tid, (None, None))[0]
            for tid in tids
            if sched_endpoints.get(tid, (None, None))[0] is not None
        ]
        block_start_sec = min(starts) if starts else None

        worst_dev_abs = None
        for tid in tids:
            runs_for_trip = runs_by_trip.get(tid, {})
            prox = runs_for_trip.get("proximity")
            tu = runs_for_trip.get("trip_update")
            candidates = []
            if prox is not None and prox.origin_dev_sec is not None:
                candidates.append(abs(prox.origin_dev_sec))
            if tu is not None and tu.destination_dev_sec is not None:
                candidates.append(abs(tu.destination_dev_sec))
            if candidates:
                trip_worst = max(candidates)
                if worst_dev_abs is None or trip_worst > worst_dev_abs:
                    worst_dev_abs = trip_worst

        any_observed = any(tid in runs_by_trip and runs_by_trip[tid] for tid in tids)

        rows.append(
            {
                "block_id": bid,
                "trip_count": len(tids),
                "routes": sorted(routes_by_block[bid]),
                "scheduled_start": _gtfs_seconds_to_eastern_iso(block_start_sec, service_date),
                "scheduled_start_sec": block_start_sec,
                "worst_deviation_seconds": worst_dev_abs,
                "any_observed": any_observed,
            }
        )

    # Rank by trip_count desc, then worst observed deviation desc — the
    # twin "biggest" / "worst" sort surfaces dispatch chains that have
    # the largest cascade footprint. Tie-breaks on block_id so output is
    # stable across calls.
    rows.sort(
        key=lambda r: (
            -r["trip_count"],
            -(r["worst_deviation_seconds"] or 0),
            r["block_id"],
        )
    )

    return {
        "service_date": service_date.isoformat(),
        "blocks": rows[:limit],
    }


# ---------------------------------------------------------------------------
# Schedule audit (NOTES-60)
# ---------------------------------------------------------------------------
#
# System-wide table of under-padded / over-padded segments — direct input
# to schedule-revision work. Reads `route_diagnostic_segment` rows
# materialized nightly by `pipelines/refresh_route_diagnostic_profile.py`
# (PR #107), so this endpoint is an O(1) read of pre-aggregated rows
# rather than an ad-hoc scan of `stop_events`.
#
# Sign convention (mirrors `src/route_diagnostics.py:compute_segment_slip`):
#   mean_slip_sec = AVG(observed_gap_sec − scheduled_gap_sec)
#   positive  → observed > scheduled → bus took longer than the schedule
#               allots → schedule is UNDER-padded for the segment
#   negative  → observed < scheduled → bus ran faster than the schedule
#               allots → schedule is OVER-padded for the segment
#
# Lookback window used by the materialization is hard-coded to 30 days
# in `pipelines/refresh_route_diagnostic_profile.py:main`; we mirror it
# here so the daily-trip-count estimate (n_observations / lookback_days)
# stays calibrated. If the pipeline default ever changes, update both.

# Default lookback window for the diagnostic materialization. Matches
# `pipelines/refresh_route_diagnostic_profile.py:main` default. Used to
# convert `n_observations` (sum across the window) into a daily-trip
# estimate so the "would save X min/day" column reads as a per-day value.
SCHEDULE_AUDIT_LOOKBACK_DAYS = 30


def get_schedule_audit(
    db: Session,
    *,
    route_id: str | None = None,
    direction_id: int | None = None,
    period: str = "all",
    sign: str = "all",
    limit: int = 100,
) -> dict:
    """Return ranked under-/over-padded segments from the diagnostic table.

    Reads `route_diagnostic_segment` rows for the requested filters and
    returns one row per (route_id, direction_id, period, from_stop_id,
    to_stop_id) segment, with the from-/to-stop names joined via the
    current GTFS `stops` snapshot (`is_current=True`). Ranking is by
    absolute mean slip weighted by daily trip count — the same
    "biggest leverage first" sort schedule planners want when triaging
    where to add or recover padding.

    Per CLAUDE.md:
      - Segment aggregation already groups by `(route_id, direction_id,
        from_stop_id, to_stop_id)` via the diagnostic table's unique
        constraint — we preserve that grouping by reading the rows
        unchanged.
      - The `stops` join filters `is_current=True`.

    Args:
        route_id: If set, restrict to one route_id.
        direction_id: If set (0 or 1), restrict to one direction.
        period: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late` — matches the period values in the
            diagnostic table.
        sign: One of `all` / `under` / `over`. `under` returns only
            segments where `mean_slip_sec > 0` (bus is slower than
            scheduled — under-padded); `over` returns only
            `mean_slip_sec < 0` (bus is faster than scheduled —
            over-padded).
        limit: Cap on returned rows (default 100, max 500).

    Returns:
        Dict with `period`, `sign`, `lookback_days`, `n_rows`, and
        `segments` (ranked list, each row carries route_id,
        route_short_name, direction_id, from_stop_id, from_stop_name,
        to_stop_id, to_stop_name, mean_slip_sec, daily_trip_count,
        and minutes_per_day).
    """
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    # Read every segment row for the requested filter. The table is bounded
    # (one row per route × direction × period × segment ≈ a few hundred per
    # route × six periods), so an unfiltered scan stays in the tens of
    # thousands of rows even at full WMATA scale — cheap to pull and sort
    # in Python. Heavier filtering happens at row-emission time after the
    # join with stops + routes so column-projection stays simple.
    q = db.query(RouteDiagnosticSegment).filter(
        RouteDiagnosticSegment.period == period,
    )
    if route_id is not None:
        q = q.filter(RouteDiagnosticSegment.route_id == route_id)
    if direction_id is not None:
        q = q.filter(RouteDiagnosticSegment.direction_id == direction_id)
    if sign == "under":
        q = q.filter(RouteDiagnosticSegment.mean_slip_sec > 0)
    elif sign == "over":
        q = q.filter(RouteDiagnosticSegment.mean_slip_sec < 0)

    seg_rows = q.all()
    if not seg_rows:
        return {
            "period": period,
            "sign": sign,
            "lookback_days": SCHEDULE_AUDIT_LOOKBACK_DAYS,
            "n_rows": 0,
            "segments": [],
        }

    # Bulk-load the route_short_name and stop_name maps so per-row joins
    # don't N+1 the DB. `is_current=True` per CLAUDE.md.
    route_ids = {r.route_id for r in seg_rows}
    stop_ids = {r.from_stop_id for r in seg_rows} | {r.to_stop_id for r in seg_rows}

    route_name_map = {
        rid: (rsn, rln)
        for rid, rsn, rln in (
            db.query(Route.route_id, Route.route_short_name, Route.route_long_name)
            .filter(Route.route_id.in_(route_ids), Route.is_current)
            .all()
        )
    }
    stop_name_map = dict(
        db.query(Stop.stop_id, Stop.stop_name)
        .filter(Stop.stop_id.in_(stop_ids), Stop.is_current)
        .all()
    )

    out_rows: list[dict] = []
    for r in seg_rows:
        # Daily trip count estimate — n_observations is the count across
        # the materialization lookback window, so divide by the window
        # length to get a per-day trip count. Float math (not integer)
        # because routes that don't run every weekday still produce
        # fractional daily averages.
        daily_trip_count = r.n_observations / SCHEDULE_AUDIT_LOOKBACK_DAYS
        # Minutes saved per day if the segment's mean slip were eliminated:
        # per-trip saving (sec) × trips/day ÷ 60. Signed — under-padded
        # rows save positive minutes (eliminating positive slip = less
        # delay); over-padded rows save negative minutes (eliminating
        # negative slip = less excess padding, i.e., recoverable
        # service-hours).
        minutes_per_day = r.mean_slip_sec * daily_trip_count / 60.0
        rsn, rln = route_name_map.get(r.route_id, (None, None))
        out_rows.append(
            {
                "route_id": r.route_id,
                "route_short_name": rsn,
                "route_long_name": rln,
                "direction_id": r.direction_id,
                "from_stop_id": r.from_stop_id,
                "from_stop_name": stop_name_map.get(r.from_stop_id),
                "to_stop_id": r.to_stop_id,
                "to_stop_name": stop_name_map.get(r.to_stop_id),
                "from_seq": r.from_seq,
                "to_seq": r.to_seq,
                "period": r.period,
                "mean_slip_sec": r.mean_slip_sec,
                "n_observations": r.n_observations,
                "daily_trip_count": daily_trip_count,
                "minutes_per_day": minutes_per_day,
                "is_timepoint": r.is_timepoint,
            }
        )

    # Default sort: absolute slip magnitude weighted by trip volume — i.e.,
    # the absolute value of `minutes_per_day`. This puts the biggest
    # leverage segments first regardless of sign; the `sign` filter is the
    # mechanism for picking only over- or under-padded.
    out_rows.sort(
        key=lambda r: (
            -abs(r["minutes_per_day"]),
            r["route_id"],
            r["direction_id"],
            r["from_seq"],
        )
    )

    return {
        "period": period,
        "sign": sign,
        "lookback_days": SCHEDULE_AUDIT_LOOKBACK_DAYS,
        "n_rows": len(out_rows),
        "segments": out_rows[:limit],
    }


# ---------------------------------------------------------------------------
# Route diagnostic profile (RouteDetail diagnosis panel, PR #124)
# ---------------------------------------------------------------------------


def get_route_diagnostic_profile(
    db: Session,
    route_id: str,
    period: str = "all",
) -> dict:
    """Return the full diagnostic profile for one route and period.

    Reads the three materialized diagnostic tables for one (route_id, period)
    combination and returns them as three parallel lists: ``segments`` (per-
    segment slip + cumulative slip), ``timepoints`` (per-timepoint behavior
    classification), and ``direction_asymmetry`` (per-direction early%/late%).

    Segment rows include ``from_stop_name`` and ``to_stop_name`` joined from
    the current GTFS ``stops`` snapshot so the frontend can label axes and
    timepoint markers without a separate fetch.

    Timepoint rows include the stop name for display in the behavior table.

    Args:
        db: Active SQLAlchemy session.
        route_id: Route identifier (e.g. ``'D80'``).
        period: One of ``all`` / ``am_peak`` / ``midday`` / ``pm_peak`` /
            ``evening`` / ``late``. Defaults to ``all``.

    Returns:
        Dict with ``route_id``, ``period``, ``segments``, ``timepoints``, and
        ``direction_asymmetry``. Each list is ordered by
        ``(direction_id, from_seq)`` / ``(direction_id, timepoint_stop_id)`` /
        ``direction_id`` respectively. Returns empty lists when no
        materialized data exists for the route+period combination.
    """
    seg_rows = (
        db.query(RouteDiagnosticSegment)
        .filter(
            RouteDiagnosticSegment.route_id == route_id,
            RouteDiagnosticSegment.period == period,
        )
        .order_by(RouteDiagnosticSegment.direction_id, RouteDiagnosticSegment.from_seq)
        .all()
    )

    tp_rows = (
        db.query(RouteDiagnosticTimepoint)
        .filter(
            RouteDiagnosticTimepoint.route_id == route_id,
            RouteDiagnosticTimepoint.period == period,
        )
        .order_by(RouteDiagnosticTimepoint.direction_id, RouteDiagnosticTimepoint.timepoint_stop_id)
        .all()
    )

    dir_rows = (
        db.query(RouteDiagnosticDirection)
        .filter(
            RouteDiagnosticDirection.route_id == route_id,
            RouteDiagnosticDirection.period == period,
        )
        .order_by(RouteDiagnosticDirection.direction_id)
        .all()
    )

    # Bulk-load stop names so the frontend doesn't need a second trip.
    # is_current=True per CLAUDE.md.
    stop_ids: set[str] = set()
    for r in seg_rows:
        stop_ids.add(r.from_stop_id)
        stop_ids.add(r.to_stop_id)
    for r in tp_rows:
        stop_ids.add(r.timepoint_stop_id)

    stop_name_map: dict[str, str] = {}
    if stop_ids:
        stop_name_map = dict(
            db.query(Stop.stop_id, Stop.stop_name)
            .filter(Stop.stop_id.in_(stop_ids), Stop.is_current)
            .all()
        )

    segments = [
        {
            "direction_id": r.direction_id,
            "from_seq": r.from_seq,
            "from_stop_id": r.from_stop_id,
            "from_stop_name": stop_name_map.get(r.from_stop_id),
            "to_seq": r.to_seq,
            "to_stop_id": r.to_stop_id,
            "to_stop_name": stop_name_map.get(r.to_stop_id),
            "mean_slip_sec": r.mean_slip_sec,
            "cum_slip_sec": r.cum_slip_sec,
            "n_observations": r.n_observations,
            "is_timepoint": r.is_timepoint,
        }
        for r in seg_rows
    ]

    timepoints = [
        {
            "direction_id": r.direction_id,
            "timepoint_stop_id": r.timepoint_stop_id,
            "stop_name": stop_name_map.get(r.timepoint_stop_id),
            "classification": r.classification,
            "median_dev_entering": r.median_dev_entering,
            "median_dev_leaving": r.median_dev_leaving,
            "p10_dev_entering": r.p10_dev_entering,
            "p10_dev_leaving": r.p10_dev_leaving,
            "n_observations": r.n_observations,
        }
        for r in tp_rows
    ]

    direction_asymmetry = [
        {
            "direction_id": r.direction_id,
            "early_pct": r.early_pct,
            "late_pct": r.late_pct,
            "signature": r.signature,
            "n_observations": r.n_observations,
        }
        for r in dir_rows
    ]

    return {
        "route_id": route_id,
        "period": period,
        "segments": segments,
        "timepoints": timepoints,
        "direction_asymmetry": direction_asymmetry,
    }


# ---------------------------------------------------------------------------
# Cross-route segment diagnostic (NOTES-59)
# ---------------------------------------------------------------------------
#
# Reads `cross_route_segment_rollup` rows materialized nightly by
# `pipelines/refresh_cross_route_segments.py` (NOTES-59). Returns a ranked
# list of stop-pairs that appear on ≥2 routes, ordered by total
# trip-volume-weighted slip descending.

# Default lookback for the diagnostic materialization — mirrors the 30-day
# window used by `refresh_route_diagnostic_profile.py`. Used to convert
# n_total_observations into a per-day trip count estimate.
CROSS_ROUTE_SEGMENT_LOOKBACK_DAYS = 30


def get_cross_route_segments(
    db: Session,
    *,
    period: str = "all",
    limit: int = 100,
) -> dict:
    """Return the ranked cross-route segment diagnostic for the given period.

    Reads ``cross_route_segment_rollup`` rows materialized nightly by
    ``pipelines/refresh_cross_route_segments.py``.  Each row represents a
    ``(from_stop_id, to_stop_id)`` stop-pair that is traversed by at least
    2 distinct routes.  Rows are ordered by ``total_weighted_slip_sec``
    descending — the infrastructure-investment ranked list.

    ``slip_min_per_trip`` is ``total_weighted_slip_sec / n_total_observations
    / 60`` — the average per-trip slip in minutes across all routes and
    directions that traverse the pair.  A positive value means buses are
    slower than scheduled on this segment on average; a high magnitude with
    many routes indicates a shared chokepoint.

    ``peak_period`` is populated on ``period='all'`` rows only (the named
    period with highest weighted slip for the pair).

    The ``contributing_routes`` field is the deserialized JSON array from
    the rollup table — a list of per-route breakdown rows sorted by trip
    volume.

    Per CLAUDE.md: ``stops`` join uses ``is_current=True``.

    Args:
        db: SQLAlchemy session.
        period: One of ``all`` / ``am_peak`` / ``midday`` / ``pm_peak`` /
            ``evening`` / ``late`` — must match the materialized period set.
        limit: Cap on returned rows (default 100, max 500).

    Returns:
        Dict with ``period``, ``lookback_days``, ``n_rows``, and ``segments``
        (ranked list, each row carries from/to stop ids + names, route count,
        route names summary, total weighted slip, slip_min_per_trip, peak_period,
        and contributing_routes drilldown).
    """
    import json as _json

    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    rollup_rows = (
        db.query(CrossRouteSegmentRollup)
        .filter(CrossRouteSegmentRollup.period == period)
        .order_by(CrossRouteSegmentRollup.total_weighted_slip_sec.desc())
        .limit(limit)
        .all()
    )

    if not rollup_rows:
        return {
            "period": period,
            "lookback_days": CROSS_ROUTE_SEGMENT_LOOKBACK_DAYS,
            "n_rows": 0,
            "segments": [],
        }

    # Bulk-load stop names for all from/to stop_ids.
    stop_ids = {r.from_stop_id for r in rollup_rows} | {r.to_stop_id for r in rollup_rows}
    stop_name_map: dict[str, str | None] = dict(
        db.query(Stop.stop_id, Stop.stop_name)
        .filter(Stop.stop_id.in_(stop_ids), Stop.is_current)
        .all()
    )

    out: list[dict] = []
    for r in rollup_rows:
        contributing = _json.loads(r.contributing_routes_json)
        # Average per-trip slip across all contributing routes.
        slip_min_per_trip = (
            (r.total_weighted_slip_sec / r.n_total_observations / 60.0)
            if r.n_total_observations > 0
            else 0.0
        )
        # Compact route names list for the "Contributing routes" column.
        route_short_names = sorted({c["route_short_name"] or c["route_id"] for c in contributing})
        out.append(
            {
                "from_stop_id": r.from_stop_id,
                "from_stop_name": stop_name_map.get(r.from_stop_id),
                "to_stop_id": r.to_stop_id,
                "to_stop_name": stop_name_map.get(r.to_stop_id),
                "n_routes": r.n_routes,
                "route_short_names": route_short_names,
                "total_weighted_slip_sec": r.total_weighted_slip_sec,
                "slip_min_per_trip": slip_min_per_trip,
                "n_total_observations": r.n_total_observations,
                "peak_period": r.peak_period,
                "contributing_routes": contributing,
            }
        )

    return {
        "period": period,
        "lookback_days": CROSS_ROUTE_SEGMENT_LOOKBACK_DAYS,
        "n_rows": len(out),
        "segments": out,
    }
