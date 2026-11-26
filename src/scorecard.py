"""
Period scorecard: a citable, self-describing summary of one fixed period (issue #267).

The first "defensible, citable finding" generator for the advocacy north star
(CLAUDE.md). WMATA is the deep subject (system KPIs, day-type split, best and
worst routes); peer agencies (SFMTA/Muni first) get the same system headline
KPIs from the same code path, with comparability caveats and no route detail.

Read-only by construction: every function here only SELECTs, and only from
the kept-forever daily aggregate tables -- ``system_metrics_daily`` and
``route_metrics_daily_overlay`` -- plus the versioned GTFS tables for route
names, feed versions and calendar exceptions. Stop-level rows
(``stop_events`` / ``runs``) are pruned by the retention tiers
(``src/retention.py``), so a scorecard that read them could not be
re-generated for a past period; this one can.

Period aggregation (one rule, both agencies):

* **System headline figures are the mean of daily values.** Each eligible
  service date's ``system_metrics_daily`` value counts once. This is the
  rule the API's own period views use for the same table
  (``api.aggregations.get_agency_comparison_data`` window means and the
  system-trend prior-window value, both via ``_mean_skip_null``), so the
  scorecard does not introduce a second definition of a system KPI. The
  table stores only finalized daily ratios, not sufficient statistics.
* **Route figures are pooled over the period** from the per-(route, date)
  sufficient statistics in ``route_metrics_daily_overlay``, using the
  API's own reducers (``_hydrate_overlay_row`` + the ``_reduce_*``
  helpers behind the windowed route scorecard). Pooling is exact for these
  statistics, and a per-route daily ratio on a thin day is noisy.
* A pooled system figure is also computed from the same overlay rows as a
  disclosed sensitivity check (OTP and service delivered over every
  route; EWT / SWT / bunching over bus routes only, matching the system
  rollup's bus-only filter), together with a per-date check that the
  overlay reproduces each stored daily system value.

Exclusions: a date in the period with no ``system_metrics_daily`` row, a
row not stamped ``complete``, or a null value for a metric is excluded from
that metric and listed with its reason. Route pooling uses only dates whose
system row is ``complete`` (the same day-set rule the API's windowed
scorecard applies via ``_partial_service_dates_in_window``) and only overlay
rows that are themselves ``complete``.

``build_scorecard`` returns a JSON-serializable dict holding every number;
``render_markdown`` renders the report purely from that dict, so every
figure the Markdown prints is in the JSON.
"""

from __future__ import annotations

import statistics
from datetime import date as date_type
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from api.aggregations import (
    _METRIC_TO_COLUMN,
    _aggregate_live_metrics_window,
    _hydrate_overlay_row,
    _live_metric_fields,
    _mean_skip_null,
    _reduce_bunching,
    _reduce_ewt,
    _reduce_otp_split,
    _reduce_service_delivered,
)
from src.agency_config import load_agency_config
from src.data_completeness import agency_coverage_threshold
from src.ewt import _day_type_for, bus_route_ids
from src.frequent_routes import (
    HIGH_FREQ_GATE_SEC,
    MEDIUM_FREQ_GATE_SEC,
    load_frequent_route_ids,
)
from src.gtfs_versioning import resolve_gtfs_snapshot_for_date
from src.models import (
    CalendarDate,
    GTFSSnapshot,
    Route,
    RouteMetricsDailyOverlay,
    SystemMetricsDaily,
)
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC

SCHEMA_VERSION = 1

# Metrics reported at system level, in display order. `swt` is printed as
# context for EWT (scheduled wait + excess wait = actual wait); the four
# headline KPIs from the issue are otp / service_delivered / ewt / bunching.
SYSTEM_METRICS: tuple[str, ...] = ("otp", "service_delivered", "ewt", "swt", "bunching")
HEADLINE_METRICS: tuple[str, ...] = ("otp", "service_delivered", "ewt", "bunching")
# Observed metrics get unusual-day flags; SWT is schedule-only.
FLAGGED_METRICS: tuple[str, ...] = HEADLINE_METRICS

METRIC_LABELS: dict[str, str] = {
    "otp": "On-time performance",
    "service_delivered": "Service delivered",
    "ewt": "Excess wait time (frequent service)",
    "swt": "Scheduled wait time (frequent service)",
    "bunching": "Bunching rate",
}

DAY_TYPES: tuple[str, ...] = ("weekday", "saturday", "sunday")

# Route-ranking floors. A route below its floor is listed, not ranked, so a
# thin route cannot top (or bottom) a ranking on a handful of observations.
# OTP observations are stop-level and correlated within a trip, hence the
# additional delivered-trips floor (~10 trips per weekday over a month).
ROUTE_OTP_MIN_OBSERVATIONS = 1000
ROUTE_OTP_MIN_DELIVERED_TRIPS = 300
# EWT needs a real pool of observed headways, and below ~0.5 observed /
# scheduled headway coverage the API itself flags EWT as "data thin"
# (`_live_metric_fields`' `ewt_coverage_ratio` comment).
ROUTE_EWT_MIN_OBSERVED_HEADWAYS = 1000
ROUTE_EWT_MIN_COVERAGE = 0.5
RANKING_TOP_N = 5

# A day is flagged (still included) when its value is further than this many
# robust standard deviations (1.4826 x median absolute deviation) from the
# period median.
OUTLIER_MAD_MULTIPLIER = 3.0
_MAD_TO_SD = 1.4826

# Tolerances for "the overlay reproduces the stored daily system value".
_CONSISTENCY_TOLERANCE: dict[str, float] = {
    "otp": 0.01,  # percentage points
    "service_delivered": 1e-4,
    "ewt": 0.1,  # seconds
    "swt": 0.1,
    "bunching": 1e-4,
}


# ---------------------------------------------------------------------------
# Daily system values
# ---------------------------------------------------------------------------


def period_dates(start: date_type, end: date_type) -> list[date_type]:
    """Return every calendar date in ``[start, end]`` inclusive.

    Raises ValueError when ``end`` precedes ``start``.
    """
    if end < start:
        raise ValueError(f"period end {end} is before start {start}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def read_system_rows(db: Session, start: date_type, end: date_type) -> dict[str, dict]:
    """Read ``system_metrics_daily`` rows for the period, keyed by ISO date.

    Each value carries the five metric values under their metric keys
    (``otp``, ``service_delivered``, ...) plus ``data_quality``,
    ``coverage_pct`` and ``computed_at``. Dates without a row are absent.
    """
    rows = (
        db.query(SystemMetricsDaily)
        .filter(
            SystemMetricsDaily.service_date >= start.isoformat(),
            SystemMetricsDaily.service_date <= end.isoformat(),
        )
        .all()
    )
    out: dict[str, dict] = {}
    for row in rows:
        entry = {metric: getattr(row, _METRIC_TO_COLUMN[metric]) for metric in SYSTEM_METRICS}
        entry["data_quality"] = row.data_quality
        entry["coverage_pct"] = row.coverage_pct
        entry["computed_at"] = row.computed_at
        out[row.service_date] = entry
    return out


def exclusion_reason(row: dict | None, metric: str) -> str | None:
    """Return why a date's value for ``metric`` is excluded, or None if it counts.

    Reasons, checked in order: ``"missing"`` (no row for the date),
    ``"<data_quality>"`` (row not stamped ``complete``, e.g. ``"partial"``),
    ``"null"`` (complete row but no value for this metric).
    """
    if row is None:
        return "missing"
    if row.get("data_quality") != "complete":
        return str(row.get("data_quality") or "unstamped")
    if row.get(metric) is None:
        return "null"
    return None


def flag_outliers(
    values_by_date: dict[str, float],
    group_of: dict[str, str] | None = None,
    k: float = OUTLIER_MAD_MULTIPLIER,
) -> list:
    """Return ``[{date, value}]`` for days unusually far from their group's median.

    Each day's residual is its value minus the median of its group
    (``group_of[date]``, e.g. the day type; one group when omitted). The
    robust SD is ``1.4826 x MAD`` of all residuals pooled across groups, so
    small groups (four Sundays in a month) are judged against a scale
    estimated from the whole period. A day is flagged when its residual
    exceeds ``k`` robust SDs. Needs at least three values and a non-zero
    MAD; otherwise nothing is flagged. Disclosure only -- flagged days stay
    in the period mean.
    """
    if len(values_by_date) < 3:
        return []
    group_of = group_of or {}
    groups: dict[str, list[float]] = {}
    for d, v in values_by_date.items():
        groups.setdefault(group_of.get(d, ""), []).append(v)
    group_median = {g: statistics.median(vs) for g, vs in groups.items()}
    residuals = {d: v - group_median[group_of.get(d, "")] for d, v in values_by_date.items()}
    mad = statistics.median(abs(r) for r in residuals.values())
    if mad == 0:
        return []
    limit = k * _MAD_TO_SD * mad
    return [
        {"date": d, "value": values_by_date[d]}
        for d in sorted(values_by_date)
        if abs(residuals[d]) > limit
    ]


def summarize_daily_metric(dates: list[date_type], rows: dict[str, dict], metric: str) -> dict:
    """Summarize one metric's daily values over ``dates`` under the period rule.

    Returns ``mean`` (mean of the included daily values -- the period
    figure), ``median``, ``min`` / ``max`` (each ``{value, date}``),
    ``n_days`` / ``n_days_included`` / ``coverage`` (included / days),
    ``excluded`` (``[{date, reason}]``) and ``flagged_days`` (see
    `flag_outliers`, relative to same-day-type days; disclosure only).
    Every statistic is None when no day is included.
    """
    included: dict[str, float] = {}
    excluded: list[dict] = []
    for d in dates:
        iso = d.isoformat()
        row = rows.get(iso)
        reason = exclusion_reason(row, metric)
        if reason is None:
            included[iso] = float(row[metric])
        else:
            excluded.append({"date": iso, "reason": reason})

    # Unusual-day flags compare each day with days of the same day type
    # (weekend service legitimately differs from weekday service). SWT is
    # schedule-side only, so it is not flagged.
    flagged: list[dict] = []
    if metric in FLAGGED_METRICS:
        day_types = {iso: _day_type_for(date_type.fromisoformat(iso)) for iso in included}
        flagged = flag_outliers(included, day_types)

    values = list(included.values())
    mean = _mean_skip_null(values)
    if values:
        min_date = min(included, key=lambda k: (included[k], k))
        max_date = max(included, key=lambda k: (included[k], k))
        min_block = {"value": included[min_date], "date": min_date}
        max_block = {"value": included[max_date], "date": max_date}
        median = statistics.median(values)
    else:
        min_block = max_block = None
        median = None
    return {
        "mean": mean,
        "median": median,
        "min": min_block,
        "max": max_block,
        "n_days": len(dates),
        "n_days_included": len(values),
        "coverage": (len(values) / len(dates)) if dates else None,
        "excluded": excluded,
        "flagged_days": flagged,
    }


def day_type_split(dates: list[date_type], rows: dict[str, dict]) -> dict[str, dict]:
    """Mean of daily values per day type (weekday / saturday / sunday) per metric.

    Day type is the calendar weekday (``src.ewt._day_type_for``, the
    codebase's convention), not the schedule that actually ran -- holiday
    schedules are disclosed separately (`schedule_exception_dates`).
    Returns ``{day_type: {metric: {mean, n_days_included, n_days}}}``.
    """
    out: dict[str, dict] = {}
    for day_type in DAY_TYPES:
        subset = [d for d in dates if _day_type_for(d) == day_type]
        out[day_type] = {}
        for metric in SYSTEM_METRICS:
            summary = summarize_daily_metric(subset, rows, metric)
            out[day_type][metric] = {
                "mean": summary["mean"],
                "n_days_included": summary["n_days_included"],
                "n_days": summary["n_days"],
            }
    return out


def daily_table(dates: list[date_type], rows: dict[str, dict]) -> list[dict]:
    """One entry per date: day type, data_quality, coverage_pct and each metric value.

    The disclosure table behind the Data quality section; ``data_quality``
    is None for a date with no row.
    """
    table = []
    for d in dates:
        iso = d.isoformat()
        row = rows.get(iso)
        entry = {
            "date": iso,
            "day_type": _day_type_for(d),
            "data_quality": row.get("data_quality") if row else None,
            "coverage_pct": row.get("coverage_pct") if row else None,
        }
        for metric in SYSTEM_METRICS:
            entry[metric] = row.get(metric) if row else None
        table.append(entry)
    return table


def eligible_dates(dates: list[date_type], rows: dict[str, dict]) -> list[date_type]:
    """Dates whose system row exists and is stamped ``complete``.

    The day-set for every pooled (overlay-based) figure, mirroring the
    API's windowed-scorecard exclusion of ``partial`` dates.
    """
    return [d for d in dates if (rows.get(d.isoformat()) or {}).get("data_quality") == "complete"]


# ---------------------------------------------------------------------------
# Overlay (per-route sufficient statistics)
# ---------------------------------------------------------------------------


def read_overlay_bundles(db: Session, dates: list[date_type]) -> dict[str, dict[str, dict]]:
    """Read ``complete`` overlay rows for ``dates`` as ``{iso_date: {route_id: bundle}}``.

    Bundles are hydrated with the API's ``_hydrate_overlay_row`` so the
    pooled figures here are the ones the dashboard's route scorecard
    would compute for the same day-set. Every requested date appears in
    the result (possibly empty).
    """
    out: dict[str, dict[str, dict]] = {d.isoformat(): {} for d in dates}
    if not dates:
        return out
    rows = (
        db.query(RouteMetricsDailyOverlay)
        .filter(
            RouteMetricsDailyOverlay.service_date.in_(list(out)),
            RouteMetricsDailyOverlay.data_quality == "complete",
        )
        .all()
    )
    for row in rows:
        out[row.service_date][row.route_id] = _hydrate_overlay_row(row)
    return out


def pool_route_metrics(bundles_by_date: dict[str, dict[str, dict]]) -> dict[str, dict]:
    """Pool each route's sufficient statistics over the period.

    Delegates to the API's ``_aggregate_live_metrics_window`` (OTP counts,
    trip counts, EWT Σh / Σh², bunching counts summed, then finalized) and
    flattens with ``_live_metric_fields``. Adds ``otp_n`` (pooled
    all-stops OTP observations), ``days_with_data``, and the median of the
    route's daily OTP / EWT values (``otp_median_day`` /
    ``ewt_median_day``) so a pooled figure driven by a few extreme days is
    visible as such.
    """
    per_date = [bundles_by_date[iso] for iso in sorted(bundles_by_date)]
    pooled = _aggregate_live_metrics_window(per_date)
    out: dict[str, dict] = {}
    for route_id, metrics in pooled.items():
        fields = _live_metric_fields(metrics)
        all_block = (metrics.get("otp_split") or {}).get("all_timepoints") or {}
        fields["otp_n"] = int(all_block.get("n") or 0)
        fields["days_with_data"] = sum(1 for day in per_date if route_id in day)
        daily_otp = [
            ((day[route_id].get("otp_split") or {}).get("all_timepoints") or {}).get("on_time_pct")
            for day in per_date
            if route_id in day
        ]
        daily_ewt = [
            (day[route_id].get("ewt") or {}).get("ewt_seconds")
            for day in per_date
            if route_id in day
        ]
        daily_otp = [v for v in daily_otp if v is not None]
        daily_ewt = [v for v in daily_ewt if v is not None]
        fields["otp_median_day"] = statistics.median(daily_otp) if daily_otp else None
        fields["ewt_median_day"] = statistics.median(daily_ewt) if daily_ewt else None
        out[route_id] = fields
    return out


def _pooled_system_values(bundles: list[dict], bus_bundles: list[dict]) -> dict:
    """Pool a list of route bundles into system-level values.

    OTP and service delivered pool ``bundles`` (every route); EWT, SWT and
    bunching pool ``bus_bundles`` (route_type 3 only), matching the
    system rollup in ``api.aggregations._system_ewt_and_bunching_for_date``.
    EWT is clamped at 0 like the stored daily system value.
    """
    if not bundles:
        return dict.fromkeys(SYSTEM_METRICS)
    otp = _reduce_otp_split([b["otp_split"] for b in bundles])["all_timepoints"]
    sd = _reduce_service_delivered([b["service_delivered"] for b in bundles])
    if bus_bundles:
        ewt = _reduce_ewt([b["ewt"] for b in bus_bundles])
        bun = _reduce_bunching([b["bunching"] for b in bus_bundles])
    else:
        ewt = {"ewt_seconds": None, "swt_seconds": None}
        bun = {"bunching_rate": None}
    otp_n = otp.get("n") or 0
    otp_pct = (otp["on_time"] * 100.0 / otp_n) if otp_n else None
    sd_ratio = (
        (sd["delivered_trips"] / sd["scheduled_trips"]) if sd.get("scheduled_trips") else None
    )
    ewt_value = ewt.get("ewt_seconds")
    return {
        "otp": otp_pct,
        "service_delivered": sd_ratio,
        "ewt": max(0.0, ewt_value) if ewt_value is not None else None,
        "swt": ewt.get("swt_seconds"),
        "bunching": bun.get("bunching_rate"),
    }


def pooled_system_crosscheck(
    bundles_by_date: dict[str, dict[str, dict]],
    rows: dict[str, dict],
    bus_ids_by_date: dict[str, set[str]],
) -> dict:
    """Pooled-period system figures plus a per-date reproduction check.

    ``pooled`` pools every eligible date's route bundles into one figure
    per metric (the sensitivity alternative to the mean-of-days headline).
    ``consistency`` re-derives each date's system values from that date's
    overlay rows and compares them with the stored ``system_metrics_daily``
    values (``_CONSISTENCY_TOLERANCE``), reporting matching dates and any
    mismatch -- evidence that the two kept tables tell the same story.
    """
    all_bundles: list[dict] = []
    all_bus: list[dict] = []
    matching = 0
    mismatches: list[dict] = []
    for iso in sorted(bundles_by_date):
        day = bundles_by_date[iso]
        bus_ids = bus_ids_by_date.get(iso, set())
        bundles = list(day.values())
        bus_bundles = [b for rid, b in day.items() if rid in bus_ids]
        all_bundles.extend(bundles)
        all_bus.extend(bus_bundles)

        derived = _pooled_system_values(bundles, bus_bundles)
        stored = rows.get(iso) or {}
        day_ok = True
        for metric in SYSTEM_METRICS:
            s, o = stored.get(metric), derived[metric]
            if s is None and o is None:
                continue
            if s is None or o is None or abs(s - o) > _CONSISTENCY_TOLERANCE[metric]:
                day_ok = False
                mismatches.append({"date": iso, "metric": metric, "stored": s, "overlay": o})
        matching += int(day_ok)

    return {
        "pooled": _pooled_system_values(all_bundles, all_bus),
        "consistency": {
            "dates_checked": len(bundles_by_date),
            "dates_matching": matching,
            "mismatches": mismatches,
            "tolerance": dict(_CONSISTENCY_TOLERANCE),
        },
    }


# ---------------------------------------------------------------------------
# GTFS context
# ---------------------------------------------------------------------------


def gtfs_snapshots_for_period(
    db: Session,
    dates: list[date_type],
    rows: dict[str, dict],
    tz_name: str,
) -> tuple[dict[str, int | None], list[dict]]:
    """Resolve the GTFS snapshot each date's aggregates were derived against.

    Uses the derivation pipelines' own rule
    (``resolve_gtfs_snapshot_for_date``), considering only snapshots loaded
    before the date's ``system_metrics_daily.computed_at`` -- a snapshot
    loaded afterwards cannot have been used. Returns
    ``({iso_date: snapshot_id}, feed_versions)`` where ``feed_versions``
    lists each distinct snapshot with its ``feed_version``, load time,
    resolution reasons and the dates it served.
    """
    loaded = {
        sid: (loaded_at, feed_version)
        for sid, loaded_at, feed_version in db.query(
            GTFSSnapshot.snapshot_id, GTFSSnapshot.snapshot_date, GTFSSnapshot.feed_version
        ).all()
    }
    by_date: dict[str, int | None] = {}
    grouped: dict[int | None, dict] = {}
    for d in dates:
        iso = d.isoformat()
        computed_at = (rows.get(iso) or {}).get("computed_at")
        exclude = frozenset(
            sid
            for sid, (loaded_at, _) in loaded.items()
            if computed_at is not None and loaded_at is not None and loaded_at > computed_at
        )
        choice = resolve_gtfs_snapshot_for_date(db, d, tz_name, exclude_snapshot_ids=exclude)
        by_date[iso] = choice.snapshot_id
        entry = grouped.setdefault(
            choice.snapshot_id,
            {
                "snapshot_id": choice.snapshot_id,
                "feed_version": loaded.get(choice.snapshot_id, (None, None))[1],
                "loaded_at_utc": _iso_or_none(loaded.get(choice.snapshot_id, (None, None))[0]),
                "reasons": set(),
                "dates": [],
            },
        )
        entry["reasons"].add(choice.reason)
        entry["dates"].append(iso)
    feed_versions = []
    for entry in grouped.values():
        entry["reasons"] = sorted(entry["reasons"])
        feed_versions.append(entry)
    feed_versions.sort(key=lambda e: e["dates"][0])
    return by_date, feed_versions


def schedule_exception_dates(db: Session, snapshot_by_date: dict[str, int | None]) -> list[dict]:
    """Dates whose GTFS schedule removes regular service (``calendar_dates`` type 2).

    These are the dates where the timetable that ran differs from the
    regular pattern for that weekday (typically holidays), read from the
    snapshot resolved for each date. Returns ``[{date, day_type,
    services_removed, services_added}]``.
    """
    out = []
    for iso, snapshot_id in sorted(snapshot_by_date.items()):
        if snapshot_id is None:
            continue
        gtfs_date = iso.replace("-", "")
        exceptions = (
            db.query(CalendarDate.exception_type)
            .filter(CalendarDate.snapshot_id == snapshot_id, CalendarDate.date == gtfs_date)
            .all()
        )
        removed = sum(1 for (t,) in exceptions if t == 2)
        if removed:
            out.append(
                {
                    "date": iso,
                    "day_type": _day_type_for(date_type.fromisoformat(iso)),
                    "services_removed": removed,
                    "services_added": sum(1 for (t,) in exceptions if t == 1),
                }
            )
    return out


def current_route_names(db: Session) -> dict[str, dict]:
    """``{route_id: {short_name, long_name}}`` from the current GTFS routes (``is_current``)."""
    return {
        route_id: {"short_name": short, "long_name": long_}
        for route_id, short, long_ in db.query(
            Route.route_id, Route.route_short_name, Route.route_long_name
        )
        .filter(Route.is_current)
        .all()
    }


# ---------------------------------------------------------------------------
# Route rankings
# ---------------------------------------------------------------------------


def _ranking_entry(route_id: str, fields: dict, names: dict[str, dict], value_key: str) -> dict:
    """Shape one ranked route for the JSON (headline value plus context fields)."""
    name = names.get(route_id) or {}
    return {
        "route_id": route_id,
        "route_name": name.get("short_name") or route_id,
        "route_long_name": name.get("long_name"),
        "in_current_gtfs": route_id in names,
        "value": fields.get(value_key),
        "median_day": fields.get(
            "ewt_median_day" if value_key == "ewt_seconds" else "otp_median_day"
        ),
        "otp_pct": fields.get("otp_all_pct"),
        "otp_n": fields.get("otp_n"),
        "service_delivered_ratio": fields.get("service_delivered_ratio"),
        "delivered_trips": fields.get("service_delivered_delivered"),
        "scheduled_trips": fields.get("service_delivered_scheduled"),
        "ewt_seconds": fields.get("ewt_seconds"),
        "ewt_n_observed": fields.get("ewt_n_observed"),
        "ewt_coverage_ratio": fields.get("ewt_coverage_ratio"),
        "days_with_data": fields.get("days_with_data"),
    }


def _best_and_worst(ranked: list[dict], top_n: int) -> tuple[list[dict], list[dict]]:
    """Split a best-first list into ``top_n`` best and up to ``top_n`` worst (worst first).

    The worst list never repeats a route already in the best list, so a
    short eligible list is split rather than shown twice.
    """
    best = ranked[:top_n]
    worst_start = max(len(best), len(ranked) - top_n)
    worst = list(reversed(ranked[worst_start:]))
    return best, worst


def rank_routes(
    route_metrics: dict[str, dict],
    frequent_ids: frozenset[str],
    names: dict[str, dict],
    top_n: int = RANKING_TOP_N,
) -> dict:
    """Best / worst routes under the headline rule, with stated floors.

    Designated frequent routes (``config/frequent_routes.yaml``) are ranked
    by pooled EWT (lower is better); every other route by pooled
    all-stops OTP (higher is better). Routes below a floor are listed in
    ``below_floor`` instead of ranked; designated routes with no overlay
    data at all are listed in ``no_data``. Ties break on route_id.
    """
    frequent_ranked, frequent_below = [], []
    other_ranked, other_below = [], []
    for route_id in sorted(route_metrics):
        f = route_metrics[route_id]
        if route_id in frequent_ids:
            ok = (
                f.get("ewt_seconds") is not None
                and (f.get("ewt_n_observed") or 0) >= ROUTE_EWT_MIN_OBSERVED_HEADWAYS
                and (f.get("ewt_coverage_ratio") or 0) >= ROUTE_EWT_MIN_COVERAGE
            )
            (frequent_ranked if ok else frequent_below).append(route_id)
        else:
            ok = (
                f.get("otp_all_pct") is not None
                and (f.get("otp_n") or 0) >= ROUTE_OTP_MIN_OBSERVATIONS
                and (f.get("service_delivered_delivered") or 0) >= ROUTE_OTP_MIN_DELIVERED_TRIPS
            )
            (other_ranked if ok else other_below).append(route_id)

    frequent_ranked.sort(key=lambda r: (route_metrics[r]["ewt_seconds"], r))
    other_ranked.sort(key=lambda r: (-route_metrics[r]["otp_all_pct"], r))

    f_best, f_worst = _best_and_worst(frequent_ranked, top_n)
    o_best, o_worst = _best_and_worst(other_ranked, top_n)
    return {
        "frequent": {
            "metric": "ewt",
            "direction": "lower_is_better",
            "floor": {
                "min_observed_headways": ROUTE_EWT_MIN_OBSERVED_HEADWAYS,
                "min_coverage_ratio": ROUTE_EWT_MIN_COVERAGE,
            },
            "n_designated": len(frequent_ids),
            "n_ranked": len(frequent_ranked),
            "below_floor": frequent_below,
            "no_data": sorted(frequent_ids - set(route_metrics)),
            "best": [_ranking_entry(r, route_metrics[r], names, "ewt_seconds") for r in f_best],
            "worst": [_ranking_entry(r, route_metrics[r], names, "ewt_seconds") for r in f_worst],
        },
        "non_frequent": {
            "metric": "otp",
            "direction": "higher_is_better",
            "floor": {
                "min_otp_observations": ROUTE_OTP_MIN_OBSERVATIONS,
                "min_delivered_trips": ROUTE_OTP_MIN_DELIVERED_TRIPS,
            },
            "n_ranked": len(other_ranked),
            "below_floor": other_below,
            "best": [_ranking_entry(r, route_metrics[r], names, "otp_all_pct") for r in o_best],
            "worst": [_ranking_entry(r, route_metrics[r], names, "otp_all_pct") for r in o_worst],
        },
    }


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def _iso_or_none(value: datetime | None) -> str | None:
    """ISO-format a datetime (naive UTC by storage convention), or None."""
    return value.isoformat(timespec="seconds") if value is not None else None


def build_agency_section(
    db: Session,
    agency: str,
    start: date_type,
    end: date_type,
    *,
    deep: bool,
) -> dict:
    """Compute one agency's scorecard section (read-only).

    ``deep=True`` (the subject agency) adds the best / worst route
    rankings. Every agency, deep or not, goes
    through the same daily-value, exclusion, day-type, cross-check and
    GTFS code path, so peer headline figures are computed identically.
    """
    cfg = load_agency_config(agency)
    dates = period_dates(start, end)
    rows = read_system_rows(db, start, end)
    eligible = eligible_dates(dates, rows)

    snapshot_by_date, feed_versions = gtfs_snapshots_for_period(db, dates, rows, cfg.timezone)
    bus_ids_cache: dict[int | None, set[str]] = {}
    bus_ids_by_date: dict[str, set[str]] = {}
    for d in eligible:
        sid = snapshot_by_date.get(d.isoformat())
        if sid not in bus_ids_cache:
            bus_ids_cache[sid] = bus_route_ids(db, gtfs_snapshot_id=sid)
        bus_ids_by_date[d.isoformat()] = bus_ids_cache[sid]

    bundles = read_overlay_bundles(db, eligible)
    computed = [r["computed_at"] for r in rows.values() if r.get("computed_at") is not None]

    section = {
        "agency": agency,
        "display_name": cfg.display_name,
        "timezone": cfg.timezone,
        "polling": {
            "trip_updates_sec": cfg.tick_sec * cfg.trip_updates_every_ticks,
            "vehicle_positions_sec": cfg.tick_sec * cfg.vehicle_positions_every_ticks,
        },
        "completeness_threshold": agency_coverage_threshold(cfg),
        "n_days": len(dates),
        "n_days_complete": len(eligible),
        "metrics": {m: summarize_daily_metric(dates, rows, m) for m in SYSTEM_METRICS},
        "day_types": day_type_split(dates, rows),
        "daily": daily_table(dates, rows),
        "pooled_crosscheck": pooled_system_crosscheck(bundles, rows, bus_ids_by_date),
        "gtfs_feed_versions": feed_versions,
        "schedule_exception_dates": schedule_exception_dates(db, snapshot_by_date),
        "aggregates_computed_at_utc": {
            "min": _iso_or_none(min(computed)) if computed else None,
            "max": _iso_or_none(max(computed)) if computed else None,
        },
    }
    if deep:
        frequent_ids = load_frequent_route_ids(agency)
        route_metrics = pool_route_metrics(bundles)
        section["routes"] = {
            "n_routes_with_data": len(route_metrics),
            "frequent_designation_source": (
                "config/frequent_routes.yaml" if frequent_ids else "none (no designated list)"
            ),
            "rankings": rank_routes(route_metrics, frequent_ids, current_route_names(db)),
        }
    return section


def comparability_caveats(sections: dict[str, dict]) -> list[str]:
    """Plain-language caveats for reading the peer columns next to WMATA.

    Polling cadences and completeness thresholds are read from each
    section (i.e. from ``config/agencies/*.yaml``), not hard-coded.
    """
    cadence = "; ".join(
        f"{s['display_name']}: trip updates every {s['polling']['trip_updates_sec']} s, "
        f"vehicle positions every {s['polling']['vehicle_positions_sec']} s, "
        f"'complete' at coverage >= {s['completeness_threshold']:.3f}"
        for s in sections.values()
    )
    return [
        "Different networks. WMATA's GTFS feed is bus-only. SFMTA's feed also carries Muni "
        "Metro light rail and cable car. OTP and service delivered pool every route in each "
        "feed (so Muni's include rail and cable car); EWT, SWT and bunching are restricted to "
        "bus routes (GTFS route_type 3) for both agencies.",
        f"One OTP window for both. Both agencies are measured on the same {OTP_EARLY_SEC // 60:+d}"
        f"/+{OTP_LATE_SEC // 60} minute window at every observed stop. Neither agency publishes "
        "OTP this way (WMATA's scorecard uses timepoints; SFMTA publishes against its own "
        "standard), so these figures do not match either agency's official reporting.",
        "Frequent-service scope differs. WMATA's per-route EWT gate uses WMATA's published "
        "frequent-route list (15 min for high-frequency and undesignated routes, 20 min for "
        "medium-frequency). SFMTA has no equivalent list, so every Muni route uses the 15-minute "
        "per-cell-hour gate. System EWT and SWT pool only cell-hours that pass the gate.",
        "Feed semantics differ. Muni real-time data comes through the 511.org regional "
        "aggregator, which has a known duplicate stop_sequence artifact affecting per-stop "
        "matching; treat Muni figures as directionally reliable rather than exact. Polling "
        f"cadence and completeness thresholds differ by agency ({cadence}).",
        "No ridership weighting. Neither dataset carries passenger counts; system figures "
        "weight observations (stops, trips, headways) within a day and days equally across the "
        "period, not riders.",
        "Service dates are agency-local (Eastern for WMATA, Pacific for SFMTA).",
    ]


def build_scorecard(
    sessions: dict[str, Session],
    start: date_type,
    end: date_type,
    *,
    subject: str = "wmata",
    generated_at_utc: datetime,
    generator: str = "scripts/generate_scorecard.py",
    git_commit: str | None = None,
) -> dict:
    """Assemble the full scorecard dict (JSON-serializable) from open read-only sessions.

    ``sessions`` maps agency name to a session on that agency's database;
    ``subject`` must be one of them and gets the deep section, every other
    agency the shallow (headline-only) one. ``generated_at_utc`` is
    supplied by the caller (naive UTC) so tests can pin it.
    """
    if subject not in sessions:
        raise ValueError(f"subject agency {subject!r} has no session")
    period_dates(start, end)  # validates ordering
    order = [subject] + sorted(a for a in sessions if a != subject)
    sections = {
        agency: build_agency_section(sessions[agency], agency, start, end, deep=agency == subject)
        for agency in order
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "title": f"{sections[subject]['display_name']} bus scorecard, {start} to {end}",
        "period": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "n_days": len(period_dates(start, end)),
        },
        "generated_at_utc": generated_at_utc.isoformat(timespec="seconds"),
        "generator": generator,
        "git_commit": git_commit,
        "subject": subject,
        "peers": order[1:],
        "method": {
            "otp_window_sec": {"early": OTP_EARLY_SEC, "late": OTP_LATE_SEC},
            "system_period_rule": "mean_of_daily_values",
            "route_period_rule": "pooled_sufficient_statistics",
            "excluded_when": ["no system_metrics_daily row", "row not 'complete'", "metric null"],
            "route_floors": {
                "otp_min_observations": ROUTE_OTP_MIN_OBSERVATIONS,
                "otp_min_delivered_trips": ROUTE_OTP_MIN_DELIVERED_TRIPS,
                "ewt_min_observed_headways": ROUTE_EWT_MIN_OBSERVED_HEADWAYS,
                "ewt_min_coverage_ratio": ROUTE_EWT_MIN_COVERAGE,
                "top_n": RANKING_TOP_N,
            },
            "ewt_gate_sec": {"high_freq": HIGH_FREQ_GATE_SEC, "medium_freq": MEDIUM_FREQ_GATE_SEC},
            "outlier_rule": {"mad_multiplier": OUTLIER_MAD_MULTIPLIER, "mad_to_sd": _MAD_TO_SD},
            "sources": {
                "system": "system_metrics_daily",
                "routes": "route_metrics_daily_overlay",
                "route_names": "routes (is_current)",
                "feed_versions": "gtfs_snapshots + calendar / calendar_dates",
            },
        },
        "agencies": sections,
        "caveats": comparability_caveats(sections) if len(sections) > 1 else [],
    }


# ---------------------------------------------------------------------------
# Markdown rendering (from the dict only)
# ---------------------------------------------------------------------------


def fmt_metric(metric: str, value: float | None) -> str:
    """Format a metric value for display; ``n/a`` for None.

    OTP is a percentage already; service delivered and bunching are
    ratios shown as percentages; EWT / SWT are seconds shown as minutes
    with the seconds alongside.
    """
    if value is None:
        return "n/a"
    if metric == "otp":
        return f"{value:.1f}%"
    if metric in ("service_delivered", "bunching"):
        return f"{value * 100:.1f}%"
    if metric in ("ewt", "swt"):
        return f"{value / 60:.2f} min ({value:.0f} s)"
    return f"{value:.3f}"


def _fmt_pct(value: float | None, digits: int = 0) -> str:
    """Format a 0..1 fraction as a percentage string; ``n/a`` for None."""
    return "n/a" if value is None else f"{value * 100:.{digits}f}%"


def _md_table(header: list[str], body: list[list[str]]) -> list[str]:
    """Render a Markdown table as a list of lines."""
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines.extend("| " + " | ".join(row) + " |" for row in body)
    return lines


def _render_headline(section: dict) -> list[str]:
    """System KPI table for one agency: period mean, median, range, days used."""
    body = []
    for metric in SYSTEM_METRICS:
        s = section["metrics"][metric]
        rng = (
            f"{fmt_metric(metric, s['min']['value'])} ({s['min']['date']}) to "
            f"{fmt_metric(metric, s['max']['value'])} ({s['max']['date']})"
            if s["min"]
            else "n/a"
        )
        body.append(
            [
                METRIC_LABELS[metric],
                f"**{fmt_metric(metric, s['mean'])}**",
                fmt_metric(metric, s["median"]),
                rng,
                f"{s['n_days_included']}/{s['n_days']}",
            ]
        )
    return _md_table(
        ["Metric", "Period figure (mean of days)", "Median day", "Lowest to highest day", "Days"],
        body,
    )


def _render_day_types(section: dict) -> list[str]:
    """Weekday / Saturday / Sunday table of mean daily values."""
    body = []
    for metric in SYSTEM_METRICS:
        row = [METRIC_LABELS[metric]]
        for day_type in DAY_TYPES:
            cell = section["day_types"][day_type][metric]
            row.append(f"{fmt_metric(metric, cell['mean'])} (n={cell['n_days_included']})")
        body.append(row)
    return _md_table(["Metric", "Weekday", "Saturday", "Sunday"], body)


def _render_ranking_table(entries: list[dict], metric: str) -> list[str]:
    """Table of ranked routes with the headline value and context columns."""
    body = []
    for i, e in enumerate(entries, start=1):
        if metric == "ewt":
            evidence = (
                f"{e['ewt_n_observed']:,} headways, coverage {_fmt_pct(e['ewt_coverage_ratio'])}"
            )
        else:
            evidence = f"{e['otp_n']:,} stop observations, {e['delivered_trips']:,} trips"
        name = e["route_name"] + ("" if e["in_current_gtfs"] else " (not in current GTFS)")
        body.append(
            [
                str(i),
                name,
                e["route_long_name"] or "",
                f"**{fmt_metric(metric, e['value'])}**",
                fmt_metric(metric, e["median_day"]),
                fmt_metric("otp", e["otp_pct"]),
                fmt_metric("service_delivered", e["service_delivered_ratio"]),
                evidence,
            ]
        )
    return _md_table(
        [
            "#",
            "Route",
            "Name",
            f"{METRIC_LABELS[metric]} (pooled)",
            "Median day",
            "OTP",
            "Service delivered",
            "Evidence",
        ],
        body,
    )


def _render_routes(section: dict) -> list[str]:
    """Best / worst route tables for the deep agency."""
    routes = section["routes"]
    freq = routes["rankings"]["frequent"]
    other = routes["rankings"]["non_frequent"]
    lines = [
        f"Routes with any complete overlay data in the period: {routes['n_routes_with_data']}. "
        f"Frequent designation: {routes['frequent_designation_source']} "
        f"({freq['n_designated']} routes).",
        "",
        "Route figures are pooled over the period. The median-day column is the median of the "
        "route's daily values; where the two differ widely, a few days dominate the period "
        "figure.",
        "",
        "### Frequent routes, ranked by excess wait time (lower is better)",
        "",
        f"Floor: at least {freq['floor']['min_observed_headways']:,} observed headways and "
        f"observed/scheduled headway coverage of at least "
        f"{_fmt_pct(freq['floor']['min_coverage_ratio'])}. "
        f"Ranked: {freq['n_ranked']}; below floor: {len(freq['below_floor'])}"
        + (f" ({', '.join(freq['below_floor'])})" if freq["below_floor"] else "")
        + f"; designated but no data: {len(freq['no_data'])}"
        + (f" ({', '.join(freq['no_data'])})" if freq["no_data"] else "")
        + ".",
        "",
        "Lowest EWT:",
        "",
        *_render_ranking_table(freq["best"], "ewt"),
        "",
        "Highest EWT:",
        "",
        *_render_ranking_table(freq["worst"], "ewt"),
        "",
        "### Other routes, ranked by on-time performance (higher is better)",
        "",
        f"Floor: at least {other['floor']['min_otp_observations']:,} stop observations and "
        f"{other['floor']['min_delivered_trips']:,} delivered trips in the period. "
        f"Ranked: {other['n_ranked']}; below floor: {len(other['below_floor'])}"
        + (f" ({', '.join(other['below_floor'])})" if other["below_floor"] else "")
        + ".",
        "",
        "Highest OTP:",
        "",
        *_render_ranking_table(other["best"], "otp"),
        "",
        "Lowest OTP:",
        "",
        *_render_ranking_table(other["worst"], "otp"),
    ]
    return lines


def _render_crosscheck(section: dict) -> list[str]:
    """Pooled-vs-mean sensitivity table and overlay reproduction line."""
    cc = section["pooled_crosscheck"]
    body = [
        [
            METRIC_LABELS[m],
            fmt_metric(m, section["metrics"][m]["mean"]),
            fmt_metric(m, cc["pooled"][m]),
        ]
        for m in SYSTEM_METRICS
    ]
    cons = cc["consistency"]
    lines = _md_table(["Metric", "Mean of days (headline)", "Pooled over period"], body)
    lines += [
        "",
        f"Overlay reproduces the stored daily system values on {cons['dates_matching']} of "
        f"{cons['dates_checked']} complete dates.",
    ]
    if cons["mismatches"]:
        lines.append(
            "Mismatches: "
            + "; ".join(
                f"{m['date']} {m['metric']} stored {fmt_metric(m['metric'], m['stored'])} vs "
                f"overlay {fmt_metric(m['metric'], m['overlay'])}"
                for m in cons["mismatches"]
            )
            + "."
        )
    return lines


def _render_data_quality(section: dict) -> list[str]:
    """Exclusions, flagged days, schedule exceptions and the per-date table."""
    lines = [
        f"Dates in period: {section['n_days']}; stamped complete: {section['n_days_complete']} "
        f"(coverage {_fmt_pct(section['n_days_complete'] / section['n_days'])}). "
        f"Completeness threshold for this agency: ingest coverage >= "
        f"{section['completeness_threshold']:.3f}.",
        "",
    ]
    any_excluded = False
    for metric in SYSTEM_METRICS:
        excluded = section["metrics"][metric]["excluded"]
        if excluded:
            any_excluded = True
            lines.append(
                f"- Excluded from {METRIC_LABELS[metric]}: "
                + ", ".join(f"{e['date']} ({e['reason']})" for e in excluded)
            )
    if not any_excluded:
        lines.append("- No date was excluded from any metric.")
    any_flagged = False
    for metric in SYSTEM_METRICS:
        flagged = section["metrics"][metric]["flagged_days"]
        if flagged:
            any_flagged = True
            lines.append(
                f"- Unusual days for {METRIC_LABELS[metric]} (included in the mean): "
                + ", ".join(f"{f['date']} ({fmt_metric(metric, f['value'])})" for f in flagged)
            )
    if not any_flagged:
        lines.append("- No unusual days flagged.")
    exceptions = section["schedule_exception_dates"]
    if exceptions:
        lines.append(
            "- Schedule exceptions (GTFS removes regular service; day type still follows the "
            "calendar weekday): " + ", ".join(f"{e['date']} ({e['day_type']})" for e in exceptions)
        )
    else:
        lines.append("- No GTFS schedule exceptions in the period.")
    lines += ["", "<details><summary>Daily values</summary>", ""]
    body = [
        [
            r["date"],
            r["day_type"],
            r["data_quality"] or "missing",
            _fmt_pct(r["coverage_pct"], 1),
            *[fmt_metric(m, r[m]) for m in SYSTEM_METRICS],
        ]
        for r in section["daily"]
    ]
    lines += _md_table(
        [
            "Date",
            "Day type",
            "Quality",
            "Coverage",
            "OTP",
            "Service delivered",
            "EWT",
            "SWT",
            "Bunching",
        ],
        body,
    )
    lines += ["", "</details>"]
    return lines


def _render_feed_versions(section: dict) -> list[str]:
    """One bullet per GTFS snapshot in force during the period."""
    lines = []
    for fv in section["gtfs_feed_versions"]:
        dates = fv["dates"]
        span = dates[0] if len(dates) == 1 else f"{dates[0]} to {dates[-1]} ({len(dates)} dates)"
        lines.append(
            f"- {section['display_name']}: feed_version {fv['feed_version']} "
            f"(snapshot {fv['snapshot_id']}, loaded {fv['loaded_at_utc']} UTC) for {span}; "
            f"resolution: {', '.join(fv['reasons'])}"
        )
    return lines


def render_markdown(scorecard: dict) -> str:
    """Render the scorecard dict as a Markdown report.

    Reads nothing but ``scorecard``, so every number printed is in the
    JSON written alongside it.
    """
    subject = scorecard["agencies"][scorecard["subject"]]
    peers = [scorecard["agencies"][p] for p in scorecard["peers"]]
    m = scorecard["method"]
    floors = m["route_floors"]
    early_min = m["otp_window_sec"]["early"] // 60
    late_min = m["otp_window_sec"]["late"] // 60
    lines = [
        f"# {scorecard['title']}",
        "",
        f"Period: {scorecard['period']['start']} to {scorecard['period']['end']} "
        f"({scorecard['period']['n_days']} service dates). "
        f"Generated {scorecard['generated_at_utc']} UTC by `{scorecard['generator']}`"
        + (f" at commit `{scorecard['git_commit'][:12]}`" if scorecard.get("git_commit") else "")
        + ". Every figure below is in the accompanying JSON file.",
        "",
        "## Summary",
        "",
    ]
    header = ["Metric", subject["display_name"], *[p["display_name"] for p in peers]]
    body = []
    for metric in HEADLINE_METRICS:
        row = [METRIC_LABELS[metric]]
        for s in [subject, *peers]:
            ms = s["metrics"][metric]
            row.append(f"{fmt_metric(metric, ms['mean'])} ({ms['n_days_included']} days)")
        body.append(row)
    lines += _md_table(header, body)
    lines += [
        "",
        "Period figures are means of daily system values over dates stamped complete. "
        f"OTP uses a {early_min:+d}/+{late_min} minute window. Days with unusual values are "
        "listed under Data quality and are included in these means.",
    ]
    if peers:
        lines.append(
            "Peer columns are shallow comparison points; read the comparability caveats "
            "before comparing across agencies."
        )

    lines += [
        "",
        f"## {subject['display_name']}",
        "",
        "### System KPIs",
        "",
        *_render_headline(subject),
        "",
        "### By day type (mean of daily values)",
        "",
        *_render_day_types(subject),
        "",
    ]
    if "routes" in subject:
        lines += ["## Best and worst routes", "", *_render_routes(subject), ""]

    for peer in peers:
        lines += [
            f"## {peer['display_name']} (comparison, headline only)",
            "",
            *_render_headline(peer),
            "",
            "### By day type (mean of daily values)",
            "",
            *_render_day_types(peer),
            "",
        ]
    if scorecard["caveats"]:
        lines += ["### Comparability caveats", ""]
        lines += [f"- {c}" for c in scorecard["caveats"]]
        lines.append("")

    lines += [
        "## Method",
        "",
        "**Metrics.**",
        "",
        f"- On-time performance (OTP): share of observed stop arrivals between "
        f"{-early_min} minutes early and {late_min} minutes late versus the GTFS schedule, "
        "at every observed stop (source: `proximity` stop events, derived from vehicle "
        "positions). Higher is better.",
        "- Service delivered: delivered scheduled trips divided by scheduled trips "
        "(source: `runs`, against the GTFS schedule for the date). Higher is better.",
        "- Excess wait time (EWT): actual average wait minus scheduled average wait for a "
        "rider arriving at random, where average wait = sum(h^2) / (2 sum(h)) over headways h, "
        "pooled over (direction, stop, hour) cells whose scheduled headway is at most "
        f"{m['ewt_gate_sec']['high_freq'] // 60} minutes ({m['ewt_gate_sec']['medium_freq'] // 60} "
        "minutes for WMATA medium-frequency routes). Source: `trip_update` stop events. "
        "Lower is better. Scheduled wait time (SWT) is the scheduled side of the same pool.",
        "- Bunching rate: share of observed consecutive-arrival headways shorter than "
        "max(0.25 x scheduled headway, 2 minutes) (source: `trip_update` stop events, "
        "SCHEDULED trips only). Lower is better.",
        "",
        "**Period aggregation.** A system figure for the period is the mean of the daily "
        "system values: each eligible service date counts once, regardless of how much service "
        "ran that day. This is the same rule the dashboard API uses for multi-day system "
        "figures from the same table, applied identically to every agency. The stored daily "
        "values are finalized ratios, so there is no per-day weight to pool with at system "
        "level. Route figures are pooled: early / on-time / late counts, scheduled and "
        "delivered trips, headway sums and bunching counts are summed over the period before "
        "the ratio is taken, which is exact for these statistics. The sensitivity table below "
        "shows the pooled alternative for system figures.",
        "",
        "**Exclusions.** A date is excluded from a metric when it has no daily system row, "
        "when its row is not stamped `complete` (ingest coverage below the agency's "
        "threshold), or when the metric is null that day. Excluded dates are listed under "
        "Data quality and never averaged in. Route figures use only dates whose system row is "
        "complete and only per-route rows that are themselves complete. A day whose value is "
        f"more than {m['outlier_rule']['mad_multiplier']:g} robust standard deviations "
        f"({m['outlier_rule']['mad_to_sd']} x median absolute deviation) from the median of "
        "same-day-type days in the period (scale estimated from all days' deviations) is "
        "flagged as unusual for disclosure but kept in the mean. SWT, being schedule-only, "
        "is not flagged.",
        "",
        f"**Route rankings.** Routes on WMATA's frequent-service list "
        f"(`config/frequent_routes.yaml`) are ranked by EWT; all other routes by OTP. A route "
        f"is ranked only if it meets the floor: for EWT, at least "
        f"{floors['ewt_min_observed_headways']:,} observed headways and observed/scheduled "
        f"headway coverage of at least {_fmt_pct(floors['ewt_min_coverage_ratio'])}; for OTP, "
        f"at least {floors['otp_min_observations']:,} stop observations and "
        f"{floors['otp_min_delivered_trips']:,} delivered trips. The top and bottom "
        f"{floors['top_n']} are shown. Route names come from the current GTFS feed.",
        "",
        "**Sources.** Only the kept-forever daily aggregate tables are read: "
        "`system_metrics_daily` (one row per service date) and `route_metrics_daily_overlay` "
        "(per-route, per-date sufficient statistics). Stop-level rows are pruned after a "
        "retention window, so reading only these tables keeps this report reproducible for "
        "past periods. Nothing is written to either database.",
        "",
        "**GTFS feed versions in force.** For each date, the snapshot the derivation pipeline "
        "resolves (the newest loaded snapshot whose calendar schedules service that date), "
        "considering only snapshots loaded before that date's aggregates were computed:",
        "",
    ]
    for s in [subject, *peers]:
        lines += _render_feed_versions(s)
    lines += [""]
    for s in [subject, *peers]:
        ca = s["aggregates_computed_at_utc"]
        lines.append(
            f"- {s['display_name']} daily aggregates computed between {ca['min']} and "
            f"{ca['max']} UTC."
        )
    lines += ["", "### Sensitivity: mean of days vs pooled over the period", ""]
    for s in [subject, *peers]:
        lines += [f"**{s['display_name']}**", "", *_render_crosscheck(s), ""]
    lines += ["## Data quality", ""]
    for s in [subject, *peers]:
        lines += [f"### {s['display_name']}", "", *_render_data_quality(s), ""]
    return "\n".join(lines).rstrip() + "\n"
