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
from collections import Counter
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from sqlalchemy import func
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
from src.agency_config import CONFIG_DIR as AGENCY_CONFIG_DIR
from src.agency_config import load_agency_config
from src.data_completeness import MIN_COVERAGE_FOR_MATERIALIZATION, agency_coverage_threshold
from src.ewt import (
    EWT_LONG_GAP_RULE_SINCE_UTC,
    MAX_OBSERVED_HEADWAY_SEC,
    _day_type_for,
    bus_route_ids,
    ewt_computed_with_cap,
)
from src.frequent_routes import (
    HIGH_FREQ_GATE_SEC,
    MEDIUM_FREQ_GATE_SEC,
    load_frequent_route_ids,
)
from src.gtfs_calendar import scheduled_service_ids_for_date
from src.gtfs_versioning import REASON_COVERS, resolve_gtfs_snapshot_for_date
from src.models import (
    GTFSSnapshot,
    Route,
    RouteMetricsDailyOverlay,
    SystemMetricsDaily,
    Trip,
)
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC

SCHEMA_VERSION = 2

ANNOTATIONS_PATH = Path(__file__).resolve().parent.parent / "config" / "scorecard_annotations.yaml"

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

# Dominance guard: a route whose pooled figure is far from its median day is
# driven by a few days, so it is listed separately instead of ranked. EWT:
# |pooled - median day| > max(|median day|, 60 s), i.e. the pooled figure is
# more than double the median day (or the median day is not positive) by at
# least a minute. OTP: |pooled - median day| > 10 percentage points.
ROUTE_EWT_DOMINANCE_MIN_SEC = 60.0
ROUTE_OTP_DOMINANCE_PP = 10.0

# A headline figure is "materially moved" when removing flagged days (or
# routes under investigation) changes it by more than this fraction.
MATERIAL_SHIFT = 0.05

# Service-pattern detection: the usual service set for a weekday is the most
# common set on the same weekday within +/- this many weeks of the date; a
# date whose set differs is classified by its scheduled-trip ratio to the
# usual set (below 1 - tolerance = reduced, holiday-type).
SERVICE_BASELINE_WEEKS = 4
SERVICE_TRIP_RATIO_TOLERANCE = 0.10

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


def day_type_split(
    dates: list[date_type],
    rows: dict[str, dict],
    holiday_type_dates: frozenset[str] = frozenset(),
) -> dict[str, dict]:
    """Mean of daily values per day type (weekday / saturday / sunday) per metric.

    Day type is the calendar weekday (``src.ewt._day_type_for``, the
    codebase's convention), not the schedule that actually ran. Dates in
    ``holiday_type_dates`` (reduced or another day type's service, see
    `service_pattern_dates`) stay in their calendar day type; for each day
    type containing any, ``holiday_type_dates`` lists them and
    ``excluding_holiday_type`` gives the same means without them, so the
    report can footnote the split. Returns ``{day_type: {metric: {mean,
    n_days_included, n_days}, "holiday_type_dates": [...],
    "excluding_holiday_type": {metric: {...}} | None}}``.
    """
    out: dict[str, dict] = {}
    for day_type in DAY_TYPES:
        subset = [d for d in dates if _day_type_for(d) == day_type]
        held = sorted(d.isoformat() for d in subset if d.isoformat() in holiday_type_dates)
        regular = [d for d in subset if d.isoformat() not in holiday_type_dates]
        out[day_type] = {}
        for metric in SYSTEM_METRICS:
            summary = summarize_daily_metric(subset, rows, metric)
            out[day_type][metric] = {
                "mean": summary["mean"],
                "n_days_included": summary["n_days_included"],
                "n_days": summary["n_days"],
            }
        out[day_type]["holiday_type_dates"] = held
        excluding = None
        if held:
            excluding = {}
            for metric in SYSTEM_METRICS:
                summary = summarize_daily_metric(regular, rows, metric)
                excluding[metric] = {
                    "mean": summary["mean"],
                    "n_days_included": summary["n_days_included"],
                }
        out[day_type]["excluding_holiday_type"] = excluding
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
    visible as such. Route EWT (pooled and daily) is clamped at 0 like the
    stored system figure; ``ewt_seconds_unclamped`` keeps the raw value.
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
        daily_ewt = [max(0.0, v) for v in daily_ewt if v is not None]
        fields["otp_median_day"] = statistics.median(daily_otp) if daily_otp else None
        fields["ewt_median_day"] = statistics.median(daily_ewt) if daily_ewt else None
        # Clamp EWT at 0, as the stored system figure is, so a negative pooled
        # value can never rank "best" with a negative wait. The raw value is kept.
        raw_ewt = fields.get("ewt_seconds")
        fields["ewt_seconds_unclamped"] = raw_ewt
        fields["ewt_seconds"] = max(0.0, raw_ewt) if raw_ewt is not None else None
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


def system_daily_excluding_routes(
    bundles_by_date: dict[str, dict[str, dict]],
    bus_ids_by_date: dict[str, set[str]],
    exclude: set[str],
) -> dict[str, dict]:
    """Per-date system values re-derived from the overlay with ``exclude`` routes removed.

    Valid as a stand-in for the stored daily values because the overlay
    reproduces them exactly (see `pooled_system_crosscheck`). Used only to
    show how far routes under investigation move a headline figure.
    """
    out = {}
    for iso, day in bundles_by_date.items():
        bus_ids = bus_ids_by_date.get(iso, set())
        kept = {rid: b for rid, b in day.items() if rid not in exclude}
        out[iso] = _pooled_system_values(
            list(kept.values()), [b for rid, b in kept.items() if rid in bus_ids]
        )
    return out


# ---------------------------------------------------------------------------
# GTFS context
# ---------------------------------------------------------------------------


def gtfs_snapshots_for_period(
    db: Session,
    dates: list[date_type],
    rows: dict[str, dict],
    tz_name: str,
) -> tuple[dict[str, int | None], list[dict], dict[str, str]]:
    """Resolve each date's GTFS snapshot with the pipelines' rule over the retained snapshots.

    Uses ``resolve_gtfs_snapshot_for_date`` over every snapshot still in the
    database -- what a re-derivation today would use. Snapshots loaded after
    a date's aggregates were computed are NOT excluded: the snapshot de-dup
    (``scripts/dedup_gtfs_snapshots.py``) keeps the newest of a run of
    content-identical loads and proves no date's resolved content changes,
    so the load actually used may be gone while an identical later load
    remains. Excluding by load time would then fall back to an older feed
    that never covered the date. Each feed-version entry instead lists the
    dates whose aggregates were computed before that snapshot was loaded
    (``dates_computed_before_load``), so the report can say so. Returns
    ``({iso_date: snapshot_id}, feed_versions, {iso_date: reason})``, where
    ``reason`` is ``covers`` or a fallback.
    """
    loaded = {
        sid: (loaded_at, feed_version)
        for sid, loaded_at, feed_version in db.query(
            GTFSSnapshot.snapshot_id, GTFSSnapshot.snapshot_date, GTFSSnapshot.feed_version
        ).all()
    }
    by_date: dict[str, int | None] = {}
    reason_by_date: dict[str, str] = {}
    grouped: dict[int | None, dict] = {}
    for d in dates:
        iso = d.isoformat()
        computed_at = (rows.get(iso) or {}).get("computed_at")
        choice = resolve_gtfs_snapshot_for_date(db, d, tz_name)
        by_date[iso] = choice.snapshot_id
        reason_by_date[iso] = choice.reason
        entry = grouped.setdefault(
            choice.snapshot_id,
            {
                "snapshot_id": choice.snapshot_id,
                "feed_version": loaded.get(choice.snapshot_id, (None, None))[1],
                "loaded_at_utc": _iso_or_none(loaded.get(choice.snapshot_id, (None, None))[0]),
                "reasons": set(),
                "dates": [],
                "dates_computed_before_load": [],
            },
        )
        entry["reasons"].add(choice.reason)
        entry["dates"].append(iso)
        loaded_at = loaded.get(choice.snapshot_id, (None, None))[0]
        if computed_at is not None and loaded_at is not None and loaded_at > computed_at:
            entry["dates_computed_before_load"].append(iso)
    feed_versions = []
    for entry in grouped.values():
        entry["reasons"] = sorted(entry["reasons"])
        feed_versions.append(entry)
    feed_versions.sort(key=lambda e: e["dates"][0])
    return by_date, feed_versions, reason_by_date


class _ServiceCalendar:
    """Memoized GTFS service-set and scheduled-trip lookups for one database.

    Read-only; caches ``scheduled_service_ids_for_date`` per (date, snapshot)
    and trip counts per (snapshot, service set), since the baseline
    computation revisits the same dates many times.
    """

    def __init__(self, db: Session) -> None:
        """Bind to a session with empty caches."""
        self.db = db
        self._active: dict[tuple[date_type, int | None], frozenset[str]] = {}
        self._trips: dict[tuple[int | None, frozenset[str]], int] = {}

    def active(self, d: date_type, snapshot_id: int | None) -> frozenset[str]:
        """Service ids scheduled on ``d`` in ``snapshot_id`` (GTFS calendar + calendar_dates)."""
        key = (d, snapshot_id)
        if key not in self._active:
            self._active[key] = frozenset(scheduled_service_ids_for_date(self.db, d, snapshot_id))
        return self._active[key]

    def trips(self, snapshot_id: int | None, services: frozenset[str]) -> int:
        """Scheduled trips in ``snapshot_id`` belonging to ``services``."""
        key = (snapshot_id, services)
        if key not in self._trips:
            if not services:
                self._trips[key] = 0
            else:
                q = self.db.query(func.count(Trip.id)).filter(Trip.service_id.in_(sorted(services)))
                q = q.filter(
                    Trip.snapshot_id == snapshot_id if snapshot_id is not None else Trip.is_current
                )
                self._trips[key] = int(q.scalar() or 0)
        return self._trips[key]

    def usual(self, d: date_type, snapshot_id: int | None) -> tuple[str, list[frozenset[str]]]:
        """The most common non-empty service set on ``d``'s weekday within +/- N weeks.

        ``d`` itself is left out. Returns ``(status, sets)``: ``("unique",
        [set])`` when one set is most common, ``("tie", [sets...])`` when
        several sets share the top count (typically a schedule change in
        the middle of the window), ``("none", [])`` when no neighbouring
        date has scheduled service.
        """
        sets = [
            s
            for k in range(-SERVICE_BASELINE_WEEKS, SERVICE_BASELINE_WEEKS + 1)
            if k != 0
            for s in [self.active(d + timedelta(weeks=k), snapshot_id)]
            if s
        ]
        if not sets:
            return "none", []
        ranked = Counter(sets).most_common()
        top = [s for s, n in ranked if n == ranked[0][1]]
        if len(top) > 1:
            return "tie", top
        return "unique", top

    def recurs(self, d: date_type, snapshot_id: int | None, active: frozenset[str]) -> bool:
        """True when ``active`` is a recurring pattern rather than a one-off on ``d``.

        Recurring means another date of the same day type in the same
        Monday-Sunday week runs the same set, or the same weekday one week
        before or after does -- i.e. the set persists, as after a schedule
        change inside a feed. An alternate timetable used on scattered dates
        (WMATA's service 1 on 10/13 and 11/03, three weeks apart) does not
        persist and stays a substitution. Holiday-type dates are detected
        before this check.
        """
        if not active:
            return False
        day_type = _day_type_for(d)
        week_start = d - timedelta(days=d.weekday())
        for offset in range(7):
            other = week_start + timedelta(days=offset)
            if other != d and _day_type_for(other) == day_type:
                if self.active(other, snapshot_id) == active:
                    return True
        return any(self.active(d + timedelta(weeks=k), snapshot_id) == active for k in (-1, 1))

    def comparison_trips(self, d: date_type, snapshot_id: int | None) -> int | None:
        """Median scheduled trips on comparable dates, or None with fewer than two.

        Weekday: the other weekdays of the same Monday-Friday week. Weekend
        day: the same weekday +/- 1..N weeks. Dates with no scheduled service
        are ignored.
        """
        if _day_type_for(d) == "weekday":
            week_start = d - timedelta(days=d.weekday())
            others = [week_start + timedelta(days=i) for i in range(5)]
        else:
            others = [
                d + timedelta(weeks=k)
                for k in range(-SERVICE_BASELINE_WEEKS, SERVICE_BASELINE_WEEKS + 1)
            ]
        counts = [
            self.trips(snapshot_id, s)
            for o in others
            if o != d
            for s in [self.active(o, snapshot_id)]
            if s
        ]
        return statistics.median(counts) if len(counts) >= 2 else None

    def weekend_set(self, d: date_type, snapshot_id: int | None, day_type: str) -> frozenset[str]:
        """The service set of the Saturday or Sunday in ``d``'s Monday-Sunday week."""
        week_start = d - timedelta(days=d.weekday())
        return self.active(
            week_start + timedelta(days=5 if day_type == "saturday" else 6), snapshot_id
        )


# Service-pattern kinds. Holiday-type kinds are footnoted in the day-type
# split; the others are listed but leave the split alone.
HOLIDAY_TYPE_KINDS = frozenset({"other_day_type", "no_service", "reduced"})


def service_pattern_dates(
    db: Session,
    snapshot_by_date: dict[str, int | None],
    reason_by_date: dict[str, str] | None = None,
    observed_service_dates: frozenset[str] = frozenset(),
) -> list[dict]:
    """Dates whose scheduled service is not the regular service for that weekday, or is unknown.

    Works for feeds that express regular service through ``calendar`` (WMATA)
    and for feeds that add each weekday through ``calendar_dates`` type-1
    rows (SFMTA): it compares *active* service sets and scheduled-trip
    counts, never raw exception rows. Checks, in order:

    1. ``feed_not_covering`` -- the resolved feed does not schedule the date
       (snapshot resolution fell back, per ``reason_by_date``, or the active
       set is empty although the system row shows observed service, per
       ``observed_service_dates``). Pattern unknown; not holiday-type.
    2. Holiday-type, judged locally so schedule changes nearby don't matter:
       ``other_day_type`` (a weekday running the same week's Saturday or
       Sunday set), ``no_service`` (nothing scheduled, feed covers it),
       ``reduced`` (scheduled trips below ``1 - SERVICE_TRIP_RATIO_TOLERANCE``
       of the median of comparable dates, see `comparison_trips`).
    Between 1 and 2, a date running the unique usual set for its weekday
    (most common within +/- N weeks) is regular and not listed -- so a feed
    whose weekdays and weekends share one service id is not misread.

    3. Against that usual set: ``service_change``
       when the set recurs (`_ServiceCalendar.recurs`) or is one of tied
       usual sets -- a schedule change inside the feed, not an exception;
       ``undetermined`` when no usual set can be established (no
       neighbour with service, or a tie the date does not match) and the
       set does not recur; ``increased`` when trips exceed the comparison
       by more than the tolerance; else ``substitution`` (a one-off swap of
       service ids with a comparable trip count).

    Returns ``[{date, day_type, kind, holiday_type, detail, runs_as,
    active_services, usual_services, scheduled_trips, comparison_trips,
    trip_ratio}]``; ``trip_ratio`` is scheduled / comparison trips.
    """
    reason_by_date = reason_by_date or {}
    cal = _ServiceCalendar(db)
    out = []
    for iso, snapshot_id in sorted(snapshot_by_date.items()):
        d = date_type.fromisoformat(iso)
        day_type = _day_type_for(d)
        active = cal.active(d, snapshot_id)
        trips = cal.trips(snapshot_id, active)
        entry = {
            "date": iso,
            "day_type": day_type,
            "kind": None,
            "holiday_type": False,
            "detail": None,
            "runs_as": None,
            "active_services": sorted(active),
            "usual_services": [],
            "scheduled_trips": trips,
            "comparison_trips": None,
            "trip_ratio": None,
        }
        reason = reason_by_date.get(iso, REASON_COVERS)
        if reason != REASON_COVERS or (not active and iso in observed_service_dates):
            entry["kind"] = "feed_not_covering"
            entry["detail"] = (
                f"snapshot resolution: {reason}"
                if reason != REASON_COVERS
                else "no scheduled service in the resolved feed, but service was observed"
            )
            out.append(entry)
            continue

        comparison = cal.comparison_trips(d, snapshot_id)
        ratio = (trips / comparison) if comparison else None
        entry["comparison_trips"] = comparison
        entry["trip_ratio"] = ratio

        # A date running the unique usual set for its weekday is regular. When
        # the window is tied (a schedule change inside it), a date running one
        # of the tied sets is regular if it matches the previous week's same
        # weekday (or, with no service then, the next week's).
        status, top = cal.usual(d, snapshot_id)
        entry["usual_services"] = [sorted(t) for t in top]
        if status == "unique" and active == top[0]:
            continue
        if status == "tie" and active in top:
            reference = cal.active(d - timedelta(weeks=1), snapshot_id) or cal.active(
                d + timedelta(weeks=1), snapshot_id
            )
            if active == reference:
                continue

        # 2. Holiday-type, judged against the same week / nearby same weekdays.
        if day_type == "weekday" and active:
            for weekend in ("saturday", "sunday"):
                if active == cal.weekend_set(d, snapshot_id, weekend):
                    entry["runs_as"] = weekend
                    break
        if entry["runs_as"] is not None:
            entry["kind"] = "other_day_type"
        elif not active:
            entry["kind"] = "no_service"
        elif ratio is not None and ratio < 1 - SERVICE_TRIP_RATIO_TOLERANCE:
            entry["kind"] = "reduced"
        if entry["kind"] is not None:
            entry["holiday_type"] = True
            out.append(entry)
            continue

        # 3. Otherwise classify the difference from the usual set.
        if status == "none":
            # No earlier or later pattern to compare with: nothing to call a change.
            entry["kind"] = "undetermined"
            entry["detail"] = "no neighbouring same-weekday date with scheduled service"
        elif cal.recurs(d, snapshot_id, active) or (status == "tie" and active in top):
            entry["kind"] = "service_change"
            entry["detail"] = "recurring pattern; a schedule change within the feed"
        elif status == "tie":
            entry["kind"] = "undetermined"
            entry["detail"] = "neighbouring dates are split between patterns"
        elif ratio is not None and ratio > 1 + SERVICE_TRIP_RATIO_TOLERANCE:
            entry["kind"] = "increased"
        else:
            entry["kind"] = "substitution"
        out.append(entry)
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
        "ewt_seconds_unclamped": fields.get("ewt_seconds_unclamped"),
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


def is_dominated(metric: str, pooled: float | None, median_day: float | None) -> bool:
    """True when a route's pooled figure is far from its median day (a few days dominate).

    EWT: ``|pooled - median| > max(|median|, ROUTE_EWT_DOMINANCE_MIN_SEC)``.
    OTP: ``|pooled - median| > ROUTE_OTP_DOMINANCE_PP`` percentage points.
    False when either value is missing.
    """
    if pooled is None or median_day is None:
        return False
    if metric == "ewt":
        return abs(pooled - median_day) > max(abs(median_day), ROUTE_EWT_DOMINANCE_MIN_SEC)
    return abs(pooled - median_day) > ROUTE_OTP_DOMINANCE_PP


def dominated_routes(route_metrics: dict[str, dict]) -> list[dict]:
    """Routes whose pooled EWT or OTP clears its floor but is dominated by a few days.

    Applied identically to every agency, regardless of frequent-route
    designation (which only WMATA has), so the "without these routes"
    robustness variant uses one rule for subject and peers. Returns
    ``[{route_id, metrics: ["ewt" | "otp", ...]}]`` sorted by route_id.
    """
    out = []
    for route_id in sorted(route_metrics):
        f = route_metrics[route_id]
        hits = []
        if (
            f.get("ewt_seconds") is not None
            and (f.get("ewt_n_observed") or 0) >= ROUTE_EWT_MIN_OBSERVED_HEADWAYS
            and (f.get("ewt_coverage_ratio") or 0) >= ROUTE_EWT_MIN_COVERAGE
            and is_dominated("ewt", f.get("ewt_seconds"), f.get("ewt_median_day"))
        ):
            hits.append("ewt")
        if (
            f.get("otp_all_pct") is not None
            and (f.get("otp_n") or 0) >= ROUTE_OTP_MIN_OBSERVATIONS
            and (f.get("service_delivered_delivered") or 0) >= ROUTE_OTP_MIN_DELIVERED_TRIPS
            and is_dominated("otp", f.get("otp_all_pct"), f.get("otp_median_day"))
        ):
            hits.append("otp")
        if hits:
            out.append({"route_id": route_id, "metrics": hits})
    return out


def rank_routes(
    route_metrics: dict[str, dict],
    frequent_ids: frozenset[str],
    names: dict[str, dict],
    top_n: int = RANKING_TOP_N,
    route_issues: dict[str, str] | None = None,
) -> dict:
    """Best / worst routes under the headline rule, with stated floors and a dominance guard.

    Designated frequent routes (``config/frequent_routes.yaml``) are ranked
    by pooled EWT (lower is better); every other route by pooled
    all-stops OTP (higher is better). Routes below a floor are listed in
    ``below_floor`` instead of ranked; designated routes with no overlay
    data at all are listed in ``no_data``. Routes that clear the floor but
    whose pooled figure is far from their median day (`is_dominated`) are
    listed in ``dominated`` instead of ranked, each with the tracking issue
    from ``route_issues`` when one is annotated (else None). Ties break on
    route_id.
    """
    frequent_ranked, frequent_below, frequent_dominated = [], [], []
    other_ranked, other_below, other_dominated = [], [], []
    for route_id in sorted(route_metrics):
        f = route_metrics[route_id]
        if route_id in frequent_ids:
            ok = (
                f.get("ewt_seconds") is not None
                and (f.get("ewt_n_observed") or 0) >= ROUTE_EWT_MIN_OBSERVED_HEADWAYS
                and (f.get("ewt_coverage_ratio") or 0) >= ROUTE_EWT_MIN_COVERAGE
            )
            if not ok:
                frequent_below.append(route_id)
            elif is_dominated("ewt", f.get("ewt_seconds"), f.get("ewt_median_day")):
                frequent_dominated.append(route_id)
            else:
                frequent_ranked.append(route_id)
        else:
            ok = (
                f.get("otp_all_pct") is not None
                and (f.get("otp_n") or 0) >= ROUTE_OTP_MIN_OBSERVATIONS
                and (f.get("service_delivered_delivered") or 0) >= ROUTE_OTP_MIN_DELIVERED_TRIPS
            )
            if not ok:
                other_below.append(route_id)
            elif is_dominated("otp", f.get("otp_all_pct"), f.get("otp_median_day")):
                other_dominated.append(route_id)
            else:
                other_ranked.append(route_id)

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
            "dominated": [
                {
                    **_ranking_entry(r, route_metrics[r], names, "ewt_seconds"),
                    "issue": (route_issues or {}).get(r),
                }
                for r in frequent_dominated
            ],
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
            "dominated": [
                {
                    **_ranking_entry(r, route_metrics[r], names, "otp_all_pct"),
                    "issue": (route_issues or {}).get(r),
                }
                for r in other_dominated
            ],
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


EMPTY_ANNOTATIONS: dict = {"dates": [], "routes": []}


def load_annotations(path: Path | None = None) -> dict:
    """Load hand-entered known-issue annotations (``config/scorecard_annotations.yaml``).

    Returns ``{"dates": [{agency, date, issue, note}], "routes": [{agency,
    route_id, issue, note}]}``; a missing file yields no annotations.
    Annotations only add issue references to the report -- they never change
    a figure -- and the report labels them as hand-entered. An entry naming
    an agency with no ``config/agencies/<name>.yaml`` raises ValueError, so a
    typo cannot silently drop it.
    """
    path = path or ANNOTATIONS_PATH
    if not path.exists():
        return {"dates": [], "routes": []}
    raw = yaml.safe_load(path.read_text()) or {}
    known_agencies = {p.stem for p in AGENCY_CONFIG_DIR.glob("*.yaml")}
    out: dict = {"dates": [], "routes": []}
    for key, id_field in (("dates", "date"), ("routes", "route_id")):
        for e in raw.get(key) or []:
            agency = str(e["agency"])
            if agency not in known_agencies:
                raise ValueError(
                    f"{path}: annotation {e!r} names unknown agency {agency!r} "
                    f"(known: {', '.join(sorted(known_agencies))})"
                )
            out[key].append(
                {
                    "agency": agency,
                    id_field: str(e[id_field]),
                    "issue": e.get("issue"),
                    "note": e.get("note"),
                }
            )
    return out


def _mean_excluding(summary_values: dict[str, float], drop: set[str]) -> float | None:
    """Mean of ``summary_values`` with the ``drop`` dates left out (None if nothing left)."""
    return _mean_skip_null([v for d, v in summary_values.items() if d not in drop])


def _relative_shift(base: float | None, other: float | None) -> float | None:
    """``|other - base| / |base|``, or None when undefined."""
    if base is None or other is None or base == 0:
        return None
    return abs(other - base) / abs(base)


def headline_robustness(
    dates: list[date_type],
    rows: dict[str, dict],
    metrics: dict[str, dict],
    date_issues: dict[str, str],
    excluding_routes_daily: dict[str, dict] | None,
    routes_under_investigation: list[str],
) -> dict[str, dict]:
    """How much each headline figure depends on flagged days or routes under investigation.

    For each headline metric: ``mean_excluding_flagged`` (period mean with
    the metric's flagged days removed), ``mean_excluding_routes`` (mean of
    daily system values re-derived without ``routes_under_investigation``,
    over the same included dates; None when there are none), whether
    either moves the mean by more than ``MATERIAL_SHIFT`` (``moved``), and
    the issue references attached to the flagged days (``issues``).
    """
    out: dict[str, dict] = {}
    for metric in HEADLINE_METRICS:
        s = metrics[metric]
        excluded = {e["date"] for e in s["excluded"]}
        included = {
            d.isoformat(): float(rows[d.isoformat()][metric])
            for d in dates
            if d.isoformat() not in excluded
        }
        flagged = {f["date"] for f in s["flagged_days"]}
        mean_wo_flagged = _mean_excluding(included, flagged) if flagged else None
        mean_wo_routes = None
        if excluding_routes_daily and routes_under_investigation:
            mean_wo_routes = _mean_skip_null(
                [(excluding_routes_daily.get(iso) or {}).get(metric) for iso in included]
            )
        shifts = [
            x
            for x in (
                _relative_shift(s["mean"], mean_wo_flagged),
                _relative_shift(s["mean"], mean_wo_routes),
            )
            if x is not None
        ]
        out[metric] = {
            "mean": s["mean"],
            "median_day": s["median"],
            "flagged_dates": sorted(flagged),
            "mean_excluding_flagged": mean_wo_flagged,
            "routes_under_investigation": list(routes_under_investigation)
            if mean_wo_routes is not None
            else [],
            "mean_excluding_routes": mean_wo_routes,
            "moved": any(x > MATERIAL_SHIFT for x in shifts),
            "issues": sorted({date_issues[d] for d in flagged if d in date_issues}),
        }
    return out


def ewt_method_coverage(
    db: Session, dates: list[date_type], rows: dict[str, dict], ewt_summary: dict
) -> dict:
    """Which of the period's EWT dates were computed under the 120-minute cap (issue #269).

    The stored tables carry no method marker; a date counts as capped when
    its ``system_metrics_daily`` row AND every one of its ``complete``
    overlay rows have ``computed_at`` at or after
    ``src.ewt.EWT_LONG_GAP_RULE_SINCE_UTC`` (``ewt_computed_with_cap``).
    Only dates whose system EWT is included in the period figure are
    classified. ``status`` is ``capped`` (all), ``old`` (none), ``mixed``
    or ``none`` (no EWT dates).
    """
    excluded = {e["date"] for e in ewt_summary["excluded"]}
    included = [d.isoformat() for d in dates if d.isoformat() not in excluded]
    overlay_min: dict[str, datetime] = {}
    if included:
        overlay_min = dict(
            db.query(
                RouteMetricsDailyOverlay.service_date,
                func.min(RouteMetricsDailyOverlay.computed_at),
            )
            .filter(
                RouteMetricsDailyOverlay.service_date.in_(included),
                RouteMetricsDailyOverlay.data_quality == "complete",
            )
            .group_by(RouteMetricsDailyOverlay.service_date)
            .all()
        )
    capped, old = [], []
    for iso in included:
        system_ok = ewt_computed_with_cap((rows.get(iso) or {}).get("computed_at"))
        overlay_ok = iso not in overlay_min or ewt_computed_with_cap(overlay_min[iso])
        (capped if system_ok and overlay_ok else old).append(iso)
    if not included:
        status = "none"
    elif not old:
        status = "capped"
    elif not capped:
        status = "old"
    else:
        status = "mixed"
    return {
        "rule_since_utc": EWT_LONG_GAP_RULE_SINCE_UTC.isoformat(timespec="seconds"),
        "status": status,
        "n_dates": len(included),
        "capped_dates": capped,
        "old_method_dates": old,
    }


def ewt_methods_comparable(a: dict | None, b: dict | None) -> bool:
    """True when two agencies' EWT figures were computed under one method.

    Comparable only when each agency's period is uniformly ``capped`` or
    uniformly ``old`` and both agree. Missing coverage (older JSON) is
    treated as comparable so old reports re-render unchanged.
    """
    if a is None or b is None:
        return True
    sa, sb = a["status"], b["status"]
    if "none" in (sa, sb):
        return True
    return sa == sb and sa in ("capped", "old")


def compare_direction(sections: dict[str, dict], subject: str) -> dict[str, dict]:
    """Whether the subject-vs-peer difference keeps its direction across robust variants.

    For each peer and headline metric, compares the sign of (subject -
    peer) using the period means, the median days, the means with flagged
    days removed, and the means with dominated routes removed (the same
    rule applied to every agency; an agency with none keeps its mean).
    ``direction_robust`` is False when the signs disagree -- the report then
    says the difference is not a finding. For EWT it is also False when the
    two agencies' periods were not computed under one method (issue #269
    long-gap cap; ``method_comparable``, see `ewt_method_coverage`).
    """
    out: dict[str, dict] = {}
    s_rob = sections[subject]["robustness"]
    for peer, section in sections.items():
        if peer == subject:
            continue
        p_rob = section["robustness"]
        out[peer] = {}
        for metric in HEADLINE_METRICS:
            a, b = s_rob[metric], p_rob[metric]
            variants = {
                "mean": (a["mean"], b["mean"]),
                "median_day": (a["median_day"], b["median_day"]),
                "excluding_flagged": (
                    a["mean_excluding_flagged"]
                    if a["mean_excluding_flagged"] is not None
                    else a["mean"],
                    b["mean_excluding_flagged"]
                    if b["mean_excluding_flagged"] is not None
                    else b["mean"],
                ),
            }
            if a["mean_excluding_routes"] is not None or b["mean_excluding_routes"] is not None:
                variants["excluding_routes"] = (
                    a["mean_excluding_routes"]
                    if a["mean_excluding_routes"] is not None
                    else a["mean"],
                    b["mean_excluding_routes"]
                    if b["mean_excluding_routes"] is not None
                    else b["mean"],
                )
            signs = {
                name: (x > y) - (x < y)
                for name, (x, y) in variants.items()
                if x is not None and y is not None
            }
            comparable = metric != "ewt" or ewt_methods_comparable(
                sections[subject].get("ewt_method"), section.get("ewt_method")
            )
            out[peer][metric] = {
                "differences": {
                    name: (x - y) if x is not None and y is not None else None
                    for name, (x, y) in variants.items()
                },
                "direction_robust": len(set(signs.values())) <= 1 and comparable,
                "method_comparable": comparable,
            }
    return out


def build_agency_section(
    db: Session,
    agency: str,
    start: date_type,
    end: date_type,
    *,
    deep: bool,
    annotations: dict | None = None,
) -> dict:
    """Compute one agency's scorecard section (read-only).

    ``deep=True`` (the subject agency) adds the best / worst route
    rankings. Every agency, deep or not, goes
    through the same daily-value, exclusion, day-type, cross-check,
    robustness and GTFS code path, so peer headline figures are computed
    identically. ``annotations`` (see `load_annotations`) attaches known
    issue references to dates and dominated routes.
    """
    annotations = annotations or EMPTY_ANNOTATIONS
    cfg = load_agency_config(agency)
    dates = period_dates(start, end)
    rows = read_system_rows(db, start, end)
    eligible = eligible_dates(dates, rows)
    period_isos = {d.isoformat() for d in dates}
    known_dates = [
        a for a in annotations["dates"] if a["agency"] == agency and a["date"] in period_isos
    ]
    date_issues = {a["date"]: a["issue"] for a in known_dates if a.get("issue")}
    route_issues = {
        a["route_id"]: a["issue"]
        for a in annotations.get("routes", [])
        if a["agency"] == agency and a.get("issue")
    }

    snapshot_by_date, feed_versions, reason_by_date = gtfs_snapshots_for_period(
        db, dates, rows, cfg.timezone
    )
    bus_ids_cache: dict[int | None, set[str]] = {}
    bus_ids_by_date: dict[str, set[str]] = {}
    for d in eligible:
        sid = snapshot_by_date.get(d.isoformat())
        if sid not in bus_ids_cache:
            bus_ids_cache[sid] = bus_route_ids(db, gtfs_snapshot_id=sid)
        bus_ids_by_date[d.isoformat()] = bus_ids_cache[sid]

    bundles = read_overlay_bundles(db, eligible)
    computed = [r["computed_at"] for r in rows.values() if r.get("computed_at") is not None]
    observed = frozenset(
        iso
        for iso, r in rows.items()
        if r.get("otp") is not None or (r.get("service_delivered") or 0) > 0
    )
    service_patterns = service_pattern_dates(db, snapshot_by_date, reason_by_date, observed)
    holiday_type = frozenset(p["date"] for p in service_patterns if p["holiday_type"])
    metrics = {m: summarize_daily_metric(dates, rows, m) for m in SYSTEM_METRICS}
    for m in SYSTEM_METRICS:
        for f in metrics[m]["flagged_days"]:
            f["issue"] = date_issues.get(f["date"])

    # Every agency gets the same dominance rule for the robustness variant;
    # only the deep subject gets rankings (and printed route detail).
    route_metrics = pool_route_metrics(bundles)
    dominated = dominated_routes(route_metrics)
    under_investigation = [e["route_id"] for e in dominated]
    for e in dominated:
        e["issue"] = route_issues.get(e["route_id"])
    routes_block = None
    if deep:
        frequent_ids = load_frequent_route_ids(agency)
        routes_block = {
            "n_routes_with_data": len(route_metrics),
            "frequent_designation_source": (
                "config/frequent_routes.yaml" if frequent_ids else "none (no designated list)"
            ),
            "rankings": rank_routes(
                route_metrics, frequent_ids, current_route_names(db), route_issues=route_issues
            ),
        }
    excluding_routes_daily = (
        system_daily_excluding_routes(bundles, bus_ids_by_date, set(under_investigation))
        if under_investigation
        else None
    )

    section = {
        "agency": agency,
        "display_name": cfg.display_name,
        "timezone": cfg.timezone,
        "polling": {
            "trip_updates_sec": cfg.tick_sec * cfg.trip_updates_every_ticks,
            "vehicle_positions_sec": cfg.tick_sec * cfg.vehicle_positions_every_ticks,
        },
        "completeness_threshold": agency_coverage_threshold(cfg),
        "completeness": completeness_rule(cfg),
        "n_days": len(dates),
        "n_days_complete": len(eligible),
        "metrics": metrics,
        "robustness": headline_robustness(
            dates, rows, metrics, date_issues, excluding_routes_daily, under_investigation
        ),
        "day_types": day_type_split(dates, rows, holiday_type),
        "daily": daily_table(dates, rows),
        "pooled_crosscheck": pooled_system_crosscheck(bundles, rows, bus_ids_by_date),
        "gtfs_feed_versions": feed_versions,
        "service_pattern_dates": service_patterns,
        "known_issues": known_dates,
        "dominated_routes": dominated,
        "ewt_method": ewt_method_coverage(db, dates, rows, metrics["ewt"]),
        "aggregates_computed_at_utc": {
            "min": _iso_or_none(min(computed)) if computed else None,
            "max": _iso_or_none(max(computed)) if computed else None,
        },
    }
    if routes_block is not None:
        section["routes"] = routes_block
    return section


# The day PR #274 (issues #270 / #272) merged and the feed-gap leg of the
# completeness stamp took effect. Stamps written before it were decided on
# coverage alone. Not used to classify dates: a stored row's `computed_at`
# cannot tell which rule decided its stamp, because a hand-corrected stamp
# (the #274 owner SQL) leaves `computed_at` alone and the EWT-only recompute
# (`scripts/recompute_ewt_aggregates.py`) bumps it without re-assessing the
# stamp. The report therefore states the date and gives no per-rule count.
COMPLETENESS_GAP_RULE_SINCE = "2026-11-26"


def completeness_rule(cfg) -> dict:
    """The agency's two-part ``data_quality`` rule as recorded in the scorecard JSON.

    Leg 1 (coverage): the share of the day's minutes with any vehicle-position,
    trip-update or collector-heartbeat record must reach
    ``coverage_threshold`` = ``coverage_target`` (0.80) x ``cadence_ceiling``
    (the coverage achievable at the agency's polling cadence;
    ``src.data_completeness.agency_coverage_threshold``). Leg 2 (feed gaps):
    neither feed may be silent longer than ``feed_gap_max_minutes`` between
    ``service_start_hour`` and ``service_end_hour`` agency-local
    (``AgencyConfig.completeness``, issue #270). All values are read from
    ``config/agencies/<agency>.yaml``.
    """
    threshold = agency_coverage_threshold(cfg)
    policy = cfg.completeness
    return {
        "coverage_threshold": threshold,
        "coverage_target": MIN_COVERAGE_FOR_MATERIALIZATION,
        "cadence_ceiling": threshold / MIN_COVERAGE_FOR_MATERIALIZATION,
        "feed_gap_max_minutes": policy.feed_gap_max_minutes,
        "service_start_hour": policy.service_start_hour,
        "service_end_hour": policy.service_end_hour,
        "gap_rule_since": COMPLETENESS_GAP_RULE_SINCE,
    }


_ZONE_LABELS = {"America/New_York": "Eastern", "America/Los_Angeles": "Pacific"}


def _local_window(section: dict) -> str:
    """``05:00-24:00 Eastern``-style label for an agency's gap-rule window."""
    rule = section["completeness"]
    zone = _ZONE_LABELS.get(section.get("timezone"), section.get("timezone") or "local")
    return f"{rule['service_start_hour']:02d}:00-{rule['service_end_hour']:02d}:00 {zone}"


def completeness_rule_text(section: dict) -> str:
    """One agency's completeness rule in plain words, from its section's ``completeness`` block.

    Falls back to the coverage-only wording for scorecard JSON generated
    before the gap rule was recorded.
    """
    rule = section.get("completeness")
    if rule is None:
        return f"ingest coverage >= {section['completeness_threshold']:.3f}"
    ceiling = rule["cadence_ceiling"]
    ceiling_note = (
        ""
        if abs(ceiling - 1.0) < 1e-9
        else f", i.e. {rule['coverage_target']:.2f} x the {ceiling:.3f} ceiling its polling "
        "cadence allows"
    )
    return (
        f"ingest coverage >= {rule['coverage_threshold']:.3f}{ceiling_note}, and neither the "
        f"trip-update nor the vehicle-position feed silent for more than "
        f"{rule['feed_gap_max_minutes']} minutes between {_local_window(section)}"
    )


def comparability_caveats(sections: dict[str, dict]) -> list[str]:
    """Plain-language caveats for reading the peer columns next to WMATA.

    Polling cadences and completeness thresholds are read from each
    section (i.e. from ``config/agencies/*.yaml``), not hard-coded.
    """
    cadence = "; ".join(
        f"{s['display_name']}: trip updates every {s['polling']['trip_updates_sec']} s, "
        f"vehicle positions every {s['polling']['vehicle_positions_sec']} s, "
        f"'complete' at {completeness_rule_text(s)}"
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
    provenance: dict | None = None,
    annotations: dict | None = None,
) -> dict:
    """Assemble the full scorecard dict (JSON-serializable) from open read-only sessions.

    ``sessions`` maps agency name to a session on that agency's database;
    ``subject`` must be one of them and gets the deep section, every other
    agency the shallow (headline-only) one. ``generated_at_utc`` is
    supplied by the caller (naive UTC) so tests can pin it. ``provenance``
    (from the CLI: git commit, dirty paths, source hashes) is recorded
    verbatim. ``annotations`` defaults to `load_annotations()`.
    """
    if subject not in sessions:
        raise ValueError(f"subject agency {subject!r} has no session")
    period_dates(start, end)  # validates ordering
    if annotations is None:
        annotations = load_annotations()
    order = [subject] + sorted(a for a in sessions if a != subject)
    sections = {
        agency: build_agency_section(
            sessions[agency],
            agency,
            start,
            end,
            deep=agency == subject,
            annotations=annotations,
        )
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
        "provenance": provenance or {},
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
            "route_dominance_guard": {
                "ewt_min_sec": ROUTE_EWT_DOMINANCE_MIN_SEC,
                "otp_pp": ROUTE_OTP_DOMINANCE_PP,
            },
            "route_ewt_clamped_at_zero": True,
            "material_shift": MATERIAL_SHIFT,
            "service_pattern": {
                "baseline_weeks": SERVICE_BASELINE_WEEKS,
                "trip_ratio_tolerance": SERVICE_TRIP_RATIO_TOLERANCE,
                "day_type_rule": "calendar weekday; holiday-type dates footnoted",
            },
            "ewt_gate_sec": {"high_freq": HIGH_FREQ_GATE_SEC, "medium_freq": MEDIUM_FREQ_GATE_SEC},
            # Issue #269: observed and scheduled headways longer than this are
            # dropped from EWT/SWT by the current code. Stored rows computed
            # before the rule keep the old method until recomputed; each agency
            # section's `ewt_method` says which in-period dates are which.
            "ewt_max_headway_sec": MAX_OBSERVED_HEADWAY_SEC,
            "outlier_rule": {"mad_multiplier": OUTLIER_MAD_MULTIPLIER, "mad_to_sd": _MAD_TO_SD},
            "sources": {
                "system": "system_metrics_daily",
                "routes": "route_metrics_daily_overlay",
                "route_names": "routes (is_current)",
                "feed_versions": "gtfs_snapshots + calendar / calendar_dates",
            },
        },
        "agencies": sections,
        "comparison": compare_direction(sections, subject),
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
    lines = _md_table(["Metric", "Weekday", "Saturday", "Sunday"], body)
    patterns = {p["date"]: p for p in section.get("service_pattern_dates", [])}
    for day_type in DAY_TYPES:
        block = section["day_types"][day_type]
        if not block.get("holiday_type_dates"):
            continue
        held = ", ".join(
            f"{d} ({_describe_pattern(patterns[d])})" if d in patterns else d
            for d in block["holiday_type_dates"]
        )
        excl = block["excluding_holiday_type"]
        means = ", ".join(
            f"{METRIC_LABELS[m].lower()} {fmt_metric(m, excl[m]['mean'])}" for m in HEADLINE_METRICS
        )
        lines += [
            "",
            f"Note: the {day_type} column includes holiday-type service dates: {held}. Day "
            f"type follows the calendar; without these dates the {day_type} means are: "
            f"{means}.",
        ]
    return lines


def _describe_pattern(p: dict) -> str:
    """Plain description of one service-pattern date's classification."""
    kind = p["kind"]
    if kind == "feed_not_covering":
        return "schedule pattern unknown: the feed in use did not cover this date"
    if kind == "undetermined":
        return f"schedule pattern undetermined ({p['detail']})"
    if kind == "service_change":
        return "schedule change within the feed (a new pattern that persists, not an exception)"
    if kind == "other_day_type":
        return f"ran the usual {p['runs_as']} service"
    if kind == "no_service":
        return "no scheduled service"
    ratio = p.get("trip_ratio")
    share = (
        f"{ratio * 100:.0f}% of the scheduled trips on comparable dates"
        if ratio is not None
        else "no comparable dates"
    )
    if kind == "reduced":
        return f"reduced service, {share}"
    if kind == "increased":
        return f"added service, {share}"
    return f"timetable substitution not seen on nearby dates, comparable trip count ({share})"


def group_pattern_runs(patterns: list[dict]) -> list[dict]:
    """Collapse consecutive same-description pattern dates into runs for display.

    Dates join a run when they share kind, description and (except for
    schedule changes, which may use per-weekday service ids) active services,
    and fall within 3 days of the previous date (so a weekday run spans the
    weekend). Returns ``[{start, end, n_dates, day_types, description}]``.
    """
    runs: list[dict] = []
    for p in patterns:
        desc = _describe_pattern(p)
        # A schedule change may use different service ids per weekday (WMATA),
        # so its run is keyed on the description alone.
        services = () if p["kind"] == "service_change" else tuple(p["active_services"])
        key = (p["kind"], services, desc)
        d = date_type.fromisoformat(p["date"])
        if (
            runs
            and runs[-1]["_key"] == key
            and (d - date_type.fromisoformat(runs[-1]["end"])).days <= 3
        ):
            runs[-1]["end"] = p["date"]
            runs[-1]["n_dates"] += 1
            if p["day_type"] not in runs[-1]["day_types"]:
                runs[-1]["day_types"].append(p["day_type"])
        else:
            runs.append(
                {
                    "_key": key,
                    "start": p["date"],
                    "end": p["date"],
                    "n_dates": 1,
                    "day_types": [p["day_type"]],
                    "description": desc,
                }
            )
    for r in runs:
        del r["_key"]
    return runs


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
        *_render_dominated(freq, "ewt"),
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
        "",
        *_render_dominated(other, "otp"),
    ]
    return lines


def _issue_suffix(e: dict) -> str:
    """Tracking-issue suffix for a dominated route: annotated issue, or 'not yet tracked'."""
    return f" (hand-entered annotation: {e['issue']})" if e.get("issue") else " (not yet tracked)"


def _render_dominated(group: dict, metric: str) -> list[str]:
    """List routes withheld from a ranking because a few days dominate their pooled figure."""
    if not group.get("dominated"):
        return []
    return [
        "Not ranked: pooled figure dominated by a few days. "
        + "; ".join(
            f"{e['route_name']} {e['route_long_name'] or ''}".rstrip()
            + f": pooled {fmt_metric(metric, e['value'])}, median day "
            f"{fmt_metric(metric, e['median_day'])}" + _issue_suffix(e)
            for e in group["dominated"]
        )
        + ".",
        "",
    ]


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


def _data_quality_rule_sentence(section: dict) -> str:
    """The completeness-rule sentence on an agency's Data quality line.

    Older JSON (no ``completeness`` block) keeps the original
    coverage-only sentence verbatim.
    """
    if section.get("completeness") is None:
        return (
            f"Completeness threshold for this agency: ingest coverage >= "
            f"{section['completeness_threshold']:.3f}."
        )
    return (
        f"Completeness rule for this agency: {completeness_rule_text(section)}. Dates stamped "
        f"before {section['completeness'].get('gap_rule_since', COMPLETENESS_GAP_RULE_SINCE)} "
        "were judged on coverage alone (plus any hand corrections). A date's stored coverage "
        "can sit on either side of the threshold when its stamp was decided by the feed-gap "
        "test, was hand-corrected, or was kept from an earlier assessment."
    )


def _render_data_quality(section: dict) -> list[str]:
    """Exclusions, flagged days, schedule exceptions and the per-date table."""
    lines = [
        f"Dates in period: {section['n_days']}; stamped complete: {section['n_days_complete']} "
        f"(coverage {_fmt_pct(section['n_days_complete'] / section['n_days'])}). "
        + _data_quality_rule_sentence(section),
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
                + ", ".join(
                    f"{f['date']} ({fmt_metric(metric, f['value'])}"
                    + (f"; {f['issue']}" if f.get("issue") else "")
                    + ")"
                    for f in flagged
                )
            )
    if not any_flagged:
        lines.append("- No unusual days flagged.")
    patterns = section["service_pattern_dates"]
    if patterns:
        lines.append(
            "- Dates whose scheduled service is not the usual service for that weekday, or "
            "could not be determined (day type still follows the calendar): "
            + "; ".join(
                (
                    r["start"]
                    if r["n_dates"] == 1
                    else f"{r['start']} to {r['end']} ({r['n_dates']} dates)"
                )
                + f" ({'/'.join(r['day_types'])}: {r['description']})"
                for r in group_pattern_runs(patterns)
            )
        )
    else:
        lines.append("- Every date ran the usual scheduled service for its weekday.")
    for k in section.get("known_issues", []):
        lines.append(
            f"- Known issue for {k['date']} (hand-entered annotation): {k['note']} ({k['issue']})."
        )
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
            + (
                f". {len(fv['dates_computed_before_load'])} of these dates were computed before "
                "this snapshot was loaded, against an earlier load no longer retained"
                if fv.get("dates_computed_before_load")
                else ""
            )
        )
    return lines


def _render_provenance(prov: dict) -> list[str]:
    """Provenance line: generating commit, dirty state and source content hashes.

    The commit identifies the generator code on its branch at generation
    time and may become unreachable after a squash merge; the sha256 of
    each source file identifies the code independently of git history.
    """
    if not prov:
        return ["Provenance: not recorded."]
    commit = prov.get("git_commit")
    dirty = prov.get("git_dirty_paths")
    if dirty is None:
        state = "working-tree state unknown"
    elif dirty:
        state = "uncommitted changes in " + ", ".join(f"`{p}`" for p in dirty)
    else:
        state = "working tree clean apart from the output directory"
    files = prov.get("source_sha256", {})
    combined = prov.get("source_digest")
    hashes = (
        f"sha256 `{combined[:16]}` over {len(files)} files (every repository module loaded "
        "during generation plus the config files read; per-file hashes in the JSON)"
        if combined
        else "not recorded"
    )
    return [
        f"Provenance: generator commit `{commit[:12] if commit else 'unknown'}` (the commit "
        "checked out at generation time; it may not be reachable after a squash merge), "
        f"{state}. Source content digest: {hashes}. Library versions and the databases' "
        "contents are not covered by the digest."
    ]


def _render_summary(scorecard: dict, subject: dict, peers: list[dict]) -> list[str]:
    """Summary table (mean with median day) plus robustness notes and comparison cautions.

    A cell gets a numbered note when removing flagged days or routes under
    investigation moves its mean by more than the material-shift threshold;
    a metric whose subject-vs-peer difference changes direction across the
    robust variants gets an explicit "not a finding" line.
    """
    header = ["Metric", subject["display_name"], *[p["display_name"] for p in peers]]
    body: list[list[str]] = []
    notes: list[str] = []
    for metric in HEADLINE_METRICS:
        row = [METRIC_LABELS[metric]]
        for s in [subject, *peers]:
            ms = s["metrics"][metric]
            rob = s["robustness"][metric]
            cell = (
                f"{fmt_metric(metric, ms['mean'])} (median day "
                f"{fmt_metric(metric, ms['median'])}; {ms['n_days_included']} days)"
            )
            if rob["moved"]:
                parts = []
                if rob["mean_excluding_flagged"] is not None:
                    flagged = ", ".join(_dated_label(ms, d) for d in rob["flagged_dates"])
                    parts.append(
                        f"without its unusual days ({flagged}) the mean is "
                        f"{fmt_metric(metric, rob['mean_excluding_flagged'])}"
                    )
                if rob["mean_excluding_routes"] is not None:
                    dominated = s.get("dominated_routes") or []
                    if "routes" in s:  # deep agency: route detail may be printed
                        which = ", ".join(
                            e["route_id"]
                            + (f" ({e['issue']})" if e.get("issue") else " (not yet tracked)")
                            for e in dominated
                        )
                    else:
                        which = f"{len(dominated)} routes; route detail not shown for peers"
                    parts.append(
                        f"without routes whose figures are dominated by a few days ({which}) "
                        f"it is {fmt_metric(metric, rob['mean_excluding_routes'])}"
                    )
                notes.append(
                    f"{s['display_name']} {METRIC_LABELS[metric].lower()}: "
                    + "; ".join(parts)
                    + "."
                )
                cell += f" [{len(notes)}]"
            row.append(cell)
        body.append(row)
    lines = _md_table(header, body)
    if notes:
        lines += ["", "Figures materially moved by a few days or routes:", ""]
        lines += [f"{i}. {n}" for i, n in enumerate(notes, start=1)]
    cautions = []
    for peer, by_metric in (scorecard.get("comparison") or {}).items():
        peer_name = scorecard["agencies"][peer]["display_name"]
        for metric, comp in by_metric.items():
            if comp["direction_robust"]:
                continue
            if not comp.get("method_comparable", True):
                cautions.append(
                    f"- {METRIC_LABELS[metric]}: {subject['display_name']} and {peer_name} "
                    "figures in this period were not all computed under the same method (the "
                    "120-minute long-gap rule; see the method coverage note below), so the "
                    "difference between them is not a finding."
                )
            else:
                cautions.append(
                    f"- {METRIC_LABELS[metric]}: the difference between "
                    f"{subject['display_name']} and {peer_name} changes direction depending on "
                    "whether the mean, the median day, or the mean without unusual days or "
                    "routes is used, so it is not a finding."
                )
    if cautions:
        lines += ["", "Differences that are not robust:", "", *cautions]
    method_notes = [
        n for s in [subject, *peers] if (n := _ewt_method_note(s, scorecard["method"])) is not None
    ]
    if method_notes:
        lines += ["", "Excess wait time method coverage:", "", *method_notes]
    return lines


def _date_runs(isos: list[str]) -> str:
    """Compress ISO dates into ``a to b`` runs of consecutive days, comma-separated."""
    runs: list[list[date_type]] = []
    for iso in sorted(isos):
        d = date_type.fromisoformat(iso)
        if runs and (d - runs[-1][-1]).days == 1:
            runs[-1].append(d)
        else:
            runs.append([d])
    return ", ".join(
        r[0].isoformat() if len(r) == 1 else f"{r[0].isoformat()} to {r[-1].isoformat()}"
        for r in runs
    )


def _ewt_method_note(section: dict, method: dict) -> str | None:
    """Summary bullet naming an agency's EWT dates computed without the long-gap rule.

    None when the agency's EWT dates are all capped, it has none, or the
    JSON predates method coverage.
    """
    cov = section.get("ewt_method")
    if cov is None or cov["status"] in ("capped", "none") or "ewt_max_headway_sec" not in method:
        return None
    minutes = method["ewt_max_headway_sec"] // 60
    n_old, n = len(cov["old_method_dates"]), cov["n_dates"]
    if cov["status"] == "mixed":
        return (
            f"- {section['display_name']}: {n_old} of {n} excess-wait-time dates were computed "
            f"before the {minutes}-minute long-gap rule and have not been recomputed, so they "
            f"still count every gap ({_date_runs(cov['old_method_dates'])}). Their EWT is not "
            "comparable with dates under the rule; the period mean mixes both."
        )
    return (
        f"- {section['display_name']}: all {n} excess-wait-time dates were computed before "
        f"the {minutes}-minute long-gap rule and still count every gap."
    )


def _dated_label(metric_summary: dict, iso: str) -> str:
    """``iso`` followed by its tracking issue in parentheses when the flagged day has one."""
    for f in metric_summary["flagged_days"]:
        if f["date"] == iso and f.get("issue"):
            return f"{iso} ({f['issue']})"
    return iso


def _ewt_long_gap_sentence(method: dict, sections: dict[str, dict] | None = None) -> str:
    """Method-text sentence for the EWT long-gap rule (issue #269), or "" for older JSON.

    Keyed on ``method["ewt_max_headway_sec"]`` so re-rendering a scorecard
    JSON generated before the rule existed does not claim it was applied.
    When ``sections`` carry ``ewt_method`` coverage, the sentence says which
    agencies' dates were computed under the rule rather than asserting it
    for all of them.
    """
    cap = method.get("ewt_max_headway_sec")
    if cap is None:
        return ""
    text = (
        f" Under the current method any headway longer than {cap // 60} minutes, observed or "
        "scheduled, is left out of both averages whatever its cause: such a gap is almost "
        "always a service break that two consecutive arrivals happen to span (a missing last "
        "trip before a scheduled break, a run of skipped stops, or a first stop the real-time "
        "feed cannot see), and because waits are weighted by the square of the headway, one "
        "such gap would otherwise outweigh a whole day of normal service. The trade-off is "
        "that a genuine gap of more than two hours is not counted as waiting time. When the "
        "trips did not run it still shows in service delivered; when buses ran but skipped "
        "stops, no headline figure here captures it."
    )
    covs = [
        (s["display_name"], s["ewt_method"])
        for s in (sections or {}).values()
        if s.get("ewt_method") is not None
    ]
    if covs:
        parts = []
        for name, cov in covs:
            if cov["status"] == "capped":
                parts.append(f"{name}: all {cov['n_dates']} dates")
            elif cov["status"] == "none":
                parts.append(f"{name}: no EWT dates")
            else:
                parts.append(
                    f"{name}: {len(cov['capped_dates'])} of {cov['n_dates']} dates "
                    "(the rest predate the rule; listed in the Summary)"
                )
        text += (
            " Stored daily values computed before the rule (introduced "
            f"{covs[0][1]['rule_since_utc']} UTC) keep the old method until recomputed, which "
            "is possible only while a date's stop-level rows are retained. Dates under the "
            "rule in this report: " + "; ".join(parts) + "."
        )
    return text


def _exclusion_stamp_clause(sections: list[dict]) -> str:
    """The stamp part of the Method "Exclusions" sentence.

    Scorecard JSON that predates the recorded completeness rule (no
    ``completeness`` block in some section) gets the original wording
    verbatim, so re-rendering a committed report reproduces it byte for
    byte; newer JSON gets the two-part rule from `_completeness_method_text`.
    """
    if not sections or any(s.get("completeness") is None for s in sections):
        return (
            "when its row is not stamped `complete` (ingest coverage below the agency's "
            "threshold), or when the metric is null that day."
        )
    return (
        "when its row is not stamped `complete`, or when the metric is null that day."
        + _completeness_method_text(sections)
    )


def _completeness_method_text(sections: list[dict]) -> str:
    """Method-section sentences for the two-part completeness rule, per agency.

    Only called when every section carries its ``completeness`` block.
    Says plainly which rule decided which stamps: the feed-gap leg exists
    only since ``COMPLETENESS_GAP_RULE_SINCE``; older stamps were decided on
    coverage alone (plus hand corrections). No per-rule count is given
    because the stored rows do not record it — see
    ``COMPLETENESS_GAP_RULE_SINCE`` for why ``computed_at`` cannot.
    """
    per_agency = "; ".join(f"{s['display_name']}: {completeness_rule_text(s)}" for s in sections)
    since = sections[0]["completeness"].get("gap_rule_since", COMPLETENESS_GAP_RULE_SINCE)
    return (
        " A date is stamped `complete` only if (1) enough of the day's data landed: the share "
        "of its minutes with at least one vehicle-position, trip-update or collector-heartbeat "
        "record reaches 80% of what the agency's polling cadence can achieve, and (2) neither "
        "the trip-update nor the vehicle-position feed went silent for longer than the gap "
        "limit inside the agency-local service window (each feed checked separately). "
        f"Per agency: {per_agency}. Dates failing either test are stamped `partial` and "
        "excluded. The rule measures whether the data was collected, not how good the service "
        f"was: a day of poor service with complete data is included. Test (2) exists since "
        f"{since}: a date stamped before then was judged on coverage alone (plus any hand "
        "corrections recorded in the annotations), and the gap test applies to dates stamped "
        "or re-assessed since. The stored rows do not record which rule decided a stamp, so "
        "this report does not count dates per rule. When a feed's raw records for a date have "
        "since been pruned, its test is not re-run and the date keeps its existing stamp."
    )


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
        f"Generated {scorecard['generated_at_utc']} UTC by `{scorecard['generator']}`. "
        "Every figure below is in the accompanying JSON file.",
        "",
        *_render_provenance(scorecard.get("provenance") or {}),
        "",
        "## Summary",
        "",
        *_render_summary(scorecard, subject, peers),
        "",
        "Period figures are means of daily system values over dates stamped complete; the "
        "median day is shown beside each. "
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
        "Lower is better. Scheduled wait time (SWT) is the scheduled side of the same pool."
        + _ewt_long_gap_sentence(m, scorecard["agencies"]),
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
        + _exclusion_stamp_clause([subject, *peers])
        + " Excluded dates are listed under "
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
        f"{floors['otp_min_delivered_trips']:,} delivered trips. A route that clears the "
        "floor but whose pooled figure is far from its median day is listed separately, not "
        "ranked: for EWT when the pooled and median-day values differ by more than the "
        f"median day itself or {m['route_dominance_guard']['ewt_min_sec']:.0f} s, whichever is "
        f"larger; for OTP when they differ by more than "
        f"{m['route_dominance_guard']['otp_pp']:.0f} percentage points. Route EWT, pooled and "
        "daily, is clamped at 0 like the system figure (the dashboard's route view shows it "
        f"unclamped). The top and bottom {floors['top_n']} are shown. Route names come from "
        "the current GTFS feed.",
        "",
        "**Day types and service changes.** Day type is the calendar weekday. A date's "
        "scheduled service is the set of GTFS services active that day (from `calendar` and "
        "`calendar_dates`). If the feed in use did not cover the date (snapshot resolution "
        "fell back to a feed whose calendar excludes it, or no service is scheduled although "
        "service was observed), its schedule pattern is reported as unknown. A date running "
        "the most common service set for its weekday within "
        f"{m['service_pattern']['baseline_weeks']} weeks either side is regular. Otherwise a "
        "date is holiday-type when a weekday runs the same week's Saturday or Sunday service, "
        "has no scheduled service, or has more than "
        f"{m['service_pattern']['trip_ratio_tolerance'] * 100:.0f}% fewer scheduled trips "
        "than comparable dates (the other weekdays of its week; for weekend days, the same "
        "weekday in nearby weeks). Holiday-type dates stay in their calendar day type and "
        "are footnoted under the day-type table, with the means recomputed without them. "
        "Remaining differences are a schedule change within the feed when the new service set "
        "persists (elsewhere in the week for the same day type, or on the same weekday a week "
        "before or after), "
        "added service, or a timetable substitution not seen on nearby dates; and "
        "undetermined when no usual set can be established (no nearby date with service, or "
        "nearby dates split between patterns the date does not match). Unknown, undetermined, "
        "schedule-change and substitution dates are listed but do not alter the split.",
        "",
        "**Robustness notes.** For each headline figure the report also computes the mean "
        "without that metric's unusual days, and the mean of daily system values re-derived "
        "from the per-route table without routes whose pooled EWT or OTP is dominated by a "
        "few days (the ranking guard and floors, applied to every route of every agency "
        f"alike). A figure that either moves by more than "
        f"{m['material_shift'] * 100:.0f}% is marked in the Summary. A cross-agency "
        "difference whose direction is not the same for the mean, the median day and those "
        "variants is called out as not a finding. Known data issues are referenced by issue "
        "number from hand-entered annotations (`config/scorecard_annotations.yaml`), labelled "
        "as such; an annotation is not checked against the data and never changes a figure.",
        "",
        "**Sources.** Only the kept-forever daily aggregate tables are read: "
        "`system_metrics_daily` (one row per service date) and `route_metrics_daily_overlay` "
        "(per-route, per-date sufficient statistics). Stop-level rows are pruned after a "
        "retention window, so reading only these tables keeps this report reproducible for "
        "past periods. Nothing is written to either database.",
        "",
        "**GTFS feed versions in force.** For each date, the snapshot the derivation pipeline "
        "resolves today among the retained snapshots (the newest whose calendar schedules "
        "service that date). Duplicate loads of the same feed are de-duplicated, keeping the "
        "newest, after checking that no date's resolved schedule changes; so where a date's "
        "aggregates predate the retained snapshot's load, the load actually used is gone and "
        "the retained one is reported, with a count of such dates:",
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
