"""
Recompute ONLY the stored EWT columns under the 120-minute long-gap cap (issue #269).

Issue #269 changed how excess wait time is computed: headways longer than
120 minutes are dropped from AWT and SWT. Stored daily aggregates computed
before the change still hold the old (uncapped) figures. This tool rewrites
just the EWT columns for every date that still has stop-level
``trip_update`` rows, and leaves every other column exactly as stored:

- ``route_metrics_daily_overlay``: ``ewt_obs_sum_h``, ``ewt_obs_sum_h_sq``,
  ``ewt_n_observed_headways``, ``ewt_sched_sum_h``, ``ewt_sched_sum_h_sq``,
  ``ewt_n_scheduled_headways`` (and ``computed_at``);
- ``system_metrics_daily``: ``ewt_seconds``, ``swt_seconds`` (and
  ``computed_at``).

Why EWT-only rather than re-running the per-date rollup pipelines
(``pipelines.upsert_route_metrics_overlay`` / ``upsert_system_metrics_daily``):
those rewrite every column, re-stamp ``data_quality`` / ``coverage_pct``,
and recompute OTP, service delivered and bunching from whatever stop-level
rows exist today. On real history that is not a no-op (it demotes dates
whose completeness inputs have been pruned and moves service delivered,
bunching and coverage on some dates). The request was to fix wait time, so
this tool never writes those columns and never touches a stamp.

It still reuses the existing compute paths
(``src.route_metrics_overlay.compute_route_metrics_overlay_for_date`` and
``src.system_metrics.compute_system_metrics_for_date``, pinned to each
date's resolved GTFS snapshot exactly as ``run_daily_batch.py`` does), so the
EWT it writes is exactly what the pipelines would write. Because those
functions also compute the other metrics, the dry-run reports, per date,
every stored column whose recomputed value differs; columns outside the
EWT set are listed as "not written" so drift is visible without being
applied. Writes go through ``src.upsert_helpers.upsert_rows`` with the
update set restricted to the EWT columns, one transaction per date.

``computed_at`` is stamped on every written row, including rows whose EWT
did not change: it is how ``src.ewt.ewt_computed_with_cap`` (and so the
scorecard's method disclosure) tells capped rows from old ones. Side
effect: the scorecard's "aggregates computed before this feed was loaded"
note then dates from this write for those rows, although their service
delivered was computed earlier.

Refused (skipped and listed; nothing written for the date):

- a recomputed route has no stored overlay row (inserting it would need a
  completeness stamp), or a stored route WITH observed headways is absent
  from the recompute (it would stay on the old method). A stored route
  with no observed headways that the recompute no longer produces is
  harmless — the cap cannot change it — so it is kept as stored and only
  its ``computed_at`` is stamped (reported as "kept as stored");
- the recompute would null a stored non-null system ``ewt_seconds``;
- the date's stop-level rows are a fragment of what the stored aggregate was
  built from (issue #276): the recomputed ``ewt_n_observed_headways``,
  summed over routes, is below ``FRAGMENT_MIN_RATIO`` of the stored sum. A
  forced single-date re-derive can leave a small tail of rows on the
  previous, already-pruned date; without this guard that date would look
  recomputable and a good kept aggregate would be overwritten from ~5% of
  the day. A stored total of NULL/0 cannot be judged and is not refused.

Failures (an exception while computing or writing) are reported separately
as FAILED and make the exit status 1. Use ``--exclude-date`` to leave
specific dates alone.

Usage::

    uv run python -m scripts.recompute_ewt_aggregates --agency wmata            # dry run
    uv run python -m scripts.recompute_ewt_aggregates --agency wmata --apply    # write
    uv run python -m scripts.recompute_ewt_aggregates --agency sfmta \\
        --start 2026-10-20 --end 2026-10-31 --exclude-date 2026-10-21

Cost: ~20 s per WMATA date and ~10 s per SFMTA date (both computes run in
full; the dry run costs the same).
"""

from __future__ import annotations

import argparse
import math
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy.orm import Session

from src.agency_config import AgencyConfig, load_agency_config, resolve_agency_db_url
from src.ewt import EWT_LONG_GAP_RULE_SINCE_UTC
from src.gtfs_versioning import resolve_gtfs_snapshot_for_date
from src.models import RouteMetricsDailyOverlay, SystemMetricsDaily
from src.retention import stop_level_sources
from src.timezones import utcnow_naive
from src.upsert_helpers import upsert_rows

EWT_OVERLAY_COLUMNS: tuple[str, ...] = (
    "ewt_obs_sum_h",
    "ewt_obs_sum_h_sq",
    "ewt_n_observed_headways",
    "ewt_sched_sum_h",
    "ewt_sched_sum_h_sq",
    "ewt_n_scheduled_headways",
)
EWT_SYSTEM_COLUMNS: tuple[str, ...] = ("ewt_seconds", "swt_seconds")

# Recomputed for comparison only; never written by this tool.
OTHER_OVERLAY_COLUMNS: tuple[str, ...] = (
    "otp_origin_early",
    "otp_origin_on_time",
    "otp_origin_late",
    "otp_destination_early",
    "otp_destination_on_time",
    "otp_destination_late",
    "otp_all_early",
    "otp_all_on_time",
    "otp_all_late",
    "scheduled_trips",
    "delivered_trips",
    "bunching_count",
    "bunching_total_headways",
)
OTHER_SYSTEM_COLUMNS: tuple[str, ...] = (
    "otp_percentage",
    "service_delivered_ratio",
    "bunching_rate",
)

# Refuse a date when its recomputed observed-headway count (summed over routes)
# is below this share of the stored count (issue #276). The 120-minute long-gap
# rule (#269) legitimately lowers the count a little; fragments sit at a few
# percent. Retune here only.
FRAGMENT_MIN_RATIO = 0.5

OVERLAY_CONSTRAINT = "uq_route_metrics_overlay_route_date"
SYSTEM_CONSTRAINT = "system_metrics_daily_pkey"

OverlayCompute = Callable[..., list[dict]]
SystemCompute = Callable[..., dict]


@dataclass
class DatePlan:
    """What the recompute would do for one service date.

    ``status`` is ``"write"`` (EWT columns would be rewritten),
    ``"refused"`` (policy says skip; ``reason`` says why) or ``"failed"``
    (an exception; ``reason`` carries it). ``changed`` counts, per stored
    column, the rows whose recomputed value differs (system columns count
    0/1, overlay columns count routes); ``system_ewt`` holds the stored and
    recomputed system ``(ewt_seconds, swt_seconds)``.
    """

    service_date: date
    snapshot_id: int | None = None
    status: str = "write"
    reason: str | None = None
    changed: Counter = field(default_factory=Counter)
    system_ewt: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)
    overlay_new: dict[str, dict] = field(default_factory=dict)
    system_new: dict = field(default_factory=dict)
    n_routes: int = 0
    kept_routes: list[str] = field(default_factory=list)

    @property
    def ewt_changes(self) -> dict[str, int]:
        """Changed-row counts for the columns this tool writes."""
        cols = EWT_OVERLAY_COLUMNS + EWT_SYSTEM_COLUMNS
        return {c: n for c, n in self.changed.items() if c in cols and n}

    @property
    def other_changes(self) -> dict[str, int]:
        """Changed-row counts for recomputed columns this tool does NOT write."""
        cols = OTHER_OVERLAY_COLUMNS + OTHER_SYSTEM_COLUMNS
        return {c: n for c, n in self.changed.items() if c in cols and n}


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD into a date; argparse hands the raw string in."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def _differs(old, new) -> bool:
    """True when a stored and recomputed value differ beyond float noise (None-aware)."""
    if old is None or new is None:
        return (old is None) != (new is None)
    return not math.isclose(float(old), float(new), rel_tol=1e-9, abs_tol=1e-9)


def is_fragment_of_stored(stored_n: int | None, recomputed_n: int | None) -> bool:
    """True when the recomputed observed-headway count is under ``FRAGMENT_MIN_RATIO`` of stored.

    A NULL or zero stored count gives nothing to compare against, so the
    guard cannot judge and returns False (it does not refuse on that ground).

    Known limit (issue #283): the check is per date, summed over routes, so a
    date that is a fragment for only some routes passes (e.g. one that lost
    its own post-midnight tail: the system total drops only a few percent
    but owl routes lose most of their observed headways), and those routes'
    overlay EWT would be rewritten from partial data.
    """
    if not stored_n:
        return False
    return (recomputed_n or 0) < FRAGMENT_MIN_RATIO * stored_n


def candidate_dates(db: Session, start: date | None, end: date | None) -> list[date]:
    """``system_metrics_daily`` dates in ``[start, end]`` that still have trip_update rows.

    Dates whose ``trip_update`` stop_events were pruned cannot be recomputed
    and are not candidates; they keep the old method.
    """
    q = db.query(SystemMetricsDaily.service_date)
    if start is not None:
        q = q.filter(SystemMetricsDaily.service_date >= start.isoformat())
    if end is not None:
        q = q.filter(SystemMetricsDaily.service_date <= end.isoformat())
    out = []
    for (iso,) in q.order_by(SystemMetricsDaily.service_date).all():
        d = date.fromisoformat(iso)
        if "trip_update" in stop_level_sources(db, d):
            out.append(d)
    return out


def plan_date(
    db: Session,
    cfg: AgencyConfig,
    service_date: date,
    *,
    compute_overlay: OverlayCompute | None = None,
    compute_system: SystemCompute | None = None,
) -> DatePlan:
    """Recompute one date's metrics WITHOUT writing and compare with what is stored.

    ``compute_overlay`` / ``compute_system`` default to the production
    compute functions; tests inject fakes. Any exception is captured as a
    ``failed`` plan rather than raised, so one bad date never stops a run.
    """
    if compute_overlay is None:
        from src.route_metrics_overlay import compute_route_metrics_overlay_for_date

        compute_overlay = compute_route_metrics_overlay_for_date
    if compute_system is None:
        from src.system_metrics import compute_system_metrics_for_date

        compute_system = compute_system_metrics_for_date

    plan = DatePlan(service_date=service_date)
    iso = service_date.isoformat()
    try:
        plan.snapshot_id = resolve_gtfs_snapshot_for_date(
            db, service_date, cfg.timezone
        ).override_id
        new_rows = compute_overlay(
            db, service_date, plan.snapshot_id, tz_name=cfg.timezone, agency=cfg.name
        )
        new_system = compute_system(
            db, service_date, plan.snapshot_id, tz_name=cfg.timezone, agency=cfg.name
        )
    except Exception as exc:  # noqa: BLE001 — reported per date, run continues
        db.rollback()
        plan.status, plan.reason = "failed", f"{type(exc).__name__}: {exc}"
        return plan

    stored_rows = {
        r.route_id: r
        for r in db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == iso)
        .all()
    }
    stored_system = db.get(SystemMetricsDaily, iso)
    new_by_route = {r["route_id"]: r for r in new_rows}
    plan.n_routes = len(stored_rows)

    for route_id, new in new_by_route.items():
        old = stored_rows.get(route_id)
        if old is None:
            continue
        for col in EWT_OVERLAY_COLUMNS + OTHER_OVERLAY_COLUMNS:
            if _differs(getattr(old, col), new.get(col)):
                plan.changed[col] += 1
    for col in EWT_SYSTEM_COLUMNS + OTHER_SYSTEM_COLUMNS:
        if _differs(getattr(stored_system, col), new_system.get(col)):
            plan.changed[col] += 1
    plan.system_ewt = {
        col: (getattr(stored_system, col), new_system.get(col)) for col in EWT_SYSTEM_COLUMNS
    }
    plan.overlay_new = {
        route_id: {c: new_by_route[route_id][c] for c in EWT_OVERLAY_COLUMNS}
        for route_id in new_by_route
    }
    plan.system_new = {c: new_system.get(c) for c in EWT_SYSTEM_COLUMNS}

    missing = sorted(set(new_by_route) - set(stored_rows))
    extra = sorted(set(stored_rows) - set(new_by_route))
    # A stored route the recompute no longer produces is harmless when it has
    # no observed headways: the cap cannot change it (typically a route from
    # a schedule-only overlay row whose snapshot resolution has since moved).
    # It is kept as stored (only computed_at is stamped); one WITH observed
    # headways would stay uncapped, so the date is refused.
    plan.kept_routes = [r for r in extra if not stored_rows[r].ewt_n_observed_headways]
    extra_observed = [r for r in extra if r not in plan.kept_routes]
    if missing or extra_observed:
        parts = []
        if missing:
            parts.append(f"{len(missing)} recomputed route(s) have no stored row ({missing[:5]})")
        if extra_observed:
            parts.append(
                f"{len(extra_observed)} stored route(s) with observed headways absent from "
                f"the recompute ({extra_observed[:5]})"
            )
        plan.status, plan.reason = "refused", "; ".join(parts)
    elif stored_system.ewt_seconds is not None and new_system.get("ewt_seconds") is None:
        plan.status, plan.reason = "refused", "recompute would null a stored ewt_seconds"
    else:
        stored_n = sum(r.ewt_n_observed_headways or 0 for r in stored_rows.values())
        new_n = sum(r.get("ewt_n_observed_headways") or 0 for r in new_rows)
        if is_fragment_of_stored(stored_n, new_n):
            plan.status = "refused"
            plan.reason = (
                f"stop-level rows look like a fragment of the stored aggregate: recomputed "
                f"{new_n} observed headways vs {stored_n} stored "
                f"(< {FRAGMENT_MIN_RATIO:.0%}); likely spillover rows on a pruned date"
            )
    return plan


def plan_recompute(
    db: Session,
    cfg: AgencyConfig,
    start: date | None = None,
    end: date | None = None,
    exclude: frozenset[date] = frozenset(),
    *,
    compute_overlay: OverlayCompute | None = None,
    compute_system: SystemCompute | None = None,
    progress: Callable[[DatePlan], None] | None = None,
) -> list[DatePlan]:
    """Plan every candidate date in the range (read-only); excluded dates are skipped."""
    plans = []
    for d in candidate_dates(db, start, end):
        if d in exclude:
            continue
        plan = plan_date(db, cfg, d, compute_overlay=compute_overlay, compute_system=compute_system)
        plans.append(plan)
        if progress is not None:
            progress(plan)
    return plans


def _row_dict(row) -> dict:
    """Every mapped column of an ORM row as a plain dict (for a full-row upsert payload)."""
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}


def apply_plan(db: Session, plan: DatePlan, now: datetime | None = None) -> None:
    """Write one ``write`` plan's EWT columns (plus ``computed_at``) in one transaction.

    The upsert payload is the stored row with the EWT columns replaced, so
    the INSERT half always satisfies NOT NULL and the ON CONFLICT half
    updates only ``EWT_*_COLUMNS`` and ``computed_at``. Rolls back and
    re-raises on any error.
    """
    if plan.status != "write":
        raise ValueError(f"plan for {plan.service_date} is {plan.status}, not 'write'")
    now = now or utcnow_naive()
    iso = plan.service_date.isoformat()
    try:
        stored = (
            db.query(RouteMetricsDailyOverlay)
            .filter(RouteMetricsDailyOverlay.service_date == iso)
            .all()
        )
        overlay_payload = []
        kept_payload = []
        for row in stored:
            d = _row_dict(row)
            d.pop("id", None)
            d["computed_at"] = now
            if row.route_id in plan.overlay_new:
                d.update(plan.overlay_new[row.route_id])
                overlay_payload.append(d)
            else:  # a no-observation route kept as stored (see plan_date)
                kept_payload.append(d)
        if kept_payload:
            upsert_rows(
                db,
                RouteMetricsDailyOverlay,
                kept_payload,
                OVERLAY_CONSTRAINT,
                ["computed_at"],
                commit=False,
            )
        if overlay_payload:
            upsert_rows(
                db,
                RouteMetricsDailyOverlay,
                overlay_payload,
                OVERLAY_CONSTRAINT,
                [*EWT_OVERLAY_COLUMNS, "computed_at"],
                commit=False,
            )
        system = _row_dict(db.get(SystemMetricsDaily, iso))
        system.update(plan.system_new)
        system["computed_at"] = now
        upsert_rows(
            db,
            SystemMetricsDaily,
            [system],
            SYSTEM_CONSTRAINT,
            [*EWT_SYSTEM_COLUMNS, "computed_at"],
            commit=False,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise


def _fmt(v: float | None) -> str:
    """Seconds with two decimals, or NULL."""
    return "NULL" if v is None else f"{v:.2f}"


def format_plan(plan: DatePlan, applied: bool = False) -> str:
    """One report line per date (refusals and failures are labelled distinctly)."""
    iso = plan.service_date.isoformat()
    snap = "current" if plan.snapshot_id is None else str(plan.snapshot_id)
    if plan.status == "failed":
        return f"FAILED   {iso} snapshot={snap}: {plan.reason}"
    if plan.status == "refused":
        return f"REFUSED  {iso} snapshot={snap}: {plan.reason}"
    ewt_old, ewt_new = plan.system_ewt["ewt_seconds"]
    swt_old, swt_new = plan.system_ewt["swt_seconds"]
    routes = plan.changed.get("ewt_obs_sum_h_sq", 0)
    verb = "WROTE   " if applied else "WOULD   "
    line = (
        f"{verb} {iso} snapshot={snap}: system ewt {_fmt(ewt_old)} -> {_fmt(ewt_new)}, "
        f"swt {_fmt(swt_old)} -> {_fmt(swt_new)}; overlay EWT sums changed on "
        f"{routes}/{plan.n_routes} routes"
    )
    if plan.kept_routes:
        line += (
            f"; {len(plan.kept_routes)} stored no-observation route(s) kept as stored "
            f"({plan.kept_routes[:5]})"
        )
    if plan.other_changes:
        line += "; NOT written (differs on recompute): " + ", ".join(
            f"{c}x{n}" for c, n in sorted(plan.other_changes.items())
        )
    return line


def summarize(plans: list[DatePlan]) -> list[str]:
    """Totals: dates per status, which columns would change on how many dates."""
    status = Counter(p.status for p in plans)
    ewt_dates: Counter = Counter()
    other_dates: Counter = Counter()
    for p in plans:
        if p.status != "write":
            continue
        for c in p.ewt_changes:
            ewt_dates[c] += 1
        for c in p.other_changes:
            other_dates[c] += 1
    lines = [
        f"dates: {len(plans)} (write {status['write']}, refused {status['refused']}, "
        f"failed {status['failed']})",
        "EWT columns that change (dates): "
        + (", ".join(f"{c}={n}" for c, n in sorted(ewt_dates.items())) or "none"),
        "other columns that differ on recompute, NOT written (dates): "
        + (", ".join(f"{c}={n}" for c, n in sorted(other_dates.items())) or "none"),
        "data_quality / coverage_pct: never written by this tool",
    ]
    lines += [format_plan(p) for p in plans if p.status in ("refused", "failed")]
    return lines


def main() -> int:
    """CLI: dry-run by default; ``--apply`` writes the EWT columns of every ``write`` date."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--agency", default="wmata", help="config/agencies/<agency>.yaml")
    parser.add_argument("--start", type=_parse_date, default=None, help="first date (inclusive)")
    parser.add_argument("--end", type=_parse_date, default=None, help="last date (inclusive)")
    parser.add_argument(
        "--exclude-date",
        action="append",
        type=_parse_date,
        default=[],
        help="leave this date untouched (repeatable)",
    )
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()

    if args.apply and utcnow_naive() < EWT_LONG_GAP_RULE_SINCE_UTC:
        print("refusing --apply before EWT_LONG_GAP_RULE_SINCE_UTC", file=sys.stderr)
        return 1

    cfg = load_agency_config(args.agency)
    db = get_db(cfg)
    try:
        mode = "APPLY" if args.apply else "DRY RUN (nothing written)"
        print(f"recompute_ewt_aggregates {args.agency}: {mode}", flush=True)
        failed = False

        def _progress(plan: DatePlan) -> None:
            """Print each date as it is planned; write it immediately under --apply."""
            nonlocal failed
            applied = False
            if args.apply and plan.status == "write":
                try:
                    apply_plan(db, plan)
                    applied = True
                except Exception as exc:  # noqa: BLE001 — reported, run continues
                    plan.status, plan.reason = "failed", f"write: {type(exc).__name__}: {exc}"
            failed = failed or plan.status == "failed"
            print(format_plan(plan, applied=applied), flush=True)

        plans = plan_recompute(
            db, cfg, args.start, args.end, frozenset(args.exclude_date), progress=_progress
        )
        print("", flush=True)
        for line in summarize(plans):
            print(line, flush=True)
        return 1 if failed else 0
    finally:
        db.close()


def get_db(cfg: AgencyConfig) -> Session:
    """Open a session on the agency's own database."""
    from src.database import get_session

    return get_session(db_url=resolve_agency_db_url(cfg))


if __name__ == "__main__":
    sys.exit(main())
