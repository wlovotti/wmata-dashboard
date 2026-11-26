"""
Re-stamp ``data_quality`` / ``coverage_pct`` on already-materialized dates.

``system_metrics_daily`` and ``route_metrics_daily_overlay`` carry a
per-date completeness stamp written at materialization time by
``src.data_completeness``. When the completeness *signal* changes (as
NOTES-104 did, adding ``trip_update_state`` poll timestamps to the
numerator so replayed / archive-loaded dates can certify complete), the
stamps on history go stale even though the metric values themselves are
unchanged. This pipeline re-evaluates only the stamp for a date range and
rewrites those two columns in place. It never touches a metric column.

The stamp is re-decided by ``src.data_completeness.assess_data_quality``
— coverage leg plus the per-feed gap leg (issue #270) — so what this tool
can change is bounded by which inputs are still in the database:

- It can **promote** ``partial`` → ``complete`` only when every leg is
  assessable (the date's ``trip_update_state`` rows AND its raw
  ``vehicle_positions`` UTC days are present) and all pass.
- It can **demote** ``complete`` → ``partial`` when an assessable leg
  fails — a real outage an earlier, weaker check missed (e.g. a feed gap
  longer than the agency's ``completeness.feed_gap_max_minutes``).
- It will NOT change a date whose inputs are pruned (raw VP and
  trip-update state are kept only ``retention.raw_days``, 7): the stored
  stamp is reported ``kept`` and nothing is written. That is why the
  2026-11 SFMTA corrections (10/21, 8/08, 8/09 → partial; 9/29 →
  complete) need an owner-run UPDATE instead (see the #270/#272 PR).

Only dates whose stamp FLIPS are written (issue #272): an unchanged
stamp's stored ``coverage_pct`` is never rewritten, and a flip writes the
new coverage only when it was actually re-measured with every input
present — so the tool can never replace a real stored coverage with the
~0 a pruned date measures. A wide ``--start`` over old history is
therefore safe, and every kept date is listed so the decision is visible.

Dry-run by default: prints one line per date with the current and
proposed stamp, the number of rows affected, and the decision's
disclosure (coverage, each feed's longest gap, the rule that made it
partial, any leg not assessed); writes nothing. ``--apply`` performs the
update inside a single transaction across the whole range; any failure
rolls the entire range back.

Usage::

    uv run python pipelines/restamp_data_quality.py --agency sfmta \\
        --start 2026-07-22 --end 2026-08-30            # dry-run
    uv run python pipelines/restamp_data_quality.py --agency sfmta \\
        --start 2026-07-22 --end 2026-08-30 --apply    # write

Dates with neither a ``system_metrics_daily`` row nor any overlay row
are reported as ``absent`` and skipped -- this tool never creates rows;
use the per-date upsert pipelines for dates that were never
materialized. A date with overlay rows but no system row (the system
pipeline failed while the overlay one succeeded) is still re-stamped on
the overlay side.

Cost: one coverage query plus two gap queries per non-absent date (well
under a few seconds per date on either agency's retained window).
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy import update
from sqlalchemy.orm import Session

from src.agency_config import AgencyConfig, load_agency_config, resolve_agency_db_url
from src.data_completeness import QualityDecision, agency_coverage_threshold, assess_data_quality
from src.database import get_session
from src.models import RouteMetricsDailyOverlay, SystemMetricsDaily


@dataclass(frozen=True)
class RestampRow:
    """One date's current vs. proposed completeness stamp."""

    service_date: date
    has_system_row: bool
    overlay_rows: int
    old_quality: str | None
    old_coverage: float | None
    new_quality: str | None
    new_coverage: float | None
    kept: bool
    coverage_measured: bool = False
    decision: QualityDecision | None = None

    @property
    def absent(self) -> bool:
        """True when the date has no row in either table."""
        return not self.has_system_row and self.overlay_rows == 0

    @property
    def changed(self) -> bool:
        """True when applying would flip ``data_quality`` for this date."""
        return not self.absent and self.old_quality != self.new_quality


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD into a date; argparse hands the raw string in."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def plan_restamp(db: Session, cfg: AgencyConfig, start: date, end: date) -> list[RestampRow]:
    """Compute the proposed stamp for every date in ``[start, end]`` without writing.

    The prior stamp comes from the ``system_metrics_daily`` row when there
    is one, else from any overlay row (all overlay rows for a date carry
    the same stamp). Absent dates skip the coverage query entirely.

    Args:
        db: Session bound to ``cfg``'s database.
        cfg: Agency config; supplies the timezone for the day window and
            the cadence-aware threshold.
        start: First service date (inclusive).
        end: Last service date (inclusive).

    Returns:
        One ``RestampRow`` per date, in order.
    """
    threshold = agency_coverage_threshold(cfg)
    rows: list[RestampRow] = []
    d = start
    while d <= end:
        iso = d.isoformat()
        system_row = db.get(SystemMetricsDaily, iso)
        overlay_q = db.query(RouteMetricsDailyOverlay).filter(
            RouteMetricsDailyOverlay.service_date == iso
        )
        overlay_count = overlay_q.count()
        # All overlay rows for a date carry the same stamp (the overlay
        # pipeline writes them in one pass); order for determinism anyway.
        prior_row = (
            system_row
            if system_row is not None
            else overlay_q.order_by(RouteMetricsDailyOverlay.route_id).first()
        )
        if prior_row is None:
            rows.append(RestampRow(d, False, 0, None, None, None, None, False))
        else:
            prior = (prior_row.data_quality, prior_row.coverage_pct)
            decision = assess_data_quality(
                db,
                d,
                threshold=threshold,
                tz_name=cfg.timezone,
                prior=prior,
                gap_policy=cfg.completeness,
            )
            rows.append(
                RestampRow(
                    service_date=d,
                    has_system_row=system_row is not None,
                    overlay_rows=overlay_count,
                    old_quality=prior[0],
                    old_coverage=prior[1],
                    new_quality=decision.data_quality,
                    new_coverage=decision.coverage_pct,
                    kept=decision.kept,
                    coverage_measured=decision.coverage_measured,
                    decision=decision,
                )
            )
        d += timedelta(days=1)
    return rows


def apply_restamp(db: Session, rows: list[RestampRow]) -> int:
    """Write the proposed stamps for every date whose stamp flips, in one transaction.

    Updates ``data_quality`` on ``system_metrics_daily`` (when the date has
    a row) and on every ``route_metrics_daily_overlay`` row for the date,
    plus ``coverage_pct`` when the new coverage was re-measured with every
    input present. Dates whose stamp does not change — including absent
    and ``kept`` dates — are skipped entirely (issue #272): their stored
    coverage (possibly NULL on rows that predate the column, or a real
    value measured before the inputs were pruned) must not be overwritten
    by a re-measurement that may be missing inputs. Commits once at the
    end; rolls everything back on error.

    Args:
        db: Session bound to the target database.
        rows: Output of :func:`plan_restamp`.

    Returns:
        Number of dates written.
    """
    written = 0
    try:
        for row in rows:
            if not row.changed:
                continue
            iso = row.service_date.isoformat()
            values = {"data_quality": row.new_quality}
            if row.coverage_measured:
                values["coverage_pct"] = row.new_coverage
            if row.has_system_row:
                db.execute(
                    update(SystemMetricsDaily)
                    .where(SystemMetricsDaily.service_date == iso)
                    .values(**values)
                )
            if row.overlay_rows:
                db.execute(
                    update(RouteMetricsDailyOverlay)
                    .where(RouteMetricsDailyOverlay.service_date == iso)
                    .values(**values)
                )
            written += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    return written


def _format_row(row: RestampRow) -> str:
    """Render one plan row for the console."""
    if row.absent:
        return f"{row.service_date}  absent (no system or overlay rows)"
    old_cov = f" {row.old_coverage:.3f}" if row.old_coverage is not None else " (null)"
    old = f"{row.old_quality}{old_cov}"
    new_cov = f" {row.new_coverage:.3f}" if row.new_coverage is not None else " (null)"
    tables = ("sys+" if row.has_system_row else "sys-") + f"overlay={row.overlay_rows}"
    if row.changed:
        flag = "  FLIP" + ("" if row.coverage_measured else " (stamp only; coverage kept)")
    elif row.kept:
        flag = "  kept (an input is no longer retained; no write)"
    else:
        flag = "  unchanged (no write)"
    line = f"{row.service_date}  old={old:<17} new={row.new_quality}{new_cov}  {tables}{flag}"
    if row.decision is not None:
        line += f"\n    {row.decision.describe()}"
        for note in row.decision.not_assessed:
            line += f"\n    not assessed: {note}"
    return line


def main() -> int:
    """CLI entry point; return 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--agency", default="wmata", help="Agency name (config/agencies/<agency>.yaml)"
    )
    parser.add_argument(
        "--start", type=_parse_date, required=True, help="First service date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=_parse_date, required=True, help="Last service date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Write the stamps (default: dry-run, print only)"
    )
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must not precede --start")

    cfg = load_agency_config(args.agency)
    db = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        rows = plan_restamp(db, cfg, args.start, args.end)
        for row in rows:
            print(_format_row(row))
        flips = sum(1 for r in rows if r.changed)
        kept = sum(1 for r in rows if r.kept)
        absent = sum(1 for r in rows if r.absent)
        print(
            f"agency={cfg.name} threshold={agency_coverage_threshold(cfg):.3f} "
            f"feed_gap_max={cfg.completeness.feed_gap_max_minutes}min "
            f"service_hours={cfg.completeness.service_start_hour:02d}-"
            f"{cfg.completeness.service_end_hour:02d} "
            f"dates={len(rows)} flips={flips} kept={kept} absent={absent}"
        )
        if not args.apply:
            print("dry-run: nothing written (pass --apply to write)")
            return 0
        written = apply_restamp(db, rows)
        print(f"applied: {written} date(s) re-stamped in one transaction")
        return 0
    except Exception as exc:  # noqa: BLE001 - surface any failure as exit 1 with context
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
