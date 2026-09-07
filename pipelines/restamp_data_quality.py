"""
Re-stamp ``data_quality`` / ``coverage_pct`` on already-materialized dates.

``system_metrics_daily`` and ``route_metrics_daily_overlay`` carry a
per-date completeness stamp written at materialization time by
``src.data_completeness``. When the completeness *signal* changes (as
NOTES-104 did, adding ``trip_update_state`` poll timestamps to the
numerator so replayed / archive-loaded dates can certify complete), the
stamps on history go stale even though the metric values themselves are
unchanged. This pipeline recomputes only the stamp for a date range and
rewrites those two columns in place -- it does not recompute any metric,
so it is cheap (seconds for a 40-date window) and cannot drift a value.

Dry-run by default: prints one line per date with the current and
proposed stamp and the number of overlay rows affected, and writes
nothing. ``--apply`` performs the update inside a single transaction
across the whole range; any failure rolls the entire range back.

Usage::

    uv run python pipelines/restamp_data_quality.py --agency sfmta \\
        --start 2026-07-22 --end 2026-08-30            # dry-run
    uv run python pipelines/restamp_data_quality.py --agency sfmta \\
        --start 2026-07-22 --end 2026-08-30 --apply    # write

Dates with no ``system_metrics_daily`` row are reported as ``absent`` and
skipped -- this tool never creates rows; use the per-date upsert
pipelines for dates that were never materialized.
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy import update
from sqlalchemy.orm import Session

from src.agency_config import AgencyConfig, load_agency_config, resolve_agency_db_url
from src.data_completeness import agency_coverage_threshold, coverage_pct_for_date
from src.database import get_session
from src.models import RouteMetricsDailyOverlay, SystemMetricsDaily


@dataclass(frozen=True)
class RestampRow:
    """One date's current vs. proposed completeness stamp."""

    service_date: date
    old_quality: str | None
    old_coverage: float | None
    new_quality: str
    new_coverage: float
    overlay_rows: int

    @property
    def absent(self) -> bool:
        """True when the date has no ``system_metrics_daily`` row to re-stamp."""
        return self.old_quality is None

    @property
    def changed(self) -> bool:
        """True when applying would flip ``data_quality`` for this date."""
        return not self.absent and self.old_quality != self.new_quality


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD into a date; argparse hands the raw string in."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def plan_restamp(db: Session, cfg: AgencyConfig, start: date, end: date) -> list[RestampRow]:
    """Compute the proposed stamp for every date in ``[start, end]`` without writing.

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
        existing = db.get(SystemMetricsDaily, iso)
        overlay_count = (
            db.query(RouteMetricsDailyOverlay)
            .filter(RouteMetricsDailyOverlay.service_date == iso)
            .count()
        )
        pct = coverage_pct_for_date(db, d, tz_name=cfg.timezone)
        rows.append(
            RestampRow(
                service_date=d,
                old_quality=existing.data_quality if existing else None,
                old_coverage=existing.coverage_pct if existing else None,
                new_quality="complete" if pct >= threshold else "partial",
                new_coverage=pct,
                overlay_rows=overlay_count,
            )
        )
        d += timedelta(days=1)
    return rows


def apply_restamp(db: Session, rows: list[RestampRow]) -> int:
    """Write the proposed stamps for every non-absent row in one transaction.

    Updates ``data_quality`` and ``coverage_pct`` on ``system_metrics_daily``
    (one row per date) and ``route_metrics_daily_overlay`` (every route row
    for the date). Commits once at the end; rolls everything back on error.

    Args:
        db: Session bound to the target database.
        rows: Output of :func:`plan_restamp`.

    Returns:
        Number of dates written.
    """
    written = 0
    try:
        for row in rows:
            if row.absent:
                continue
            iso = row.service_date.isoformat()
            values = {"data_quality": row.new_quality, "coverage_pct": row.new_coverage}
            db.execute(
                update(SystemMetricsDaily)
                .where(SystemMetricsDaily.service_date == iso)
                .values(**values)
            )
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
        old = "absent"
    else:
        old = (
            f"{row.old_quality} {row.old_coverage:.3f}"
            if row.old_coverage is not None
            else row.old_quality
        )
    flag = "  FLIP" if row.changed else ""
    return (
        f"{row.service_date}  old={old:<17} new={row.new_quality} {row.new_coverage:.3f}"
        f"  overlay_rows={row.overlay_rows}{flag}"
    )


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
        absent = sum(1 for r in rows if r.absent)
        print(
            f"agency={cfg.name} threshold={agency_coverage_threshold(cfg):.3f} "
            f"dates={len(rows)} flips={flips} absent={absent}"
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
