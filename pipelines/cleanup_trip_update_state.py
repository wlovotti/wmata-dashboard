"""Cleanup cron for trip_update_state.

Runs daily. Single rule: delete rows whose ``service_date`` is older
than the retention window (default 7 days). With ``service_date`` in
the PK, every row carries its own day-of-data attribution, so cleanup
no longer needs to reason about ``derived_at`` or a safety-net pass.

The ``derived_at`` column is preserved as a per-row diagnostic ("did
derivation run for this row?") but is no longer load-bearing for
cleanup.

Multi-agency (NOTES-100): pass ``--agency`` (matching a
``config/agencies/<agency>.yaml``) to clean up a non-WMATA database
with the correct retention-cutoff timezone. This is REQUIRED for a
non-default agency, not optional housekeeping polish:
``run_daily_batch.py`` skips this pipeline entirely for `--agency
sfmta` (it isn't wired into the per-date orchestration), so it must be
run separately with the matching `--agency` — running it bare
(default `wmata`) would prune `DATABASE_URL` (the WMATA table), not
the SFMTA one, leaving `sfmta_dashboard.trip_update_state` to grow
unbounded. ``bin/pull-and-derive.sh`` covers this by invoking
``--agency sfmta`` directly as its own step after the SFMTA derive
call (the SFMTA trip_update_state retention hookup, PR #229); an ad
hoc non-default agency added later still needs its own invocation.

Usage:
    uv run python pipelines/cleanup_trip_update_state.py
    uv run python pipelines/cleanup_trip_update_state.py --retention-days 14
    uv run python pipelines/cleanup_trip_update_state.py --dry-run
    uv run python pipelines/cleanup_trip_update_state.py --agency sfmta
"""

import argparse
import sys
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import TripUpdateState
from src.timezones import local_today


def run_cleanup(
    db: Session, retention_days: int = 7, tz_name: str = "America/New_York"
) -> dict[str, int]:
    """Delete rows with ``service_date`` older than ``retention_days`` ago.

    Args:
        db: Active SQLAlchemy session. Caller is responsible for
            committing or rolling back after this returns.
        retention_days: How many local days of state to keep. Days are
            counted from today (inclusive); the cutoff is
            ``local_today(tz_name) - retention_days``. Default 7.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site) used to resolve
            "today" for the cutoff.

    Returns:
        ``{"deleted": <row_count>}`` — the number of rows removed.
    """
    cutoff = local_today(tz_name) - timedelta(days=retention_days)
    stmt = delete(TripUpdateState).where(TripUpdateState.service_date < cutoff)
    result = db.execute(stmt)
    return {"deleted": result.rowcount or 0}


def main() -> int:
    """CLI entry point for the cleanup cron.

    Parses ``--retention-days`` and ``--dry-run`` from argv, opens a DB
    session, calls :func:`run_cleanup`, then commits (or rolls back on
    dry-run).

    Returns:
        Exit code 0 on success.
    """
    parser = argparse.ArgumentParser(description="Delete aged trip_update_state rows.")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Number of Eastern days of state to retain (default: 7).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute counts without deleting (rolls back).",
    )
    parser.add_argument(
        "--agency",
        default="wmata",
        help=(
            "Agency name matching config/agencies/<agency>.yaml (default: "
            "'wmata'). Selects the retention-cutoff timezone and the "
            "target database. REQUIRED for a non-default agency -- see "
            "module docstring."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    cfg = load_agency_config(args.agency)
    db = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        counts = run_cleanup(db, retention_days=args.retention_days, tz_name=cfg.timezone)
        if args.dry_run:
            db.rollback()
            print(f"DRY-RUN: would delete {counts}")
        else:
            db.commit()
            print(f"Cleanup complete: {counts}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
