"""Cleanup cron for trip_update_state.

Runs daily. Single rule: delete rows whose ``service_date`` is older
than the retention window (default 7 days). With ``service_date`` in
the PK, every row carries its own day-of-data attribution, so cleanup
no longer needs to reason about ``derived_at`` or a safety-net pass.

The ``derived_at`` column is preserved as a per-row diagnostic ("did
derivation run for this row?") but is no longer load-bearing for
cleanup.

Usage:
    uv run python pipelines/cleanup_trip_update_state.py
    uv run python pipelines/cleanup_trip_update_state.py --retention-days 14
    uv run python pipelines/cleanup_trip_update_state.py --dry-run
"""

import argparse
import sys
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import TripUpdateState
from src.timezones import eastern_today


def run_cleanup(db: Session, retention_days: int = 7) -> dict[str, int]:
    """Delete rows with ``service_date`` older than ``retention_days`` ago.

    Args:
        db: Active SQLAlchemy session. Caller is responsible for
            committing or rolling back after this returns.
        retention_days: How many Eastern days of state to keep. Days are
            counted from today (inclusive); the cutoff is
            ``eastern_today() - retention_days``. Default 7.

    Returns:
        ``{"deleted": <row_count>}`` — the number of rows removed.
    """
    cutoff = eastern_today() - timedelta(days=retention_days)
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
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        counts = run_cleanup(db, retention_days=args.retention_days)
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
