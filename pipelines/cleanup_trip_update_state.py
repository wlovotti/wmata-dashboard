"""Cleanup cron for trip_update_state.

Runs daily. Two passes:

1. Normal lifecycle: rows whose stop_events were materialized
   (``derived_at IS NOT NULL``) and that materialization happened more
   than 2 days ago.
2. Safety net: rows that were NEVER derived but whose final_snapshot_ts
   is older than 7 days. Catches un-derivable trips (e.g., trips with no
   ``vehicle_position`` to anchor service_date) so the table can't grow
   unbounded.

The 2-day window provides a re-derivation buffer without requiring the
parquet archive. Beyond that, re-derivation falls back to parquet.

Usage:
    uv run python pipelines/cleanup_trip_update_state.py
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
from src.timezones import utcnow_naive


def run_cleanup(db: Session) -> dict:
    """Run both cleanup passes against the given session.

    Pass 1 — Normal lifecycle: delete rows with ``derived_at`` older than
    2 days. These have already been materialized into ``stop_events``.

    Pass 2 — Safety net: delete rows whose ``final_snapshot_ts`` is older
    than 7 days regardless of ``derived_at``. Prevents unbounded growth
    from trips that can never be derived (e.g., missing vehicle anchor).

    Args:
        db: Active SQLAlchemy session. Caller is responsible for committing
            or rolling back after this returns.

    Returns:
        A dict with keys ``derived_deleted`` and ``safety_deleted``
        holding the row counts removed by each pass.
    """
    now = utcnow_naive()
    derived_cutoff = now - timedelta(days=2)
    safety_cutoff = now - timedelta(days=7)

    derived_stmt = delete(TripUpdateState).where(
        TripUpdateState.derived_at.is_not(None),
        TripUpdateState.derived_at < derived_cutoff,
    )
    derived_result = db.execute(derived_stmt)

    safety_stmt = delete(TripUpdateState).where(
        TripUpdateState.final_snapshot_ts < safety_cutoff,
    )
    safety_result = db.execute(safety_stmt)

    return {
        "derived_deleted": derived_result.rowcount or 0,
        "safety_deleted": safety_result.rowcount or 0,
    }


def main() -> int:
    """CLI entry point for the cleanup cron.

    Parses ``--dry-run`` from argv, opens a DB session, calls
    :func:`run_cleanup`, then commits (or rolls back on dry-run).

    Returns:
        Exit code 0 on success.
    """
    parser = argparse.ArgumentParser(description="Delete aged trip_update_state rows.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute counts without deleting (rolls back).",
    )
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        counts = run_cleanup(db)
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
