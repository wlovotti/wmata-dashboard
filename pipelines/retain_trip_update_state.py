"""Retention cron for ``trip_update_state`` (successor to archive_trip_update_snapshots).

Why this exists
---------------
``trip_update_state`` is a stateful table — one row per
``(trip_id, stop_sequence, service_date)`` — updated by the continuous
collector. Unlike the old append-only ``trip_update_snapshots`` table it
replaced, this table is already bounded in volume: one row per stop per trip
per day rather than one row per feed tick. It does not need archive-then-DELETE
semantics; a simple DELETE of old rows is safe.

The derivation pipeline (``pipelines/derive_stop_events_from_state.py``) reads
rows for a given ``service_date`` and then sets ``derived_at`` on those rows.
After the retention window elapses, those rows have either been derived (and
their contribution materialized in ``stop_events``) or never matched a service
date (stale collector noise). Either way, the raw state rows are safe to drop.

The existing ``pipelines/cleanup_trip_update_state.py`` handles the normal
lifecycle (derived rows > 2 days old, un-derived rows > 7 days old) **as part
of the nightly batch run** via ``run_daily_batch.py``. This script is the
**standalone retention counterpart** — a long-window guard intended to be
scheduled as an independent timer, separate from the batch. It ensures the
table stays bounded even if the nightly batch is paused or misses a day.

Retention window
----------------
14 days — matches the window used by the retired
``archive_trip_update_snapshots.py`` (its default ``--retention-days 14``).
``trip_update_state`` has far lower volume than the old snapshot table
(~1 row/stop/trip/day vs ~20 M rows/day), so 14 days of state is trivially
small on disk (~a few hundred thousand rows) and gives ample audit runway.

Usage
-----
    uv run python pipelines/retain_trip_update_state.py
    uv run python pipelines/retain_trip_update_state.py --days 30
    uv run python pipelines/retain_trip_update_state.py --dry-run

This script is invoked by the launchd timer
``scripts/launchd/com.wmata-dashboard.retain-trip-update-state.plist``.
Do NOT import and call it directly from ``run_daily_batch.py`` — the nightly
batch already runs ``cleanup_trip_update_state`` for the short-window lifecycle
management.
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


def run_retention(db: Session, retention_days: int = 14) -> dict[str, int]:
    """Delete ``trip_update_state`` rows with ``service_date`` older than ``retention_days``.

    Args:
        db: Active SQLAlchemy session. Caller is responsible for committing or
            rolling back after this returns.
        retention_days: Number of Eastern days of state to keep. The cutoff is
            ``eastern_today() - retention_days`` (exclusive — rows *on* the
            cutoff day are retained). Default 14.

    Returns:
        ``{"deleted": <row_count>}`` — the number of rows removed.
    """
    cutoff = eastern_today() - timedelta(days=retention_days)
    stmt = delete(TripUpdateState).where(TripUpdateState.service_date < cutoff)
    result = db.execute(stmt)
    return {"deleted": result.rowcount or 0}


def main() -> int:
    """CLI entry point for the standalone retention cron.

    Parses ``--days`` and ``--dry-run`` from argv, opens a DB session,
    calls :func:`run_retention`, then commits (or rolls back on dry-run).

    Returns:
        Exit code 0 on success, 1 on unexpected error.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Delete aged trip_update_state rows. "
            "Standalone timer-invoked retention guard (complement to "
            "cleanup_trip_update_state which runs inside the nightly batch)."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of Eastern days of state to retain (default: 14).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute counts without deleting (rolls back the transaction).",
    )
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        counts = run_retention(db, retention_days=args.days)
        if args.dry_run:
            db.rollback()
            print(f"DRY-RUN: would delete {counts}")
        else:
            db.commit()
            print(f"Retention complete: {counts}")
    except Exception as exc:
        db.rollback()
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
