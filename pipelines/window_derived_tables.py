"""Tier-2 retention: 365-day rolling window for `stop_events` and `runs` (NOTES-48 §3.5).

These are the granular *derived* tables. Every metric is computed from them and
the answer lands in a tiny tier-1 rollup, so the granular rows are intermediate,
not authoritative — and they are recoverable by re-derivation from the raw
archives (`replay_archive_to_state.py` rebuilds `trip_update_state`; the tier-3
positions parquet restores `vehicle_positions`; `derive_stop_events*` then
regenerates them). A nightly job deletes rows whose Eastern `service_date` is
older than a 365-day window — generous on purpose during active metric
development; tighten toward 90 days once the metric set stabilizes.

Why a STRING comparison: `stop_events.service_date` and `runs.service_date` are
`String` columns in `YYYY-MM-DD` form (NOT `Date`, unlike `trip_update_state`).
Zero-padded ISO dates sort lexicographically == chronologically, so comparing
against `(eastern_today() - retention_days).isoformat()` is correct and keeps
the DELETE pure-ORM (and SQLite-portable for tests).

Usage:
    uv run python pipelines/window_derived_tables.py
    uv run python pipelines/window_derived_tables.py --retention-days 365
    uv run python pipelines/window_derived_tables.py --dry-run
"""

import argparse
import sys
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import Run, StopEvent
from src.timezones import eastern_today

# Tables windowed by this job. Both carry a STRING `service_date`. There is no
# FK between them, so deletion order is not load-bearing — listed stop_events
# first only for log readability.
WINDOWED_MODELS = (StopEvent, Run)

DEFAULT_RETENTION_DAYS = 365


def compute_cutoff_str(retention_days: int = DEFAULT_RETENTION_DAYS) -> str:
    """Return the inclusive cutoff as an ISO `YYYY-MM-DD` string.

    Rows with `service_date < cutoff` are expired. The cutoff is
    `eastern_today() - retention_days`, formatted via `date.isoformat()` so it
    compares correctly against the string `service_date` columns.
    """
    return (eastern_today() - timedelta(days=retention_days)).isoformat()


def run_window(db: Session, retention_days: int = DEFAULT_RETENTION_DAYS) -> dict[str, int]:
    """Delete `stop_events` / `runs` rows older than the retention window.

    Args:
        db: Active SQLAlchemy session. The caller is responsible for committing
            or rolling back after this returns.
        retention_days: Eastern days of granular derived data to retain.
            Default 365 (spec §3.5).

    Returns:
        Per-table deleted-row counts keyed by table name, e.g.
        ``{"stop_events": 1234, "runs": 56}``.
    """
    cutoff = compute_cutoff_str(retention_days)
    deleted: dict[str, int] = {}
    for model in WINDOWED_MODELS:
        result = db.execute(delete(model).where(model.service_date < cutoff))
        deleted[model.__tablename__] = result.rowcount or 0
    return deleted


def main() -> int:
    """CLI entry point: parse args, open a session, run the window, commit."""
    parser = argparse.ArgumentParser(
        description="Delete stop_events/runs rows older than the 365-day retention window."
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Eastern days of granular derived data to retain (default: {DEFAULT_RETENTION_DAYS}).",
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
        counts = run_window(db, retention_days=args.retention_days)
        if args.dry_run:
            db.rollback()
            print(f"DRY-RUN: would delete {counts}")
        else:
            db.commit()
            print(f"Window cleanup complete: {counts}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
