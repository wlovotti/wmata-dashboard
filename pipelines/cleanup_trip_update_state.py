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
unbounded. ``bin/pull-and-derive.sh`` invokes ``--agency sfmta`` as its
own step, gated on that same run's SFMTA derive having actually run
and succeeded (the SFMTA trip_update_state retention hookup, PR #229)
-- but that script is manual-cadence (run on demand, not scheduled), so
retention is bounded by operator habit rather than a fixed schedule. An
ad hoc non-default agency added later still needs its own invocation.

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
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.orm import Session

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import TripUpdateState
from src.timezones import local_today

# Rows deleted per DELETE statement / commit. A single unbatched DELETE
# is sized for a daily WMATA increment, not a multi-million-row backlog
# (SFMTA's first run under the PR #229 retention hookup deletes ~10.7M of
# ~13M rows on a 2.7GB table in one shot without this) -- batching keeps
# each statement's lock and transaction size bounded regardless of how
# large the eligible set is.
DEFAULT_BATCH_SIZE = 50_000


def run_cleanup(
    db: Session,
    retention_days: int = 7,
    tz_name: str = "America/New_York",
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Delete rows with ``service_date`` older than ``retention_days`` ago.

    Deletes run in batches of ``batch_size`` rows, each its own statement
    and commit, rather than one unbatched ``DELETE`` -- see
    ``DEFAULT_BATCH_SIZE`` above for why. Batches are selected by primary
    key (``trip_id``, ``stop_sequence``, ``service_date``) rather than a
    backend-specific physical row id, so the same code path works against
    both Postgres (production/dev) and the in-memory SQLite used by
    non-integration tests.

    Args:
        db: Active SQLAlchemy session. For a real (non-dry-run) cleanup,
            this function commits after each batch itself -- the caller
            does not need to commit afterward. For ``dry_run``, nothing
            is deleted or committed, so there is nothing for the caller
            to roll back either.
        retention_days: How many local days of state to keep. Days are
            counted from today (inclusive); the cutoff is
            ``local_today(tz_name) - retention_days``. Default 7.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site) used to resolve
            "today" for the cutoff.
        dry_run: If True, only counts rows past the cutoff -- no deletes,
            no commits. Still reports the full eligible count.
        batch_size: Rows deleted per batch. Default ``DEFAULT_BATCH_SIZE``.

    Returns:
        ``{"deleted": <row_count>}`` — the number of rows removed (or,
        for ``dry_run``, the number that would be removed).
    """
    cutoff = local_today(tz_name) - timedelta(days=retention_days)

    if dry_run:
        count_stmt = (
            select(func.count())
            .select_from(TripUpdateState)
            .where(TripUpdateState.service_date < cutoff)
        )
        return {"deleted": db.execute(count_stmt).scalar_one()}

    pk_cols = (
        TripUpdateState.trip_id,
        TripUpdateState.stop_sequence,
        TripUpdateState.service_date,
    )
    # DELETE ... WHERE (pk) IN (SELECT pk FROM ... WHERE <predicate> LIMIT
    # batch_size) -- a subquery, not a materialized Python list of PK
    # tuples. Passing tens of thousands of literal tuples as bind
    # parameters (the first version of this batching) hit Postgres's
    # parser/planner stack depth limit at batch_size=50_000
    # ("StatementTooComplex: stack depth limit exceeded"); a subquery
    # keeps the statement's parameter count constant regardless of
    # batch_size.
    batch_subq = select(*pk_cols).where(TripUpdateState.service_date < cutoff).limit(batch_size)
    batch_stmt = delete(TripUpdateState).where(tuple_(*pk_cols).in_(batch_subq))
    total_deleted = 0
    while True:
        result = db.execute(batch_stmt)
        deleted = result.rowcount or 0
        if deleted == 0:
            break
        db.commit()
        total_deleted += deleted
        print(
            f"cleanup_trip_update_state: deleted batch of {deleted} rows "
            f"(running total {total_deleted})"
        )
        if deleted < batch_size:
            break
    return {"deleted": total_deleted}


def main() -> int:
    """CLI entry point for the cleanup cron.

    Parses ``--retention-days`` and ``--dry-run`` from argv, opens a DB
    session, and calls :func:`run_cleanup`. ``run_cleanup`` itself
    commits per batch (or, for ``--dry-run``, only counts and never
    touches the transaction) -- there is nothing left for this function
    to commit or roll back afterward.

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
        counts = run_cleanup(
            db,
            retention_days=args.retention_days,
            tz_name=cfg.timezone,
            dry_run=args.dry_run,
        )
        if args.dry_run:
            print(f"DRY-RUN: would delete {counts}")
        else:
            print(f"Cleanup complete: {counts}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
