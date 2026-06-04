"""Drop the retired Phase F tables: ``trip_update_snapshots`` and (if present) ``stop_events_v2``.

IMPORTANT — THIS IS A DESTRUCTIVE MANUAL ACTION
================================================
This script is NOT run automatically by any pipeline or timer. It must be
invoked explicitly by the operator after verifying that the retirement PR
(NOTES-72 Phase F) has been merged and the production system has been running
cleanly on the new path for at least one week.

Both table drops are IRREVERSIBLE. Before running:

  1. Confirm ``trip_update_snapshots`` has no new rows being written — the
     collector's snapshot dual-write was stopped in Phase E.2 (PR #151).
     A quick check: ``SELECT MAX(snapshot_ts) FROM trip_update_snapshots;``
     should show the last row is weeks old.

  2. Optionally run ``archive_trip_update_snapshots`` one final time (or
     verify the archive directory is complete) if you want the raw data
     preserved on disk before the table is dropped.

  3. Run this script with ``--yes`` to confirm you have read the above.
     Without ``--yes``, it prints the plan and exits without touching the DB.

Usage
-----
    # Preview (safe — no DB changes):
    uv run python scripts/migrate_drop_phase_f.py

    # Execute the drops (IRREVERSIBLE):
    uv run python scripts/migrate_drop_phase_f.py --yes
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

# Tables targeted by this migration, in drop order.
# stop_events_v2 is conditional — only dropped if it still exists (it is a
# side table used during Phase D validation, not a SQLAlchemy-modeled table).
PRIMARY_TABLE = "trip_update_snapshots"
CONDITIONAL_TABLE = "stop_events_v2"


def table_exists(engine, table_name: str) -> bool:
    """Return True if ``table_name`` exists in the current Postgres schema.

    Uses SQLAlchemy's reflection API rather than a raw query so the result
    is dialect-neutral for test runs on SQLite.
    """
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def run_migration(engine, confirm: bool = False) -> None:
    """Drop Phase F tables.

    Args:
        engine: SQLAlchemy engine connected to the target database.
        confirm: If False, print the plan and return without making changes.
            If True, execute the irreversible DROP statements.

    Raises:
        SystemExit: If ``confirm=False`` (plan-only mode) — exits with code 0
            after printing the plan so the caller sees the output.
    """
    primary_exists = table_exists(engine, PRIMARY_TABLE)
    conditional_exists = table_exists(engine, CONDITIONAL_TABLE)

    plan_lines = []
    if primary_exists:
        plan_lines.append(f"  DROP TABLE {PRIMARY_TABLE};  -- {PRIMARY_TABLE} exists")
    else:
        plan_lines.append(f"  (skip) {PRIMARY_TABLE} does not exist — already dropped")

    if conditional_exists:
        plan_lines.append(f"  DROP TABLE {CONDITIONAL_TABLE};  -- {CONDITIONAL_TABLE} exists")
    else:
        plan_lines.append(
            f"  (skip) {CONDITIONAL_TABLE} does not exist — never created or already dropped"
        )

    print("Phase F retirement — table drops:")
    for line in plan_lines:
        print(line)

    if not confirm:
        print("\nDRY-RUN: no changes made. Re-run with --yes to execute the irreversible drops.")
        sys.exit(0)

    print("\nExecuting drops (IRREVERSIBLE)...")
    with engine.begin() as conn:
        if primary_exists:
            conn.execute(text(f"DROP TABLE {PRIMARY_TABLE}"))
            print(f"  Dropped {PRIMARY_TABLE}.")
        if conditional_exists:
            conn.execute(text(f"DROP TABLE {CONDITIONAL_TABLE}"))
            print(f"  Dropped {CONDITIONAL_TABLE}.")

    print("Done. Both tables are gone.")


def main() -> int:
    """CLI entry point.

    Returns:
        0 on success (including plan-only mode — but plan-only exits via
        ``sys.exit(0)`` inside :func:`run_migration`).
        1 on unexpected error.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Drop retired Phase F tables: trip_update_snapshots "
            "and (if present) stop_events_v2. IRREVERSIBLE — requires --yes."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Confirm you have read the docstring and want to execute the "
            "irreversible DROP statements. Without this flag the script "
            "prints the plan and exits without touching the DB."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    engine = get_engine()

    try:
        run_migration(engine, confirm=args.yes)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
