"""Create stop_events_v2 as a structural clone of stop_events.

Used during Phase D side-by-side validation: the new derivation pipeline
writes here, the comparison script diffs it against the production
``stop_events``. Dropped after Phase E cutover.

The side table needs a UNIQUE constraint with a v2-specific name so
the new derivation's UPSERT can target it via ON CONFLICT ON CONSTRAINT
without colliding with the original constraint name.

Usage:
    uv run python scripts/migrate_create_stop_events_v2.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine

CLONE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stop_events_v2
    (LIKE stop_events INCLUDING DEFAULTS);
"""

ADD_CONSTRAINT_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_stop_events_v2_run_stop_source'
    ) THEN
        ALTER TABLE stop_events_v2
            ADD CONSTRAINT uq_stop_events_v2_run_stop_source
            UNIQUE (service_date, trip_id, stop_sequence, source);
    END IF;
END $$;
"""


def run_migration(engine) -> None:
    """Apply the migration. Idempotent — safe to re-run."""
    with engine.begin() as conn:
        conn.execute(text(CLONE_TABLE_SQL))
        conn.execute(text(ADD_CONSTRAINT_SQL))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    run_migration(get_engine())
    print("stop_events_v2 ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
