"""Add service_date column to trip_update_state and rebuild the PK.

The original PK ``(trip_id, stop_sequence)`` lets each subsequent day's
snapshot overwrite the prior day's state, making historical re-derivation
impossible (WMATA's GTFS-RT trip_ids repeat day-over-day on scheduled
routes). Adding ``service_date`` to the PK preserves one row per
``(trip_id, stop_sequence, service_date)``.

Idempotent: every statement is conditional (``IF NOT EXISTS`` /
``IF EXISTS``). Safe to re-run.

Pre-requisite: stop the continuous_combined_collector before running.
The collector's TripUpdateState model in older code lacks the
service_date column and will fail to INSERT after this migration sets
NOT NULL. Restart the collector with the new code after migration
completes.

Usage:
    uv run python scripts/migrate_add_service_date_to_state.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine

ADD_COLUMN_SQL = """
ALTER TABLE trip_update_state
    ADD COLUMN IF NOT EXISTS service_date DATE;
"""

BACKFILL_SQL = """
UPDATE trip_update_state
SET service_date =
    (final_snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
WHERE service_date IS NULL;
"""

SET_NOT_NULL_SQL = """
ALTER TABLE trip_update_state
    ALTER COLUMN service_date SET NOT NULL;
"""

DROP_PK_SQL = """
ALTER TABLE trip_update_state DROP CONSTRAINT IF EXISTS trip_update_state_pkey;
"""

ADD_PK_SQL = """
ALTER TABLE trip_update_state
    ADD CONSTRAINT trip_update_state_pkey
    PRIMARY KEY (trip_id, stop_sequence, service_date);
"""

CREATE_SERVICE_DATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_tus_service_date
    ON trip_update_state (service_date);
"""


def run_migration(engine) -> None:
    """Apply the migration in a single transaction. Safe to re-run."""
    with engine.begin() as conn:
        conn.execute(text(ADD_COLUMN_SQL))
        conn.execute(text(BACKFILL_SQL))
        conn.execute(text(SET_NOT_NULL_SQL))
        conn.execute(text(DROP_PK_SQL))
        conn.execute(text(ADD_PK_SQL))
        conn.execute(text(CREATE_SERVICE_DATE_INDEX_SQL))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    engine = get_engine()
    print("Adding service_date column + rebuilding trip_update_state PK...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
