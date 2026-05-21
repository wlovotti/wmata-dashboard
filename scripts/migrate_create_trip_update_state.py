"""Create the ``trip_update_state`` table and its indexes.

Idempotent: ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``,
so re-running is safe.

Usage:
    uv run python scripts/migrate_create_trip_update_state.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trip_update_state (
    trip_id                     VARCHAR    NOT NULL,
    stop_sequence               INTEGER    NOT NULL,
    service_date                DATE       NOT NULL,
    stop_id                     VARCHAR    NOT NULL,
    vehicle_id                  VARCHAR,
    final_snapshot_ts           TIMESTAMP  NOT NULL,
    final_schedule_relationship VARCHAR,
    last_pred_snapshot_ts       TIMESTAMP,
    last_predicted_arrival_ts   TIMESTAMP,
    derived_at                  TIMESTAMP,
    PRIMARY KEY (trip_id, stop_sequence, service_date)
);
"""

CREATE_INDEX_FINAL_SNAPSHOT_TS = """
CREATE INDEX IF NOT EXISTS idx_tus_final_snapshot_ts
    ON trip_update_state (final_snapshot_ts);
"""

CREATE_INDEX_TRIP_ID = """
CREATE INDEX IF NOT EXISTS idx_tus_trip_id
    ON trip_update_state (trip_id);
"""

CREATE_INDEX_SERVICE_DATE = """
CREATE INDEX IF NOT EXISTS idx_tus_service_date
    ON trip_update_state (service_date);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run.

    Fresh-install schema includes ``service_date`` in the PK from the
    start (see 2026-05-20 spec addendum). The companion migration
    ``migrate_add_service_date_to_state.py`` covers the case where an
    older DB had the original 2-column PK; on a freshly initialized DB,
    that companion script is a no-op because the column already exists.
    """
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text(CREATE_INDEX_FINAL_SNAPSHOT_TS))
        conn.execute(text(CREATE_INDEX_TRIP_ID))
        conn.execute(text(CREATE_INDEX_SERVICE_DATE))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    engine = get_engine()
    print("Creating trip_update_state table + indexes...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
