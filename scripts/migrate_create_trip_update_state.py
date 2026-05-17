"""Create the ``trip_update_state`` table and its indexes.

Idempotent: ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``,
so re-running is safe.

Usage:
    uv run python scripts/migrate_create_trip_update_state.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trip_update_state (
    trip_id                     VARCHAR    NOT NULL,
    stop_sequence               INTEGER    NOT NULL,
    stop_id                     VARCHAR    NOT NULL,
    vehicle_id                  VARCHAR,
    final_snapshot_ts           TIMESTAMP  NOT NULL,
    final_schedule_relationship VARCHAR,
    last_pred_snapshot_ts       TIMESTAMP,
    last_predicted_arrival_ts   TIMESTAMP,
    derived_at                  TIMESTAMP,
    PRIMARY KEY (trip_id, stop_sequence)
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

_DDL_STATEMENTS = [CREATE_TABLE_SQL, CREATE_INDEX_FINAL_SNAPSHOT_TS, CREATE_INDEX_TRIP_ID]


def run_migration(engine) -> None:
    """Apply the migration.  Safe to re-run.

    Accepts either a SQLAlchemy ``Engine`` or an already-open ``Connection``.
    When a ``Connection`` is passed (e.g. from a test fixture that wraps the
    connection in an outer transaction), the DDL is executed directly on it
    rather than opening a nested transaction via ``engine.begin()``.
    """
    if isinstance(engine, Connection):
        for stmt in _DDL_STATEMENTS:
            engine.execute(text(stmt))
    else:
        with engine.begin() as conn:
            for stmt in _DDL_STATEMENTS:
                conn.execute(text(stmt))


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
