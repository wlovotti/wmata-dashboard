"""Create the ``collector_heartbeats`` table.

Idempotent: ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``,
so re-running is safe.

The ``collector_heartbeats`` table stores one row per tick of the
continuous combined collector (every 30 s). It replaces
``trip_update_snapshots.snapshot_ts`` as the primary minute-bucket
coverage signal in ``src/data_completeness.py`` after the Phase E.2
NOTES-72 collector cutover.

Usage:
    uv run python scripts/migrate_create_collector_heartbeats.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS collector_heartbeats (
    ts               TIMESTAMP  NOT NULL,
    collector_name   VARCHAR    NOT NULL DEFAULT 'combined',
    PRIMARY KEY (ts, collector_name)
);
"""

CREATE_INDEX_TS = """
CREATE INDEX IF NOT EXISTS idx_collector_heartbeats_ts
    ON collector_heartbeats (ts);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run.

    Creates the ``collector_heartbeats`` table with a composite primary
    key on ``(ts, collector_name)`` and an additional index on ``ts``
    alone to support the range-scan pattern used by
    ``src/data_completeness.py``.
    """
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text(CREATE_INDEX_TS))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    engine = get_engine()
    print("Creating collector_heartbeats table + indexes...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
