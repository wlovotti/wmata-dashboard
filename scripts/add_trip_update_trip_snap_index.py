"""
Add `idx_tu_trip_snap` to `trip_update_snapshots` — a (trip_id, snapshot_ts)
index that fits the lazy/live derivation access pattern.

`pipelines/derive_stop_events_trip_updates.py` filters with
`WHERE trip_id IN (~100-200 ids) AND snapshot_ts BETWEEN x AND y`. The
existing `idx_tu_trip_stop_snap (trip_id, stop_id, snapshot_ts)` is unused
for this shape because `stop_id` sits between the two filtered columns;
without it, the planner falls back to scanning ~54M rows in the
`snapshot_ts` index and hash-joining against the trip set. Adding
`(trip_id, snapshot_ts)` lets the planner do ~95-220 short trip-id-keyed
lookups instead. On a busy route (217 active trips, 1.06M snapshots in
the window) the SQL drops from ~9 s to ~5 s; on the median route (95
trips, 408k snapshots) it drops from ~9 s to ~1.9 s.

Uses CREATE INDEX CONCURRENTLY because the table is ~140M rows / 15 GB.
Concurrent build holds only a SHARE UPDATE EXCLUSIVE lock and lets the
collector keep INSERTing during the build. Concurrent index builds need
their own transaction — autocommit must be on. Build can take ~5-10 min.

The pipeline still nudges the planner per-transaction with
`SET LOCAL random_page_cost = 1.1` so the new index wins the cost battle
against the bulk snapshot_ts scan. That's a code-side change; this
migration only adds the index.

Idempotent: skips if the index already exists. Safe to re-run.

Usage:
  uv run python scripts/add_trip_update_trip_snap_index.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

INDEX_NAME = "idx_tu_trip_snap"
TABLE_NAME = "trip_update_snapshots"
INDEX_DDL = (
    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} ON {TABLE_NAME} (trip_id, snapshot_ts)"
)


def main() -> None:
    """Add the (trip_id, snapshot_ts) index if missing; skip otherwise."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    if TABLE_NAME not in inspector.get_table_names():
        raise RuntimeError(f"{TABLE_NAME} does not exist")

    # SQLAlchemy's inspector.get_indexes() lists all indexes regardless of
    # validity. An INVALID concurrent build leaves a name reservation behind;
    # we want to detect that case and warn rather than silently skip.
    with engine.connect() as conn:
        invalid = conn.execute(
            text(
                "SELECT c.relname "
                "FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid "
                "WHERE c.relname = :name AND NOT i.indisvalid"
            ),
            {"name": INDEX_NAME},
        ).first()
        if invalid:
            raise RuntimeError(
                f"  {INDEX_NAME}: exists but is INVALID — a previous CREATE INDEX "
                "CONCURRENTLY failed mid-build. Drop it (or REINDEX CONCURRENTLY) "
                "and re-run this script."
            )

    existing = {ix["name"] for ix in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME in existing:
        print(f"  {INDEX_NAME}: already present, skipped.")
        return

    # CONCURRENTLY requires its own transaction with autocommit. SQLAlchemy
    # opens a transaction by default, so use AUTOCOMMIT isolation explicitly.
    print(f"  Building {INDEX_NAME} CONCURRENTLY (this may take several minutes)...")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(INDEX_DDL))

    inspector = inspect(engine)
    after = {ix["name"] for ix in inspector.get_indexes(TABLE_NAME)}
    if INDEX_NAME not in after:
        raise RuntimeError(f"{INDEX_NAME} build did not complete")
    print(f"  {INDEX_NAME}: created.")


if __name__ == "__main__":
    main()
