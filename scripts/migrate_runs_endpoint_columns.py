"""
Add the four endpoint-related columns to an existing `runs` table.

Adds: sched_first_seq, sched_last_seq, origin_dev_sec, destination_dev_sec.
All nullable, no defaults — re-running `pipelines/aggregate_runs.py`
backfills them.

Idempotent: uses ADD COLUMN IF NOT EXISTS (Postgres 9.6+). Safe to re-run.
Does not touch any other table.

Usage:
  uv run python scripts/migrate_runs_endpoint_columns.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

NEW_COLUMNS = [
    ("sched_first_seq", "INTEGER"),
    ("sched_last_seq", "INTEGER"),
    ("origin_dev_sec", "INTEGER"),
    ("destination_dev_sec", "INTEGER"),
]


def main():
    """Add the four endpoint-related columns if missing; report status."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    if "runs" not in inspector.get_table_names():
        raise RuntimeError("runs table does not exist — run scripts/create_runs_table.py first")

    existing = {c["name"] for c in inspector.get_columns("runs")}
    with engine.begin() as conn:
        for name, sql_type in NEW_COLUMNS:
            if name in existing:
                print(f"  {name}: already present, skipped.")
                continue
            conn.execute(text(f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {name} {sql_type}"))
            print(f"  {name}: added.")

    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns("runs")}
    missing = [name for name, _ in NEW_COLUMNS if name not in cols]
    if missing:
        raise RuntimeError(f"columns still missing after ALTER: {missing}")
    print(f"runs table now has {len(cols)} columns.")


if __name__ == "__main__":
    main()
