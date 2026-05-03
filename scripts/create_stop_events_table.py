"""
Create the stop_events table on an existing database.

Idempotent — Base.metadata.create_all only creates tables that do not yet
exist. Safe to re-run. Does not touch any other table.

Usage:
  uv run python scripts/create_stop_events_table.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import StopEvent


def main():
    """Create the stop_events table if it does not yet exist; report status."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existed = "stop_events" in inspector.get_table_names()

    StopEvent.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    if "stop_events" not in inspector.get_table_names():
        raise RuntimeError("stop_events table not present after create_all")

    if existed:
        print("stop_events table already existed — no change.")
    else:
        print("stop_events table created.")

    cols = inspector.get_columns("stop_events")
    print(f"  columns: {len(cols)}")
    indexes = inspector.get_indexes("stop_events")
    print(f"  indexes: {len(indexes)}")
    uniques = inspector.get_unique_constraints("stop_events")
    print(f"  unique constraints: {len(uniques)}")


if __name__ == "__main__":
    main()
