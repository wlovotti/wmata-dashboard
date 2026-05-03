"""
Create the runs table on an existing database.

Idempotent — `create(checkfirst=True)` only creates the table if it does not
yet exist. Safe to re-run. Does not touch any other table.

Usage:
  uv run python scripts/create_runs_table.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import Run


def main():
    """Create the runs table if it does not yet exist; report status."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existed = "runs" in inspector.get_table_names()

    Run.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    if "runs" not in inspector.get_table_names():
        raise RuntimeError("runs table not present after create")

    if existed:
        print("runs table already existed — no change.")
    else:
        print("runs table created.")

    cols = inspector.get_columns("runs")
    print(f"  columns: {len(cols)}")
    indexes = inspector.get_indexes("runs")
    print(f"  indexes: {len(indexes)}")
    uniques = inspector.get_unique_constraints("runs")
    print(f"  unique constraints: {len(uniques)}")


if __name__ == "__main__":
    main()
