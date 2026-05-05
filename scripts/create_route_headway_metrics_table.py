"""
Create the route_headway_metrics table on an existing database (NOTES-13).

Idempotent — `create(checkfirst=True)` only creates the table if it does not
yet exist. Safe to re-run. Does not touch any other table.

Usage:
  uv run python scripts/create_route_headway_metrics_table.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import RouteHeadwayMetrics


def main():
    """Create the route_headway_metrics table if absent; report status."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existed = "route_headway_metrics" in inspector.get_table_names()

    RouteHeadwayMetrics.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    if "route_headway_metrics" not in inspector.get_table_names():
        raise RuntimeError("route_headway_metrics table not present after create")

    if existed:
        print("route_headway_metrics table already existed — no change.")
    else:
        print("route_headway_metrics table created.")

    cols = inspector.get_columns("route_headway_metrics")
    print(f"  columns: {len(cols)}")
    indexes = inspector.get_indexes("route_headway_metrics")
    print(f"  indexes: {len(indexes)}")
    uniques = inspector.get_unique_constraints("route_headway_metrics")
    print(f"  unique constraints: {len(uniques)}")


if __name__ == "__main__":
    main()
