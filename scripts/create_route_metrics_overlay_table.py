"""
Create the route_metrics_daily_overlay table on an existing database.

Idempotent — Base.metadata.create_all only creates tables that do not yet
exist. Safe to re-run. Does not touch any other table.

Run this once after pulling the materialization PR, then backfill
historical dates with `pipelines/upsert_route_metrics_overlay.py`.

Usage:
  uv run python scripts/create_route_metrics_overlay_table.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import RouteMetricsDailyOverlay


def main():
    """Create the route_metrics_daily_overlay table if it does not yet exist."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existed = "route_metrics_daily_overlay" in inspector.get_table_names()

    RouteMetricsDailyOverlay.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    if "route_metrics_daily_overlay" not in inspector.get_table_names():
        raise RuntimeError("route_metrics_daily_overlay table not present after create_all")

    if existed:
        print("route_metrics_daily_overlay table already existed — no change.")
    else:
        print("route_metrics_daily_overlay table created.")

    cols = inspector.get_columns("route_metrics_daily_overlay")
    print(f"  columns: {len(cols)}")
    indexes = inspector.get_indexes("route_metrics_daily_overlay")
    print(f"  indexes: {len(indexes)}")
    uniques = inspector.get_unique_constraints("route_metrics_daily_overlay")
    print(f"  unique constraints: {len(uniques)}")


if __name__ == "__main__":
    main()
