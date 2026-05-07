"""
Create the `system_metrics_daily` table on an existing database (NOTES-48).

Idempotent — `create(checkfirst=True)` only creates the table if it does not
yet exist. Safe to re-run. Does not touch any other table.

Usage:
  uv run python scripts/create_system_metrics_daily_table.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import SystemMetricsDaily


def main():
    """Create the system_metrics_daily table if absent; report status."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existed = "system_metrics_daily" in inspector.get_table_names()

    SystemMetricsDaily.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    if "system_metrics_daily" not in inspector.get_table_names():
        raise RuntimeError("system_metrics_daily table not present after create")

    if existed:
        print("system_metrics_daily table already existed - no change.")
    else:
        print("system_metrics_daily table created.")

    cols = inspector.get_columns("system_metrics_daily")
    print(f"  columns: {len(cols)}")
    for c in cols:
        print(f"    - {c['name']}: {c['type']}")


if __name__ == "__main__":
    main()
