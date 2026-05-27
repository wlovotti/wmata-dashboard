"""
Create `corridors` and `corridor_route_membership` tables (NOTES-62).

These tables hold the directional cross-route corridor identity:
- corridors: canonical corridor metadata (stop-anchored endpoints, route_set,
  cardinal bearing, WKT geometry).
- corridor_route_membership: per-(corridor, route) join with stop_sequence
  range for slip aggregation against route_diagnostic_segment.

Both tables are wiped and rebuilt by `pipelines/refresh_corridors.py` on
every GTFS reload.

Idempotent: skips creation when either table already exists.

Usage:
  uv run python scripts/migrate_create_corridors.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import Corridor, CorridorRouteMembership


def main() -> None:
    """Create `corridors` and `corridor_route_membership` if missing."""
    load_dotenv()
    engine = get_engine()

    # Order matters: parent table first (FK from membership -> corridors).
    tables = [Corridor.__table__, CorridorRouteMembership.__table__]

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    for tbl in tables:
        if tbl.name in existing_tables:
            print(f"  {tbl.name}: already present, skipped.")
        else:
            tbl.create(bind=engine)
            print(f"  {tbl.name}: created.")

    # Sanity-check both ended up present.
    inspector = inspect(engine)
    final_tables = set(inspector.get_table_names())
    for tbl in tables:
        if tbl.name not in final_tables:
            raise RuntimeError(f"{tbl.name} still missing after create")

    print("corridors migration complete.")


if __name__ == "__main__":
    main()
