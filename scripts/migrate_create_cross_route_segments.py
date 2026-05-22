"""
Create `cross_route_segment_rollup` table (NOTES-59).

This table holds the cross-route segment diagnostic — per-(from_stop_id,
to_stop_id, period) aggregated slip across all routes that traverse the
same stop-pair.  Materialized nightly by
`pipelines/refresh_cross_route_segments.py`.

Idempotent: skips creation when the table already exists.

Usage:
  uv run python scripts/migrate_create_cross_route_segments.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import CrossRouteSegmentRollup


def main() -> None:
    """Create `cross_route_segment_rollup` if missing."""
    load_dotenv()
    engine = get_engine()

    tbl = CrossRouteSegmentRollup.__table__

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if tbl.name in existing_tables:
        print(f"  {tbl.name}: already present, skipped.")
    else:
        tbl.create(bind=engine)
        print(f"  {tbl.name}: created.")

    # Sanity-check.
    inspector = inspect(engine)
    if tbl.name not in set(inspector.get_table_names()):
        raise RuntimeError(f"{tbl.name} still missing after create")

    print("cross_route_segment_rollup migration complete.")


if __name__ == "__main__":
    main()
