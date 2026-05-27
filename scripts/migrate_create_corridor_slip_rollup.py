"""Create the `corridor_slip_rollup` table (NOTES-62).

Per-(corridor_id, period) slip aggregation materialized nightly by
`pipelines/refresh_corridor_slip.py` from the existing
`route_diagnostic_segment` rows. See `CorridorSlipRollup` in
`src/models.py` for column-level docs.

Idempotent: skips creation when the table already exists. Depends on
`corridors` already existing (FK target).

Usage:
  uv run python scripts/migrate_create_corridor_slip_rollup.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import CorridorSlipRollup


def main() -> None:
    """Create `corridor_slip_rollup` if missing."""
    load_dotenv()
    engine = get_engine()

    tbl = CorridorSlipRollup.__table__
    inspector = inspect(engine)
    if tbl.name in set(inspector.get_table_names()):
        print(f"  {tbl.name}: already present, skipped.")
        return

    tbl.create(bind=engine)
    print(f"  {tbl.name}: created.")

    inspector = inspect(engine)
    if tbl.name not in set(inspector.get_table_names()):
        raise RuntimeError(f"{tbl.name} still missing after create")
    print("corridor_slip_rollup migration complete.")


if __name__ == "__main__":
    main()
