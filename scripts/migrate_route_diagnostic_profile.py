"""
Create the three `route_diagnostic_*` tables (NOTES-57).

These materialize the per-route diagnostic primitives — segment slip,
timepoint behavior classification, direction asymmetry — computed by
`src/route_diagnostics.py` and refreshed nightly by
`pipelines/refresh_route_diagnostic_profile.py`. See the model
docstrings on `RouteDiagnosticSegment`, `RouteDiagnosticTimepoint`,
and `RouteDiagnosticDirection` for the storage rationale and the
queries each table serves.

Idempotent: uses `CREATE TABLE IF NOT EXISTS` (via SQLAlchemy
`create_all` on the relevant Table objects). Safe to re-run after
landing — `create_all` is a no-op when the tables already exist.

Usage:
  uv run python scripts/migrate_route_diagnostic_profile.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import (
    RouteDiagnosticDirection,
    RouteDiagnosticSegment,
    RouteDiagnosticTimepoint,
)


def main() -> None:
    """Create the three route_diagnostic_* tables if missing."""
    load_dotenv()
    engine = get_engine()

    targets = [
        RouteDiagnosticSegment.__table__,
        RouteDiagnosticTimepoint.__table__,
        RouteDiagnosticDirection.__table__,
    ]

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    for tbl in targets:
        if tbl.name in existing_tables:
            print(f"  {tbl.name}: already present, skipped.")
        else:
            tbl.create(bind=engine)
            print(f"  {tbl.name}: created.")

    # Sanity-check that every target exists now.
    inspector = inspect(engine)
    final_tables = set(inspector.get_table_names())
    for tbl in targets:
        if tbl.name not in final_tables:
            raise RuntimeError(f"{tbl.name} still missing after create_all")

    print(f"route_diagnostic_* migration complete ({len(targets)} tables).")


if __name__ == "__main__":
    main()
