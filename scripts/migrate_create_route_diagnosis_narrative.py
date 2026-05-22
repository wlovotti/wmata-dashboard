"""
Create the ``route_diagnosis_narrative`` table (route diagnosis narrative, PR #141).

The table caches LLM-generated narrative text produced offline by
``scripts/generate_route_diagnosis.py``. The API serves the cache read-only;
Claude is never called at request time.

Idempotent: skips creation when the table already exists.

Usage::

    uv run python scripts/migrate_create_route_diagnosis_narrative.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import RouteDiagnosisNarrative


def main() -> None:
    """Create route_diagnosis_narrative if it does not already exist."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existing = set(inspector.get_table_names())

    tbl = RouteDiagnosisNarrative.__table__
    if tbl.name in existing:
        print(f"  {tbl.name}: already present, skipped.")
    else:
        tbl.create(bind=engine)
        print(f"  {tbl.name}: created.")

    # Sanity-check.
    inspector = inspect(engine)
    if tbl.name not in set(inspector.get_table_names()):
        raise RuntimeError(f"{tbl.name} still missing after create")

    print("route_diagnosis_narrative migration complete.")


if __name__ == "__main__":
    main()
