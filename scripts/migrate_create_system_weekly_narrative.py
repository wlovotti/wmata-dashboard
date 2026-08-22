"""
Create the ``system_weekly_narrative`` table (system weekly narrative, PR #219).

Sibling migration to ``scripts/migrate_create_route_diagnosis_narrative.py``
(PR #141). The table caches LLM-generated narrative text produced offline by
``scripts/generate_system_weekly_narrative.py``. The API serves the cache
read-only; Claude is never called at request time.

Idempotent: skips creation when the table already exists.

Usage::

    uv run python scripts/migrate_create_system_weekly_narrative.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import SystemWeeklyNarrative


def main() -> None:
    """Create system_weekly_narrative if it does not already exist."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existing = set(inspector.get_table_names())

    tbl = SystemWeeklyNarrative.__table__
    if tbl.name in existing:
        print(f"  {tbl.name}: already present, skipped.")
    else:
        tbl.create(bind=engine)
        print(f"  {tbl.name}: created.")

    # Sanity-check.
    inspector = inspect(engine)
    if tbl.name not in set(inspector.get_table_names()):
        raise RuntimeError(f"{tbl.name} still missing after create")

    print("system_weekly_narrative migration complete.")


if __name__ == "__main__":
    main()
