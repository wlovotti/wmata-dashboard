"""
Fail loudly if the live database is missing tables or columns the
SQLAlchemy models declare.

Walks every table in `Base.metadata.tables`, asks SQLAlchemy's `inspect()`
for the live columns of that table, and asserts every model column name
is present. Live extras (legacy columns, manual additions) are tolerated
— this check is one-directional. The point is to catch the case where a
model column ships without a matching `scripts/migrate_*.py`, or where a
migration exists but never gets run against the live Postgres.

Origin: PR #67 (closes NOTES-31) added `Run.stops_observable`, shipped
`scripts/migrate_runs_stops_observable.py`, merged with green CI, and
500'd the Recent Runs API endpoint immediately because the migration
hadn't been run against the live Postgres. The SQLite test lane rebuilds
the schema from `Base.metadata` every run, so model and test DB are
tautologically aligned and a missing migration is invisible.

Run after `scripts/migrate_all.py` in CI's Postgres lane. Safe to run by
hand against a live database to sanity-check schema state.

Usage:
  uv run python scripts/check_schema_drift.py
"""

from __future__ import annotations

import sys

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import Base


def main() -> None:
    """Compare model schema against live DB; exit non-zero on drift."""
    load_dotenv()
    engine = get_engine()
    inspector = inspect(engine)

    live_tables = set(inspector.get_table_names())
    model_tables = dict(Base.metadata.tables)

    missing_tables: list[str] = []
    column_drift: list[tuple[str, list[str]]] = []

    for table_name, table in model_tables.items():
        if table_name not in live_tables:
            missing_tables.append(table_name)
            continue
        live_cols = {c["name"] for c in inspector.get_columns(table_name)}
        model_cols = {c.name for c in table.columns}
        missing = sorted(model_cols - live_cols)
        if missing:
            column_drift.append((table_name, missing))

    if not missing_tables and not column_drift:
        print(
            f"Schema drift check passed: {len(model_tables)} model table(s) "
            "all present with every declared column."
        )
        return

    print("Schema drift detected:", file=sys.stderr)
    if missing_tables:
        print("  Missing tables:", file=sys.stderr)
        for name in sorted(missing_tables):
            print(f"    - {name}", file=sys.stderr)
    if column_drift:
        print("  Tables with missing columns:", file=sys.stderr)
        for table_name, missing in sorted(column_drift):
            print(f"    - {table_name}: {missing}", file=sys.stderr)
    print(
        "\nThis means the live Postgres is missing schema declared in src/models.py. "
        "Either a migration script was not run, or a model column was added without "
        "a corresponding scripts/migrate_*.py.",
        file=sys.stderr,
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
