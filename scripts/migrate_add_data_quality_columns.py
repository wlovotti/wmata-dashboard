"""
Add ``data_quality`` and ``coverage_pct`` columns to ``system_metrics_daily``
and ``route_metrics_daily_overlay`` (NOTES-76).

``data_quality`` is a ``VARCHAR NOT NULL DEFAULT 'complete'`` column that
flags whether a row was materialized from a full ingest day (``'complete'``)
or a partial one (``'partial'``). Existing rows are backfilled as
``'complete'`` since they were materialized under the old "refuse on partial"
guard, which never wrote a row for partial days.

``coverage_pct`` is a ``DOUBLE PRECISION`` (nullable) column holding the raw
ingest-coverage fraction (0.0–1.0) for diagnostics. Existing rows receive
``NULL`` — the guard did not record coverage for rows it accepted, and
backfilling from historical ingest tables is out of scope.

Idempotent: uses ``ADD COLUMN IF NOT EXISTS`` (Postgres 9.6+). Safe to
re-run against a live database.

Run this migration before restarting the upsert pipelines so existing rows
have the column available; the pipelines write both columns on every new
upsert going forward.

Usage:
  uv run python scripts/migrate_add_data_quality_columns.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

# New columns and their SQL type/default for each target table.
# format: (table_name, column_name, sql_definition)
NEW_COLUMNS: list[tuple[str, str, str]] = [
    ("system_metrics_daily", "data_quality", "VARCHAR NOT NULL DEFAULT 'complete'"),
    ("system_metrics_daily", "coverage_pct", "DOUBLE PRECISION"),
    ("route_metrics_daily_overlay", "data_quality", "VARCHAR NOT NULL DEFAULT 'complete'"),
    ("route_metrics_daily_overlay", "coverage_pct", "DOUBLE PRECISION"),
]


def main() -> None:
    """Add data_quality and coverage_pct columns to the two metrics tables."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    for table_name, col_name, sql_def in NEW_COLUMNS:
        if table_name not in existing_tables:
            print(f"  {table_name}: table does not exist, skipping.")
            continue

        existing_cols = {c["name"] for c in inspector.get_columns(table_name)}
        if col_name in existing_cols:
            print(f"  {table_name}.{col_name}: already present, skipped.")
            continue

        with engine.begin() as conn:
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {sql_def}")
            )
        print(f"  {table_name}.{col_name}: added.")

    # Verify all columns are present after the migration.
    inspector = inspect(engine)
    missing: list[str] = []
    for table_name, col_name, _ in NEW_COLUMNS:
        if table_name not in inspector.get_table_names():
            continue
        cols = {c["name"] for c in inspector.get_columns(table_name)}
        if col_name not in cols:
            missing.append(f"{table_name}.{col_name}")

    if missing:
        raise RuntimeError(f"Columns still missing after ALTER: {missing}")

    print("Migration complete: data_quality + coverage_pct added to both metrics tables.")


if __name__ == "__main__":
    main()
