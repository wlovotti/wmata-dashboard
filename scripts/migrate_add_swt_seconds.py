"""
Add the ``swt_seconds`` column to ``system_metrics_daily`` (NOTES-115).

``swt_seconds`` holds the pooled random-incidence scheduled wait time,
computed nightly from the *identical* frequent cell-hour pool as
``ewt_seconds`` (the rollup already computes and discards it), so that
``swt_seconds + ewt_seconds`` equals the pooled average wait time (up to
EWT's clamp at 0). It backs the agency-comparison page's scheduled-wait
KPI, which surfaces the "promise" term the other headline metrics
measure performance against (see
``docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md``).

``swt_seconds`` is a nullable ``DOUBLE PRECISION`` column. Existing rows
receive ``NULL`` — the nightly upsert did not compute or store SWT before
this change, so there is nothing to backfill from; a separate user-run
backfill (`pipelines/upsert_system_metrics_daily.py` re-run per date)
populates history going forward. Until backfilled, the frontend tile
renders its em-dash null state — nothing breaks.

Idempotent: uses ``ADD COLUMN IF NOT EXISTS`` (Postgres 9.6+). Safe to
re-run against a live database.

Run this migration (or `scripts/migrate_all.py`, which auto-discovers
it) before running the API or any pipeline against this code — the
nightly batch's `upsert_system_metrics_for_date` and the comparison
endpoint's `SystemMetricsDaily` query both reference the column
unconditionally.

Usage:
  uv run python scripts/migrate_add_swt_seconds.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

TABLE_NAME = "system_metrics_daily"
COLUMN_NAME = "swt_seconds"
COLUMN_SQL_DEF = "DOUBLE PRECISION"


def main() -> None:
    """Add the swt_seconds column to system_metrics_daily."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    if TABLE_NAME not in existing_tables:
        print(f"  {TABLE_NAME}: table does not exist, skipping.")
        return

    existing_cols = {c["name"] for c in inspector.get_columns(TABLE_NAME)}
    if COLUMN_NAME in existing_cols:
        print(f"  {TABLE_NAME}.{COLUMN_NAME}: already present, skipped.")
    else:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS "
                    f"{COLUMN_NAME} {COLUMN_SQL_DEF}"
                )
            )
        print(f"  {TABLE_NAME}.{COLUMN_NAME}: added.")

    # Verify the column is present after the migration.
    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns(TABLE_NAME)}
    if COLUMN_NAME not in cols:
        raise RuntimeError(f"Column still missing after ALTER: {TABLE_NAME}.{COLUMN_NAME}")

    print(f"Migration complete: {COLUMN_NAME} added to {TABLE_NAME}.")


if __name__ == "__main__":
    main()
