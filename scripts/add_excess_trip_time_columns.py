"""
Add excess trip time columns to `route_metrics_daily` (NOTES-43).

Adds four columns:
  - `excess_trip_time_pct` (float) — share of qualifying trips on the day
    whose actual end-to-end duration exceeded 110% of scheduled. Source:
    `src/excess_trip_time.py:compute_excess_trip_time` `pct_over_110`.
  - `median_actual_trip_time_sec` (int) — median observed trip duration
    in seconds across qualifying trips.
  - `median_scheduled_trip_time_sec` (int) — median scheduled trip duration
    in seconds. Pair with `median_actual_trip_time_sec` for the
    "trips ran X min, schedule Y min" subline.
  - `excess_trip_time_n_trips` (int) — count of qualifying trips that
    observed both literal endpoints (via the proximity-origin /
    TU-destination rule from `src/excess_trip_time.py`).

`route_metrics_daily` is the legacy aggregation table targeted for
deprecation in NOTES-19. We layer on top of it consciously here:
NOTES-43's UI surface (KPI card + trend) follows the existing
`route_metrics_daily`-backed pattern for per-route trends, so adding
columns is the lowest-risk wire-through. The eventual stop_events-based
rollup will absorb these alongside the rest.

Idempotent: each column uses `ADD COLUMN IF NOT EXISTS`, so re-running
is a no-op. Backfill for existing dates lands via
`pipelines/compute_daily_metrics.py --recalculate`.

Usage:
  uv run python scripts/add_excess_trip_time_columns.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine

TABLE_NAME = "route_metrics_daily"
COLUMNS = [
    ("excess_trip_time_pct", "DOUBLE PRECISION"),
    ("median_actual_trip_time_sec", "INTEGER"),
    ("median_scheduled_trip_time_sec", "INTEGER"),
    ("excess_trip_time_n_trips", "INTEGER"),
]


def main() -> None:
    """Add excess-trip-time columns to `route_metrics_daily` if missing."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    if TABLE_NAME not in inspector.get_table_names():
        raise RuntimeError(f"{TABLE_NAME} does not exist")

    existing = {col["name"] for col in inspector.get_columns(TABLE_NAME)}
    print(f"Current columns on {TABLE_NAME}: {len(existing)}")

    with engine.begin() as conn:
        for col_name, col_type in COLUMNS:
            if col_name in existing:
                print(f"  {col_name}: already present, skipped.")
                continue
            ddl = f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            print(f"  {col_name}: adding ({col_type})...")
            conn.execute(text(ddl))
            print(f"  {col_name}: added.")

    inspector = inspect(engine)
    after = {col["name"] for col in inspector.get_columns(TABLE_NAME)}
    missing = [name for name, _ in COLUMNS if name not in after]
    if missing:
        raise RuntimeError(f"Failed to add columns: {missing}")
    print(f"All {len(COLUMNS)} excess-trip-time columns are present on {TABLE_NAME}.")


if __name__ == "__main__":
    main()
