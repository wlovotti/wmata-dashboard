"""
Add `stops_observable` to the existing `runs` table and backfill it.

Closes NOTES-31 — the GTFS-RT TripUpdates feed never publishes the origin
stop's StopTimeUpdate (predictions are only for *upcoming* stops, and a
trip doesn't enter the TU stream until after origin departure), so on
every trip_update run `stops_observed` runs ~1 short of `stops_scheduled`.
That bias breaks completeness ratios that compare the two directly.
`stops_observable` is the per-source count of stops the source can
structurally see:

    stops_observable = stops_scheduled - 1   if source = 'trip_update'
    stops_observable = stops_scheduled       if source = 'proximity'

Idempotent: ADD COLUMN IF NOT EXISTS, and the backfill UPDATE is
re-runnable (no harm in recomputing the same value). Safe to re-run.
Does not touch any other table.

Usage:
  uv run python scripts/migrate_runs_stops_observable.py
"""

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.database import get_engine


def main():
    """Add `stops_observable` if missing, then backfill it from existing rows."""
    load_dotenv()
    engine = get_engine()

    inspector = inspect(engine)
    if "runs" not in inspector.get_table_names():
        raise RuntimeError("runs table does not exist — run scripts/create_runs_table.py first")

    existing = {c["name"] for c in inspector.get_columns("runs")}
    with engine.begin() as conn:
        if "stops_observable" in existing:
            print("  stops_observable: already present, skipped column add.")
        else:
            conn.execute(text("ALTER TABLE runs ADD COLUMN IF NOT EXISTS stops_observable INTEGER"))
            print("  stops_observable: column added.")

        # Backfill: derive from stops_scheduled and source. The CASE handles
        # the source asymmetry; COALESCE guards trip_update rows with
        # stops_scheduled = 0 (no GTFS match) so we don't write -1.
        result = conn.execute(
            text(
                """
                UPDATE runs
                SET stops_observable = CASE
                    WHEN stops_scheduled IS NULL THEN NULL
                    WHEN source = 'trip_update' THEN GREATEST(stops_scheduled - 1, 0)
                    ELSE stops_scheduled
                END
                """
            )
        )
        print(f"  stops_observable: backfilled {result.rowcount} rows.")

    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns("runs")}
    if "stops_observable" not in cols:
        raise RuntimeError("stops_observable still missing after ALTER")
    print(f"runs table now has {len(cols)} columns.")


if __name__ == "__main__":
    main()
