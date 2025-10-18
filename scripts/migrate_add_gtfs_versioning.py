"""
Migration: Add GTFS Snapshot Versioning

This script adds versioning support to GTFS static data tables, allowing
historical data preservation instead of destructive overwrites.

New columns added to: routes, stops, trips, stop_times, calendar, calendar_dates
New table created: gtfs_snapshots

Run this ONCE before reloading GTFS data with versioning.

Usage:
  python scripts/migrate_add_gtfs_versioning.py
"""

from sqlalchemy import text

from src.database import get_session

# SQL migrations for each table
MIGRATIONS = {
    "gtfs_snapshots": """
        CREATE TABLE IF NOT EXISTS gtfs_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date DATETIME NOT NULL,
            feed_version VARCHAR,
            routes_count INTEGER,
            stops_count INTEGER,
            trips_count INTEGER,
            stop_times_count INTEGER,
            shapes_count INTEGER,
            calendar_entries INTEGER,
            calendar_exceptions INTEGER,
            notes VARCHAR,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_date)
        );
        CREATE INDEX IF NOT EXISTS idx_snapshot_date ON gtfs_snapshots(snapshot_date);
    """,
    "routes": """
        ALTER TABLE routes ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE routes ADD COLUMN valid_from DATETIME;
        ALTER TABLE routes ADD COLUMN valid_to DATETIME;
        ALTER TABLE routes ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE routes SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        DROP INDEX IF EXISTS idx_route_id;
        CREATE INDEX IF NOT EXISTS idx_route_current ON routes(route_id, is_current);
        CREATE INDEX IF NOT EXISTS idx_route_snapshot ON routes(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_route_valid_from ON routes(valid_from);
        CREATE INDEX IF NOT EXISTS idx_route_valid_to ON routes(valid_to);
        CREATE INDEX IF NOT EXISTS idx_route_is_current ON routes(is_current);
    """,
    "stops": """
        ALTER TABLE stops ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE stops ADD COLUMN valid_from DATETIME;
        ALTER TABLE stops ADD COLUMN valid_to DATETIME;
        ALTER TABLE stops ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE stops SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        DROP INDEX IF EXISTS idx_stop_id;
        CREATE INDEX IF NOT EXISTS idx_stop_current ON stops(stop_id, is_current);
        CREATE INDEX IF NOT EXISTS idx_stop_snapshot ON stops(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_stop_valid_from ON stops(valid_from);
        CREATE INDEX IF NOT EXISTS idx_stop_valid_to ON stops(valid_to);
        CREATE INDEX IF NOT EXISTS idx_stop_is_current ON stops(is_current);
    """,
    "trips": """
        ALTER TABLE trips ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE trips ADD COLUMN valid_from DATETIME;
        ALTER TABLE trips ADD COLUMN valid_to DATETIME;
        ALTER TABLE trips ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE trips SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        DROP INDEX IF EXISTS idx_trip_id;
        CREATE INDEX IF NOT EXISTS idx_trip_current ON trips(trip_id, is_current);
        CREATE INDEX IF NOT EXISTS idx_trip_snapshot ON trips(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_trip_valid_from ON trips(valid_from);
        CREATE INDEX IF NOT EXISTS idx_trip_valid_to ON trips(valid_to);
        CREATE INDEX IF NOT EXISTS idx_trip_is_current ON trips(is_current);
    """,
    "stop_times": """
        ALTER TABLE stop_times ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE stop_times ADD COLUMN valid_from DATETIME;
        ALTER TABLE stop_times ADD COLUMN valid_to DATETIME;
        ALTER TABLE stop_times ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE stop_times SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        CREATE INDEX IF NOT EXISTS idx_stoptime_current ON stop_times(trip_id, is_current);
        CREATE INDEX IF NOT EXISTS idx_stoptime_snapshot ON stop_times(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_stoptime_valid_from ON stop_times(valid_from);
        CREATE INDEX IF NOT EXISTS idx_stoptime_valid_to ON stop_times(valid_to);
        CREATE INDEX IF NOT EXISTS idx_stoptime_is_current ON stop_times(is_current);
    """,
    "calendar": """
        ALTER TABLE calendar ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE calendar ADD COLUMN valid_from DATETIME;
        ALTER TABLE calendar ADD COLUMN valid_to DATETIME;
        ALTER TABLE calendar ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE calendar SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        DROP INDEX IF EXISTS idx_service_id;
        CREATE INDEX IF NOT EXISTS idx_calendar_current ON calendar(service_id, is_current);
        CREATE INDEX IF NOT EXISTS idx_calendar_snapshot ON calendar(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_calendar_valid_from ON calendar(valid_from);
        CREATE INDEX IF NOT EXISTS idx_calendar_valid_to ON calendar(valid_to);
        CREATE INDEX IF NOT EXISTS idx_calendar_is_current ON calendar(is_current);
    """,
    "calendar_dates": """
        ALTER TABLE calendar_dates ADD COLUMN snapshot_id INTEGER REFERENCES gtfs_snapshots(snapshot_id);
        ALTER TABLE calendar_dates ADD COLUMN valid_from DATETIME;
        ALTER TABLE calendar_dates ADD COLUMN valid_to DATETIME;
        ALTER TABLE calendar_dates ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT 1;
        UPDATE calendar_dates SET valid_from = CURRENT_TIMESTAMP WHERE valid_from IS NULL;
        CREATE INDEX IF NOT EXISTS idx_calendardate_current ON calendar_dates(date, is_current);
        CREATE INDEX IF NOT EXISTS idx_calendardate_snapshot ON calendar_dates(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_calendardate_valid_from ON calendar_dates(valid_from);
        CREATE INDEX IF NOT EXISTS idx_calendardate_valid_to ON calendar_dates(valid_to);
        CREATE INDEX IF NOT EXISTS idx_calendardate_is_current ON calendar_dates(is_current);
    """,
}


def run_migrations():
    """Apply all versioning migrations"""
    db = get_session()

    try:
        print("=" * 70)
        print("GTFS Snapshot Versioning Migration")
        print("=" * 70)

        for table_name, migration_sql in MIGRATIONS.items():
            print(f"\nMigrating {table_name}...")
            try:
                for statement in migration_sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        db.execute(text(statement))
                        db.commit()
                print(f"  ✓ {table_name} migrated successfully")
            except Exception as e:
                print(f"  ⚠ {table_name}: {e}")
                # Don't fail on individual table errors (columns might already exist)
                db.rollback()

        print("\n" + "=" * 70)
        print("✓ Migration complete!")
        print("=" * 70)
        print("\nYour existing GTFS data has been marked as 'current'.")
        print("Next time you reload GTFS, old records will be marked as 'inactive'")
        print("instead of being deleted.")
        print("\nTo reload GTFS with versioning:")
        print("  python scripts/reload_gtfs_complete.py")

    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    run_migrations()
