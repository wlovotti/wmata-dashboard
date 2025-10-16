"""
Migration script to add missing GTFS-RT fields to vehicle_positions table

This script adds 10 new fields to the vehicle_positions table to capture
all available data from the GTFS-RT vehicle positions feed.

New fields:
- vehicle_label (vehicle display label)
- bearing (direction vehicle is facing, 0-360 degrees)
- speed (speed in meters/second)
- stop_id (current or next stop)
- current_status (0=incoming, 1=stopped, 2=in_transit)
- direction_id (0 or 1 for trip direction)
- trip_start_time (HH:MM:SS format)
- trip_start_date (YYYYMMDD format)
- schedule_relationship (0=scheduled, 1=added, 2=unscheduled, 3=canceled)
- occupancy_status (passenger load, 0-7 scale)

Usage:
  python scripts/migrate_vehicle_positions.py
"""

import sys

from sqlalchemy import inspect, text

from src.database import get_engine


def check_column_exists(engine, table_name, column_name):
    """Check if a column exists in a table"""
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return column_name in columns


def migrate_vehicle_positions(engine):
    """Add missing fields to vehicle_positions table"""
    print("=" * 70)
    print("Vehicle Positions Table Migration")
    print("=" * 70)

    # Check database type
    db_url = str(engine.url)
    is_sqlite = db_url.startswith("sqlite")

    print(f"\nDatabase: {db_url}")
    print(f"Type: {'SQLite' if is_sqlite else 'PostgreSQL'}")
    print()

    # Define columns to add
    columns_to_add = [
        ("vehicle_label", "VARCHAR"),
        ("bearing", "FLOAT" if is_sqlite else "DOUBLE PRECISION"),
        ("speed", "FLOAT" if is_sqlite else "DOUBLE PRECISION"),
        ("stop_id", "VARCHAR"),
        ("current_status", "INTEGER"),
        ("direction_id", "INTEGER"),
        ("trip_start_time", "VARCHAR"),
        ("trip_start_date", "VARCHAR"),
        ("schedule_relationship", "INTEGER"),
        ("occupancy_status", "INTEGER"),
    ]

    with engine.connect() as conn:
        print("Adding missing columns to vehicle_positions table:")
        print("-" * 70)

        added_count = 0
        for col_name, col_type in columns_to_add:
            if not check_column_exists(engine, "vehicle_positions", col_name):
                print(f"→ Adding vehicle_positions.{col_name}...")
                conn.execute(
                    text(f"ALTER TABLE vehicle_positions ADD COLUMN {col_name} {col_type}")
                )
                conn.commit()
                print(f"  ✓ Added {col_name}")
                added_count += 1
            else:
                print(f"✓ vehicle_positions.{col_name} already exists")

    print("\n" + "=" * 70)
    if added_count > 0:
        print(f"Migration Complete! Added {added_count} columns.")
    else:
        print("Migration Complete! All columns already exist.")
    print("=" * 70)

    if added_count > 0:
        print("\nThe vehicle_positions table now captures all GTFS-RT fields:")
        print("  - Vehicle label and identification")
        print("  - Position data (lat, lon, bearing, speed)")
        print("  - Stop information (current stop, status)")
        print("  - Trip details (direction, start time/date)")
        print("  - Schedule relationship and occupancy status")
        print("\nExisting data will have NULL values for new fields.")
        print("New data collection will populate these fields automatically.")


def main():
    """Run the migration"""
    engine = get_engine()

    try:
        migrate_vehicle_positions(engine)
        print("\n✓ Migration successful!")
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
