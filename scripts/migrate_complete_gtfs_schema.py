"""
Complete GTFS Schema Migration

This script migrates the database to include ALL GTFS fields and tables.

Changes:
1. Add missing fields to existing tables (routes, stops, stop_times)
2. Create new tables (agencies, calendar, calendar_dates, feed_info, timepoints, timepoint_times)

WARNING: This migration requires reloading data for routes, stops, and stop_times
to populate the new fields. Run scripts/reload_gtfs_complete.py after this migration.
"""
from src.database import get_engine
from sqlalchemy import text, inspect
import sys


def check_table_exists(engine, table_name):
    """Check if a table exists in the database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def check_column_exists(engine, table_name, column_name):
    """Check if a column exists in a table"""
    if not check_table_exists(engine, table_name):
        return False
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def migrate_complete_gtfs(engine):
    """Migrate to complete GTFS schema"""
    print("=" * 70)
    print("Complete GTFS Schema Migration")
    print("=" * 70)

    # Check database type
    db_url = str(engine.url)
    is_sqlite = db_url.startswith('sqlite')

    print(f"\nDatabase: {db_url}")
    print(f"Type: {'SQLite' if is_sqlite else 'PostgreSQL'}")
    print()

    with engine.connect() as conn:

        # ===== CREATE NEW TABLES =====

        print("STEP 1: Creating new GTFS tables")
        print("-" * 70)

        # Create agencies table
        if not check_table_exists(engine, 'agencies'):
            print("→ Creating agencies table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE agencies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        agency_id VARCHAR UNIQUE NOT NULL,
                        agency_name VARCHAR NOT NULL,
                        agency_url VARCHAR,
                        agency_timezone VARCHAR,
                        agency_lang VARCHAR,
                        agency_phone VARCHAR,
                        agency_fare_url VARCHAR,
                        agency_email VARCHAR,
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX ix_agencies_agency_id ON agencies (agency_id)"))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE agencies (
                        id SERIAL PRIMARY KEY,
                        agency_id VARCHAR UNIQUE NOT NULL,
                        agency_name VARCHAR NOT NULL,
                        agency_url VARCHAR,
                        agency_timezone VARCHAR,
                        agency_lang VARCHAR,
                        agency_phone VARCHAR,
                        agency_fare_url VARCHAR,
                        agency_email VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX ix_agencies_agency_id ON agencies (agency_id)"))
            conn.commit()
            print("  ✓ Created agencies table")
        else:
            print("✓ agencies table already exists")

        # Create calendar table
        if not check_table_exists(engine, 'calendar'):
            print("→ Creating calendar table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE calendar (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service_id VARCHAR UNIQUE NOT NULL,
                        monday INTEGER NOT NULL,
                        tuesday INTEGER NOT NULL,
                        wednesday INTEGER NOT NULL,
                        thursday INTEGER NOT NULL,
                        friday INTEGER NOT NULL,
                        saturday INTEGER NOT NULL,
                        sunday INTEGER NOT NULL,
                        start_date VARCHAR NOT NULL,
                        end_date VARCHAR NOT NULL,
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX ix_calendar_service_id ON calendar (service_id)"))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE calendar (
                        id SERIAL PRIMARY KEY,
                        service_id VARCHAR UNIQUE NOT NULL,
                        monday INTEGER NOT NULL,
                        tuesday INTEGER NOT NULL,
                        wednesday INTEGER NOT NULL,
                        thursday INTEGER NOT NULL,
                        friday INTEGER NOT NULL,
                        saturday INTEGER NOT NULL,
                        sunday INTEGER NOT NULL,
                        start_date VARCHAR NOT NULL,
                        end_date VARCHAR NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX ix_calendar_service_id ON calendar (service_id)"))
            conn.commit()
            print("  ✓ Created calendar table")
        else:
            print("✓ calendar table already exists")

        # Create calendar_dates table
        if not check_table_exists(engine, 'calendar_dates'):
            print("→ Creating calendar_dates table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE calendar_dates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service_id VARCHAR NOT NULL,
                        date VARCHAR NOT NULL,
                        exception_type INTEGER NOT NULL,
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX ix_calendar_dates_service_id ON calendar_dates (service_id)"))
                conn.execute(text("CREATE INDEX ix_calendar_dates_date ON calendar_dates (date)"))
                conn.execute(text("CREATE INDEX idx_service_date ON calendar_dates (service_id, date)"))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE calendar_dates (
                        id SERIAL PRIMARY KEY,
                        service_id VARCHAR NOT NULL,
                        date VARCHAR NOT NULL,
                        exception_type INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX ix_calendar_dates_service_id ON calendar_dates (service_id)"))
                conn.execute(text("CREATE INDEX ix_calendar_dates_date ON calendar_dates (date)"))
                conn.execute(text("CREATE INDEX idx_service_date ON calendar_dates (service_id, date)"))
            conn.commit()
            print("  ✓ Created calendar_dates table")
        else:
            print("✓ calendar_dates table already exists")

        # Create feed_info table
        if not check_table_exists(engine, 'feed_info'):
            print("→ Creating feed_info table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE feed_info (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        feed_publisher_name VARCHAR NOT NULL,
                        feed_publisher_url VARCHAR,
                        feed_lang VARCHAR,
                        feed_start_date VARCHAR,
                        feed_end_date VARCHAR,
                        feed_version VARCHAR,
                        feed_contact_email VARCHAR,
                        feed_contact_url VARCHAR,
                        created_at DATETIME
                    )
                """))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE feed_info (
                        id SERIAL PRIMARY KEY,
                        feed_publisher_name VARCHAR NOT NULL,
                        feed_publisher_url VARCHAR,
                        feed_lang VARCHAR,
                        feed_start_date VARCHAR,
                        feed_end_date VARCHAR,
                        feed_version VARCHAR,
                        feed_contact_email VARCHAR,
                        feed_contact_url VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            conn.commit()
            print("  ✓ Created feed_info table")
        else:
            print("✓ feed_info table already exists")

        # Create timepoints table
        if not check_table_exists(engine, 'timepoints'):
            print("→ Creating timepoints table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE timepoints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stop_id VARCHAR UNIQUE NOT NULL,
                        stop_code VARCHAR,
                        stop_name VARCHAR NOT NULL,
                        stop_desc VARCHAR,
                        stop_lat FLOAT NOT NULL,
                        stop_lon FLOAT NOT NULL,
                        zone_id VARCHAR,
                        stop_url VARCHAR,
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX ix_timepoints_stop_id ON timepoints (stop_id)"))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE timepoints (
                        id SERIAL PRIMARY KEY,
                        stop_id VARCHAR UNIQUE NOT NULL,
                        stop_code VARCHAR,
                        stop_name VARCHAR NOT NULL,
                        stop_desc VARCHAR,
                        stop_lat DOUBLE PRECISION NOT NULL,
                        stop_lon DOUBLE PRECISION NOT NULL,
                        zone_id VARCHAR,
                        stop_url VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX ix_timepoints_stop_id ON timepoints (stop_id)"))
            conn.commit()
            print("  ✓ Created timepoints table")
        else:
            print("✓ timepoints table already exists")

        # Create timepoint_times table
        if not check_table_exists(engine, 'timepoint_times'):
            print("→ Creating timepoint_times table...")
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE timepoint_times (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trip_id VARCHAR NOT NULL,
                        stop_id VARCHAR NOT NULL,
                        arrival_time VARCHAR NOT NULL,
                        departure_time VARCHAR NOT NULL,
                        stop_sequence INTEGER NOT NULL,
                        stop_headsign VARCHAR,
                        pickup_type INTEGER,
                        drop_off_type INTEGER,
                        shape_dist_traveled FLOAT,
                        timepoint INTEGER,
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX ix_timepoint_times_trip_id ON timepoint_times (trip_id)"))
                conn.execute(text("CREATE INDEX ix_timepoint_times_stop_id ON timepoint_times (stop_id)"))
                conn.execute(text("CREATE INDEX idx_timepoint_trip_sequence ON timepoint_times (trip_id, stop_sequence)"))
            else:  # PostgreSQL
                conn.execute(text("""
                    CREATE TABLE timepoint_times (
                        id SERIAL PRIMARY KEY,
                        trip_id VARCHAR NOT NULL,
                        stop_id VARCHAR NOT NULL,
                        arrival_time VARCHAR NOT NULL,
                        departure_time VARCHAR NOT NULL,
                        stop_sequence INTEGER NOT NULL,
                        stop_headsign VARCHAR,
                        pickup_type INTEGER,
                        drop_off_type INTEGER,
                        shape_dist_traveled DOUBLE PRECISION,
                        timepoint INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX ix_timepoint_times_trip_id ON timepoint_times (trip_id)"))
                conn.execute(text("CREATE INDEX ix_timepoint_times_stop_id ON timepoint_times (stop_id)"))
                conn.execute(text("CREATE INDEX idx_timepoint_trip_sequence ON timepoint_times (trip_id, stop_sequence)"))
            conn.commit()
            print("  ✓ Created timepoint_times table")
        else:
            print("✓ timepoint_times table already exists")

        # ===== ADD MISSING COLUMNS TO EXISTING TABLES =====

        print("\nSTEP 2: Adding missing columns to existing tables")
        print("-" * 70)

        # Update routes table
        routes_columns = [
            ('agency_id', 'VARCHAR'),
            ('route_desc', 'VARCHAR'),
            ('route_url', 'VARCHAR'),
            ('route_color', 'VARCHAR'),
            ('route_text_color', 'VARCHAR')
        ]

        if check_table_exists(engine, 'routes'):
            for col_name, col_type in routes_columns:
                if not check_column_exists(engine, 'routes', col_name):
                    print(f"→ Adding routes.{col_name}...")
                    conn.execute(text(f"ALTER TABLE routes ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                    print(f"  ✓ Added {col_name}")
                else:
                    print(f"✓ routes.{col_name} already exists")

        # Update stops table
        stops_columns = [
            ('stop_code', 'VARCHAR'),
            ('stop_desc', 'VARCHAR'),
            ('zone_id', 'VARCHAR'),
            ('stop_url', 'VARCHAR')
        ]

        if check_table_exists(engine, 'stops'):
            for col_name, col_type in stops_columns:
                if not check_column_exists(engine, 'stops', col_name):
                    print(f"→ Adding stops.{col_name}...")
                    conn.execute(text(f"ALTER TABLE stops ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                    print(f"  ✓ Added {col_name}")
                else:
                    print(f"✓ stops.{col_name} already exists")

        # Update stop_times table
        stop_times_columns = [
            ('stop_headsign', 'VARCHAR'),
            ('pickup_type', 'INTEGER'),
            ('drop_off_type', 'INTEGER'),
            ('shape_dist_traveled', 'FLOAT' if is_sqlite else 'DOUBLE PRECISION'),
            ('timepoint', 'INTEGER')
        ]

        if check_table_exists(engine, 'stop_times'):
            for col_name, col_type in stop_times_columns:
                if not check_column_exists(engine, 'stop_times', col_name):
                    print(f"→ Adding stop_times.{col_name}...")
                    conn.execute(text(f"ALTER TABLE stop_times ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                    print(f"  ✓ Added {col_name}")
                else:
                    print(f"✓ stop_times.{col_name} already exists")

    print("\n" + "=" * 70)
    print("Migration Complete!")
    print("=" * 70)
    print("\n⚠️  IMPORTANT NEXT STEPS:")
    print("1. The routes, stops, and stop_times tables have new fields")
    print("2. Run: python scripts/reload_gtfs_complete.py")
    print("3. This will DROP and recreate routes, stops, stop_times with complete data")
    print("4. All new tables will be populated with GTFS data")


def main():
    """Run the migration"""
    engine = get_engine()

    try:
        migrate_complete_gtfs(engine)
        print("\n✓ Migration successful!")
        print("\n⚠️  Remember to run scripts/reload_gtfs_complete.py next!")
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
