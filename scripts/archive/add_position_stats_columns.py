"""
Database migration: Add position statistics columns to route_metrics_summary table

This migration adds pre-computed position statistics to eliminate slow counting queries
in the API layer. These columns are populated by the nightly metrics computation job.

New columns:
- total_positions_7d: Total vehicle position records over past 7 days
- unique_vehicles_7d: Number of unique vehicles seen over past 7 days
- unique_trips_7d: Number of unique trips over past 7 days
- last_position_timestamp: Timestamp of most recent position record

Run this script once to add the columns to an existing database.
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./wmata_dashboard.db")


def add_columns():
    """Add position statistics columns to route_metrics_summary table"""
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    print("=" * 70)
    print("Adding Position Statistics Columns to route_metrics_summary")
    print("=" * 70)
    print(f"\nDatabase: {DATABASE_URL}")
    print()

    with engine.connect() as conn:
        # Check if columns already exist
        if DATABASE_URL.startswith("sqlite"):
            result = conn.execute(
                text(
                    "SELECT COUNT(*) as count FROM pragma_table_info('route_metrics_summary') "
                    "WHERE name IN ('total_positions_7d', 'unique_vehicles_7d', 'unique_trips_7d', 'last_position_timestamp')"
                )
            )
        else:  # PostgreSQL
            result = conn.execute(
                text(
                    "SELECT COUNT(*) as count FROM information_schema.columns "
                    "WHERE table_name = 'route_metrics_summary' "
                    "AND column_name IN ('total_positions_7d', 'unique_vehicles_7d', 'unique_trips_7d', 'last_position_timestamp')"
                )
            )

        existing_count = result.scalar()

        if existing_count > 0:
            print(f"⚠️  Found {existing_count} column(s) already exist.")
            response = input("Do you want to continue anyway? (y/n): ")
            if response.lower() != "y":
                print("Aborted.")
                return False

        print("Adding columns...")

        # Add columns (safe to run multiple times - will fail gracefully if columns exist)
        try:
            conn.execute(
                text("ALTER TABLE route_metrics_summary ADD COLUMN total_positions_7d INTEGER")
            )
            print("  ✓ Added total_positions_7d")
        except Exception as e:
            print(f"  ⚠️  total_positions_7d: {e}")

        try:
            conn.execute(
                text("ALTER TABLE route_metrics_summary ADD COLUMN unique_vehicles_7d INTEGER")
            )
            print("  ✓ Added unique_vehicles_7d")
        except Exception as e:
            print(f"  ⚠️  unique_vehicles_7d: {e}")

        try:
            conn.execute(
                text("ALTER TABLE route_metrics_summary ADD COLUMN unique_trips_7d INTEGER")
            )
            print("  ✓ Added unique_trips_7d")
        except Exception as e:
            print(f"  ⚠️  unique_trips_7d: {e}")

        try:
            conn.execute(
                text(
                    "ALTER TABLE route_metrics_summary ADD COLUMN last_position_timestamp TIMESTAMP"
                )
            )
            print("  ✓ Added last_position_timestamp")
        except Exception as e:
            print(f"  ⚠️  last_position_timestamp: {e}")

        conn.commit()

    print()
    print("=" * 70)
    print("✓ Migration completed!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Update src/models.py to add these fields to RouteMetricsSummary")
    print("2. Update pipelines/compute_daily_metrics.py to populate these columns")
    print("3. Run the metrics pipeline to backfill data")
    print()

    return True


if __name__ == "__main__":
    add_columns()
