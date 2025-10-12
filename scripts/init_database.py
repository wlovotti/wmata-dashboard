"""
One-time database initialization script
Run this once to set up the database and load GTFS static data

Usage:
  python scripts/init_database.py              # Interactive mode (prompts for confirmation)
  python scripts/init_database.py --no-confirm # Non-interactive mode (for automation)
"""
import os
import sys
from dotenv import load_dotenv
from src.wmata_collector import WMATADataCollector
from src.database import get_session, init_db

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")


def main():
    print("=" * 60)
    print("WMATA Dashboard - Database Initialization")
    print("=" * 60)
    print("\nThis script will:")
    print("1. Create database tables")
    print("2. Download GTFS static data (~40MB)")
    print("3. Load routes, stops, trips, and schedules into database")
    print("\nThis may take 5-10 minutes depending on your connection.")
    print("You only need to run this once (or when GTFS data updates).")
    print("=" * 60)

    # Check for --no-confirm flag
    if "--no-confirm" not in sys.argv:
        response = input("\nContinue? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    else:
        print("\n[Running in non-interactive mode]")

    # Initialize database
    print("\n[1/2] Initializing database...")
    init_db()

    # Get database session
    db = get_session()

    try:
        # Initialize collector with database session
        collector = WMATADataCollector(API_KEY, db_session=db)

        # Download and save GTFS static data
        print("\n[2/2] Downloading and loading GTFS static data...")
        if not collector.download_gtfs_static(save_to_db=True):
            print("\n✗ Failed to download GTFS static data")
            return

        print("\n" + "=" * 60)
        print("✓ Database initialization complete!")
        print("=" * 60)
        print("\nYou can now run:")
        print("  - wmata_collector.py (one-time data collection)")
        print("  - continuous_collector.py (continuous collection every 60s)")

    finally:
        db.close()


if __name__ == "__main__":
    main()
