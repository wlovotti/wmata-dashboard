"""
One-time database initialization script
Run this once to set up the database and load GTFS static data

Usage:
  python scripts/init_database.py              # Interactive mode (prompts for confirmation)
  python scripts/init_database.py --no-confirm # Non-interactive mode (for automation)
"""
import os
import sys
import subprocess
from dotenv import load_dotenv
from src.database import init_db

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")


def main():
    print("=" * 70)
    print("WMATA Dashboard - Database Initialization")
    print("=" * 70)
    print("\nThis script will:")
    print("1. Create database tables with complete GTFS schema")
    print("2. Download GTFS static data (~40MB)")
    print("3. Load ALL GTFS data into database:")
    print("   - Agencies, Routes, Stops, Trips, Stop Times, Shapes")
    print("   - Calendar, Calendar Dates, Feed Info")
    print("   - Timepoints, Timepoint Times")
    print("\nThis may take 10-15 minutes depending on your connection.")
    print("You only need to run this once (or when GTFS data updates).")
    print("=" * 70)

    # Check for --no-confirm flag
    if "--no-confirm" not in sys.argv:
        response = input("\nContinue? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    else:
        print("\n[Running in non-interactive mode]")

    # Initialize database tables
    print("\n[1/2] Creating database tables with complete GTFS schema...")
    init_db()
    print("✓ Database tables created")

    # Load complete GTFS data using the reload script
    print("\n[2/2] Loading complete GTFS data...")
    print("-" * 70)

    try:
        # Run the complete GTFS reload script
        result = subprocess.run(
            [sys.executable, "scripts/reload_gtfs_complete.py"],
            check=True,
            capture_output=False  # Show output in real-time
        )

        print("-" * 70)
        print("✓ GTFS data loaded successfully")

    except subprocess.CalledProcessError as e:
        print(f"\n✗ Failed to load GTFS data: {e}")
        print("You can try running: python scripts/reload_gtfs_complete.py")
        return
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return

    print("\n" + "=" * 70)
    print("✓ Database initialization complete!")
    print("=" * 70)
    print("\nDatabase now contains:")
    print("  - Complete GTFS schema (all 11 files)")
    print("  - All GTFS fields (no dropped columns)")
    print("  - ~7 million records total")
    print("\nYou can now run:")
    print("  - python src/wmata_collector.py (one-time data collection)")
    print("  - python scripts/continuous_collector.py (continuous collection)")
    print("  - python src/analytics.py (calculate metrics)")


if __name__ == "__main__":
    main()
