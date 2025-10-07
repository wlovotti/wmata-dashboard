"""
Continuous data collector that runs every 60 seconds to collect vehicle positions
"""
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from wmata_collector import WMATADataCollector
from database import get_session, init_db

# Load environment variables
load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")


def collect_vehicle_positions_only():
    """Collect only vehicle positions (assumes GTFS static data already loaded)"""
    db = get_session()

    try:
        collector = WMATADataCollector(API_KEY, db_session=db)

        # Get all real-time vehicle positions
        vehicles = collector.get_realtime_vehicle_positions()

        # Save all vehicle positions to database
        if vehicles:
            collector._save_vehicle_positions(vehicles)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Saved {len(vehicles)} vehicle positions")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No vehicles found")

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error: {e}")

    finally:
        db.close()


def main():
    print("WMATA Continuous Data Collector")
    print("=" * 50)
    print("This script will collect vehicle positions every 60 seconds")
    print("Press Ctrl+C to stop")
    print("=" * 50)

    # Initialize database on first run
    init_db()

    # Collect initial GTFS static data
    print("\nLoading initial GTFS static data...")
    db = get_session()
    try:
        collector = WMATADataCollector(API_KEY, db_session=db)
        collector.download_gtfs_static(save_to_db=True)
    finally:
        db.close()

    print("\nStarting continuous collection...")

    try:
        while True:
            collect_vehicle_positions_only()
            time.sleep(60)  # Wait 60 seconds between collections

    except KeyboardInterrupt:
        print("\n\nStopping continuous collection...")
        print("Data collection stopped successfully!")


if __name__ == "__main__":
    main()
