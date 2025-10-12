"""
C53 data collector - runs for 30 cycles (30 minutes) to collect data for OTP testing
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


def collect_once(cycle_num, total_cycles, route_id='C53'):
    """
    Collect vehicle positions once for specified route

    Args:
        cycle_num: Current cycle number
        total_cycles: Total number of cycles
        route_id: Route to collect (default: C53)
    """
    db = get_session()

    try:
        collector = WMATADataCollector(API_KEY, db_session=db)

        # Get all real-time vehicle positions
        vehicles = collector.get_realtime_vehicle_positions()

        # Filter for specified route
        route_vehicles = [v for v in vehicles if v['route_id'] and route_id in v['route_id']]

        if route_vehicles:
            collector._save_vehicle_positions(route_vehicles)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: Saved {len(route_vehicles)} {route_id} vehicle positions")
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: No {route_id} vehicles found (total vehicles: {len(vehicles)})")

    except Exception as e:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: Error - {e}")

    finally:
        db.close()


def main():
    # Number of collection cycles (30 minutes)
    NUM_CYCLES = 30
    DELAY_SECONDS = 60  # Wait 60 seconds between collections
    ROUTE = 'C53'

    print("=" * 70)
    print(f"WMATA {ROUTE} Data Collector")
    print("=" * 70)
    print(f"Collecting {ROUTE} vehicle positions {NUM_CYCLES} times")
    print(f"Delay between collections: {DELAY_SECONDS} seconds")
    print(f"Estimated time: ~{(NUM_CYCLES * DELAY_SECONDS) / 60:.0f} minutes")
    print(f"Route {ROUTE} runs 24/7 (1am-1am), so data collection at this hour is valid")
    print("Press Ctrl+C to stop early")
    print("=" * 70)
    print()

    # Initialize database
    init_db()

    try:
        for i in range(1, NUM_CYCLES + 1):
            collect_once(i, NUM_CYCLES, ROUTE)

            # Don't sleep after the last collection
            if i < NUM_CYCLES:
                print(f"  Waiting {DELAY_SECONDS} seconds until next collection...")
                time.sleep(DELAY_SECONDS)

        print()
        print("=" * 70)
        print(f"✓ {ROUTE} data collection complete!")
        print("=" * 70)

        # Show summary
        db = get_session()
        try:
            from models import VehiclePosition
            total_records = db.query(VehiclePosition).count()
            route_records = db.query(VehiclePosition).filter_by(route_id=ROUTE).count()
            unique_vehicles = db.query(VehiclePosition.vehicle_id).filter_by(
                route_id=ROUTE
            ).distinct().count()

            print(f"\nDatabase Summary:")
            print(f"  Total vehicle positions: {total_records}")
            print(f"  {ROUTE} vehicle positions: {route_records}")
            print(f"  {ROUTE} unique vehicles: {unique_vehicles}")
        finally:
            db.close()

    except KeyboardInterrupt:
        print("\n\nCollection stopped by user.")
        print("Data collected so far has been saved to the database.")


if __name__ == "__main__":
    main()
