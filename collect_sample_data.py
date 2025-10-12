"""
Sample data collector - runs multiple collection cycles for testing/development
Collects vehicle positions for a specified route in batches with delays between each collection

Usage:
    python collect_sample_data.py [route_id] [num_cycles]

Examples:
    python collect_sample_data.py C51 20      # Collect C51 for 20 cycles
    python collect_sample_data.py C53 30      # Collect C53 for 30 cycles
    python collect_sample_data.py             # Default: C51 for 20 cycles
"""
import os
import sys
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


def collect_once(cycle_num, total_cycles, route_id):
    """
    Collect vehicle positions once for specified route

    Args:
        cycle_num: Current cycle number
        total_cycles: Total number of cycles
        route_id: Route to collect (e.g., 'C51', 'C53')
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
    # Parse command line arguments
    route_id = sys.argv[1] if len(sys.argv) > 1 else 'C51'
    num_cycles = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    DELAY_SECONDS = 60  # Wait 60 seconds between collections

    print("=" * 70)
    print("WMATA Sample Data Collector")
    print("=" * 70)
    print(f"Collecting {route_id} vehicle positions {num_cycles} times")
    print(f"Delay between collections: {DELAY_SECONDS} seconds")
    print(f"Estimated time: ~{(num_cycles * DELAY_SECONDS) / 60:.1f} minutes")
    print("Press Ctrl+C to stop early")
    print("=" * 70)
    print()

    # Initialize database
    init_db()

    try:
        for i in range(1, num_cycles + 1):
            collect_once(i, num_cycles, route_id)

            # Don't sleep after the last collection
            if i < num_cycles:
                print(f"  Waiting {DELAY_SECONDS} seconds until next collection...")
                time.sleep(DELAY_SECONDS)

        print()
        print("=" * 70)
        print("✓ Sample data collection complete!")
        print("=" * 70)

        # Show summary
        db = get_session()
        try:
            from models import VehiclePosition
            total_records = db.query(VehiclePosition).count()
            route_records = db.query(VehiclePosition).filter_by(route_id=route_id).count()
            unique_vehicles = db.query(VehiclePosition.vehicle_id).filter_by(
                route_id=route_id
            ).distinct().count()

            print(f"\nDatabase Summary:")
            print(f"  Total vehicle positions: {total_records}")
            print(f"  {route_id} vehicle positions: {route_records}")
            print(f"  {route_id} unique vehicles: {unique_vehicles}")
        finally:
            db.close()

    except KeyboardInterrupt:
        print("\n\nCollection stopped by user.")
        print("Data collected so far has been saved to the database.")


if __name__ == "__main__":
    main()
