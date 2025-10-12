"""
Sample data collector - runs multiple collection cycles for testing/development
Collects vehicle positions for one or more routes in batches with delays between each collection

Usage:
    python collect_sample_data.py [route_id1,route_id2,...|all] [num_cycles]

Examples:
    python collect_sample_data.py all 60             # Collect ALL vehicles for 60 cycles
    python collect_sample_data.py C51 20              # Collect C51 for 20 cycles
    python collect_sample_data.py C51,D80,C53 30     # Collect 3 routes for 30 cycles
    python collect_sample_data.py C51,D80,C53,C21 60 # Collect 4 routes for 60 cycles
    python collect_sample_data.py                     # Default: C51 for 20 cycles
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add parent directory to path so we can import from src/
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.wmata_collector import WMATADataCollector
from src.database import get_session, init_db

# Load environment variables
load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")


def collect_once(cycle_num, total_cycles, route_ids, collect_all=False):
    """
    Collect vehicle positions once for specified route(s) or all routes

    Args:
        cycle_num: Current cycle number
        total_cycles: Total number of cycles
        route_ids: List of routes to collect (e.g., ['C51', 'C53']), ignored if collect_all=True
        collect_all: If True, collect all vehicles regardless of route
    """
    db = get_session()

    try:
        collector = WMATADataCollector(API_KEY, db_session=db)

        # Get all real-time vehicle positions (single API call)
        vehicles = collector.get_realtime_vehicle_positions()

        # Filter for specified routes, or keep all if collect_all=True
        if collect_all:
            route_vehicles = vehicles
        else:
            route_vehicles = [v for v in vehicles if v['route_id'] and v['route_id'] in route_ids]

        if route_vehicles:
            collector._save_vehicle_positions(route_vehicles)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Count vehicles by route for detailed output
            route_counts = {}
            for v in route_vehicles:
                route_id = v.get('route_id', 'UNKNOWN')
                route_counts[route_id] = route_counts.get(route_id, 0) + 1

            # Format output (show top 10 routes if collecting all)
            if collect_all:
                top_routes = sorted(route_counts.items(), key=lambda x: x[1], reverse=True)[:10]
                counts_str = ', '.join([f"{r}: {c}" for r, c in top_routes])
                if len(route_counts) > 10:
                    counts_str += f" ... ({len(route_counts)} routes total)"
                print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: Saved {len(route_vehicles)} vehicles ({counts_str})")
            else:
                counts_str = ', '.join([f"{r}: {c}" for r, c in sorted(route_counts.items())])
                print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: Saved {len(route_vehicles)} vehicles ({counts_str})")
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if collect_all:
                print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: No vehicles found")
            else:
                routes_str = ','.join(route_ids)
                print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: No vehicles found for {routes_str} (total vehicles: {len(vehicles)})")

    except Exception as e:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Cycle {cycle_num}/{total_cycles}: Error - {e}")

    finally:
        db.close()


def main():
    # Parse command line arguments
    route_arg = sys.argv[1] if len(sys.argv) > 1 else 'C51'
    num_cycles = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    # Check if collecting all routes
    collect_all = route_arg.lower() == 'all'

    # Parse route IDs - support comma-separated list (ignored if collect_all=True)
    route_ids = [r.strip() for r in route_arg.split(',')] if not collect_all else []

    DELAY_SECONDS = 60  # Wait 60 seconds between collections

    print("=" * 70)
    print("WMATA Sample Data Collector")
    print("=" * 70)
    if collect_all:
        print(f"Collecting ALL vehicle positions {num_cycles} times")
    else:
        routes_str = ', '.join(route_ids)
        print(f"Collecting {routes_str} vehicle positions {num_cycles} times")
    print(f"Delay between collections: {DELAY_SECONDS} seconds")
    print(f"Estimated time: ~{(num_cycles * DELAY_SECONDS) / 60:.1f} minutes")
    print("Press Ctrl+C to stop early")
    print("=" * 70)
    print()

    # Initialize database
    init_db()

    try:
        for i in range(1, num_cycles + 1):
            collect_once(i, num_cycles, route_ids, collect_all=collect_all)

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
            from src.models import VehiclePosition
            from sqlalchemy import func

            total_records = db.query(VehiclePosition).count()

            print(f"\nDatabase Summary:")
            print(f"  Total vehicle positions: {total_records}")

            if collect_all:
                # Show top 10 routes by record count
                top_routes = db.query(
                    VehiclePosition.route_id,
                    func.count(VehiclePosition.id).label('count'),
                    func.count(func.distinct(VehiclePosition.vehicle_id)).label('vehicles')
                ).group_by(VehiclePosition.route_id).order_by(func.count(VehiclePosition.id).desc()).limit(10).all()

                print(f"\n  Top 10 routes by positions collected:")
                for route_id, count, vehicles in top_routes:
                    print(f"    {route_id}: {count} positions, {vehicles} unique vehicles")
            else:
                # Show breakdown by route
                print()
                for route_id in route_ids:
                    route_records = db.query(VehiclePosition).filter_by(route_id=route_id).count()
                    unique_vehicles = db.query(VehiclePosition.vehicle_id).filter_by(
                        route_id=route_id
                    ).distinct().count()
                    print(f"  {route_id} vehicle positions: {route_records}")
                    print(f"  {route_id} unique vehicles: {unique_vehicles}")

        finally:
            db.close()

    except KeyboardInterrupt:
        print("\n\nCollection stopped by user.")
        print("Data collected so far has been saved to the database.")


if __name__ == "__main__":
    main()
