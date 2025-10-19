"""
Test collecting BusPositions data and saving to database
"""
import os

from dotenv import load_dotenv

from src.database import get_session
from src.models import BusPosition
from src.wmata_collector import WMATADataCollector

load_dotenv()
API_KEY = os.getenv("WMATA_API_KEY")

db = get_session()

try:
    collector = WMATADataCollector(API_KEY, db_session=db)

    print("=" * 70)
    print("Testing BusPositions Collection")
    print("=" * 70)

    # Test collecting for a specific route
    print("\n1. Collecting BusPositions for route C51...")
    positions = collector.get_bus_positions(route_id='C51')

    if positions:
        print(f"\n   Retrieved {len(positions)} positions")
        print(f"   Sample: Vehicle {positions[0]['VehicleID']}, Deviation: {positions[0].get('Deviation')} min")

        # Save to database
        print("\n2. Saving to database...")
        collector._save_bus_positions(positions)

        # Verify
        count = db.query(BusPosition).filter_by(route_id='C51').count()
        print("\n3. Verification:")
        print(f"   Total C51 BusPositions in database: {count}")

        # Show a sample record
        sample = db.query(BusPosition).filter_by(route_id='C51').first()
        if sample:
            print("\n   Sample record:")
            print(f"     Vehicle: {sample.vehicle_id}")
            print(f"     Deviation: {sample.deviation} minutes")
            print(f"     Timestamp: {sample.timestamp}")
            print(f"     Direction: {sample.direction_text}")
            print(f"     Block: {sample.block_number}")
    else:
        print("   No vehicles found on route C51")

    print("\n" + "=" * 70)
    print("✓ BusPositions collection test complete")
    print("=" * 70)

finally:
    db.close()
