"""
Update existing trips with shape_id and block_id from GTFS data.

This script downloads fresh GTFS data and updates the shape_id and block_id fields
for all existing trips in the database.
"""
import os
import sys
import requests
import zipfile
import io
import csv
from dotenv import load_dotenv
from src.database import get_session
from src.models import Trip

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

BASE_URL = "https://api.wmata.com/gtfs"


def main():
    print("=" * 70)
    print("Updating Trip Shape IDs and Block IDs from GTFS")
    print("=" * 70)

    # Download GTFS data
    print("\nDownloading GTFS data...")
    url = f"{BASE_URL}/bus-gtfs-static.zip"
    headers = {"api_key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"✗ Error downloading GTFS: {response.status_code}")
            return

        print("✓ Downloaded GTFS data")

        # Extract and parse trips.txt
        print("Parsing trips.txt...")
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        content = zip_file.read('trips.txt').decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))
        trips_data = list(reader)

        print(f"✓ Found {len(trips_data)} trips in GTFS")

        # Update database
        db = get_session()
        try:
            print("\nUpdating trips in database...")
            updated = 0
            not_found = 0

            for i, trip_data in enumerate(trips_data):
                if i % 10000 == 0 and i > 0:
                    print(f"  Progress: {i}/{len(trips_data)} trips processed...")

                trip = db.query(Trip).filter_by(trip_id=trip_data['trip_id']).first()
                if trip:
                    shape_id = trip_data.get('shape_id')
                    block_id = trip_data.get('block_id')
                    if shape_id:
                        trip.shape_id = shape_id
                    if block_id:
                        trip.block_id = block_id
                    if shape_id or block_id:
                        updated += 1
                else:
                    not_found += 1

                # Commit in batches
                if i % 1000 == 0:
                    db.commit()

            # Final commit
            db.commit()

            print(f"\n✓ Updated {updated} trips with shape_id and/or block_id")
            if not_found > 0:
                print(f"  (Note: {not_found} trips from GTFS not found in database)")

            # Verify update
            trips_with_shapes = db.query(Trip).filter(Trip.shape_id.isnot(None)).count()
            trips_with_blocks = db.query(Trip).filter(Trip.block_id.isnot(None)).count()
            total_trips = db.query(Trip).count()

            print(f"\nDatabase status:")
            print(f"  Total trips: {total_trips}")
            print(f"  Trips with shape_id: {trips_with_shapes} ({(trips_with_shapes/total_trips*100):.1f}%)")
            print(f"  Trips with block_id: {trips_with_blocks} ({(trips_with_blocks/total_trips*100):.1f}%)")

        finally:
            db.close()

        print("\n" + "=" * 70)
        print("✓ Update complete!")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
