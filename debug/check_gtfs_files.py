"""
Check what files are in WMATA's GTFS static feed
"""
import io
import os
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WMATA_API_KEY")

print("Downloading WMATA GTFS static feed...")
url = "https://api.wmata.com/gtfs/bus-gtfs-static.zip"
headers = {"api_key": API_KEY}

response = requests.get(url, headers=headers, timeout=30)
if response.status_code != 200:
    print(f"Error: {response.status_code}")
    exit(1)

print("Checking files in GTFS zip...")
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

print("\nFiles in GTFS feed:")
for file_info in zip_file.filelist:
    print(f"  {file_info.filename}: {file_info.file_size:,} bytes")

# Check if frequencies.txt exists
if 'frequencies.txt' in zip_file.namelist():
    print("\n✓ frequencies.txt EXISTS!")
    content = zip_file.read('frequencies.txt').decode('utf-8-sig')
    lines = content.strip().split('\n')
    print(f"  Lines: {len(lines)}")
    print(f"  Header: {lines[0] if lines else 'empty'}")
    if len(lines) > 1:
        print(f"  Sample row: {lines[1]}")
else:
    print("\n✗ frequencies.txt does NOT exist in GTFS feed")

# Check a sample trip to see if it has stop_times
print("\nChecking if trips have stop_times...")
import csv

trips_content = zip_file.read('trips.txt').decode('utf-8-sig')
trips = list(csv.DictReader(io.StringIO(trips_content)))

# Get C51 trips
c51_trips = [t for t in trips if t.get('route_id') == 'C51']
print(f"C51 trips in GTFS: {len(c51_trips)}")

if c51_trips:
    print("\nSample C51 trip IDs from GTFS static:")
    for trip in c51_trips[:10]:
        print(f"  {trip['trip_id']} - {trip.get('trip_headsign', 'N/A')}")

    # Check if these have stop_times
    stop_times_content = zip_file.read('stop_times.txt').decode('utf-8-sig')
    stop_times = list(csv.DictReader(io.StringIO(stop_times_content)))

    sample_trip_id = c51_trips[0]['trip_id']
    stop_times_for_sample = [st for st in stop_times if st['trip_id'] == sample_trip_id]
    print(f"\nStop times for trip {sample_trip_id}: {len(stop_times_for_sample)}")

    # Now check vehicle trip IDs
    print("\nChecking if REALTIME vehicle trip_ids exist in GTFS static...")
    # These are the trip IDs we saw from vehicles
    vehicle_trip_ids = ['34188020', '6151020', '35104020', '23342090', '7215090']

    for vt_id in vehicle_trip_ids:
        trip_in_static = any(t['trip_id'] == vt_id for t in trips)
        stops_for_vt = len([st for st in stop_times if st['trip_id'] == vt_id])
        print(f"  {vt_id}: in trips.txt={trip_in_static}, stop_times={stops_for_vt}")
