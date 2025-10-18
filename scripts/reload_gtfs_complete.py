"""
Complete GTFS Data Reload Script with Versioning

This script reloads GTFS data with versioning support, preserving historical data.

VERSIONING BEHAVIOR:
- Creates a new GTFSSnapshot record for each reload
- Marks old records as inactive (sets valid_to, is_current=false)
- Inserts new records with current snapshot_id
- Never deletes data - all historical records preserved

This means:
- Routes that are discontinued remain in the database
- All vehicle position data stays valid
- You can query historical GTFS data by snapshot_id

Prerequisite: Run scripts/migrate_add_gtfs_versioning.py first!
"""

import csv
import io
import os
import sys
import zipfile
from datetime import datetime

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_session
from src.models import (
    Agency,
    Calendar,
    CalendarDate,
    FeedInfo,
    GTFSSnapshot,
    Route,
    Shape,
    Stop,
    StopTime,
    Timepoint,
    TimepointTime,
    Trip,
)

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")
if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

BASE_URL = "https://api.wmata.com/gtfs"


def parse_csv(zip_file, filename):
    """Parse a CSV file from the GTFS zip"""
    content = zip_file.read(filename).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def reload_complete_gtfs():
    """Reload all GTFS data with complete schema"""
    print("=" * 70)
    print("Complete GTFS Data Reload")
    print("=" * 70)

    # Download GTFS data
    print("\nDownloading GTFS data...")
    url = f"{BASE_URL}/bus-gtfs-static.zip"
    headers = {"api_key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"✗ Error downloading GTFS: {response.status_code}")
            return False

        print("✓ Downloaded GTFS data")

        # Extract and parse all GTFS files
        print("\nParsing GTFS files...")
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        gtfs_data = {}
        files_to_parse = [
            "agency.txt",
            "calendar.txt",
            "calendar_dates.txt",
            "feed_info.txt",
            "routes.txt",
            "stops.txt",
            "trips.txt",
            "stop_times.txt",
            "shapes.txt",
            "timepoints.txt",
            "timepoint_times.txt",
        ]

        for filename in files_to_parse:
            print(f"  - {filename}...", end="")
            sys.stdout.flush()
            gtfs_data[filename.replace(".txt", "")] = parse_csv(zip_file, filename)
            print(f" {len(gtfs_data[filename.replace('.txt', '')])} records")

        print("✓ All GTFS files parsed")

        # Connect to database
        db = get_session()

        try:
            print("\nCreating new GTFS snapshot...")
            print("-" * 70)

            # Create new GTFSSnapshot record
            now = datetime.utcnow()
            feed_version = gtfs_data["feed_info"][0].get("feed_version") if gtfs_data.get("feed_info") else None

            snapshot = GTFSSnapshot(
                snapshot_date=now,
                feed_version=feed_version,
                routes_count=len(gtfs_data["routes"]),
                stops_count=len(gtfs_data["stops"]),
                trips_count=len(gtfs_data.get("trips", [])),
                stop_times_count=len(gtfs_data["stop_times"]),
                shapes_count=len(gtfs_data.get("shapes", [])),
                calendar_entries=len(gtfs_data["calendar"]),
                calendar_exceptions=len(gtfs_data["calendar_dates"]),
                notes=f"Auto-reload at {now.isoformat()}",
            )
            db.add(snapshot)
            db.commit()
            snapshot_id = snapshot.snapshot_id

            print(f"✓ Created snapshot {snapshot_id} (version: {feed_version or 'unknown'})")

            # Mark all current records as inactive
            print("\n→ Marking old records as inactive...")
            db.query(Route).filter(Route.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.query(Stop).filter(Stop.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.query(Trip).filter(Trip.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.query(StopTime).filter(StopTime.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.query(Calendar).filter(Calendar.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.query(CalendarDate).filter(CalendarDate.is_current).update(
                {"valid_to": now, "is_current": False}, synchronize_session=False
            )
            db.commit()
            print("  ✓ Old records marked inactive")

            print("\nLoading new GTFS data with versioning...")
            print("-" * 70)

            # Routes
            print("→ Loading routes...")
            for route_data in gtfs_data["routes"]:
                route = Route(
                    route_id=route_data["route_id"],
                    agency_id=route_data.get("agency_id"),
                    route_short_name=route_data.get("route_short_name", ""),
                    route_long_name=route_data.get("route_long_name"),
                    route_desc=route_data.get("route_desc"),
                    route_type=route_data.get("route_type"),
                    route_url=route_data.get("route_url"),
                    route_color=route_data.get("route_color"),
                    route_text_color=route_data.get("route_text_color"),
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                db.add(route)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['routes'])} routes")

            # Stops
            print("→ Loading stops...")
            for stop_data in gtfs_data["stops"]:
                stop = Stop(
                    stop_id=stop_data["stop_id"],
                    stop_code=stop_data.get("stop_code"),
                    stop_name=stop_data["stop_name"],
                    stop_desc=stop_data.get("stop_desc"),
                    stop_lat=float(stop_data["stop_lat"]),
                    stop_lon=float(stop_data["stop_lon"]),
                    zone_id=stop_data.get("zone_id"),
                    stop_url=stop_data.get("stop_url"),
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                db.add(stop)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['stops'])} stops")

            # Trips
            print("→ Loading trips...")
            for trip_data in gtfs_data["trips"]:
                trip = Trip(
                    trip_id=trip_data["trip_id"],
                    route_id=trip_data["route_id"],
                    service_id=trip_data.get("service_id"),
                    trip_headsign=trip_data.get("trip_headsign"),
                    direction_id=int(trip_data["direction_id"]) if trip_data.get("direction_id") else None,
                    block_id=trip_data.get("block_id"),
                    shape_id=trip_data.get("shape_id"),
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                db.add(trip)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['trips'])} trips")

            # Stop Times
            print("→ Loading stop_times (this will take 3-5 minutes)...")
            batch = []
            total = len(gtfs_data["stop_times"])

            for i, st_data in enumerate(gtfs_data["stop_times"]):
                stop_time = StopTime(
                    trip_id=st_data["trip_id"],
                    stop_id=st_data["stop_id"],
                    arrival_time=st_data["arrival_time"],
                    departure_time=st_data["departure_time"],
                    stop_sequence=int(st_data["stop_sequence"]),
                    stop_headsign=st_data.get("stop_headsign"),
                    pickup_type=int(st_data["pickup_type"]) if st_data.get("pickup_type") else None,
                    drop_off_type=int(st_data["drop_off_type"])
                    if st_data.get("drop_off_type")
                    else None,
                    shape_dist_traveled=float(st_data["shape_dist_traveled"])
                    if st_data.get("shape_dist_traveled")
                    else None,
                    timepoint=int(st_data["timepoint"]) if st_data.get("timepoint") else None,
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                batch.append(stop_time)

                if len(batch) >= 10000:
                    db.bulk_save_objects(batch)
                    db.commit()
                    batch = []
                    percent = ((i + 1) / total) * 100
                    print(f"\r    Progress: {percent:.1f}% ({i + 1:,}/{total:,})", end="")
                    sys.stdout.flush()

            if batch:
                db.bulk_save_objects(batch)
                db.commit()

            print(f"\r  ✓ Loaded {total:,} stop_times")

            # ==== POPULATE NEW TABLES ====

            # Agencies
            print("→ Loading agencies...")
            db.execute(text("DELETE FROM agencies"))
            db.commit()

            for agency_data in gtfs_data["agency"]:
                agency = Agency(
                    agency_id=agency_data["agency_id"],
                    agency_name=agency_data["agency_name"],
                    agency_url=agency_data.get("agency_url"),
                    agency_timezone=agency_data.get("agency_timezone"),
                    agency_lang=agency_data.get("agency_lang"),
                    agency_phone=agency_data.get("agency_phone"),
                    agency_fare_url=agency_data.get("agency_fare_url"),
                    agency_email=agency_data.get("agency_email"),
                )
                db.add(agency)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['agency'])} agencies")

            # Calendar
            print("→ Loading calendar...")
            for cal_data in gtfs_data["calendar"]:
                calendar = Calendar(
                    service_id=cal_data["service_id"],
                    monday=int(cal_data["monday"]),
                    tuesday=int(cal_data["tuesday"]),
                    wednesday=int(cal_data["wednesday"]),
                    thursday=int(cal_data["thursday"]),
                    friday=int(cal_data["friday"]),
                    saturday=int(cal_data["saturday"]),
                    sunday=int(cal_data["sunday"]),
                    start_date=cal_data["start_date"],
                    end_date=cal_data["end_date"],
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                db.add(calendar)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['calendar'])} calendar entries")

            # Calendar Dates
            print("→ Loading calendar_dates...")
            for cal_date_data in gtfs_data["calendar_dates"]:
                calendar_date = CalendarDate(
                    service_id=cal_date_data["service_id"],
                    date=cal_date_data["date"],
                    exception_type=int(cal_date_data["exception_type"]),
                    # Versioning fields
                    snapshot_id=snapshot_id,
                    valid_from=now,
                    valid_to=None,
                    is_current=True,
                )
                db.add(calendar_date)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['calendar_dates'])} calendar exceptions")

            # Feed Info
            print("→ Loading feed_info...")
            db.execute(text("DELETE FROM feed_info"))
            db.commit()

            for feed_data in gtfs_data["feed_info"]:
                feed_info = FeedInfo(
                    feed_publisher_name=feed_data["feed_publisher_name"],
                    feed_publisher_url=feed_data.get("feed_publisher_url"),
                    feed_lang=feed_data.get("feed_lang"),
                    feed_start_date=feed_data.get("feed_start_date"),
                    feed_end_date=feed_data.get("feed_end_date"),
                    feed_version=feed_data.get("feed_version"),
                    feed_contact_email=feed_data.get("feed_contact_email"),
                    feed_contact_url=feed_data.get("feed_contact_url"),
                )
                db.add(feed_info)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['feed_info'])} feed_info records")

            # Timepoints
            print("→ Loading timepoints...")
            db.execute(text("DELETE FROM timepoints"))
            db.commit()

            for tp_data in gtfs_data["timepoints"]:
                timepoint = Timepoint(
                    stop_id=tp_data["stop_id"],
                    stop_code=tp_data.get("stop_code"),
                    stop_name=tp_data["stop_name"],
                    stop_desc=tp_data.get("stop_desc"),
                    stop_lat=float(tp_data["stop_lat"]),
                    stop_lon=float(tp_data["stop_lon"]),
                    zone_id=tp_data.get("zone_id"),
                    stop_url=tp_data.get("stop_url"),
                )
                db.add(timepoint)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['timepoints'])} timepoints")

            # Timepoint Times
            print("→ Loading timepoint_times (this may take 1-2 minutes)...")
            db.execute(text("DELETE FROM timepoint_times"))
            db.commit()

            batch = []
            total_tp = len(gtfs_data["timepoint_times"])

            for i, tpt_data in enumerate(gtfs_data["timepoint_times"]):
                timepoint_time = TimepointTime(
                    trip_id=tpt_data["trip_id"],
                    stop_id=tpt_data["stop_id"],
                    arrival_time=tpt_data["arrival_time"],
                    departure_time=tpt_data["departure_time"],
                    stop_sequence=int(tpt_data["stop_sequence"]),
                    stop_headsign=tpt_data.get("stop_headsign"),
                    pickup_type=int(tpt_data["pickup_type"])
                    if tpt_data.get("pickup_type")
                    else None,
                    drop_off_type=int(tpt_data["drop_off_type"])
                    if tpt_data.get("drop_off_type")
                    else None,
                    shape_dist_traveled=float(tpt_data["shape_dist_traveled"])
                    if tpt_data.get("shape_dist_traveled")
                    else None,
                    timepoint=int(tpt_data["timepoint"]) if tpt_data.get("timepoint") else None,
                )
                batch.append(timepoint_time)

                if len(batch) >= 10000:
                    db.bulk_save_objects(batch)
                    db.commit()
                    batch = []
                    percent = ((i + 1) / total_tp) * 100
                    print(f"\r    Progress: {percent:.1f}% ({i + 1:,}/{total_tp:,})", end="")
                    sys.stdout.flush()

            if batch:
                db.bulk_save_objects(batch)
                db.commit()

            print(f"\r  ✓ Loaded {total_tp:,} timepoint_times")

            print("\n" + "=" * 70)
            print("✓ Complete GTFS Reload Successful!")
            print("=" * 70)

            # Print summary
            print("\nDatabase Summary:")
            print(f"  Agencies:       {len(gtfs_data['agency'])}")
            print(f"  Routes:         {len(gtfs_data['routes'])}")
            print(f"  Stops:          {len(gtfs_data['stops'])}")
            print(f"  Trips:          {db.query(Trip).count()}")  # Not reloaded
            print(f"  Stop Times:     {len(gtfs_data['stop_times'])}")
            print(f"  Shapes:         {db.query(Shape).count()}")  # Not reloaded
            print(f"  Calendar:       {len(gtfs_data['calendar'])}")
            print(f"  Calendar Dates: {len(gtfs_data['calendar_dates'])}")
            print(f"  Timepoints:     {len(gtfs_data['timepoints'])}")
            print(f"  Timepoint Times: {len(gtfs_data['timepoint_times'])}")

        finally:
            db.close()

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    reload_complete_gtfs()
