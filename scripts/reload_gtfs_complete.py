"""
Complete GTFS Data Reload Script

This script completely reloads GTFS data with ALL fields from ALL files.

WARNING: This will DROP and recreate tables that have new fields:
- routes (adds agency_id, route_desc, route_url, route_color, route_text_color)
- stops (adds stop_code, stop_desc, zone_id, stop_url)
- stop_times (adds stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, timepoint)

And will populate new tables:
- agencies
- calendar
- calendar_dates
- feed_info
- timepoints
- timepoint_times

Prerequisite: Run scripts/migrate_complete_gtfs_schema.py first!
"""

import csv
import io
import os
import sys
import zipfile

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_session
from src.models import (
    Agency,
    Calendar,
    CalendarDate,
    FeedInfo,
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
            print("\nDropping and recreating tables with new data...")
            print("-" * 70)

            # ==== DROP AND RECREATE TABLES WITH NEW FIELDS ====

            # Routes - drop and recreate
            print("→ Dropping routes table...")
            db.execute(text("DELETE FROM routes"))
            db.commit()

            print("  Reloading routes...")
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
                )
                db.add(route)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['routes'])} routes")

            # Stops - drop and recreate
            print("→ Dropping stops table...")
            db.execute(text("DELETE FROM stops"))
            db.commit()

            print("  Reloading stops...")
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
                )
                db.add(stop)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['stops'])} stops")

            # Stop Times - drop and recreate
            print("→ Dropping stop_times table...")
            db.execute(text("DELETE FROM stop_times"))
            db.commit()

            print("  Reloading stop_times (this will take 3-5 minutes)...")
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
            db.execute(text("DELETE FROM calendar"))
            db.commit()

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
                )
                db.add(calendar)

            db.commit()
            print(f"  ✓ Loaded {len(gtfs_data['calendar'])} calendar entries")

            # Calendar Dates
            print("→ Loading calendar_dates...")
            db.execute(text("DELETE FROM calendar_dates"))
            db.commit()

            for cal_date_data in gtfs_data["calendar_dates"]:
                calendar_date = CalendarDate(
                    service_id=cal_date_data["service_id"],
                    date=cal_date_data["date"],
                    exception_type=int(cal_date_data["exception_type"]),
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
