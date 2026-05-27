import csv
import io
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from sqlalchemy.orm import Session

from src.archive_writer import JsonlArchiveWriter
from src.database import get_session, init_db
from src.models import CollectorHeartbeat, Route, Shape, Stop, StopTime, Trip, VehiclePosition
from src.timezones import eastern_date_from_naive_utc, from_epoch_naive_utc, utcnow_naive
from src.upsert_helpers import upsert_trip_update_state


def _service_date_for_row(row: dict):
    """Return the Eastern service_date for a trip-update row.

    Prefers ``trip_start_date`` (YYYYMMDD string from GTFS-RT
    ``tripDescriptor.start_date``) when present and parseable; otherwise
    falls back to the Eastern calendar day of ``snapshot_ts``.

    Module-level (not a method) so the replay tool can reuse it without
    pulling in the WMATADataCollector context.
    """
    raw = row.get("trip_start_date")
    if raw:
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            pass  # fall through to snapshot_ts inference
    return eastern_date_from_naive_utc(row["snapshot_ts"])


# Load environment variables from .env file
load_dotenv()

# Your WMATA API key from environment
API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

BASE_URL = "https://api.wmata.com/gtfs"


class WMATADataCollector:
    def __init__(
        self,
        api_key,
        db_session: Session = None,
        archive_root: Path | str | None = None,
    ):
        """Construct a collector.

        ``archive_root`` overrides the JSONL cold-archive directory. When
        ``None``, defaults to ``REPO_ROOT / archive / raw_snapshots``.
        Tests must pass ``archive_root=tmp_path`` to keep fixture rows out
        of the live archive — every instantiation opens a writer, so
        leaving the default path causes per-process orphan files to
        accumulate under the real archive on every test run.
        """
        self.api_key = api_key
        self.headers = {"api_key": api_key}
        self.gtfs_data = {}
        self.db = db_session

        # Cold archive: raw rows go to compressed JSONL daily files.
        # Path matches the existing archive_trip_update_snapshots.py
        # convention (REPO_ROOT / "archive" / ...).
        if archive_root is None:
            archive_root = Path(__file__).resolve().parent.parent / "archive" / "raw_snapshots"
        self._archive_writer = JsonlArchiveWriter(archive_dir=archive_root)

    def close(self) -> None:
        """Flush and close the archive writer. Idempotent."""
        if hasattr(self, "_archive_writer") and self._archive_writer is not None:
            self._archive_writer.close()

    def download_gtfs_static(self, save_to_db=True, timeout=30):
        """Download and parse GTFS static data"""
        print("Downloading GTFS static data (~40MB, this may take 10-20 seconds)...")
        sys.stdout.flush()

        url = f"{BASE_URL}/bus-gtfs-static.zip"

        try:
            response = requests.get(url, headers=self.headers, timeout=timeout, stream=True)

            if response.status_code != 200:
                print(f"✗ Error downloading GTFS: {response.status_code}")
                return False

            # Download with progress indicator
            total_size = int(response.headers.get("content-length", 0))
            content = bytearray()
            downloaded = 0

            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    content.extend(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(
                            f"\r  Downloading: {percent:.1f}% ({downloaded / 1024 / 1024:.1f}MB/{total_size / 1024 / 1024:.1f}MB)",
                            end="",
                        )
                        sys.stdout.flush()

            print("\n  ✓ Download complete")
            sys.stdout.flush()

        except requests.exceptions.Timeout:
            print(f"\n✗ Timeout: Download took longer than {timeout} seconds")
            return False
        except requests.exceptions.RequestException as e:
            print(f"\n✗ Network error: {e}")
            return False

        # Extract zip file in memory
        print("  Extracting and parsing GTFS files...")
        sys.stdout.flush()

        try:
            zip_file = zipfile.ZipFile(io.BytesIO(bytes(content)))

            # Parse relevant files
            print("    - Parsing routes...", end="")
            sys.stdout.flush()
            self.gtfs_data["routes"] = self._parse_csv(zip_file, "routes.txt")
            print(f" {len(self.gtfs_data['routes'])} routes")

            print("    - Parsing stops...", end="")
            sys.stdout.flush()
            self.gtfs_data["stops"] = self._parse_csv(zip_file, "stops.txt")
            print(f" {len(self.gtfs_data['stops'])} stops")

            print("    - Parsing trips...", end="")
            sys.stdout.flush()
            self.gtfs_data["trips"] = self._parse_csv(zip_file, "trips.txt")
            print(f" {len(self.gtfs_data['trips'])} trips")

            print("    - Parsing stop times...", end="")
            sys.stdout.flush()
            self.gtfs_data["stop_times"] = self._parse_csv(zip_file, "stop_times.txt")
            print(f" {len(self.gtfs_data['stop_times'])} stop times")

            print("    - Parsing shapes...", end="")
            sys.stdout.flush()
            self.gtfs_data["shapes"] = self._parse_csv(zip_file, "shapes.txt")
            print(f" {len(self.gtfs_data['shapes'])} shape points")

            print("  ✓ GTFS static data parsed successfully")
            sys.stdout.flush()

        except Exception as e:
            print(f"\n✗ Error parsing GTFS data: {e}")
            return False

        # Save to database if requested and db session available
        if save_to_db and self.db:
            self._save_gtfs_to_db()

        return True

    def _parse_csv(self, zip_file, filename):
        """Parse a CSV file from the GTFS zip"""
        content = zip_file.read(filename).decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _save_gtfs_to_db(self):
        """Save GTFS static data to database"""
        print("  Saving GTFS static data to database...")
        sys.stdout.flush()

        try:
            # Save routes (with upsert logic)
            print("    - Saving routes...", end="")
            sys.stdout.flush()
            new_routes = 0
            for route_data in self.gtfs_data["routes"]:
                route = self.db.query(Route).filter_by(route_id=route_data["route_id"]).first()
                if not route:
                    route = Route(
                        route_id=route_data["route_id"],
                        route_short_name=route_data.get("route_short_name", ""),
                        route_long_name=route_data.get("route_long_name", ""),
                        route_type=route_data.get("route_type", ""),
                    )
                    self.db.add(route)
                    new_routes += 1

            self.db.commit()
            print(f" {new_routes} new routes")

            # Save stops
            print("    - Saving stops...", end="")
            sys.stdout.flush()
            new_stops = 0
            for stop_data in self.gtfs_data["stops"]:
                stop = self.db.query(Stop).filter_by(stop_id=stop_data["stop_id"]).first()
                if not stop:
                    stop = Stop(
                        stop_id=stop_data["stop_id"],
                        stop_name=stop_data["stop_name"],
                        stop_lat=float(stop_data["stop_lat"]),
                        stop_lon=float(stop_data["stop_lon"]),
                    )
                    self.db.add(stop)
                    new_stops += 1

            self.db.commit()
            print(f" {new_stops} new stops")

            # Save trips
            print("    - Saving trips...", end="")
            sys.stdout.flush()
            new_trips = 0
            for trip_data in self.gtfs_data["trips"]:
                trip = self.db.query(Trip).filter_by(trip_id=trip_data["trip_id"]).first()
                if not trip:
                    trip = Trip(
                        trip_id=trip_data["trip_id"],
                        route_id=trip_data["route_id"],
                        service_id=trip_data.get("service_id", ""),
                        trip_headsign=trip_data.get("trip_headsign", ""),
                        direction_id=int(trip_data["direction_id"])
                        if trip_data.get("direction_id")
                        else None,
                        block_id=trip_data.get("block_id"),
                        shape_id=trip_data.get("shape_id"),
                    )
                    self.db.add(trip)
                    new_trips += 1

            self.db.commit()
            print(f" {new_trips} new trips")

            # Save stop_times with progress (optimized for first run)
            print("    - Checking if stop times already loaded...", end="")
            sys.stdout.flush()
            existing_count = self.db.query(StopTime).count()

            if existing_count > 0:
                print(f" {existing_count} stop times already in database (skipping)")
                stop_times_count = 0
            else:
                print(" database empty, loading all stop times...")
                print("      This will take 3-5 minutes for ~5.5M records...")
                sys.stdout.flush()

                stop_times_count = 0
                total_stop_times = len(self.gtfs_data["stop_times"])
                batch = []

                for i, st_data in enumerate(self.gtfs_data["stop_times"]):
                    stop_time = StopTime(
                        trip_id=st_data["trip_id"],
                        stop_id=st_data["stop_id"],
                        arrival_time=st_data["arrival_time"],
                        departure_time=st_data["departure_time"],
                        stop_sequence=int(st_data["stop_sequence"]),
                    )
                    batch.append(stop_time)
                    stop_times_count += 1

                    # Bulk insert in batches of 10000
                    if len(batch) >= 10000:
                        self.db.bulk_save_objects(batch)
                        self.db.commit()
                        batch = []
                        percent = ((i + 1) / total_stop_times) * 100
                        print(
                            f"\r      Progress: {percent:.1f}% ({i + 1:,}/{total_stop_times:,})",
                            end="",
                        )
                        sys.stdout.flush()

                # Save remaining records
                if batch:
                    self.db.bulk_save_objects(batch)
                    self.db.commit()

                print(f"\r    - Saving stop times... {stop_times_count:,} new stop times")

            # Save shapes with progress (optimized for first run)
            print("    - Checking if shapes already loaded...", end="")
            sys.stdout.flush()
            existing_shape_count = self.db.query(Shape).count()

            if existing_shape_count > 0:
                print(f" {existing_shape_count} shape points already in database (skipping)")
                shapes_count = 0
            else:
                print(" database empty, loading all shape points...")
                print("      This may take 1-2 minutes for shape data...")
                sys.stdout.flush()

                shapes_count = 0
                total_shapes = len(self.gtfs_data["shapes"])
                batch = []

                for i, shape_data in enumerate(self.gtfs_data["shapes"]):
                    shape = Shape(
                        shape_id=shape_data["shape_id"],
                        shape_pt_lat=float(shape_data["shape_pt_lat"]),
                        shape_pt_lon=float(shape_data["shape_pt_lon"]),
                        shape_pt_sequence=int(shape_data["shape_pt_sequence"]),
                        shape_dist_traveled=float(shape_data["shape_dist_traveled"])
                        if shape_data.get("shape_dist_traveled")
                        else None,
                    )
                    batch.append(shape)
                    shapes_count += 1

                    # Bulk insert in batches of 10000
                    if len(batch) >= 10000:
                        self.db.bulk_save_objects(batch)
                        self.db.commit()
                        batch = []
                        percent = ((i + 1) / total_shapes) * 100
                        print(
                            f"\r      Progress: {percent:.1f}% ({i + 1:,}/{total_shapes:,})", end=""
                        )
                        sys.stdout.flush()

                # Save remaining records
                if batch:
                    self.db.bulk_save_objects(batch)
                    self.db.commit()

                print(f"\r    - Saving shapes... {shapes_count:,} new shape points")
            print("  ✓ GTFS static data saved to database successfully")
            sys.stdout.flush()

        except Exception as e:
            print(f"\n✗ Error saving to database: {e}")
            self.db.rollback()
            raise

    def get_route_info(self, route_short_name):
        """Get information about a specific route (e.g., 'C51')"""
        routes = [r for r in self.gtfs_data["routes"] if r["route_short_name"] == route_short_name]

        if not routes:
            print(f"Route {route_short_name} not found")
            return None

        route = routes[0]
        print("\nRoute Information:")
        print(f"  Route ID: {route['route_id']}")
        print(f"  Route Name: {route['route_long_name']}")
        print(f"  Route Type: {route['route_type']}")

        return route

    def get_route_stops(self, route_id):
        """Get all stops for a specific route"""
        # Find trips for this route
        trips = [t for t in self.gtfs_data["trips"] if t["route_id"] == route_id]

        if not trips:
            return []

        # Get stop times for the first trip (as example)
        trip_id = trips[0]["trip_id"]
        stop_times = [st for st in self.gtfs_data["stop_times"] if st["trip_id"] == trip_id]

        # Sort by stop sequence
        stop_times.sort(key=lambda x: int(x["stop_sequence"]))

        # Get stop details
        stops = []
        for st in stop_times:
            stop_info = next(
                (s for s in self.gtfs_data["stops"] if s["stop_id"] == st["stop_id"]), None
            )
            if stop_info:
                stops.append(
                    {
                        "sequence": st["stop_sequence"],
                        "stop_id": st["stop_id"],
                        "stop_name": stop_info["stop_name"],
                        "lat": stop_info["stop_lat"],
                        "lon": stop_info["stop_lon"],
                        "scheduled_arrival": st["arrival_time"],
                    }
                )

        return stops

    def get_realtime_vehicle_positions(self, timeout=10):
        """Fetch real-time vehicle positions"""
        print("\nFetching real-time vehicle positions...")
        sys.stdout.flush()

        url = f"{BASE_URL}/bus-gtfsrt-vehiclepositions.pb"

        try:
            response = requests.get(url, headers=self.headers, timeout=timeout)

            if response.status_code != 200:
                print(f"✗ Error fetching vehicle positions: {response.status_code}")
                return []

            # Parse protobuf
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)

            vehicles = []
            for entity in feed.entity:
                if entity.HasField("vehicle"):
                    vehicle = entity.vehicle
                    vehicles.append(
                        {
                            # Vehicle identification
                            "vehicle_id": vehicle.vehicle.id
                            if vehicle.vehicle.HasField("id")
                            else None,
                            "vehicle_label": vehicle.vehicle.label
                            if vehicle.vehicle.HasField("label")
                            else None,
                            # Trip information
                            "route_id": vehicle.trip.route_id
                            if vehicle.trip.HasField("route_id")
                            else None,
                            "trip_id": vehicle.trip.trip_id
                            if vehicle.trip.HasField("trip_id")
                            else None,
                            "direction_id": vehicle.trip.direction_id
                            if vehicle.trip.HasField("direction_id")
                            else None,
                            "trip_start_time": vehicle.trip.start_time
                            if vehicle.trip.HasField("start_time")
                            else None,
                            "trip_start_date": vehicle.trip.start_date
                            if vehicle.trip.HasField("start_date")
                            else None,
                            "schedule_relationship": vehicle.trip.schedule_relationship
                            if vehicle.trip.HasField("schedule_relationship")
                            else None,
                            # Position data
                            "latitude": vehicle.position.latitude
                            if vehicle.position.HasField("latitude")
                            else None,
                            "longitude": vehicle.position.longitude
                            if vehicle.position.HasField("longitude")
                            else None,
                            "bearing": vehicle.position.bearing
                            if vehicle.position.HasField("bearing")
                            else None,
                            "speed": vehicle.position.speed
                            if vehicle.position.HasField("speed")
                            else None,
                            # Stop information
                            "current_stop_sequence": vehicle.current_stop_sequence
                            if vehicle.HasField("current_stop_sequence")
                            else None,
                            "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else None,
                            "current_status": vehicle.current_status
                            if vehicle.HasField("current_status")
                            else None,
                            # Additional data
                            "timestamp": vehicle.timestamp
                            if vehicle.HasField("timestamp")
                            else None,
                            "occupancy_status": vehicle.occupancy_status
                            if vehicle.HasField("occupancy_status")
                            else None,
                        }
                    )

            print(f"  ✓ Found {len(vehicles)} active vehicles")
            return vehicles

        except requests.exceptions.Timeout:
            print(f"✗ Timeout: Request took longer than {timeout} seconds")
            return []
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error: {e}")
            return []
        except Exception as e:
            print(f"✗ Error parsing vehicle positions: {e}")
            return []

    def get_route_vehicles(self, route_id, vehicles=None, save_to_db=True):
        """Filter vehicles for a specific route"""
        if vehicles is None:
            vehicles = self.get_realtime_vehicle_positions()

        route_vehicles = [v for v in vehicles if v["route_id"] == route_id]

        print(f"\nFound {len(route_vehicles)} vehicles on route {route_id}:")
        for v in route_vehicles:
            timestamp_str = (
                datetime.fromtimestamp(v["timestamp"]).strftime("%H:%M:%S")
                if v["timestamp"]
                else "N/A"
            )
            print(
                f"  Vehicle {v['vehicle_id']}: Lat={v['latitude']:.4f}, Lon={v['longitude']:.4f}, Time={timestamp_str}"
            )

        # Save to database if requested and db session available
        if save_to_db and self.db:
            self._save_vehicle_positions(route_vehicles)

        return route_vehicles

    def get_realtime_trip_updates(self, timeout=10):
        """Fetch the GTFS-RT TripUpdates feed and flatten it to per-stop rows.

        Returns a tuple ``(snapshot_ts, rows)`` where ``snapshot_ts`` is the
        feed header timestamp (datetime, UTC) and ``rows`` is a list of dicts
        — one per StopTimeUpdate — ready for ``_save_trip_updates``. Returns
        ``(None, [])`` on any network or parse error so callers can keep
        polling without crashing the loop.
        """
        print("\nFetching real-time trip updates...")
        sys.stdout.flush()

        url = f"{BASE_URL}/bus-gtfsrt-tripupdates.pb"

        try:
            response = requests.get(url, headers=self.headers, timeout=timeout)

            if response.status_code != 200:
                print(f"✗ Error fetching trip updates: {response.status_code}")
                return None, []

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)

            snapshot_ts = (
                from_epoch_naive_utc(feed.header.timestamp)
                if feed.header.timestamp
                else utcnow_naive()
            )

            rows = []
            for entity in feed.entity:
                if not entity.HasField("trip_update"):
                    continue
                tu = entity.trip_update
                trip_id = tu.trip.trip_id or None
                route_id = tu.trip.route_id or None
                vehicle_id = tu.vehicle.id if (tu.HasField("vehicle") and tu.vehicle.id) else None
                trip_start_date = (
                    tu.trip.start_date
                    if tu.trip.HasField("start_date") and tu.trip.start_date
                    else None
                )

                for stu in tu.stop_time_update:
                    if not stu.stop_id:
                        continue

                    predicted_arrival_ts = None
                    if stu.HasField("arrival") and stu.arrival.HasField("time"):
                        predicted_arrival_ts = from_epoch_naive_utc(stu.arrival.time)

                    predicted_departure_ts = None
                    if stu.HasField("departure") and stu.departure.HasField("time"):
                        predicted_departure_ts = from_epoch_naive_utc(stu.departure.time)

                    if stu.HasField("schedule_relationship"):
                        schedule_relationship = stu.ScheduleRelationship.Name(
                            stu.schedule_relationship
                        )
                    else:
                        schedule_relationship = "UNSET"

                    rows.append(
                        {
                            "snapshot_ts": snapshot_ts,
                            "trip_id": trip_id,
                            "route_id": route_id,
                            "vehicle_id": vehicle_id,
                            "stop_id": stu.stop_id,
                            "stop_sequence": stu.stop_sequence
                            if stu.HasField("stop_sequence")
                            else None,
                            "predicted_arrival_ts": predicted_arrival_ts,
                            "predicted_departure_ts": predicted_departure_ts,
                            "schedule_relationship": schedule_relationship,
                            "trip_start_date": trip_start_date,
                        }
                    )

            print(
                f"  ✓ Snapshot {snapshot_ts.isoformat()}: {len(feed.entity)} entities, "
                f"{len(rows)} stop_time_updates"
            )
            return snapshot_ts, rows

        except requests.exceptions.Timeout:
            print(f"✗ Timeout: Request took longer than {timeout} seconds")
            return None, []
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error: {e}")
            return None, []
        except Exception as e:
            print(f"✗ Error parsing trip updates: {e}")
            return None, []

    def _save_trip_updates(self, rows):
        """Upsert TripUpdate rows into ``trip_update_state`` and write a heartbeat.

        ``rows`` should be the second element returned by
        ``get_realtime_trip_updates``. Every raw row is appended to the
        JSONL cold archive before any DB writes, so the archive is the
        complete evidence trail regardless of what the DB write path does.

        The WMATA TripUpdates feed republishes every future stop on every
        poll. We pass ALL rows to ``upsert_trip_update_state`` (not a
        dedup-filtered subset) so ``final_snapshot_ts`` always reflects
        the latest poll timestamp, even when the prediction value hasn't
        changed.

        One ``collector_heartbeats`` row is written per call (per tick),
        keyed on ``snapshot_ts`` from the first row. This replaces the
        former ``trip_update_snapshots`` dual-write as the minute-bucket
        coverage signal used by ``src/data_completeness.py``.

        Note: ``trip_update_snapshots`` is no longer written to by this
        method (Phase E.2 cutover, NOTES-72). The table still exists in
        the schema and will be dropped in Phase F retirement.
        """
        if not rows:
            return 0

        for row in rows:
            # Archive EVERY raw row — the archive is the complete evidence trail.
            self._archive_writer.append(row, snapshot_ts=row["snapshot_ts"])

        # UPSERT into trip_update_state — rows missing stop_sequence can't be
        # keyed in state (it's part of the PK), so we drop them from the payload.
        # Archived rows are unaffected by this filter.
        upsert_payload = [
            {
                "trip_id": r["trip_id"],
                "stop_sequence": r["stop_sequence"],
                "service_date": _service_date_for_row(r),
                "stop_id": r["stop_id"],
                "vehicle_id": r.get("vehicle_id"),
                "snapshot_ts": r["snapshot_ts"],
                "predicted_arrival_ts": r.get("predicted_arrival_ts"),
                "schedule_relationship": r.get("schedule_relationship"),
            }
            for r in rows
            if r.get("stop_sequence") is not None
        ]
        if upsert_payload:
            upsert_trip_update_state(self.db, upsert_payload)

        # Write one heartbeat row per tick so data_completeness.py can count
        # minute-buckets of collector activity without reading snapshot rows.
        # Use the snapshot_ts from the first row as the tick timestamp.
        tick_ts = rows[0]["snapshot_ts"]
        self.db.add(CollectorHeartbeat(ts=tick_ts, collector_name="combined"))

        # Single commit covers both the state upsert and the heartbeat.
        self.db.commit()

        print(
            f"  Upserted {len(upsert_payload)} of {len(rows)} trip update rows "
            f"into trip_update_state; heartbeat ts={tick_ts.isoformat()}"
        )
        return len(upsert_payload)

    def _save_vehicle_positions(self, vehicles):
        """Save vehicle positions to database with all GTFS-RT fields"""
        saved_count = 0
        for vehicle_data in vehicles:
            vehicle_pos = VehiclePosition(
                # Vehicle identification
                vehicle_id=vehicle_data["vehicle_id"],
                vehicle_label=vehicle_data.get("vehicle_label"),
                # Trip information
                route_id=vehicle_data["route_id"],
                trip_id=vehicle_data["trip_id"],
                direction_id=vehicle_data.get("direction_id"),
                trip_start_time=vehicle_data.get("trip_start_time"),
                trip_start_date=vehicle_data.get("trip_start_date"),
                schedule_relationship=vehicle_data.get("schedule_relationship"),
                # Position data
                latitude=vehicle_data["latitude"],
                longitude=vehicle_data["longitude"],
                bearing=vehicle_data.get("bearing"),
                speed=vehicle_data.get("speed"),
                # Stop information
                current_stop_sequence=vehicle_data.get("current_stop_sequence"),
                stop_id=vehicle_data.get("stop_id"),
                current_status=vehicle_data.get("current_status"),
                # Additional data
                occupancy_status=vehicle_data.get("occupancy_status"),
                # Timestamps — naive UTC (see src/timezones.py for convention)
                timestamp=from_epoch_naive_utc(vehicle_data["timestamp"])
                if vehicle_data["timestamp"]
                else utcnow_naive(),
            )
            self.db.add(vehicle_pos)
            saved_count += 1

        self.db.commit()
        if saved_count > 0:
            print(f"  Saved {saved_count} vehicle positions to database")


def main():
    # Initialize database
    print("Initializing database...")
    init_db()

    # Get database session
    db = get_session()

    try:
        # Initialize collector with database session
        collector = WMATADataCollector(API_KEY, db_session=db)

        # Check if we have C51 route data in database
        print("\n" + "=" * 50)
        print("Testing C51 Bus Route")
        print("=" * 50)

        route = db.query(Route).filter_by(route_short_name="C51").first()

        if not route:
            print("\n✗ C51 route not found in database.")
            print("Run init_database.py first to load GTFS data.")
            return

        print(f"\n✓ Route: {route.route_short_name} - {route.route_long_name}")
        print(f"  Route ID: {route.route_id}")

        route_id = route.route_id

        # Get stop count for C51 from database
        c51_trips = db.query(Trip).filter_by(route_id=route_id).first()
        if c51_trips:
            stop_count = db.query(StopTime).filter_by(trip_id=c51_trips.trip_id).count()
            print(f"  Stops: {stop_count}")

        # Get real-time vehicle positions for C51
        print("\nCollecting real-time vehicle positions...")
        all_vehicles = collector.get_realtime_vehicle_positions()

        # Filter for C51 and save
        c51_vehicles = [v for v in all_vehicles if v["route_id"] == route_id]

        if c51_vehicles:
            print(f"\n✓ Found {len(c51_vehicles)} C51 vehicles:")
            for v in c51_vehicles:
                from datetime import datetime

                timestamp_str = (
                    datetime.fromtimestamp(v["timestamp"]).strftime("%H:%M:%S")
                    if v["timestamp"]
                    else "N/A"
                )
                print(
                    f"  Vehicle {v['vehicle_id']}: Lat={v['latitude']:.4f}, Lon={v['longitude']:.4f}, Time={timestamp_str}"
                )

            # Save to database
            collector._save_vehicle_positions(c51_vehicles)
        else:
            print(
                f"\n✗ No C51 vehicles currently active (found {len(all_vehicles)} total vehicles)"
            )

        print("\n" + "=" * 50)
        print("✓ C51 data collection complete!")
        print("=" * 50)

        # Show summary from database
        total_positions = db.query(VehiclePosition).count()
        c51_positions = db.query(VehiclePosition).filter_by(route_id=route_id).count()

        print("\nDatabase Summary:")
        print(f"  Routes in DB:          {db.query(Route).count()}")
        print(f"  Stops in DB:           {db.query(Stop).count()}")
        print(f"  Total Vehicle Records: {total_positions}")
        print(f"  C51 Vehicle Records:   {c51_positions}")

        print("\nNext steps:")
        print("1. Run this script periodically to collect more data")
        print("2. Use continuous_collector.py for automated collection")
        print("3. Calculate headways and on-time performance metrics")

    finally:
        # Close database session
        db.close()


if __name__ == "__main__":
    main()
