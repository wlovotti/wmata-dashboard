import os
import sys
from dotenv import load_dotenv
import requests
import zipfile
import io
import csv
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from sqlalchemy.orm import Session
from src.database import get_session, init_db
from src.models import Route, Stop, Trip, StopTime, VehiclePosition

# Load environment variables from .env file
load_dotenv()

# Your WMATA API key from environment
API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

BASE_URL = "https://api.wmata.com/gtfs"

class WMATADataCollector:
    def __init__(self, api_key, db_session: Session = None):
        self.api_key = api_key
        self.headers = {"api_key": api_key}
        self.gtfs_data = {}
        self.db = db_session
        
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
            total_size = int(response.headers.get('content-length', 0))
            content = bytearray()
            downloaded = 0

            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    content.extend(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\r  Downloading: {percent:.1f}% ({downloaded/1024/1024:.1f}MB/{total_size/1024/1024:.1f}MB)", end='')
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
            print("    - Parsing routes...", end='')
            sys.stdout.flush()
            self.gtfs_data['routes'] = self._parse_csv(zip_file, 'routes.txt')
            print(f" {len(self.gtfs_data['routes'])} routes")

            print("    - Parsing stops...", end='')
            sys.stdout.flush()
            self.gtfs_data['stops'] = self._parse_csv(zip_file, 'stops.txt')
            print(f" {len(self.gtfs_data['stops'])} stops")

            print("    - Parsing trips...", end='')
            sys.stdout.flush()
            self.gtfs_data['trips'] = self._parse_csv(zip_file, 'trips.txt')
            print(f" {len(self.gtfs_data['trips'])} trips")

            print("    - Parsing stop times...", end='')
            sys.stdout.flush()
            self.gtfs_data['stop_times'] = self._parse_csv(zip_file, 'stop_times.txt')
            print(f" {len(self.gtfs_data['stop_times'])} stop times")

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
        content = zip_file.read(filename).decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _save_gtfs_to_db(self):
        """Save GTFS static data to database"""
        print("  Saving GTFS static data to database...")
        sys.stdout.flush()

        try:
            # Save routes (with upsert logic)
            print("    - Saving routes...", end='')
            sys.stdout.flush()
            new_routes = 0
            for route_data in self.gtfs_data['routes']:
                route = self.db.query(Route).filter_by(route_id=route_data['route_id']).first()
                if not route:
                    route = Route(
                        route_id=route_data['route_id'],
                        route_short_name=route_data.get('route_short_name', ''),
                        route_long_name=route_data.get('route_long_name', ''),
                        route_type=route_data.get('route_type', '')
                    )
                    self.db.add(route)
                    new_routes += 1

            self.db.commit()
            print(f" {new_routes} new routes")

            # Save stops
            print("    - Saving stops...", end='')
            sys.stdout.flush()
            new_stops = 0
            for stop_data in self.gtfs_data['stops']:
                stop = self.db.query(Stop).filter_by(stop_id=stop_data['stop_id']).first()
                if not stop:
                    stop = Stop(
                        stop_id=stop_data['stop_id'],
                        stop_name=stop_data['stop_name'],
                        stop_lat=float(stop_data['stop_lat']),
                        stop_lon=float(stop_data['stop_lon'])
                    )
                    self.db.add(stop)
                    new_stops += 1

            self.db.commit()
            print(f" {new_stops} new stops")

            # Save trips
            print("    - Saving trips...", end='')
            sys.stdout.flush()
            new_trips = 0
            for trip_data in self.gtfs_data['trips']:
                trip = self.db.query(Trip).filter_by(trip_id=trip_data['trip_id']).first()
                if not trip:
                    trip = Trip(
                        trip_id=trip_data['trip_id'],
                        route_id=trip_data['route_id'],
                        service_id=trip_data.get('service_id', ''),
                        trip_headsign=trip_data.get('trip_headsign', ''),
                        direction_id=int(trip_data['direction_id']) if trip_data.get('direction_id') else None
                    )
                    self.db.add(trip)
                    new_trips += 1

            self.db.commit()
            print(f" {new_trips} new trips")

            # Save stop_times with progress (optimized for first run)
            print("    - Checking if stop times already loaded...", end='')
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
                total_stop_times = len(self.gtfs_data['stop_times'])
                batch = []

                for i, st_data in enumerate(self.gtfs_data['stop_times']):
                    stop_time = StopTime(
                        trip_id=st_data['trip_id'],
                        stop_id=st_data['stop_id'],
                        arrival_time=st_data['arrival_time'],
                        departure_time=st_data['departure_time'],
                        stop_sequence=int(st_data['stop_sequence'])
                    )
                    batch.append(stop_time)
                    stop_times_count += 1

                    # Bulk insert in batches of 10000
                    if len(batch) >= 10000:
                        self.db.bulk_save_objects(batch)
                        self.db.commit()
                        batch = []
                        percent = ((i + 1) / total_stop_times) * 100
                        print(f"\r      Progress: {percent:.1f}% ({i+1:,}/{total_stop_times:,})", end='')
                        sys.stdout.flush()

                # Save remaining records
                if batch:
                    self.db.bulk_save_objects(batch)
                    self.db.commit()

                print(f"\r    - Saving stop times... {stop_times_count:,} new stop times")
            print("  ✓ GTFS static data saved to database successfully")
            sys.stdout.flush()

        except Exception as e:
            print(f"\n✗ Error saving to database: {e}")
            self.db.rollback()
            raise
    
    def get_route_info(self, route_short_name):
        """Get information about a specific route (e.g., 'C51')"""
        routes = [r for r in self.gtfs_data['routes'] 
                  if r['route_short_name'] == route_short_name]
        
        if not routes:
            print(f"Route {route_short_name} not found")
            return None
        
        route = routes[0]
        print(f"\nRoute Information:")
        print(f"  Route ID: {route['route_id']}")
        print(f"  Route Name: {route['route_long_name']}")
        print(f"  Route Type: {route['route_type']}")
        
        return route
    
    def get_route_stops(self, route_id):
        """Get all stops for a specific route"""
        # Find trips for this route
        trips = [t for t in self.gtfs_data['trips'] 
                 if t['route_id'] == route_id]
        
        if not trips:
            return []
        
        # Get stop times for the first trip (as example)
        trip_id = trips[0]['trip_id']
        stop_times = [st for st in self.gtfs_data['stop_times'] 
                      if st['trip_id'] == trip_id]
        
        # Sort by stop sequence
        stop_times.sort(key=lambda x: int(x['stop_sequence']))
        
        # Get stop details
        stops = []
        for st in stop_times:
            stop_info = next((s for s in self.gtfs_data['stops'] 
                            if s['stop_id'] == st['stop_id']), None)
            if stop_info:
                stops.append({
                    'sequence': st['stop_sequence'],
                    'stop_id': st['stop_id'],
                    'stop_name': stop_info['stop_name'],
                    'lat': stop_info['stop_lat'],
                    'lon': stop_info['stop_lon'],
                    'scheduled_arrival': st['arrival_time']
                })
        
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
                if entity.HasField('vehicle'):
                    vehicle = entity.vehicle
                    vehicles.append({
                        'vehicle_id': vehicle.vehicle.id if vehicle.vehicle.HasField('id') else None,
                        'route_id': vehicle.trip.route_id if vehicle.trip.HasField('route_id') else None,
                        'trip_id': vehicle.trip.trip_id if vehicle.trip.HasField('trip_id') else None,
                        'latitude': vehicle.position.latitude if vehicle.position.HasField('latitude') else None,
                        'longitude': vehicle.position.longitude if vehicle.position.HasField('longitude') else None,
                        'timestamp': vehicle.timestamp if vehicle.HasField('timestamp') else None,
                        'current_stop_sequence': vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else None
                    })

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

        route_vehicles = [v for v in vehicles if v['route_id'] == route_id]

        print(f"\nFound {len(route_vehicles)} vehicles on route {route_id}:")
        for v in route_vehicles:
            timestamp_str = datetime.fromtimestamp(v['timestamp']).strftime('%H:%M:%S') if v['timestamp'] else 'N/A'
            print(f"  Vehicle {v['vehicle_id']}: Lat={v['latitude']:.4f}, Lon={v['longitude']:.4f}, Time={timestamp_str}")

        # Save to database if requested and db session available
        if save_to_db and self.db:
            self._save_vehicle_positions(route_vehicles)

        return route_vehicles

    def _save_vehicle_positions(self, vehicles):
        """Save vehicle positions to database"""
        saved_count = 0
        for vehicle_data in vehicles:
            vehicle_pos = VehiclePosition(
                vehicle_id=vehicle_data['vehicle_id'],
                route_id=vehicle_data['route_id'],
                trip_id=vehicle_data['trip_id'],
                latitude=vehicle_data['latitude'],
                longitude=vehicle_data['longitude'],
                current_stop_sequence=vehicle_data.get('current_stop_sequence'),
                timestamp=datetime.fromtimestamp(vehicle_data['timestamp']) if vehicle_data['timestamp'] else datetime.utcnow()
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
        print("\n" + "="*50)
        print("Testing C51 Bus Route")
        print("="*50)

        route = db.query(Route).filter_by(route_short_name='C51').first()

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
        c51_vehicles = [v for v in all_vehicles if v['route_id'] == route_id]

        if c51_vehicles:
            print(f"\n✓ Found {len(c51_vehicles)} C51 vehicles:")
            for v in c51_vehicles:
                from datetime import datetime
                timestamp_str = datetime.fromtimestamp(v['timestamp']).strftime('%H:%M:%S') if v['timestamp'] else 'N/A'
                print(f"  Vehicle {v['vehicle_id']}: Lat={v['latitude']:.4f}, Lon={v['longitude']:.4f}, Time={timestamp_str}")

            # Save to database
            collector._save_vehicle_positions(c51_vehicles)
        else:
            print(f"\n✗ No C51 vehicles currently active (found {len(all_vehicles)} total vehicles)")

        print("\n" + "="*50)
        print("✓ C51 data collection complete!")
        print("="*50)

        # Show summary from database
        total_positions = db.query(VehiclePosition).count()
        c51_positions = db.query(VehiclePosition).filter_by(route_id=route_id).count()

        print(f"\nDatabase Summary:")
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
