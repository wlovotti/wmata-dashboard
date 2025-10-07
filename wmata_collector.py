import os
from dotenv import load_dotenv
import requests
import zipfile
import io
import csv
from datetime import datetime
from google.transit import gtfs_realtime_pb2

# Load environment variables from .env file
load_dotenv()

# Your WMATA API key from environment
API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

BASE_URL = "https://api.wmata.com/gtfs"

class WMATADataCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {"api_key": api_key}
        self.gtfs_data = {}
        
    def download_gtfs_static(self):
        """Download and parse GTFS static data"""
        print("Downloading GTFS static data...")
        url = f"{BASE_URL}/bus-gtfs-static.zip"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error downloading GTFS: {response.status_code}")
            return False
        
        # Extract zip file in memory
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        
        # Parse relevant files
        self.gtfs_data['routes'] = self._parse_csv(zip_file, 'routes.txt')
        self.gtfs_data['trips'] = self._parse_csv(zip_file, 'trips.txt')
        self.gtfs_data['stops'] = self._parse_csv(zip_file, 'stops.txt')
        self.gtfs_data['stop_times'] = self._parse_csv(zip_file, 'stop_times.txt')
        
        print("GTFS static data downloaded and parsed successfully")
        return True
    
    def _parse_csv(self, zip_file, filename):
        """Parse a CSV file from the GTFS zip"""
        content = zip_file.read(filename).decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)
    
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
    
    def get_realtime_vehicle_positions(self):
        """Fetch real-time vehicle positions"""
        print("\nFetching real-time vehicle positions...")
        url = f"{BASE_URL}/bus-gtfsrt-vehiclepositions.pb"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching vehicle positions: {response.status_code}")
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
        
        return vehicles
    
    def get_route_vehicles(self, route_id, vehicles=None):
        """Filter vehicles for a specific route"""
        if vehicles is None:
            vehicles = self.get_realtime_vehicle_positions()
        
        route_vehicles = [v for v in vehicles if v['route_id'] == route_id]
        
        print(f"\nFound {len(route_vehicles)} vehicles on route {route_id}:")
        for v in route_vehicles:
            timestamp_str = datetime.fromtimestamp(v['timestamp']).strftime('%H:%M:%S') if v['timestamp'] else 'N/A'
            print(f"  Vehicle {v['vehicle_id']}: Lat={v['latitude']:.4f}, Lon={v['longitude']:.4f}, Time={timestamp_str}")
        
        return route_vehicles


def main():
    # Initialize collector
    collector = WMATADataCollector(API_KEY)
    
    # Download and parse GTFS static data
    if not collector.download_gtfs_static():
        return
    
    # Get C51 route information
    route = collector.get_route_info('C51')
    if not route:
        return
    
    route_id = route['route_id']
    
    # Get stops for C51
    stops = collector.get_route_stops(route_id)
    print(f"\nC51 has {len(stops)} stops:")
    for stop in stops[:5]:  # Show first 5 stops
        print(f"  {stop['sequence']}. {stop['stop_name']} (scheduled: {stop['scheduled_arrival']})")
    print("  ...")
    
    # Get real-time vehicle positions for C51
    vehicles = collector.get_route_vehicles(route_id)
    
    print("\n" + "="*50)
    print("Data collection complete!")
    print("Next steps:")
    print("1. Run this script every 30-60 seconds")
    print("2. Store vehicle positions in a database")
    print("3. Calculate headways by tracking arrivals at each stop")
    print("4. Compare actual vs scheduled times for on-time performance")


if __name__ == "__main__":
    main()
