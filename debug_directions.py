"""
Debug direction filtering for headway calculation
"""
from datetime import datetime
from database import get_session
from models import VehiclePosition, Trip

db = get_session()

try:
    print("=" * 70)
    print("Debug: Vehicle Directions")
    print("=" * 70)

    # Get today's C51 vehicles
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    vehicles = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.timestamp >= today_start
    ).all()

    print(f"\nTotal C51 vehicle positions today: {len(vehicles)}")

    # Check trip_id presence
    with_trip = [v for v in vehicles if v.trip_id]
    without_trip = [v for v in vehicles if not v.trip_id]

    print(f"Positions with trip_id: {len(with_trip)}")
    print(f"Positions without trip_id: {len(without_trip)}")

    # For vehicles with trip_id, check directions
    if with_trip:
        print("\nVehicles with trip_id - checking directions:")
        vehicle_directions = {}
        for v in with_trip:
            if v.vehicle_id not in vehicle_directions:
                trip = db.query(Trip).filter(Trip.trip_id == v.trip_id).first()
                if trip:
                    vehicle_directions[v.vehicle_id] = trip.direction_id

        for vid, direction in vehicle_directions.items():
            print(f"  Vehicle {vid}: Direction {direction}")

    # Check the 5 specific vehicles from the headway test
    test_vehicles = ['2830', '3295', '3255', '3254', '4563']
    print(f"\nChecking test vehicles: {test_vehicles}")

    for vid in test_vehicles:
        veh_positions = db.query(VehiclePosition).filter(
            VehiclePosition.vehicle_id == vid,
            VehiclePosition.route_id == 'C51',
            VehiclePosition.timestamp >= today_start
        ).all()

        if veh_positions:
            trip_ids = set([v.trip_id for v in veh_positions if v.trip_id])
            print(f"  Vehicle {vid}: {len(veh_positions)} positions, trip_ids: {trip_ids}")

            for trip_id in trip_ids:
                if trip_id:
                    trip = db.query(Trip).filter(Trip.trip_id == trip_id).first()
                    if trip:
                        print(f"    Trip {trip_id}: Direction {trip.direction_id}")

    print("\n" + "=" * 70)

finally:
    db.close()
