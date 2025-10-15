"""
Test average speed calculation using shapes data
"""
from src.database import get_session
from src.analytics import calculate_average_speed
from src.models import VehiclePosition
from sqlalchemy import func

db = get_session()

try:
    print("=" * 70)
    print("Testing Average Speed Calculation")
    print("=" * 70)

    # Get routes with sufficient data
    routes_with_data = db.query(
        VehiclePosition.route_id,
        func.count(VehiclePosition.id).label('position_count'),
        func.count(func.distinct(VehiclePosition.vehicle_id)).label('vehicle_count')
    ).group_by(
        VehiclePosition.route_id
    ).order_by(
        func.count(VehiclePosition.id).desc()
    ).limit(5).all()

    print(f"\nTesting top {len(routes_with_data)} routes by data volume")
    print("-" * 70)

    for i, (route_id, position_count, vehicle_count) in enumerate(routes_with_data, 1):
        print(f"\n[{i}] Route {route_id}")
        print(f"    Positions: {position_count:,}, Vehicles: {vehicle_count}")

        # Calculate average speed
        speed_data = calculate_average_speed(db, route_id)

        if 'error' in speed_data:
            print(f"    Error: {speed_data['error']}")
        elif speed_data.get('avg_speed_mph') is None:
            print(f"    No valid trips found")
            if 'note' in speed_data:
                print(f"    Note: {speed_data['note']}")
        else:
            print(f"    Average speed: {speed_data['avg_speed_mph']} mph ({speed_data['avg_speed_kmh']} km/h)")
            print(f"    Median speed: {speed_data['median_speed_mph']} mph")
            print(f"    Speed range: {speed_data['min_speed_mph']} - {speed_data['max_speed_mph']} mph")
            print(f"    Trips analyzed: {speed_data['trips_analyzed']}")
            print(f"    Total distance: {speed_data['total_distance_miles']} miles ({speed_data['total_distance_km']} km)")
            print(f"    Total time: {speed_data['total_time_hours']} hours")

            # Show sample trips
            if speed_data.get('sample_trips'):
                print(f"\n    Sample trips:")
                for trip in speed_data['sample_trips'][:3]:
                    print(f"      - Vehicle {trip['vehicle_id']}: {trip['speed_mph']:.1f} mph, "
                          f"{trip['distance_miles']:.1f} mi, {trip['duration_minutes']:.1f} min")

    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)

finally:
    db.close()
