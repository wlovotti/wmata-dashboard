"""
Trace a single vehicle's journey showing actual vs scheduled times at each stop
"""
import numpy as np
from datetime import datetime, timedelta
from src.database import get_session
from src.models import VehiclePosition, Trip, StopTime, Stop
from src.analytics import get_route_stops
from sqlalchemy import and_

db = get_session()

try:
    # Find a vehicle with lots of positions on one route (likely early based on stats)
    # Let's look at D80 which had 52.3% early
    route_id = 'D80'

    # Get a vehicle with many positions (find one with the most data)
    from sqlalchemy import func
    vehicle_data = db.query(
        VehiclePosition.vehicle_id,
        VehiclePosition.trip_id,
        func.count(VehiclePosition.id).label('count')
    ).filter(
        VehiclePosition.route_id == route_id,
        VehiclePosition.trip_id.isnot(None)
    ).group_by(
        VehiclePosition.vehicle_id,
        VehiclePosition.trip_id
    ).order_by(func.count(VehiclePosition.id).desc()).first()

    if not vehicle_data:
        print("No vehicles found with trip_id")
        exit(1)

    vehicle_id = vehicle_data.vehicle_id
    trip_id = vehicle_data.trip_id

    print("=" * 100)
    print(f"Vehicle Journey Trace: {vehicle_id} on Route {route_id}")
    print("=" * 100)

    # Get all positions for this vehicle/trip
    positions = db.query(VehiclePosition).filter(
        and_(
            VehiclePosition.vehicle_id == vehicle_id,
            VehiclePosition.route_id == route_id,
            VehiclePosition.trip_id == trip_id
        )
    ).order_by(VehiclePosition.timestamp).all()

    print(f"\nTotal positions collected: {len(positions)}")
    print(f"Time range: {positions[0].timestamp} to {positions[-1].timestamp}")
    print(f"Duration: {(positions[-1].timestamp - positions[0].timestamp).total_seconds() / 60:.1f} minutes")

    # Get route stops as numpy arrays for vectorized distance calculation
    stops = get_route_stops(db, route_id)
    stop_ids = np.array([s.stop_id for s in stops])
    stop_lats = np.array([s.stop_lat for s in stops])
    stop_lons = np.array([s.stop_lon for s in stops])
    stop_map = {s.stop_id: s for s in stops}

    # Get trip info
    trip = db.query(Trip).filter(Trip.trip_id == trip_id).first()
    if not trip:
        print(f"\nTrip {trip_id} not found in GTFS")
        exit(1)

    print(f"Trip ID: {trip_id}")
    print(f"Direction: {trip.direction_id}")

    # Get all scheduled stop times for this trip
    stop_times = db.query(StopTime).filter(
        StopTime.trip_id == trip_id
    ).order_by(StopTime.stop_sequence).all()

    print(f"Scheduled stops on trip: {len(stop_times)}")

    # Create schedule lookup
    schedule_map = {st.stop_id: st.arrival_time for st in stop_times}

    # Process each position and find nearest stop
    arrivals = []

    for pos in positions:
        # Vectorized distance calculation
        lat1, lon1 = np.radians(pos.latitude), np.radians(pos.longitude)
        lat2, lon2 = np.radians(stop_lats), np.radians(stop_lons)

        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        distances = 6371000 * c  # meters

        # Find nearest stop within 100m (relaxed threshold)
        min_idx = np.argmin(distances)
        min_distance = distances[min_idx]

        if min_distance > 100.0:
            continue

        nearest_stop_id = stop_ids[min_idx]

        # Get scheduled time
        if nearest_stop_id not in schedule_map:
            continue

        scheduled_time_str = schedule_map[nearest_stop_id]

        try:
            hours, minutes, seconds = map(int, scheduled_time_str.split(':'))
            scheduled_dt = pos.timestamp.replace(
                hour=hours % 24,
                minute=minutes,
                second=seconds,
                microsecond=0
            )
            if hours >= 24:
                scheduled_dt += timedelta(days=hours // 24)

            diff_seconds = (pos.timestamp - scheduled_dt).total_seconds()

            arrivals.append({
                'stop_id': nearest_stop_id,
                'stop_name': stop_map[nearest_stop_id].stop_name,
                'actual_time': pos.timestamp,
                'scheduled_time': scheduled_dt,
                'diff_seconds': diff_seconds,
                'diff_minutes': diff_seconds / 60.0,
                'distance_meters': min_distance
            })
        except (ValueError, AttributeError):
            continue

    if not arrivals:
        print("\nNo arrivals detected within 50m of stops")
        exit(0)

    # Display results
    print("\n" + "=" * 100)
    print("ARRIVALS AT STOPS")
    print("=" * 100)
    print(f"{'Stop ID':<12} {'Stop Name':<30} {'Scheduled':<20} {'Actual':<20} {'Diff (min)':<12} {'Status'}")
    print("-" * 100)

    early_count = 0
    on_time_count = 0
    late_count = 0

    for arrival in arrivals:
        diff_min = arrival['diff_minutes']

        if diff_min < -1.0:
            status = "EARLY"
            early_count += 1
        elif diff_min > 5.0:
            status = "LATE"
            late_count += 1
        else:
            status = "ON-TIME"
            on_time_count += 1

        print(f"{arrival['stop_id']:<12} {arrival['stop_name'][:29]:<30} "
              f"{arrival['scheduled_time'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
              f"{arrival['actual_time'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
              f"{diff_min:+7.1f}      {status}")

    print("\n" + "=" * 100)
    print("SUMMARY")
    print("-" * 100)
    total = len(arrivals)
    print(f"Total stops detected: {total}")
    print(f"Early (>1 min early): {early_count} ({early_count/total*100:.1f}%)")
    print(f"On-time (-1 to +5 min): {on_time_count} ({on_time_count/total*100:.1f}%)")
    print(f"Late (>5 min late): {late_count} ({late_count/total*100:.1f}%)")
    print(f"Average difference: {np.mean([a['diff_minutes'] for a in arrivals]):+.1f} minutes")

finally:
    db.close()
