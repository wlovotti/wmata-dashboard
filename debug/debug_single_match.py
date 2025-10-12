"""
Debug a single vehicle position match attempt in detail
"""
from src.database import get_session
from src.models import VehiclePosition, Trip, StopTime, Stop
from src.trip_matching import parse_gtfs_time, find_matching_trip
from src.analytics import haversine_distance

db = get_session()

try:
    # Get one vehicle position that should match
    pos = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.trip_id.isnot(None)
    ).first()

    print("=" * 80)
    print(f"DEBUGGING VEHICLE {pos.vehicle_id}")
    print("=" * 80)
    print(f"Timestamp: {pos.timestamp}")
    print(f"Position: ({pos.latitude:.6f}, {pos.longitude:.6f})")
    print(f"RT trip_id: {pos.trip_id}")

    # Get the trip this vehicle claims to be on
    rt_trip = db.query(Trip).filter(Trip.trip_id == pos.trip_id).first()
    if rt_trip:
        print(f"\nRT Trip Info:")
        print(f"  Direction: {rt_trip.direction_id}")
        print(f"  Headsign: {rt_trip.trip_headsign}")

        # Get some stop_times for this trip
        stop_times = db.query(StopTime).filter(
            StopTime.trip_id == rt_trip.trip_id
        ).order_by(StopTime.stop_sequence).limit(5).all()

        print(f"\nFirst 5 scheduled stops for this trip:")
        for st in stop_times:
            stop = db.query(Stop).filter(Stop.stop_id == st.stop_id).first()
            if stop:
                # Parse the time
                scheduled_time = parse_gtfs_time(st.arrival_time, pos.timestamp)
                time_diff = (pos.timestamp - scheduled_time).total_seconds() / 60

                # Calculate distance
                distance = haversine_distance(
                    pos.latitude, pos.longitude,
                    stop.stop_lat, stop.stop_lon
                )

                print(f"  {stop.stop_name}")
                print(f"    Scheduled: {st.arrival_time} -> {scheduled_time}")
                print(f"    Time diff: {time_diff:+.1f} min")
                print(f"    Distance: {distance:.0f} m")

    print(f"\n{'='*80}")
    print("RUNNING find_matching_trip()...")
    print('='*80)

    result = find_matching_trip(db, pos)

    if result:
        matched_trip, confidence = result
        print(f"✓ MATCH FOUND!")
        print(f"  Trip: {matched_trip.trip_id}")
        print(f"  Confidence: {confidence:.0%}")
        print(f"  Same as RT trip_id: {matched_trip.trip_id == pos.trip_id}")
    else:
        print(f"✗ NO MATCH FOUND")
        print(f"\nDebugging why it failed...")

        # Try to figure out what went wrong
        # Check candidate trips
        candidate_trips = db.query(Trip).filter(
            Trip.route_id == pos.route_id,
            Trip.direction_id == rt_trip.direction_id if rt_trip else None
        ).all()

        print(f"  Candidate trips for route {pos.route_id}, direction {rt_trip.direction_id if rt_trip else 'any'}: {len(candidate_trips)}")

        # Check if our RT trip is in candidates
        our_trip_in_candidates = any(t.trip_id == pos.trip_id for t in candidate_trips)
        print(f"  RT trip {pos.trip_id} in candidates: {our_trip_in_candidates}")

        # Check a few stop_times with verbose output
        if rt_trip:
            print(f"\n  Checking why stops don't match...")
            stop_times = db.query(StopTime).filter(
                StopTime.trip_id == rt_trip.trip_id
            ).order_by(StopTime.stop_sequence).limit(10).all()

            matches_found = 0
            for st in stop_times:
                stop = db.query(Stop).filter(Stop.stop_id == st.stop_id).first()
                if not stop:
                    continue

                scheduled_time = parse_gtfs_time(st.arrival_time, pos.timestamp)
                time_diff_minutes = (pos.timestamp - scheduled_time).total_seconds() / 60

                distance = haversine_distance(
                    pos.latitude, pos.longitude,
                    stop.stop_lat, stop.stop_lon
                )

                # Check the filters
                passes_time = -5.0 <= time_diff_minutes <= 15.0
                passes_distance = distance <= 500.0

                if passes_time and passes_distance:
                    matches_found += 1
                    print(f"    ✓ Stop {st.stop_sequence}: {stop.stop_name[:30]}")
                    print(f"        Time diff: {time_diff_minutes:+.1f} min, Distance: {distance:.0f} m")

            print(f"\n  Stops passing filters: {matches_found}/10")

finally:
    db.close()
