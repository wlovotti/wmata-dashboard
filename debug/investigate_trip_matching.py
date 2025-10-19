"""
Investigate why trip matching has such a low success rate

This script analyzes:
1. How many vehicles we have in the database
2. What trip_ids are in the GTFS-RT feed vs GTFS static data
3. What directions vehicles are traveling
4. Time/position matching quality
"""
from sqlalchemy import distinct, func

from src.database import get_session
from src.models import Route, Trip, VehiclePosition
from src.trip_matching import find_matching_trip

db = get_session()

try:
    print("=" * 80)
    print("TRIP MATCHING INVESTIGATION")
    print("=" * 80)

    # 1. Basic stats
    print("\n1. DATABASE STATISTICS")
    print("-" * 80)

    total_positions = db.query(func.count(VehiclePosition.id)).scalar()
    c51_positions = db.query(func.count(VehiclePosition.id)).filter(
        VehiclePosition.route_id == 'C51'
    ).scalar()
    unique_vehicles = db.query(func.count(distinct(VehiclePosition.vehicle_id))).filter(
        VehiclePosition.route_id == 'C51'
    ).scalar()

    print(f"Total vehicle positions: {total_positions}")
    print(f"C51 positions: {c51_positions}")
    print(f"C51 unique vehicles: {unique_vehicles}")

    # 2. GTFS-RT trip_ids vs GTFS static trip_ids
    print("\n2. TRIP ID ANALYSIS")
    print("-" * 80)

    # Get sample of trip_ids from GTFS-RT feed
    rt_trip_ids = db.query(distinct(VehiclePosition.trip_id)).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.trip_id.isnot(None)
    ).limit(10).all()

    print("Sample GTFS-RT trip_ids from C51 vehicles:")
    for (trip_id,) in rt_trip_ids:
        # Check if this trip exists in GTFS static
        static_trip = db.query(Trip).filter(Trip.trip_id == trip_id).first()
        if static_trip:
            print(f"  ✓ {trip_id} - FOUND in GTFS static")
        else:
            print(f"  ✗ {trip_id} - NOT FOUND in GTFS static")

    # Count how many RT trip_ids match static
    rt_trips_with_match = db.query(VehiclePosition).join(
        Trip, VehiclePosition.trip_id == Trip.trip_id
    ).filter(
        VehiclePosition.route_id == 'C51'
    ).count()

    rt_trips_total = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.trip_id.isnot(None)
    ).count()

    match_rate = (rt_trips_with_match / rt_trips_total * 100) if rt_trips_total > 0 else 0
    print(f"\nDirect trip_id matches: {rt_trips_with_match}/{rt_trips_total} ({match_rate:.1f}%)")

    # 3. Direction analysis (via Trip relationship)
    print("\n3. DIRECTION ANALYSIS")
    print("-" * 80)

    positions_with_trip = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.trip_id.isnot(None)
    ).count()

    print(f"Positions with trip_id: {positions_with_trip}/{c51_positions}")

    # Count by direction (via trip)
    for direction in [0, 1]:
        count = db.query(VehiclePosition).join(
            Trip, VehiclePosition.trip_id == Trip.trip_id
        ).filter(
            VehiclePosition.route_id == 'C51',
            Trip.direction_id == direction
        ).count()
        vehicles = db.query(func.count(distinct(VehiclePosition.vehicle_id))).join(
            Trip, VehiclePosition.trip_id == Trip.trip_id
        ).filter(
            VehiclePosition.route_id == 'C51',
            Trip.direction_id == direction
        ).scalar()
        print(f"  Direction {direction}: {count} positions, {vehicles} vehicles")

    # 4. Sample trip matching attempts
    print("\n4. SAMPLE TRIP MATCHING ATTEMPTS")
    print("-" * 80)

    sample_positions = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51'
    ).limit(5).all()

    for pos in sample_positions:
        print(f"\nVehicle {pos.vehicle_id} at {pos.timestamp.strftime('%H:%M:%S')}")
        print(f"  Position: ({pos.latitude:.4f}, {pos.longitude:.4f})")
        print(f"  RT trip_id: {pos.trip_id}")

        # Get direction from trip if available
        if pos.trip_id:
            trip_info = db.query(Trip).filter(Trip.trip_id == pos.trip_id).first()
            if trip_info:
                print(f"  Trip direction_id: {trip_info.direction_id}")
                print(f"  Trip headsign: {trip_info.trip_headsign}")

        # Try to match
        result = find_matching_trip(db, pos)

        if result:
            trip, confidence = result
            print(f"  ✓ MATCHED to trip {trip.trip_id} (confidence: {confidence:.0%})")
            print(f"    Trip direction: {trip.direction_id}")
            print(f"    Trip headsign: {trip.trip_headsign}")
        else:
            print("  ✗ NO MATCH FOUND")

            # Try to diagnose why
            route = db.query(Route).filter(Route.route_id == 'C51').first()
            if not route:
                print("    Problem: Route C51 not found in database!")

            # Check if there are ANY trips for this route
            trip_count = db.query(Trip).filter(Trip.route_id == 'C51').count()
            print(f"    C51 trips in database: {trip_count}")

            # Check if position has a trip_id that should give us direction
            if pos.trip_id:
                trip_from_rt = db.query(Trip).filter(Trip.trip_id == pos.trip_id).first()
                if trip_from_rt:
                    print("    Position has valid trip_id but matching failed!")
                    print(f"    Trip direction: {trip_from_rt.direction_id}")

    # 5. Time coverage analysis
    print("\n5. TIME COVERAGE ANALYSIS")
    print("-" * 80)

    first_pos = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51'
    ).order_by(VehiclePosition.timestamp.asc()).first()

    last_pos = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51'
    ).order_by(VehiclePosition.timestamp.desc()).first()

    if first_pos and last_pos:
        print("Data collection period:")
        print(f"  First: {first_pos.timestamp}")
        print(f"  Last: {last_pos.timestamp}")
        print(f"  Duration: {(last_pos.timestamp - first_pos.timestamp).total_seconds() / 3600:.1f} hours")

        # Check if we have trips scheduled during this time
        # Note: Trip times are in seconds since midnight, need to convert
        collection_start_seconds = (first_pos.timestamp.hour * 3600 +
                                   first_pos.timestamp.minute * 60 +
                                   first_pos.timestamp.second)
        collection_end_seconds = (last_pos.timestamp.hour * 3600 +
                                 last_pos.timestamp.minute * 60 +
                                 last_pos.timestamp.second)

        print("\nScheduled C51 trips during collection window:")
        from src.models import StopTime
        trips_in_window = db.query(func.count(distinct(StopTime.trip_id))).join(
            Trip, StopTime.trip_id == Trip.trip_id
        ).filter(
            Trip.route_id == 'C51',
            StopTime.arrival_time >= collection_start_seconds,
            StopTime.arrival_time <= collection_end_seconds
        ).scalar()
        print(f"  Trips scheduled: {trips_in_window}")

    print("\n" + "=" * 80)

finally:
    db.close()
