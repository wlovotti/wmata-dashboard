"""
Test multi-level OTP calculation functions

Tests the three levels of OTP analysis:
- Stop-level
- Time-period
- Line-level
"""
from src.database import get_session
from src.analytics import (
    calculate_stop_level_otp,
    calculate_time_period_otp,
    calculate_line_level_otp,
    get_route_summary
)
from src.models import Stop, StopTime, Trip, VehiclePosition
from sqlalchemy import func

db = get_session()

try:
    print("=" * 70)
    print("Testing Multi-Level OTP Calculations")
    print("=" * 70)

    # Pick a route with good data
    print("\n1. Finding route with most data...")
    top_routes = db.query(
        VehiclePosition.route_id,
        func.count(func.distinct(VehiclePosition.vehicle_id)).label('vehicle_count')
    ).group_by(
        VehiclePosition.route_id
    ).order_by(func.count(func.distinct(VehiclePosition.vehicle_id)).desc()).limit(5).all()

    print("   Top 5 routes by vehicle count:")
    for route_id, count in top_routes:
        print(f"     {route_id}: {count} unique vehicles")

    test_route = top_routes[0][0] if top_routes else 'C51'
    print(f"\n   Using {test_route} for testing")

    # Get route summary
    print(f"\n2. Route {test_route} Summary:")
    summary = get_route_summary(db, test_route)
    print(f"   Positions collected: {summary.get('vehicle_positions_collected')}")
    print(f"   Unique vehicles: {summary.get('unique_vehicles_tracked')}")
    print(f"   Duration: {summary.get('data_time_range', {}).get('duration_minutes', 0):.1f} minutes")

    # TEST 1: Line-Level OTP
    print(f"\n3. Testing LINE-LEVEL OTP for {test_route}...")
    print("-" * 70)
    line_otp = calculate_line_level_otp(db, test_route)

    if line_otp.get('arrivals_analyzed', 0) > 0:
        print(f"   ✓ Arrivals analyzed: {line_otp['arrivals_analyzed']}")
        print(f"   ✓ On-time: {line_otp['on_time_percentage']}%")
        print(f"   ✓ Early: {line_otp['early_percentage']}%")
        print(f"   ✓ Late: {line_otp['late_percentage']}%")
        print(f"   ✓ Matched vehicles: {line_otp['matched_vehicles']}")
        print(f"   ✓ Unmatched vehicles: {line_otp['unmatched_vehicles']}")
    else:
        print(f"   ✗ No arrivals analyzed (not enough data yet)")

    # TEST 2: Time-Period OTP
    print(f"\n4. Testing TIME-PERIOD OTP for {test_route}...")
    print("-" * 70)
    period_otp = calculate_time_period_otp(db, test_route)

    if period_otp.get('periods'):
        for period, stats in period_otp['periods'].items():
            arrivals = stats.get('arrivals_analyzed', 0)
            if arrivals > 0:
                print(f"   {period:20s}: {stats['on_time_percentage']:5.1f}% on-time ({arrivals} arrivals)")
            else:
                print(f"   {period:20s}: No data")
    else:
        print(f"   ✗ No period data available")

    # TEST 3: Stop-Level OTP
    print(f"\n5. Testing STOP-LEVEL OTP for {test_route}...")
    print("-" * 70)

    # Find a stop that's commonly served by this route
    print("   Finding stops on this route...")
    stops_on_route = db.query(
        Stop.stop_id,
        Stop.stop_name,
        func.count(StopTime.id).label('trip_count')
    ).join(StopTime, Stop.stop_id == StopTime.stop_id).join(
        Trip, StopTime.trip_id == Trip.trip_id
    ).filter(
        Trip.route_id == test_route
    ).group_by(Stop.stop_id).order_by(
        func.count(StopTime.id).desc()
    ).limit(5).all()

    if stops_on_route:
        print(f"   Top 5 stops by number of scheduled trips:")
        for stop_id, stop_name, trip_count in stops_on_route:
            print(f"     {stop_name[:50]:50s} ({trip_count} trips)")

        # Test OTP for first stop
        test_stop_id = stops_on_route[0][0]
        test_stop_name = stops_on_route[0][1]

        print(f"\n   Testing OTP at: {test_stop_name}")
        stop_otp = calculate_stop_level_otp(db, test_route, test_stop_id)

        if stop_otp.get('arrivals_analyzed', 0) > 0:
            print(f"   ✓ Arrivals analyzed: {stop_otp['arrivals_analyzed']}")
            print(f"   ✓ On-time: {stop_otp['on_time_percentage']}%")
            print(f"   ✓ Early: {stop_otp['early_percentage']}%")
            print(f"   ✓ Late: {stop_otp['late_percentage']}%")
            print(f"   ✓ Avg lateness: {stop_otp['avg_lateness_seconds']} seconds")
        else:
            print(f"   ✗ No arrivals at this stop (vehicles may not have been close enough)")
    else:
        print(f"   ✗ No stops found for route {test_route}")

    print("\n" + "=" * 70)
    print("Multi-Level OTP Testing Complete")
    print("=" * 70)

finally:
    db.close()
