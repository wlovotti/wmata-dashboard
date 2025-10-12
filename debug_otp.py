"""
Debug OTP calculation to understand why so many buses appear early
"""
from database import get_session
from analytics import calculate_on_time_performance
from models import VehiclePosition
from datetime import datetime

db = get_session()

try:
    print("=" * 70)
    print("Debugging OTP Early Arrivals")
    print("=" * 70)

    # Get some C51 positions to understand what we're working with
    positions = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51'
    ).order_by(VehiclePosition.timestamp.desc()).limit(5).all()

    print("\nSample vehicle positions (raw data):")
    for pos in positions:
        print(f"\n  Vehicle: {pos.vehicle_id}")
        print(f"  Timestamp: {pos.timestamp} (hour: {pos.timestamp.hour})")
        print(f"  Location: ({pos.latitude:.6f}, {pos.longitude:.6f})")
        print(f"  RT trip_id: {pos.trip_id}")

    # Now run OTP and look at detailed results
    print("\n" + "=" * 70)
    print("OTP Calculation Results:")
    print("=" * 70)

    otp = calculate_on_time_performance(db, 'C51')

    print(f"\nOverall Stats:")
    print(f"  Total positions: {db.query(VehiclePosition).filter(VehiclePosition.route_id == 'C51').count()}")
    print(f"  Matched: {otp.get('matched_vehicles')}")
    print(f"  Unmatched: {otp.get('unmatched_vehicles')}")
    print(f"  Arrivals: {otp.get('arrivals_analyzed')}")

    # Look at ALL sample arrivals in detail
    if otp.get('sample_arrivals'):
        print("\nDetailed arrival analysis:")
        for i, arrival in enumerate(otp.get('sample_arrivals'), 1):
            diff_min = arrival['difference_seconds'] / 60
            status = "ON-TIME"
            if arrival['difference_seconds'] < -60:
                status = "EARLY"
            elif arrival['difference_seconds'] > 300:
                status = "LATE"

            print(f"\n{i}. Vehicle {arrival['vehicle_id']} - {status}")
            print(f"   Stop: {arrival['stop_name']}")
            print(f"   Matched trip: {arrival['matched_trip_id']} (confidence: {arrival['match_confidence']:.0%})")
            print(f"   Scheduled: {arrival['scheduled_time']}")
            print(f"   Actual:    {arrival['actual_time']}")
            print(f"   Difference: {diff_min:+.1f} minutes ({arrival['difference_seconds']:+.0f} seconds)")
            print(f"   Distance from stop: {arrival['distance_meters']:.0f}m")

    print("\n" + "=" * 70)

finally:
    db.close()
