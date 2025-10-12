"""
Test OTP calculation with trip matching
"""
from database import get_session
from analytics import calculate_on_time_performance, get_route_summary

db = get_session()

try:
    print("=" * 70)
    print("Testing OTP Calculation with Trip Matching")
    print("=" * 70)

    # Test with C51 data
    print("\nC51 Route Summary:")
    summary = get_route_summary(db, 'C51')
    print(f"  Vehicle positions collected: {summary.get('vehicle_positions_collected')}")
    print(f"  Unique vehicles tracked: {summary.get('unique_vehicles_tracked')}")
    print(f"  Data duration: {summary.get('data_time_range', {}).get('duration_minutes', 0):.1f} minutes")

    print("\nCalculating OTP for C51...")
    otp = calculate_on_time_performance(db, 'C51')

    print("\nResults:")
    print(f"  Matched vehicles: {otp.get('matched_vehicles')}")
    print(f"  Unmatched vehicles: {otp.get('unmatched_vehicles')}")
    print(f"  Arrivals analyzed: {otp.get('arrivals_analyzed')}")
    print(f"  On-time: {otp.get('on_time_percentage')}%")
    print(f"  Early: {otp.get('early_percentage')}%")
    print(f"  Late: {otp.get('late_percentage')}%")

    if otp.get('sample_arrivals'):
        print("\nSample arrivals (first 3):")
        for arrival in otp.get('sample_arrivals')[:3]:
            diff_min = arrival['difference_seconds'] / 60
            print(f"  Vehicle {arrival['vehicle_id']} at {arrival['stop_name']}:")
            print(f"    Scheduled: {arrival['scheduled_time'].strftime('%H:%M:%S')}")
            print(f"    Actual: {arrival['actual_time'].strftime('%H:%M:%S')}")
            print(f"    Difference: {diff_min:+.1f} minutes")
            print(f"    Match confidence: {arrival['match_confidence']:.0%}")

    print("\n" + "=" * 70)

finally:
    db.close()
