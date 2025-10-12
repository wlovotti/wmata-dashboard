"""
Test On-Time Performance analysis with C51 data
"""
from datetime import datetime
from database import get_session
from analytics import calculate_on_time_performance

db = get_session()

try:
    print("=" * 70)
    print("On-Time Performance Analysis for C51")
    print("=" * 70)

    # Test with today's data
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    print("\nCalculating on-time performance...")
    otp = calculate_on_time_performance(
        db,
        'C51',
        start_time=today_start,
        end_time=today_end,
        early_threshold_seconds=-60,  # More than 1 min early
        late_threshold_seconds=300     # More than 5 min late
    )

    print(f"\nResults:")
    print(f"  Arrivals analyzed: {otp.get('arrivals_analyzed')}")
    print(f"  On-time: {otp.get('on_time_percentage')}% ({otp.get('on_time_count')} arrivals)")
    print(f"  Early: {otp.get('early_percentage')}% ({otp.get('early_count')} arrivals)")
    print(f"  Late: {otp.get('late_percentage')}% ({otp.get('late_count')} arrivals)")

    if otp.get('sample_arrivals'):
        print(f"\nSample arrivals (first 10):")
        for arrival in otp.get('sample_arrivals')[:10]:
            diff = arrival['difference_seconds']
            status = 'EARLY' if diff < -60 else ('LATE' if diff > 300 else 'ON-TIME')
            print(f"  Vehicle {arrival['vehicle_id']} at {arrival['stop_name']}")
            print(f"    Scheduled: {arrival['scheduled_time'].strftime('%H:%M:%S')}")
            print(f"    Actual: {arrival['actual_time'].strftime('%H:%M:%S')}")
            print(f"    Difference: {diff:.0f}s ({status})")
            print(f"    Distance from stop: {arrival['distance_meters']:.1f}m")
            print()

    print("=" * 70)

finally:
    db.close()
