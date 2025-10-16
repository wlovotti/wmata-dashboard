"""
Quick OTP test for a few routes with the collected data
"""
from src.database import get_session
from src.analytics import calculate_line_level_otp

db = get_session()

try:
    print("=" * 70)
    print("Multi-Route OTP Analysis")
    print("=" * 70)

    # Test a few popular routes
    test_routes = ['C51', 'C53', 'D80', 'F20', 'D4X']

    for route_id in test_routes:
        print(f"\n{route_id}:")
        print("-" * 70)

        result = calculate_line_level_otp(db, route_id)

        if not result:
            print(f"  No data available for route {route_id}")
            continue

        print(f"  Total observations: {result['total_observations']}")
        print(f"  Matched to schedule: {result['matched_observations']}")
        print(f"  On-time: {result['on_time_pct']:.1f}%")
        print(f"  Early: {result['early_pct']:.1f}%")
        print(f"  Late: {result['late_pct']:.1f}%")

        if result['avg_lateness_seconds'] is not None:
            minutes = result['avg_lateness_seconds'] / 60
            print(f"  Average lateness: {minutes:+.1f} minutes")

finally:
    db.close()
