"""
Test headway calculation with updated methodology
"""
from src.analytics import calculate_headways
from src.database import get_session

db = get_session()

try:
    print("=" * 70)
    print("Headway Analysis Test")
    print("=" * 70)

    # Test a few routes
    test_routes = ['C51', 'D80', 'F20']

    for route_id in test_routes:
        print(f"\n{route_id}:")
        print("-" * 70)

        result = calculate_headways(db, route_id)

        if 'error' in result:
            print(f"  Error: {result['error']}")
            continue

        print(f"  Reference stop: {result['stop_name']} ({result['stop_id']})")
        print(f"  Direction: {result['direction_id']}")
        print(f"  Vehicles passed stop: {result['vehicles_passed_stop']}")
        print(f"  Valid headways: {result['count']}")
        print(f"  Gaps detected: {result['gaps_detected']}")

        if result['avg_headway_minutes']:
            print(f"  Average headway: {result['avg_headway_minutes']} minutes")
            print(f"  Min headway: {result['min_headway_minutes']} minutes")
            print(f"  Max headway: {result['max_headway_minutes']} minutes")

        # Show a few sample headways
        if result['valid_headways']:
            print("\n  Sample headways:")
            for hw in result['valid_headways'][:3]:
                print(f"    Vehicle {hw['previous_vehicle']} -> {hw['current_vehicle']}: {hw['headway_minutes']} min")

finally:
    db.close()
