"""
Test headway calculation with updated methodology (route-level: per-direction averaged).
"""
from src.analytics import calculate_route_headways
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

        result = calculate_route_headways(db, route_id)

        if result['avg_headway_minutes'] is None:
            print("  No valid headways for any direction")
            continue

        print(f"  Valid headways (across directions): {result['count']}")
        print(f"  Average headway: {result['avg_headway_minutes']} minutes")
        print(f"  Min headway: {result['min_headway_minutes']} minutes")
        print(f"  Max headway: {result['max_headway_minutes']} minutes")

        for direction_id, dir_result in sorted(result['per_direction'].items()):
            if 'error' in dir_result or dir_result.get('avg_headway_minutes') is None:
                continue
            print(
                f"    dir {direction_id}: "
                f"avg={dir_result['avg_headway_minutes']} min, "
                f"count={dir_result['count']}, "
                f"stop={dir_result.get('stop_name')} ({dir_result.get('stop_id')})"
            )

finally:
    db.close()
