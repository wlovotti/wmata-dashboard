"""
Detailed test of headway calculation showing reference stop and vehicle passages
"""
from datetime import datetime
from database import get_session
from analytics import calculate_headways, find_reference_stop
from models import Stop

db = get_session()

try:
    print("=" * 70)
    print("Detailed Headway Analysis for C51")
    print("=" * 70)

    # Find the reference stop
    print("\n1. Finding Reference Stop:")
    ref_stop_id = find_reference_stop(db, 'C51')
    if ref_stop_id:
        stop = db.query(Stop).filter(Stop.stop_id == ref_stop_id).first()
        print(f"   Selected: {stop.stop_name}")
        print(f"   Stop ID: {ref_stop_id}")
        print(f"   Location: ({stop.stop_lat:.6f}, {stop.stop_lon:.6f})")
    else:
        print("   ERROR: Could not find reference stop")

    # Calculate headways for today
    print("\n2. Calculating Headways:")
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    result = calculate_headways(
        db,
        'C51',
        start_time=today_start,
        end_time=today_end,
        max_headway_minutes=120.0,
        proximity_meters=150.0
    )

    if 'error' in result:
        print(f"   ERROR: {result['error']}")
    else:
        print(f"   Reference stop: {result.get('stop_name')}")
        print(f"   Proximity threshold: {result.get('proximity_threshold_meters')}m")
        print(f"   Vehicles passed stop: {result.get('vehicles_passed_stop')}")
        print(f"   Valid headways measured: {result.get('count')}")
        print(f"   Flagged gaps: {result.get('gaps_detected')}")

        if result.get('avg_headway_minutes'):
            print(f"\n   Average headway: {result.get('avg_headway_minutes')} minutes")
            print(f"   Min headway: {result.get('min_headway_minutes')} minutes")
            print(f"   Max headway: {result.get('max_headway_minutes')} minutes")

        print("\n3. Individual Headways:")
        if result.get('valid_headways'):
            for i, hw in enumerate(result.get('valid_headways'), 1):
                print(f"   #{i}: Vehicle {hw['previous_vehicle']} → {hw['current_vehicle']}")
                print(f"       {hw['previous_time'][-8:]} → {hw['current_time'][-8:]}")
                print(f"       Headway: {hw['headway_minutes']} minutes")
        else:
            print("   No valid headways calculated")

        if result.get('flagged_gaps'):
            print("\n4. Flagged Data Gaps:")
            for gap in result.get('flagged_gaps'):
                print(f"   {gap['previous_time'][-8:]} → {gap['current_time'][-8:]}: {gap['headway_minutes']} min")

    # Try by direction
    print("\n5. Headways by Direction:")
    for direction in [0, 1]:
        result_dir = calculate_headways(
            db,
            'C51',
            start_time=today_start,
            end_time=today_end,
            direction_id=direction,
            max_headway_minutes=120.0,
            proximity_meters=150.0
        )

        if 'error' not in result_dir:
            print(f"   Direction {direction}:")
            print(f"     Vehicles passed: {result_dir.get('vehicles_passed_stop')}")
            print(f"     Valid headways: {result_dir.get('count')}")
            if result_dir.get('avg_headway_minutes'):
                print(f"     Avg headway: {result_dir.get('avg_headway_minutes')} min")

    print("\n" + "=" * 70)
    print("Analysis complete")
    print("=" * 70)
    print("\nNote: With only 20 minutes of data collection, we may see the same")
    print("buses passing the reference stop multiple times, giving artificially")
    print("short headways. Longer data collection periods are needed for accurate")
    print("headway measurement.")

finally:
    db.close()
