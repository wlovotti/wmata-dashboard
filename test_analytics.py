"""
Test the improved analytics functions with today's C51 data
"""
from datetime import datetime, timedelta
from database import get_session
from analytics import calculate_headways, get_route_summary, get_route_service_hours

db = get_session()

try:
    print("=" * 60)
    print("Testing Improved Analytics with Today's C51 Data")
    print("=" * 60)

    # Get route summary
    print("\n1. Route Summary:")
    summary = get_route_summary(db, 'C51')
    print(f"   Route: {summary.get('route_name')} - {summary.get('route_long_name')}")
    print(f"   Vehicle positions collected: {summary.get('vehicle_positions_collected')}")
    print(f"   Unique vehicles tracked: {summary.get('unique_vehicles_tracked')}")

    # Get service hours from GTFS
    print("\n2. Service Hours (from GTFS schedule):")
    start_hour, end_hour = get_route_service_hours(db, 'C51')
    print(f"   C51 operates from {start_hour}:00 to {end_hour}:00")

    # Calculate headways for today only
    print("\n3. Headway Analysis (Today's Data Only):")
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    headways = calculate_headways(
        db,
        'C51',
        start_time=today_start,
        end_time=today_end,
        max_headway_minutes=60.0,  # Flag gaps > 60 min
        use_service_hours=True
    )

    print(f"   Time range: {today_start.strftime('%Y-%m-%d')} (today only)")
    print(f"   Service hours filter: {'enabled' if headways.get('service_hours', {}).get('enabled') else 'disabled'}")
    print(f"   Unique vehicles: {headways.get('unique_vehicles')}")
    print(f"   Valid headways: {headways.get('count')}")
    print(f"   Flagged gaps: {headways.get('gaps_detected')}")

    if headways.get('avg_headway_minutes'):
        print(f"   Average headway: {headways.get('avg_headway_minutes')} minutes")
        print(f"   Min headway: {headways.get('min_headway_minutes')} minutes")
        print(f"   Max headway: {headways.get('max_headway_minutes')} minutes")
    else:
        print("   No valid headways calculated yet (need more data)")

    # Show some sample headways
    if headways.get('valid_headways'):
        print("\n   Sample valid headways (first 5):")
        for hw in headways.get('valid_headways')[:5]:
            print(f"     {hw['previous_time'][-8:]} → {hw['current_time'][-8:]}: {hw['headway_minutes']} min")

    # Show flagged gaps
    if headways.get('flagged_gaps'):
        print(f"\n   Flagged data gaps (>{headways.get('max_headway_threshold')} min):")
        for gap in headways.get('flagged_gaps')[:3]:
            print(f"     {gap['previous_time'][-8:]} → {gap['current_time'][-8:]}: {gap['headway_minutes']} min (flagged)")

    # Test by direction
    print("\n4. Headway Analysis by Direction:")
    for direction in [0, 1]:
        headways_dir = calculate_headways(
            db,
            'C51',
            start_time=today_start,
            end_time=today_end,
            direction_id=direction,
            max_headway_minutes=60.0
        )
        print(f"   Direction {direction}: {headways_dir.get('count')} valid headways, "
              f"{headways_dir.get('unique_vehicles')} vehicles")

    print("\n" + "=" * 60)
    print("Analytics test complete!")
    print("=" * 60)

finally:
    db.close()
