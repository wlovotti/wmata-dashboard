"""
Investigate the D80 headway anomaly (0.05 minute headway)
"""
from src.database import get_session
from src.analytics import calculate_headways

db = get_session()

try:
    print("=" * 80)
    print("D80 Headway Detail Investigation")
    print("=" * 80)

    result = calculate_headways(db, 'D80')

    print(f"\nReference stop: {result['stop_name']} ({result['stop_id']})")
    print(f"Direction: {result['direction_id']}")
    print(f"Location: {result['reference_stop_location']}")
    print(f"\nVehicles passed stop: {result['vehicles_passed_stop']}")
    print(f"Valid headways: {result['count']}")

    # Sort headways by time to see them in order
    headways = result['valid_headways']
    headways_sorted = sorted(headways, key=lambda x: x['previous_time'])

    print("\n" + "=" * 80)
    print("ALL HEADWAYS IN CHRONOLOGICAL ORDER")
    print("=" * 80)
    print(f"{'Prev Vehicle':<15} {'Curr Vehicle':<15} {'Prev Time':<20} {'Curr Time':<20} {'Headway':<10}")
    print("-" * 80)

    for hw in headways_sorted:
        print(f"{hw['previous_vehicle']:<15} {hw['current_vehicle']:<15} "
              f"{hw['previous_time'][-8:]:<20} {hw['current_time'][-8:]:<20} "
              f"{hw['headway_minutes']:>8.2f} min")

    # Find the suspicious one
    print("\n" + "=" * 80)
    print("SUSPICIOUS HEADWAYS (< 1 minute)")
    print("=" * 80)

    suspicious = [hw for hw in headways if hw['headway_minutes'] < 1.0]

    if suspicious:
        for hw in suspicious:
            prev_vehicle = hw['previous_vehicle']
            curr_vehicle = hw['current_vehicle']

            print(f"\nHeadway: {hw['headway_minutes']} minutes")
            print(f"Previous vehicle: {prev_vehicle} at {hw['previous_time']}")
            print(f"Current vehicle: {curr_vehicle} at {hw['current_time']}")

            # Query the actual position records to see trip_id and direction
            from src.models import VehiclePosition, Trip
            from datetime import datetime, timedelta

            prev_time = datetime.fromisoformat(hw['previous_time'])
            curr_time = datetime.fromisoformat(hw['current_time'])

            # Get prev vehicle positions around that time
            prev_positions = db.query(VehiclePosition).filter(
                VehiclePosition.vehicle_id == prev_vehicle,
                VehiclePosition.route_id == 'D80',
                VehiclePosition.timestamp >= prev_time - timedelta(minutes=1),
                VehiclePosition.timestamp <= prev_time + timedelta(minutes=1)
            ).order_by(VehiclePosition.timestamp).all()

            # Get curr vehicle positions around that time
            curr_positions = db.query(VehiclePosition).filter(
                VehiclePosition.vehicle_id == curr_vehicle,
                VehiclePosition.route_id == 'D80',
                VehiclePosition.timestamp >= curr_time - timedelta(minutes=1),
                VehiclePosition.timestamp <= curr_time + timedelta(minutes=1)
            ).order_by(VehiclePosition.timestamp).all()

            print(f"\nPrevious vehicle ({prev_vehicle}) positions around departure:")
            for p in prev_positions[:5]:
                trip = db.query(Trip).filter(Trip.trip_id == p.trip_id).first() if p.trip_id else None
                direction = trip.direction_id if trip else None
                print(f"  {p.timestamp} - trip={p.trip_id}, dir={direction}, lat={p.latitude:.4f}, lon={p.longitude:.4f}")

            print(f"\nCurrent vehicle ({curr_vehicle}) positions around departure:")
            for p in curr_positions[:5]:
                trip = db.query(Trip).filter(Trip.trip_id == p.trip_id).first() if p.trip_id else None
                direction = trip.direction_id if trip else None
                print(f"  {p.timestamp} - trip={p.trip_id}, dir={direction}, lat={p.latitude:.4f}, lon={p.longitude:.4f}")
    else:
        print("No suspicious headways found!")

finally:
    db.close()
