"""
Check how many vehicle positions have trip_ids with actual stop_times
"""
from datetime import datetime

from src.database import get_session
from src.models import StopTime, VehiclePosition

db = get_session()

try:
    print("=" * 70)
    print("Checking Valid Trips for OTP")
    print("=" * 70)

    # Get today's C51 positions
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    positions = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == 'C51',
        VehiclePosition.timestamp >= today_start
    ).all()

    print(f"\nTotal C51 positions: {len(positions)}")

    # Check how many have trip_ids with stop_times
    valid_positions = []
    unique_trips_with_stops = set()
    unique_trips_without_stops = set()

    for pos in positions:
        if pos.trip_id:
            # Check if this trip has stop_times
            stop_count = db.query(StopTime).filter(
                StopTime.trip_id == pos.trip_id
            ).count()

            if stop_count > 0:
                valid_positions.append(pos)
                unique_trips_with_stops.add(pos.trip_id)
            else:
                unique_trips_without_stops.add(pos.trip_id)

    print(f"\nPositions with valid trip_ids (have stop_times): {len(valid_positions)}")
    print(f"Positions with invalid trip_ids (no stop_times): {len(positions) - len(valid_positions)}")
    print(f"\nUnique trips with stop_times: {len(unique_trips_with_stops)}")
    print(f"Unique trips without stop_times: {len(unique_trips_without_stops)}")

    if unique_trips_with_stops:
        print(f"\nValid trip_ids: {sorted(unique_trips_with_stops)}")

    if unique_trips_without_stops:
        print(f"\nInvalid trip_ids (sample): {sorted(unique_trips_without_stops)[:10]}")

    print("\n" + "=" * 70)

finally:
    db.close()
