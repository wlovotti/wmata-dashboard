"""
Investigate why so many arrivals are classified as "early"
"""
from datetime import timedelta

from sqlalchemy import and_

from src.analytics import find_nearest_stop
from src.database import get_session
from src.models import StopTime, VehiclePosition
from src.trip_matching import find_matching_trip

db = get_session()

try:
    # Get D80 vehicle positions
    route_id = 'D80'
    positions = db.query(VehiclePosition).filter(
        VehiclePosition.route_id == route_id
    ).order_by(VehiclePosition.timestamp).all()

    print(f"Analyzing {len(positions)} positions for route {route_id}")
    print("=" * 70)

    # Collect lateness data
    lateness_values = []

    for pos in positions:
        # Match to trip
        match_result = find_matching_trip(db, pos)
        if not match_result or match_result[1] < 0.3:
            continue

        matched_trip, confidence = match_result

        # Find nearest stop
        nearest = find_nearest_stop(db, route_id, pos.latitude, pos.longitude, max_distance_meters=50.0)
        if not nearest:
            continue

        stop, distance = nearest

        # Get scheduled time
        stop_time = db.query(StopTime).filter(
            and_(
                StopTime.trip_id == matched_trip.trip_id,
                StopTime.stop_id == stop.stop_id
            )
        ).first()

        if not stop_time:
            continue

        try:
            hours, minutes, seconds = map(int, stop_time.arrival_time.split(':'))
            scheduled_dt = pos.timestamp.replace(
                hour=hours % 24,
                minute=minutes,
                second=seconds,
                microsecond=0
            )
            if hours >= 24:
                scheduled_dt += timedelta(days=hours // 24)

            diff_seconds = (pos.timestamp - scheduled_dt).total_seconds()
            diff_minutes = diff_seconds / 60.0

            lateness_values.append({
                'vehicle_id': pos.vehicle_id,
                'stop_name': stop.stop_name,
                'diff_seconds': diff_seconds,
                'diff_minutes': diff_minutes,
                'scheduled_time': scheduled_dt.strftime('%H:%M:%S'),
                'actual_time': pos.timestamp.strftime('%H:%M:%S')
            })
        except (ValueError, AttributeError):
            continue

    print(f"\nAnalyzed {len(lateness_values)} arrivals with valid schedule data")
    print("=" * 70)

    # Sort by lateness
    lateness_values.sort(key=lambda x: x['diff_minutes'])

    # Show distribution
    print("\nDistribution of lateness (minutes):")
    print("-" * 70)

    buckets = {
        'Very Early (< -5 min)': 0,
        'Early (-5 to -1 min)': 0,
        'On-Time (-1 to +5 min)': 0,
        'Late (+5 to +10 min)': 0,
        'Very Late (> +10 min)': 0
    }

    for arrival in lateness_values:
        mins = arrival['diff_minutes']
        if mins < -5:
            buckets['Very Early (< -5 min)'] += 1
        elif -5 <= mins < -1:
            buckets['Early (-5 to -1 min)'] += 1
        elif -1 <= mins <= 5:
            buckets['On-Time (-1 to +5 min)'] += 1
        elif 5 < mins <= 10:
            buckets['Late (+5 to +10 min)'] += 1
        else:
            buckets['Very Late (> +10 min)'] += 1

    for bucket, count in buckets.items():
        pct = (count / len(lateness_values) * 100) if lateness_values else 0
        print(f"  {bucket:30s}: {count:4d} ({pct:5.1f}%)")

    # Show most early arrivals
    print("\nTop 10 EARLIEST arrivals:")
    print("-" * 70)
    for arrival in lateness_values[:10]:
        print(f"  Vehicle {arrival['vehicle_id']:10s} at {arrival['stop_name'][:40]:40s}")
        print(f"    Scheduled: {arrival['scheduled_time']}, Actual: {arrival['actual_time']}, Diff: {arrival['diff_minutes']:+.1f} min")

    # Show most late arrivals
    print("\nTop 10 LATEST arrivals:")
    print("-" * 70)
    for arrival in lateness_values[-10:]:
        print(f"  Vehicle {arrival['vehicle_id']:10s} at {arrival['stop_name'][:40]:40s}")
        print(f"    Scheduled: {arrival['scheduled_time']}, Actual: {arrival['actual_time']}, Diff: {arrival['diff_minutes']:+.1f} min")

    # Statistics
    if lateness_values:
        avg_lateness = sum(a['diff_minutes'] for a in lateness_values) / len(lateness_values)
        print("\n" + "=" * 70)
        print(f"Average lateness: {avg_lateness:+.1f} minutes")
        print(f"Min lateness: {lateness_values[0]['diff_minutes']:+.1f} minutes")
        print(f"Max lateness: {lateness_values[-1]['diff_minutes']:+.1f} minutes")

finally:
    db.close()
