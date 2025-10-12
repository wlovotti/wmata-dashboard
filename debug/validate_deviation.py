"""
Validate WMATA's deviation field by comparing it to our own calculations

This checks if WMATA's reported deviation matches what we calculate from
the GTFS schedule.
"""
from src.database import get_session
from src.models import BusPosition, Trip, StopTime, Stop
from src.trip_matching import find_matching_trip
from src.analytics import find_nearest_stop, haversine_distance
from datetime import timedelta
from sqlalchemy import and_

db = get_session()

try:
    print("=" * 70)
    print("Validating WMATA's Deviation Data")
    print("=" * 70)

    # Get all bus positions with deviation data
    positions = db.query(BusPosition).filter(
        BusPosition.deviation.isnot(None)
    ).all()

    if not positions:
        print("\nNo BusPosition data with deviation found in database")
        print("Run: uv run python debug/test_collect_bus_positions.py first")
        exit(0)

    print(f"\nAnalyzing {len(positions)} positions with WMATA deviation data...")
    print("-" * 70)

    comparisons = []

    for pos in positions:
        # Try to calculate our own deviation
        # We need to match this to a scheduled trip and find nearest stop

        # Create a VehiclePosition-like object for trip matching
        # (our trip_matching expects VehiclePosition model)
        class PosProxy:
            def __init__(self, bp):
                self.vehicle_id = bp.vehicle_id
                self.route_id = bp.route_id
                self.trip_id = bp.trip_id
                self.latitude = bp.latitude
                self.longitude = bp.longitude
                self.timestamp = bp.timestamp
                self.current_stop_sequence = None

        proxy = PosProxy(pos)

        # Try trip matching
        match_result = find_matching_trip(db, proxy)
        if not match_result or match_result[1] < 0.3:
            continue

        matched_trip, confidence = match_result

        # Find nearest stop
        nearest = find_nearest_stop(db, pos.route_id, pos.latitude, pos.longitude, max_distance_meters=200.0)
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

            # Our calculated deviation in minutes
            diff_seconds = (pos.timestamp - scheduled_dt).total_seconds()
            our_deviation = diff_seconds / 60.0

            # WMATA's reported deviation
            wmata_deviation = pos.deviation

            # Difference between our calculation and WMATA's
            deviation_diff = abs(our_deviation - wmata_deviation)

            comparisons.append({
                'vehicle_id': pos.vehicle_id,
                'route_id': pos.route_id,
                'stop_name': stop.stop_name,
                'distance_to_stop': distance,
                'wmata_deviation': wmata_deviation,
                'our_deviation': our_deviation,
                'difference': deviation_diff,
                'match_confidence': confidence
            })

        except (ValueError, AttributeError):
            continue

    print(f"\nSuccessfully compared {len(comparisons)} observations")
    print("=" * 70)

    if not comparisons:
        print("\nCould not validate any deviations (need more data or better trip matches)")
        exit(0)

    # Sort by difference
    comparisons.sort(key=lambda x: x['difference'])

    # Show statistics
    avg_diff = sum(c['difference'] for c in comparisons) / len(comparisons)
    max_diff = max(c['difference'] for c in comparisons)

    print(f"\nStatistics:")
    print(f"  Average difference: {avg_diff:.2f} minutes")
    print(f"  Max difference: {max_diff:.2f} minutes")

    # Count how many are close
    within_1min = sum(1 for c in comparisons if c['difference'] <= 1.0)
    within_2min = sum(1 for c in comparisons if c['difference'] <= 2.0)

    print(f"\n  Within 1 minute: {within_1min}/{len(comparisons)} ({within_1min/len(comparisons)*100:.1f}%)")
    print(f"  Within 2 minutes: {within_2min}/{len(comparisons)} ({within_2min/len(comparisons)*100:.1f}%)")

    # Show worst mismatches
    print("\n" + "=" * 70)
    print("Top 5 LARGEST Discrepancies:")
    print("=" * 70)
    print(f"{'Vehicle':10s} {'Route':8s} {'WMATA Dev':>11s} {'Our Dev':>11s} {'Diff':>8s}")
    print("-" * 70)

    for comp in comparisons[-5:]:
        print(f"{comp['vehicle_id']:10s} {comp['route_id']:8s} "
              f"{comp['wmata_deviation']:+10.1f}m {comp['our_deviation']:+10.1f}m "
              f"{comp['difference']:7.1f}m")

    # Show best matches
    print("\n" + "=" * 70)
    print("Top 5 CLOSEST Matches:")
    print("=" * 70)
    print(f"{'Vehicle':10s} {'Route':8s} {'WMATA Dev':>11s} {'Our Dev':>11s} {'Diff':>8s}")
    print("-" * 70)

    for comp in comparisons[:5]:
        print(f"{comp['vehicle_id']:10s} {comp['route_id']:8s} "
              f"{comp['wmata_deviation']:+10.1f}m {comp['our_deviation']:+10.1f}m "
              f"{comp['difference']:7.1f}m")

    print("\n" + "=" * 70)
    print("Conclusion:")
    print("=" * 70)

    if avg_diff <= 1.0 and within_1min/len(comparisons) >= 0.7:
        print("✓ WMATA's deviation appears RELIABLE")
        print("  Most values match our calculations within 1 minute")
    elif avg_diff <= 2.0 and within_2min/len(comparisons) >= 0.7:
        print("⚠ WMATA's deviation is MOSTLY RELIABLE")
        print("  Most values match within 2 minutes, but some discrepancies")
    else:
        print("✗ WMATA's deviation appears UNRELIABLE")
        print("  Significant discrepancies from our calculations")
        print("  Recommend using our own OTP calculations instead")

finally:
    db.close()
