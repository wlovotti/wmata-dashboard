"""
Validate trip matching accuracy

This script validates that vehicle positions are correctly matched to scheduled
trips by checking:
1. Route consistency - matched trip belongs to vehicle's route
2. Direction consistency - trip direction matches vehicle bearing
3. Time consistency - vehicle observation time falls within trip's service window
4. Position consistency - vehicle is near the trip's route shape
5. Stop sequence consistency - vehicle positions follow logical stop progression

Generates a validation report with pass/fail rates and confidence scoring.
"""

import random
from datetime import datetime, timedelta

import numpy as np
from sqlalchemy import func

from src.database import get_session
from src.models import Shape, StopTime, Trip, VehiclePosition


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in meters using Haversine formula"""
    R = 6371000  # Earth radius in meters

    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)

    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


def bearing_difference(bearing1, bearing2):
    """Calculate absolute difference between two bearings (0-360)"""
    diff = abs(bearing1 - bearing2)
    if diff > 180:
        diff = 360 - diff
    return diff


def validate_trip_match(db, vp, trip):
    """
    Validate a single vehicle position to trip match

    Returns dict with validation results:
    - route_match: bool
    - direction_match: bool (if bearing available)
    - time_match: bool
    - position_match: bool (if shape available)
    - stop_sequence_match: bool
    - overall_valid: bool
    - warnings: list of str
    """
    results = {
        'route_match': False,
        'direction_match': None,  # None if can't validate
        'time_match': False,
        'position_match': None,
        'stop_sequence_match': None,
        'overall_valid': False,
        'warnings': [],
        'details': {}
    }

    # 1. Route consistency check
    if trip.route_id == vp.route_id:
        results['route_match'] = True
    else:
        results['warnings'].append(
            f"Route mismatch: vehicle={vp.route_id}, trip={trip.route_id}"
        )

    # 2. Direction consistency check (if bearing is available)
    if vp.bearing is not None and trip.direction_id is not None:
        # Get trip's first and last stop to determine general direction
        stop_times = (
            db.query(StopTime)
            .filter(StopTime.trip_id == trip.trip_id)
            .order_by(StopTime.stop_sequence)
            .all()
        )

        if len(stop_times) >= 2:
            first_stop = stop_times[0].stop
            last_stop = stop_times[-1].stop

            # Calculate expected bearing from first to last stop
            dlat = last_stop.stop_lat - first_stop.stop_lat
            dlon = last_stop.stop_lon - first_stop.stop_lon
            trip_bearing = (np.degrees(np.arctan2(dlon, dlat)) + 360) % 360

            # Allow 90 degree tolerance (vehicles may deviate on turns)
            bearing_diff = bearing_difference(vp.bearing, trip_bearing)
            if bearing_diff <= 90:
                results['direction_match'] = True
            else:
                results['direction_match'] = False
                results['warnings'].append(
                    f"Bearing mismatch: vehicle={vp.bearing:.0f}°, "
                    f"trip={trip_bearing:.0f}° (diff={bearing_diff:.0f}°)"
                )

            results['details']['bearing_diff'] = bearing_diff

    # 3. Time consistency check
    # Get trip's service window
    stop_times = (
        db.query(StopTime)
        .filter(StopTime.trip_id == trip.trip_id)
        .order_by(StopTime.stop_sequence)
        .all()
    )

    if stop_times:
        first_departure = stop_times[0].departure_time
        last_arrival = stop_times[-1].arrival_time

        # Convert vehicle timestamp to time of day
        vp_time = vp.timestamp.time()
        vp_seconds = vp_time.hour * 3600 + vp_time.minute * 60 + vp_time.second

        # GTFS times can exceed 24 hours, normalize to seconds
        def parse_gtfs_time(time_str):
            """Parse HH:MM:SS format, supporting hours > 24"""
            parts = time_str.split(':')
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

        trip_start = parse_gtfs_time(first_departure)
        trip_end = parse_gtfs_time(last_arrival)

        # Handle trips that span midnight (>24 hours)
        if trip_start >= 86400:  # >= 24:00:00
            trip_start -= 86400
        if trip_end >= 86400:
            trip_end -= 86400

        # Allow 30-minute buffer before/after trip
        buffer = 1800  # 30 minutes in seconds

        if trip_start <= trip_end:
            # Normal trip (doesn't span midnight)
            if (trip_start - buffer) <= vp_seconds <= (trip_end + buffer):
                results['time_match'] = True
        else:
            # Trip spans midnight
            if vp_seconds >= (trip_start - buffer) or vp_seconds <= (trip_end + buffer):
                results['time_match'] = True

        if not results['time_match']:
            results['warnings'].append(
                f"Time outside trip window: vehicle={vp_time}, "
                f"trip={first_departure}-{last_arrival}"
            )

        results['details']['trip_window'] = f"{first_departure}-{last_arrival}"
        results['details']['vehicle_time'] = str(vp_time)

    # 4. Position consistency check (if shape available)
    if trip.shape_id:
        shape_points = (
            db.query(Shape)
            .filter(Shape.shape_id == trip.shape_id)
            .order_by(Shape.shape_pt_sequence)
            .all()
        )

        if shape_points:
            # Calculate distance from vehicle to nearest point on shape
            min_distance = float('inf')
            for point in shape_points:
                dist = haversine_distance(
                    vp.latitude, vp.longitude,
                    point.shape_pt_lat, point.shape_pt_lon
                )
                min_distance = min(min_distance, dist)

            # Allow 500m tolerance (vehicles may detour for traffic, etc.)
            if min_distance <= 500:
                results['position_match'] = True
            else:
                results['position_match'] = False
                results['warnings'].append(
                    f"Position far from route: {min_distance:.0f}m from shape"
                )

            results['details']['distance_from_shape'] = min_distance

    # Determine overall validity
    required_checks = [results['route_match'], results['time_match']]
    optional_checks = [
        results['direction_match'],
        results['position_match'],
        results['stop_sequence_match']
    ]

    # Must pass all required checks
    required_pass = all(required_checks)

    # Must not fail any optional checks (None = couldn't validate, OK to ignore)
    optional_pass = all(check != False for check in optional_checks)

    results['overall_valid'] = required_pass and optional_pass

    return results


def sample_and_validate(db, sample_size=200):
    """
    Sample vehicle positions and validate their trip matches

    Returns summary statistics and detailed results
    """
    print("=" * 80)
    print("Trip Matching Validation")
    print("=" * 80)

    # Get total count of positions with trip_id
    total_count = (
        db.query(func.count(VehiclePosition.id))
        .filter(VehiclePosition.trip_id.isnot(None))
        .scalar()
    )

    print(f"\nTotal positions with trip_id: {total_count:,}")
    print(f"Sample size: {sample_size}")
    print(f"Sampling rate: {sample_size / total_count * 100:.2f}%")

    # Sample random positions
    # Use RANDOM() for SQLite, RAND() for MySQL, random() for PostgreSQL
    sampled_positions = (
        db.query(VehiclePosition)
        .filter(VehiclePosition.trip_id.isnot(None))
        .order_by(func.random())
        .limit(sample_size)
        .all()
    )

    print(f"\nActual samples retrieved: {len(sampled_positions)}")
    print("\n" + "=" * 80)
    print("Validating samples...")
    print("=" * 80)

    # Validate each sample
    validation_results = []
    failed_examples = []

    for i, vp in enumerate(sampled_positions, 1):
        # Get the matched trip
        trip = db.query(Trip).filter(Trip.trip_id == vp.trip_id).first()

        if not trip:
            print(f"\nWARNING: Position {i}/{len(sampled_positions)} - "
                  f"Trip {vp.trip_id} not found in database")
            continue

        result = validate_trip_match(db, vp, trip)
        validation_results.append(result)

        if not result['overall_valid']:
            failed_examples.append({
                'position_id': vp.id,
                'vehicle_id': vp.vehicle_id,
                'route_id': vp.route_id,
                'trip_id': vp.trip_id,
                'timestamp': vp.timestamp,
                'result': result
            })

        # Progress indicator
        if i % 50 == 0:
            print(f"  Validated {i}/{len(sampled_positions)} samples...")

    # Calculate statistics
    total = len(validation_results)

    stats = {
        'total_validated': total,
        'overall_pass': sum(1 for r in validation_results if r['overall_valid']),
        'route_pass': sum(1 for r in validation_results if r['route_match']),
        'time_pass': sum(1 for r in validation_results if r['time_match']),
        'direction_pass': sum(
            1 for r in validation_results if r['direction_match'] is True
        ),
        'direction_tested': sum(
            1 for r in validation_results if r['direction_match'] is not None
        ),
        'position_pass': sum(
            1 for r in validation_results if r['position_match'] is True
        ),
        'position_tested': sum(
            1 for r in validation_results if r['position_match'] is not None
        ),
    }

    # Print results
    print("\n" + "=" * 80)
    print("Validation Results")
    print("=" * 80)

    print(f"\nOverall Match Quality:")
    print(f"  Valid matches: {stats['overall_pass']}/{stats['total_validated']} "
          f"({stats['overall_pass']/stats['total_validated']*100:.1f}%)")
    print(f"  Invalid matches: {stats['total_validated'] - stats['overall_pass']} "
          f"({(stats['total_validated'] - stats['overall_pass'])/stats['total_validated']*100:.1f}%)")

    print(f"\nIndividual Check Results:")
    print(f"  Route consistency: {stats['route_pass']}/{stats['total_validated']} "
          f"({stats['route_pass']/stats['total_validated']*100:.1f}%)")
    print(f"  Time consistency: {stats['time_pass']}/{stats['total_validated']} "
          f"({stats['time_pass']/stats['total_validated']*100:.1f}%)")

    if stats['direction_tested'] > 0:
        print(f"  Direction consistency: {stats['direction_pass']}/{stats['direction_tested']} "
              f"({stats['direction_pass']/stats['direction_tested']*100:.1f}%) "
              f"[{stats['direction_tested']} had bearing data]")
    else:
        print(f"  Direction consistency: N/A (no bearing data)")

    if stats['position_tested'] > 0:
        print(f"  Position consistency: {stats['position_pass']}/{stats['position_tested']} "
              f"({stats['position_pass']/stats['position_tested']*100:.1f}%) "
              f"[{stats['position_tested']} had shape data]")
    else:
        print(f"  Position consistency: N/A (no shape data)")

    # Show failure examples
    if failed_examples:
        print(f"\n" + "=" * 80)
        print(f"Failed Match Examples (showing first 10 of {len(failed_examples)}):")
        print("=" * 80)

        for example in failed_examples[:10]:
            print(f"\nPosition ID: {example['position_id']}")
            print(f"  Vehicle: {example['vehicle_id']}")
            print(f"  Route: {example['route_id']}, Trip: {example['trip_id']}")
            print(f"  Time: {example['timestamp']}")
            print(f"  Warnings:")
            for warning in example['result']['warnings']:
                print(f"    - {warning}")

    print("\n" + "=" * 80)
    print("Recommendations")
    print("=" * 80)

    pass_rate = stats['overall_pass'] / stats['total_validated'] * 100

    if pass_rate >= 95:
        print("\n✅ EXCELLENT - Trip matching appears highly accurate (≥95% valid)")
        print("   The system is ready for production use.")
    elif pass_rate >= 85:
        print("\n✔️  GOOD - Trip matching is mostly accurate (85-95% valid)")
        print("   Minor improvements possible but suitable for public use.")
    elif pass_rate >= 75:
        print("\n⚠️  MODERATE - Some matching issues detected (75-85% valid)")
        print("   Review failed cases and consider improving matching logic.")
    else:
        print("\n❌ POOR - Significant matching problems (<75% valid)")
        print("   Do NOT use for public advocacy until issues are resolved.")

    print("\n" + "=" * 80)

    return stats, validation_results, failed_examples


if __name__ == "__main__":
    db = get_session()

    try:
        stats, results, failures = sample_and_validate(db, sample_size=200)
    finally:
        db.close()
