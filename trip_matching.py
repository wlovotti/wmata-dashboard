"""
Trip matching module for matching real-time vehicle positions to scheduled GTFS trips.

WMATA's GTFS-RT trip_ids don't match GTFS static trip_ids, so we need to approximate
which scheduled trip each vehicle is running based on route, direction, time, and position.
"""
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from sqlalchemy import and_
from sqlalchemy.orm import Session

from models import Trip, StopTime, Stop, VehiclePosition
from analytics import haversine_distance


def parse_gtfs_time(time_str: str, reference_datetime: datetime) -> datetime:
    """
    Parse GTFS time string (HH:MM:SS) into datetime.

    GTFS allows times >= 24:00:00 for trips that run past midnight.

    Args:
        time_str: Time in format "HH:MM:SS" (can be > 24:00:00)
        reference_datetime: Datetime to use as base

    Returns:
        datetime object
    """
    try:
        hours, minutes, seconds = map(int, time_str.split(':'))

        # Create datetime from reference date
        result = reference_datetime.replace(hour=hours % 24, minute=minutes, second=seconds, microsecond=0)

        # If hours >= 24, add extra days
        if hours >= 24:
            result += timedelta(days=hours // 24)

        return result
    except (ValueError, AttributeError):
        return reference_datetime


def find_matching_trip(
    db: Session,
    vehicle_pos: VehiclePosition,
    max_time_diff_minutes: float = 15.0,
    max_distance_meters: float = 500.0
) -> Optional[Tuple[Trip, float]]:
    """
    Find the scheduled trip that best matches a vehicle's real-time position.

    Matching strategy:
    1. Filter trips by route and direction (from vehicle's trip_id direction lookup)
    2. Find trips that should be active at the vehicle's timestamp
    3. For each candidate trip, find nearest stop to vehicle position
    4. Score based on distance and time difference
    5. Return best match

    Args:
        db: Database session
        vehicle_pos: VehiclePosition object with real-time data
        max_time_diff_minutes: Maximum time difference to consider (default 15 min)
        max_distance_meters: Maximum distance from scheduled stop (default 500m)

    Returns:
        Tuple of (Trip, confidence_score) or None if no match found
        Confidence score is 0-1, where 1 is perfect match
    """
    if not vehicle_pos.route_id:
        return None

    # Get vehicle's direction from its (possibly invalid) trip_id
    vehicle_direction = None
    if vehicle_pos.trip_id:
        rt_trip = db.query(Trip).filter(Trip.trip_id == vehicle_pos.trip_id).first()
        if rt_trip:
            vehicle_direction = rt_trip.direction_id

    # Get all trips for this route (optionally filtered by direction)
    trip_query = db.query(Trip).filter(Trip.route_id == vehicle_pos.route_id)
    if vehicle_direction is not None:
        trip_query = trip_query.filter(Trip.direction_id == vehicle_direction)

    candidate_trips = trip_query.all()

    if not candidate_trips:
        return None

    # For each trip, calculate match score
    best_match = None
    best_score = 0.0

    vehicle_time = vehicle_pos.timestamp
    vehicle_time_of_day = vehicle_time.time()

    for trip in candidate_trips:
        # Get all stop_times for this trip
        stop_times = db.query(StopTime).filter(
            StopTime.trip_id == trip.trip_id
        ).order_by(StopTime.stop_sequence).all()

        if not stop_times:
            continue

        # Find the stop_time closest to the vehicle's current time
        best_stop_match = None
        best_time_diff = float('inf')
        best_distance = float('inf')

        for st in stop_times:
            # Parse scheduled time
            scheduled_time = parse_gtfs_time(st.arrival_time, vehicle_time)

            # Calculate time difference (positive = vehicle is late, negative = vehicle is early)
            time_diff_seconds = (vehicle_time - scheduled_time).total_seconds()
            time_diff_minutes = time_diff_seconds / 60

            # Skip if time difference is too large (allow more lateness than earliness)
            # Buses can be late but shouldn't be super early
            if time_diff_minutes < -5.0:  # More than 5 min early is suspicious
                continue
            if time_diff_minutes > max_time_diff_minutes:  # Too late
                continue

            # Get stop location
            stop = db.query(Stop).filter(Stop.stop_id == st.stop_id).first()
            if not stop:
                continue

            # Calculate distance from vehicle to this stop
            distance = haversine_distance(
                vehicle_pos.latitude, vehicle_pos.longitude,
                stop.stop_lat, stop.stop_lon
            )

            # Skip if distance is too large
            if distance > max_distance_meters:
                continue

            # Calculate match score with realistic bias:
            # 1. Distance score: closer is better
            # 2. Time score: prefer late/on-time over early (buses rarely run early)

            distance_score = distance / max_distance_meters

            # Time scoring with bias towards late/on-time:
            # - On time (±2 min): best score (0.0-0.1)
            # - Slightly early (2-5 min): moderate penalty (0.1-0.3)
            # - Late (0-15 min): small penalty (0.0-0.5)
            abs_time_diff = abs(time_diff_minutes)

            if -2.0 <= time_diff_minutes <= 2.0:
                # On-time: minimal penalty
                time_score = abs_time_diff / 20.0
            elif time_diff_minutes < 0:
                # Early: higher penalty (buses shouldn't be early)
                time_score = 0.3 + (abs_time_diff / max_time_diff_minutes) * 0.7
            else:
                # Late: moderate penalty (this is normal)
                time_score = time_diff_minutes / max_time_diff_minutes * 0.5

            combined_score = (time_score + distance_score) / 2

            # Track best match for this trip (lower score is better)
            current_best = (best_time_diff / max_time_diff_minutes + best_distance / max_distance_meters) / 2
            if best_stop_match is None or combined_score < current_best:
                best_stop_match = st
                best_time_diff = abs_time_diff
                best_distance = distance
                best_time_diff_signed = time_diff_minutes  # Keep signed version for scoring

        # If we found a match for this trip, score it
        if best_stop_match:
            # Confidence: 1.0 = perfect match (0 time diff, 0 distance)
            # Lower confidence as time diff and distance increase
            time_confidence = 1.0 - (best_time_diff / max_time_diff_minutes)
            distance_confidence = 1.0 - (best_distance / max_distance_meters)

            # Boost confidence if vehicle is late/on-time (more realistic)
            realism_bonus = 0.0
            if -2.0 <= best_time_diff_signed <= 10.0:  # On-time or reasonably late
                realism_bonus = 0.1
            elif best_time_diff_signed < -2.0:  # Early (suspicious)
                realism_bonus = -0.1

            confidence = ((time_confidence + distance_confidence) / 2) + realism_bonus
            confidence = max(0.0, min(1.0, confidence))  # Clamp to [0, 1]

            if confidence > best_score:
                best_score = confidence
                best_match = trip

    if best_match and best_score > 0.3:  # Require at least 30% confidence
        return (best_match, best_score)

    return None


def match_vehicles_to_trips(
    db: Session,
    vehicle_positions: List[VehiclePosition],
    min_confidence: float = 0.3
) -> dict:
    """
    Match multiple vehicle positions to scheduled trips.

    Args:
        db: Database session
        vehicle_positions: List of VehiclePosition objects
        min_confidence: Minimum confidence score to accept match

    Returns:
        Dictionary mapping vehicle_id to (matched_trip, confidence_score)
    """
    matches = {}

    for pos in vehicle_positions:
        match = find_matching_trip(db, pos)
        if match and match[1] >= min_confidence:
            matches[pos.vehicle_id] = match

    return matches


if __name__ == "__main__":
    # Test the matching function
    from database import get_session

    db = get_session()

    try:
        print("=" * 70)
        print("Testing Trip Matching")
        print("=" * 70)

        # Get some recent C51 vehicle positions
        recent_positions = db.query(VehiclePosition).filter(
            VehiclePosition.route_id == 'C51'
        ).order_by(VehiclePosition.timestamp.desc()).limit(10).all()

        print(f"\nTesting with {len(recent_positions)} recent vehicle positions...")

        for pos in recent_positions:
            print(f"\n Vehicle {pos.vehicle_id} at {pos.timestamp.strftime('%H:%M:%S')}:")
            print(f"   Location: ({pos.latitude:.6f}, {pos.longitude:.6f})")
            print(f"   RT trip_id: {pos.trip_id}")

            match = find_matching_trip(db, pos)

            if match:
                trip, confidence = match
                print(f"   ✓ Matched to trip: {trip.trip_id}")
                print(f"   Confidence: {confidence:.2%}")
                print(f"   Direction: {trip.direction_id}")
            else:
                print(f"   ✗ No matching trip found")

        print("\n" + "=" * 70)

    finally:
        db.close()
