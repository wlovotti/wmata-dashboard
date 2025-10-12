"""
Analytics module for calculating transit performance metrics
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from sqlalchemy import and_, func
from sqlalchemy.orm import Session
import math

from models import VehiclePosition, Route, Trip, StopTime, Stop
from database import get_session


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth
    Returns distance in meters
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # Radius of Earth in meters
    r = 6371000
    return c * r


def get_route_service_hours(db: Session, route_id: str) -> Tuple[int, int]:
    """
    Get service start/end hours from GTFS schedule for a specific route.

    Note: GTFS uses times >=24:00:00 for service past midnight (e.g., 25:30 = 1:30am next day).
    This function returns the actual hours considering this convention.

    Returns:
        Tuple of (start_hour, end_hour) as integers
        - start_hour: 0-23 (hour of day service starts)
        - end_hour: 0-47 (can be >24 to represent early morning hours of next day)
    """
    # Get all stop_times for this route and extract hours
    # We can't use MIN/MAX on strings because "9" > "25" alphabetically
    stop_times = db.query(StopTime.arrival_time).join(Trip).filter(
        Trip.route_id == route_id
    ).all()

    if not stop_times:
        return (5, 23)  # Default

    hours = []
    for (time_str,) in stop_times:
        try:
            hour = int(time_str.split(':')[0])
            hours.append(hour)
        except (ValueError, AttributeError):
            continue

    if not hours:
        return (5, 23)

    min_hour = min(hours) % 24  # Normalize to 0-23
    max_hour = max(hours)  # Keep as-is (can be >24)

    return (min_hour, max_hour)


def get_vehicle_positions(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    direction_id: Optional[int] = None
) -> List[VehiclePosition]:
    """
    Get vehicle positions for a route within a time range, optionally filtered by direction
    """
    query = db.query(VehiclePosition).filter(VehiclePosition.route_id == route_id)

    if start_time:
        query = query.filter(VehiclePosition.timestamp >= start_time)
    if end_time:
        query = query.filter(VehiclePosition.timestamp <= end_time)

    # Filter by direction if specified
    if direction_id is not None:
        # Join with Trip to get direction_id
        query = query.join(Trip, VehiclePosition.trip_id == Trip.trip_id).filter(
            Trip.direction_id == direction_id
        )

    return query.order_by(VehiclePosition.timestamp).all()


def find_reference_stop(
    db: Session,
    route_id: str,
    direction_id: Optional[int] = None
) -> Optional[str]:
    """
    Find a good reference stop for headway measurement.

    A good reference stop is one that:
    - All (or most) trips on the route pass through
    - Is roughly in the middle of the route (not first or last stop)

    Args:
        db: Database session
        route_id: Route to analyze
        direction_id: Optional direction filter

    Returns:
        stop_id of the reference stop, or None if not found
    """
    # Get all trips for this route/direction
    trip_query = db.query(Trip).filter(Trip.route_id == route_id)
    if direction_id is not None:
        trip_query = trip_query.filter(Trip.direction_id == direction_id)

    trips = trip_query.all()
    if not trips:
        return None

    # Count how many trips pass through each stop
    stop_counts = {}
    stop_avg_sequence = {}

    for trip in trips:
        stop_times = db.query(StopTime).filter(
            StopTime.trip_id == trip.trip_id
        ).all()

        for st in stop_times:
            if st.stop_id not in stop_counts:
                stop_counts[st.stop_id] = 0
                stop_avg_sequence[st.stop_id] = []
            stop_counts[st.stop_id] += 1
            stop_avg_sequence[st.stop_id].append(st.stop_sequence)

    if not stop_counts:
        return None

    # Find stop that appears in most trips and is in the middle of the route
    max_count = max(stop_counts.values())
    common_stops = [sid for sid, count in stop_counts.items() if count >= max_count * 0.8]

    # Among common stops, pick one in the middle (by average sequence number)
    if common_stops:
        middle_stop = sorted(
            common_stops,
            key=lambda sid: sum(stop_avg_sequence[sid]) / len(stop_avg_sequence[sid])
        )[len(common_stops) // 2]
        return middle_stop

    return None


def calculate_headways(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    direction_id: Optional[int] = None,
    stop_id: Optional[str] = None,
    proximity_meters: float = 150.0,
    max_headway_minutes: float = 120.0,
    use_service_hours: bool = True
) -> Dict:
    """
    Calculate headways (time between consecutive buses) for a route.

    Headway is measured as the time between consecutive vehicles passing a reference stop.
    This is how transit agencies actually measure headway - at a specific location on the route.

    Method:
    1. Choose a reference stop (or use provided stop_id)
    2. For each vehicle, find when it passes closest to that stop
    3. Calculate time difference between consecutive vehicles passing that stop
    4. Filter by direction and service hours
    5. Flag outliers (data gaps vs actual headways)

    Args:
        db: Database session
        route_id: Route to analyze (e.g., 'C51')
        start_time: Start of analysis period
        end_time: End of analysis period
        direction_id: Optional direction filter (0 or 1 from GTFS)
        stop_id: Optional specific stop to measure headway at (if None, auto-select)
        proximity_meters: Distance threshold to consider vehicle "at stop"
        max_headway_minutes: Headways above this are flagged as data gaps
        use_service_hours: If True, only calculate headways during scheduled service hours

    Returns:
        Dictionary with headway statistics, individual measurements, and flagged gaps
    """
    # Get service hours from GTFS schedule if needed
    service_start_hour, service_end_hour = (0, 23)
    if use_service_hours:
        service_start_hour, service_end_hour = get_route_service_hours(db, route_id)

    # Determine reference stop
    if not stop_id:
        stop_id = find_reference_stop(db, route_id, direction_id)
        if not stop_id:
            return {
                'error': 'Could not find suitable reference stop',
                'route_id': route_id,
                'direction_id': direction_id
            }

    # Get stop location
    stop = db.query(Stop).filter(Stop.stop_id == stop_id).first()
    if not stop:
        return {
            'error': f'Stop {stop_id} not found',
            'route_id': route_id,
            'stop_id': stop_id
        }

    stop_lat, stop_lon = stop.stop_lat, stop.stop_lon

    # Get vehicle positions (filtered by direction if specified)
    positions = get_vehicle_positions(db, route_id, start_time, end_time, direction_id)

    if not positions:
        return {
            'route_id': route_id,
            'direction_id': direction_id,
            'stop_id': stop_id,
            'stop_name': stop.stop_name,
            'valid_headways': [],
            'flagged_gaps': [],
            'avg_headway_minutes': None,
            'min_headway_minutes': None,
            'max_headway_minutes': None,
            'count': 0,
            'unique_vehicles': 0,
            'service_hours': {'start': service_start_hour, 'end': service_end_hour}
        }

    # For each vehicle, track all positions and find closest approach to reference stop
    # IMPORTANT: Also track direction to ensure we don't mix opposite-direction buses
    vehicle_closest_approach = {}  # {vehicle_id: {'time': datetime, 'distance': float, 'direction': int}}

    for pos in positions:
        # Filter by service hours if enabled
        if use_service_hours:
            hour = pos.timestamp.hour

            # Handle GTFS times >24 (e.g., service_end_hour=25 means 1am next day)
            # Check if current hour is within service window
            if service_end_hour <= 23:
                # Service doesn't cross midnight
                if not (service_start_hour <= hour <= service_end_hour):
                    continue
            else:
                # Service crosses midnight (end_hour > 24)
                # Service runs from service_start_hour through midnight into early morning
                # Example: start=1, end=25 means 1am-1am (24hr), currently at 22:00 (hour=22)
                # 22 >= 1, so it's in service
                # Example: start=1, end=25, currently at 00:30 (hour=0)
                # 0 <= (25-24)=1, so it's in service (early morning continuation)
                end_hour_next_day = service_end_hour - 24
                if not (hour >= service_start_hour or hour <= end_hour_next_day):
                    continue

        # Calculate distance to reference stop
        distance = haversine_distance(pos.latitude, pos.longitude, stop_lat, stop_lon)

        # Get direction for this vehicle from its trip
        vehicle_direction = None
        if pos.trip_id:
            trip = db.query(Trip).filter(Trip.trip_id == pos.trip_id).first()
            if trip:
                vehicle_direction = trip.direction_id

        # Skip if direction_id was specified but doesn't match
        if direction_id is not None and vehicle_direction != direction_id:
            continue

        # Track closest approach for each vehicle
        if pos.vehicle_id not in vehicle_closest_approach:
            vehicle_closest_approach[pos.vehicle_id] = {
                'time': pos.timestamp,
                'distance': distance,
                'direction': vehicle_direction
            }
        else:
            # Update if this is closer
            if distance < vehicle_closest_approach[pos.vehicle_id]['distance']:
                vehicle_closest_approach[pos.vehicle_id] = {
                    'time': pos.timestamp,
                    'distance': distance,
                    'direction': vehicle_direction
                }

    # Filter vehicles that passed within proximity threshold
    # Group by direction if no specific direction was requested
    passage_times = []
    for vehicle_id, approach in vehicle_closest_approach.items():
        if approach['distance'] <= proximity_meters:
            passage_times.append({
                'vehicle_id': vehicle_id,
                'time': approach['time'],
                'distance': approach['distance'],
                'direction': approach['direction']
            })

    # If no direction filter specified, we should only compare vehicles in the same direction
    # Separate passages by direction
    if direction_id is None and passage_times:
        # Count vehicles by direction
        direction_counts = {}
        for passage in passage_times:
            dir_id = passage['direction']
            direction_counts[dir_id] = direction_counts.get(dir_id, 0) + 1

        # Use the direction with more vehicles
        if direction_counts:
            primary_direction = max(direction_counts, key=direction_counts.get)
            passage_times = [p for p in passage_times if p['direction'] == primary_direction]
            # Update direction_id for return value
            direction_id = primary_direction

    # Sort by passage time
    passage_times.sort(key=lambda x: x['time'])

    # Calculate headways (time between consecutive vehicles)
    valid_headways = []
    flagged_gaps = []

    for i in range(1, len(passage_times)):
        prev_passage = passage_times[i-1]
        curr_passage = passage_times[i]

        # Skip if crossing day boundary
        if prev_passage['time'].date() != curr_passage['time'].date():
            continue

        time_diff = curr_passage['time'] - prev_passage['time']
        headway_minutes = time_diff.total_seconds() / 60

        headway_record = {
            'previous_vehicle': prev_passage['vehicle_id'],
            'current_vehicle': curr_passage['vehicle_id'],
            'previous_time': prev_passage['time'].isoformat(),
            'current_time': curr_passage['time'].isoformat(),
            'headway_minutes': round(headway_minutes, 2)
        }

        # Flag outliers vs valid headways
        if headway_minutes > max_headway_minutes:
            flagged_gaps.append({
                **headway_record,
                'reason': 'exceeds_max_headway',
                'threshold': max_headway_minutes
            })
        else:
            valid_headways.append(headway_record)

    # Calculate statistics on valid headways only
    if valid_headways:
        headway_values = [h['headway_minutes'] for h in valid_headways]
        avg_headway = sum(headway_values) / len(headway_values)
        min_headway = min(headway_values)
        max_headway = max(headway_values)
    else:
        avg_headway = None
        min_headway = None
        max_headway = None

    return {
        'route_id': route_id,
        'direction_id': direction_id,
        'stop_id': stop_id,
        'stop_name': stop.stop_name,
        'reference_stop_location': {
            'lat': stop_lat,
            'lon': stop_lon
        },
        'proximity_threshold_meters': proximity_meters,
        'time_range': {
            'start': start_time.isoformat() if start_time else None,
            'end': end_time.isoformat() if end_time else None
        },
        'service_hours': {
            'start': service_start_hour,
            'end': service_end_hour,
            'enabled': use_service_hours
        },
        'valid_headways': valid_headways,
        'flagged_gaps': flagged_gaps,
        'avg_headway_minutes': round(avg_headway, 2) if avg_headway else None,
        'min_headway_minutes': round(min_headway, 2) if min_headway else None,
        'max_headway_minutes': round(max_headway, 2) if max_headway else None,
        'count': len(valid_headways),
        'gaps_detected': len(flagged_gaps),
        'vehicles_passed_stop': len(passage_times),
        'max_headway_threshold': max_headway_minutes
    }


def find_nearest_stop(
    db: Session,
    route_id: str,
    latitude: float,
    longitude: float,
    max_distance_meters: float = 200.0
) -> Optional[Tuple[Stop, float]]:
    """
    Find the nearest stop on a route to given coordinates

    Returns:
        Tuple of (Stop, distance_meters) or None if no stop within max_distance
    """
    # Get all stops for this route
    stops = db.query(Stop).join(StopTime).join(Trip).filter(
        Trip.route_id == route_id
    ).distinct().all()

    nearest_stop = None
    min_distance = float('inf')

    for stop in stops:
        distance = haversine_distance(latitude, longitude, stop.stop_lat, stop.stop_lon)
        if distance < min_distance and distance <= max_distance_meters:
            min_distance = distance
            nearest_stop = stop

    return (nearest_stop, min_distance) if nearest_stop else None


def calculate_on_time_performance(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    early_threshold_seconds: int = -60,  # More than 1 min early (LA Metro standard)
    late_threshold_seconds: int = 300,    # More than 5 min late (LA Metro standard)
    min_match_confidence: float = 0.3  # Minimum confidence for trip matching
) -> Dict:
    """
    Calculate on-time performance by comparing actual vehicle positions to schedule.

    Uses LA Metro's on-time definition (stricter than WMATA's 2min/-7min standard):
    - Early: More than 1 minute early (< -60 seconds)
    - On-time: Between 1 min early and 5 min late (-60 to +300 seconds)
    - Late: More than 5 minutes late (> +300 seconds)

    Since WMATA's GTFS-RT trip_ids don't match GTFS static trip_ids, this function:
    1. Uses approximate trip matching to find scheduled trip for each vehicle
    2. Finds vehicles near stops on their matched trip
    3. Compares actual arrival time to scheduled time from stop_times
    4. Classifies as early/on-time/late

    Args:
        db: Database session
        route_id: Route to analyze
        start_time: Start of analysis period
        end_time: End of analysis period
        early_threshold_seconds: Seconds early to be considered "early" (default: -60, LA Metro)
        late_threshold_seconds: Seconds late to be considered "late" (default: +300, LA Metro)
        min_match_confidence: Minimum confidence score for trip matching (0-1)

    Returns:
        Dictionary with on-time performance statistics
    """
    from trip_matching import find_matching_trip

    # Get vehicle positions
    positions = get_vehicle_positions(db, route_id, start_time, end_time)

    if not positions:
        return {
            'route_id': route_id,
            'on_time_percentage': None,
            'early_percentage': None,
            'late_percentage': None,
            'arrivals_analyzed': 0,
            'early_count': 0,
            'on_time_count': 0,
            'late_count': 0,
            'matched_vehicles': 0,
            'unmatched_vehicles': 0
        }

    # Track vehicle arrivals at stops
    arrivals = []  # List of {vehicle_id, stop_id, actual_time, scheduled_time, diff}
    matched_count = 0
    unmatched_count = 0

    # Process positions and match to scheduled trips
    for pos in positions:
        # Use trip matching to find the scheduled trip this vehicle is running
        match_result = find_matching_trip(db, pos)

        if not match_result or match_result[1] < min_match_confidence:
            unmatched_count += 1
            continue

        matched_trip, confidence = match_result
        matched_count += 1

        # Find nearest stop on this matched trip
        nearest = find_nearest_stop(db, route_id, pos.latitude, pos.longitude)
        if not nearest:
            continue

        stop, distance = nearest

        # Get scheduled time for the MATCHED trip at this stop
        stop_time = db.query(StopTime).filter(
            and_(
                StopTime.trip_id == matched_trip.trip_id,
                StopTime.stop_id == stop.stop_id
            )
        ).first()

        if not stop_time:
            continue

        # Parse scheduled arrival time (format: "HH:MM:SS")
        # Note: GTFS times can be > 24:00:00 for trips after midnight
        scheduled_time_str = stop_time.arrival_time
        try:
            hours, minutes, seconds = map(int, scheduled_time_str.split(':'))

            # Create a datetime from the position timestamp's date + scheduled time
            scheduled_dt = pos.timestamp.replace(
                hour=hours % 24,
                minute=minutes,
                second=seconds,
                microsecond=0
            )

            # If hours >= 24, add a day
            if hours >= 24:
                scheduled_dt += timedelta(days=hours // 24)

            # Calculate difference (actual - scheduled)
            diff_seconds = (pos.timestamp - scheduled_dt).total_seconds()

            arrivals.append({
                'vehicle_id': pos.vehicle_id,
                'stop_id': stop.stop_id,
                'stop_name': stop.stop_name,
                'actual_time': pos.timestamp,
                'scheduled_time': scheduled_dt,
                'difference_seconds': diff_seconds,
                'distance_meters': distance,
                'matched_trip_id': matched_trip.trip_id,
                'match_confidence': confidence
            })

        except (ValueError, AttributeError):
            # Skip if time parsing fails
            continue

    if not arrivals:
        return {
            'route_id': route_id,
            'on_time_percentage': None,
            'early_percentage': None,
            'late_percentage': None,
            'arrivals_analyzed': 0,
            'early_count': 0,
            'on_time_count': 0,
            'late_count': 0,
            'matched_vehicles': matched_count,
            'unmatched_vehicles': unmatched_count,
            'sample_arrivals': []
        }

    # Classify arrivals
    early_count = sum(1 for a in arrivals if a['difference_seconds'] < early_threshold_seconds)
    late_count = sum(1 for a in arrivals if a['difference_seconds'] > late_threshold_seconds)
    on_time_count = len(arrivals) - early_count - late_count

    total = len(arrivals)

    return {
        'route_id': route_id,
        'time_range': {
            'start': start_time.isoformat() if start_time else None,
            'end': end_time.isoformat() if end_time else None
        },
        'on_time_percentage': round((on_time_count / total) * 100, 2) if total > 0 else None,
        'early_percentage': round((early_count / total) * 100, 2) if total > 0 else None,
        'late_percentage': round((late_count / total) * 100, 2) if total > 0 else None,
        'arrivals_analyzed': total,
        'early_count': early_count,
        'on_time_count': on_time_count,
        'late_count': late_count,
        'matched_vehicles': matched_count,
        'unmatched_vehicles': unmatched_count,
        'thresholds': {
            'early_threshold_seconds': early_threshold_seconds,
            'late_threshold_seconds': late_threshold_seconds,
            'min_match_confidence': min_match_confidence
        },
        'sample_arrivals': arrivals[:10]  # First 10 for inspection
    }


def get_route_summary(db: Session, route_id: str) -> Dict:
    """
    Get a summary of data available for a route
    """
    route = db.query(Route).filter(Route.route_id == route_id).first()

    if not route:
        return {'error': f'Route {route_id} not found'}

    # Count trips
    trip_count = db.query(Trip).filter(Trip.route_id == route_id).count()

    # Count vehicle positions
    position_count = db.query(VehiclePosition).filter(VehiclePosition.route_id == route_id).count()

    # Get time range of collected data
    time_range = db.query(
        func.min(VehiclePosition.timestamp),
        func.max(VehiclePosition.timestamp)
    ).filter(VehiclePosition.route_id == route_id).first()

    # Count unique vehicles
    unique_vehicles = db.query(func.count(func.distinct(VehiclePosition.vehicle_id))).filter(
        VehiclePosition.route_id == route_id
    ).scalar()

    return {
        'route_id': route.route_id,
        'route_name': route.route_short_name,
        'route_long_name': route.route_long_name,
        'scheduled_trips': trip_count,
        'vehicle_positions_collected': position_count,
        'unique_vehicles_tracked': unique_vehicles,
        'data_time_range': {
            'start': time_range[0].isoformat() if time_range[0] else None,
            'end': time_range[1].isoformat() if time_range[1] else None,
            'duration_minutes': ((time_range[1] - time_range[0]).total_seconds() / 60) if time_range[0] and time_range[1] else None
        }
    }


# Example usage
if __name__ == "__main__":
    db = get_session()

    try:
        print("=" * 60)
        print("WMATA Analytics Example")
        print("=" * 60)

        # Get C51 route summary
        print("\n1. Route Summary:")
        summary = get_route_summary(db, 'C51')
        print(f"   Route: {summary.get('route_name')} - {summary.get('route_long_name')}")
        print(f"   Vehicle positions collected: {summary.get('vehicle_positions_collected')}")
        print(f"   Unique vehicles tracked: {summary.get('unique_vehicles_tracked')}")
        print(f"   Data duration: {summary.get('data_time_range', {}).get('duration_minutes', 0):.1f} minutes")

        # Calculate headways
        print("\n2. Headway Analysis:")
        headways = calculate_headways(db, 'C51')
        print(f"   Average headway: {headways.get('avg_headway_minutes')} minutes")
        print(f"   Min headway: {headways.get('min_headway_minutes')} minutes")
        print(f"   Max headway: {headways.get('max_headway_minutes')} minutes")
        print(f"   Measurements: {headways.get('count')}")

        # Calculate on-time performance
        print("\n3. On-Time Performance:")
        otp = calculate_on_time_performance(db, 'C51')
        print(f"   On-time: {otp.get('on_time_percentage')}%")
        print(f"   Early: {otp.get('early_percentage')}%")
        print(f"   Late: {otp.get('late_percentage')}%")
        print(f"   Arrivals analyzed: {otp.get('arrivals_analyzed')}")

        print("\n" + "=" * 60)

    finally:
        db.close()
