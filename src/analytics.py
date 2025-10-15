"""
Analytics module for calculating transit performance metrics
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from sqlalchemy import and_, func
from sqlalchemy.orm import Session
import math
import numpy as np

from src.models import VehiclePosition, BusPosition, Route, Trip, StopTime, Stop
from src.database import get_session


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


def deduplicate_stop_passages(
    observations: List[Dict],
    group_by_keys: List[str] = None
) -> List[Dict]:
    """
    Deduplicate vehicle observations at stops, keeping only the LAST observation.

    This represents the departure time (when the bus leaves the stop), which is the
    passenger-centric view: if a bus arrives early but waits until scheduled time
    to depart, passengers can still board during the wait period.

    This deduplication is used by both OTP and headway calculations to ensure
    consistency across metrics.

    IMPORTANT: Observations are grouped by date as well, so the same vehicle/trip/stop
    combination on different dates will NOT be deduplicated against each other.

    Args:
        observations: List of observation dictionaries with at least:
            - vehicle_id: Vehicle identifier
            - trip_id: Trip identifier
            - stop_id: Stop identifier
            - timestamp: datetime of observation (used for both grouping by date and ordering)
        group_by_keys: Keys to group by (default: ['vehicle_id', 'trip_id', 'stop_id'])
                      Note: Date is always included in the grouping key automatically

    Returns:
        List of deduplicated observations (one per unique vehicle/trip/stop/date combination),
        keeping the LAST (latest timestamp) observation for each group.

    Example:
        >>> observations = [
        ...     # Same vehicle/trip/stop, same day - will deduplicate
        ...     {'vehicle_id': '4586', 'trip_id': 'T1', 'stop_id': 'S1',
        ...      'timestamp': datetime(2025, 1, 1, 12, 0, 0), 'diff_seconds': -120},
        ...     {'vehicle_id': '4586', 'trip_id': 'T1', 'stop_id': 'S1',
        ...      'timestamp': datetime(2025, 1, 1, 12, 1, 0), 'diff_seconds': -60},
        ...     # Same vehicle/trip/stop, different day - will NOT deduplicate
        ...     {'vehicle_id': '4586', 'trip_id': 'T1', 'stop_id': 'S1',
        ...      'timestamp': datetime(2025, 1, 2, 12, 0, 0), 'diff_seconds': -120}
        ... ]
        >>> deduplicated = deduplicate_stop_passages(observations)
        >>> len(deduplicated)
        2  # One for Jan 1, one for Jan 2
        >>> deduplicated[0]['timestamp']
        datetime.datetime(2025, 1, 1, 12, 1, 0)  # Kept the LAST one from Jan 1
    """
    if group_by_keys is None:
        group_by_keys = ['vehicle_id', 'trip_id', 'stop_id']

    # Group observations by the specified keys PLUS date
    # Keep only the LAST observation (latest timestamp) for each group
    observation_map = {}  # {(key_tuple + date): latest_observation}

    for obs in observations:
        # Build key tuple from the observation
        # Include date to ensure we don't deduplicate across different days
        base_key = tuple(obs.get(k) for k in group_by_keys)
        date = obs['timestamp'].date()
        key = base_key + (date,)

        if key not in observation_map:
            observation_map[key] = obs
        else:
            # Keep the observation with the later timestamp (departure time)
            if obs['timestamp'] > observation_map[key]['timestamp']:
                observation_map[key] = obs

    # Return deduplicated observations
    return list(observation_map.values())


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
    proximity_meters: float = 50.0,
    max_headway_minutes: float = 120.0,
    use_service_hours: bool = True
) -> Dict:
    """
    Calculate headways (time between consecutive buses) for a route.

    Headway is measured as the time between consecutive vehicles passing a reference stop.
    This is how transit agencies actually measure headway - at a specific location on the route.

    Method:
    1. Choose a reference stop (or use provided stop_id)
    2. For each vehicle trip, find all observations near that stop
    3. Use LAST observation at stop (departure time) to match OTP methodology
    4. Calculate time difference between consecutive vehicle departures
    5. Filter by direction and service hours
    6. Flag outliers (data gaps vs actual headways)

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

    # Collect all observations near the reference stop
    # We'll deduplicate later to keep only LAST observation (departure time)
    observations = []

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

        # Only track positions within proximity threshold
        if distance > proximity_meters:
            continue

        # Get direction for this vehicle from its trip
        vehicle_direction = None
        if pos.trip_id:
            trip = db.query(Trip).filter(Trip.trip_id == pos.trip_id).first()
            if trip:
                vehicle_direction = trip.direction_id

        # Skip if direction_id was specified but doesn't match
        if direction_id is not None and vehicle_direction != direction_id:
            continue

        # Collect this observation for deduplication
        # trip_id ensures we don't mix different trips by same vehicle
        observations.append({
            'vehicle_id': pos.vehicle_id,
            'trip_id': pos.trip_id if pos.trip_id else f"unknown_{pos.vehicle_id}",
            'stop_id': stop_id,  # Always the same for headway (reference stop)
            'timestamp': pos.timestamp,
            'distance': distance,
            'direction': vehicle_direction
        })

    # DEDUPLICATE: Keep only LAST observation at each stop for each vehicle/trip
    # This represents the departure time (when bus leaves the stop)
    # Matches the OTP methodology for consistency
    passage_times = deduplicate_stop_passages(observations)

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

    # Sort by passage time (timestamp key from deduplicated observations)
    passage_times.sort(key=lambda x: x['timestamp'])

    # Calculate headways (time between consecutive vehicles)
    valid_headways = []
    flagged_gaps = []

    for i in range(1, len(passage_times)):
        prev_passage = passage_times[i-1]
        curr_passage = passage_times[i]

        # Skip if crossing day boundary
        if prev_passage['timestamp'].date() != curr_passage['timestamp'].date():
            continue

        time_diff = curr_passage['timestamp'] - prev_passage['timestamp']
        headway_minutes = time_diff.total_seconds() / 60

        headway_record = {
            'previous_vehicle': prev_passage['vehicle_id'],
            'current_vehicle': curr_passage['vehicle_id'],
            'previous_time': prev_passage['timestamp'].isoformat(),
            'current_time': curr_passage['timestamp'].isoformat(),
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


# Cache for route stops to avoid repeated database queries
_route_stops_cache = {}

def get_route_stops(db: Session, route_id: str) -> List[Stop]:
    """
    Get all stops for a route, with caching to avoid repeated queries.

    Returns:
        List of Stop objects for the route
    """
    if route_id not in _route_stops_cache:
        stops = db.query(Stop).join(StopTime).join(Trip).filter(
            Trip.route_id == route_id
        ).distinct().all()
        _route_stops_cache[route_id] = stops

    return _route_stops_cache[route_id]


def find_nearest_stop(
    db: Session,
    route_id: str,
    latitude: float,
    longitude: float,
    max_distance_meters: float = 200.0
) -> Optional[Tuple[Stop, float]]:
    """
    Find the nearest stop on a route to given coordinates.

    Uses caching to avoid repeated database queries for the same route.

    Returns:
        Tuple of (Stop, distance_meters) or None if no stop within max_distance
    """
    # Get all stops for this route (cached)
    stops = get_route_stops(db, route_id)

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
    from src.trip_matching import find_matching_trip

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


def calculate_stop_level_otp(
    db: Session,
    route_id: str,
    stop_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    proximity_meters: float = 50.0,
    early_threshold_seconds: int = -60,
    late_threshold_seconds: int = 300,
    min_match_confidence: float = 0.3
) -> Dict:
    """
    Calculate on-time performance for a specific stop on a specific route.

    This analyzes how reliably buses arrive on-time at a particular stop,
    which helps identify problem areas along a route.

    Args:
        db: Database session
        route_id: Route to analyze (e.g., 'C51')
        stop_id: Specific stop to analyze
        start_time: Start of analysis period
        end_time: End of analysis period
        proximity_meters: Distance threshold to consider vehicle "at stop" (default: 50m)
        early_threshold_seconds: Threshold for "early" (-60 = 1 min early, LA Metro)
        late_threshold_seconds: Threshold for "late" (+300 = 5 min late, LA Metro)
        min_match_confidence: Minimum confidence for trip matching

    Returns:
        Dictionary with stop-level OTP statistics
    """
    from src.trip_matching import find_matching_trip

    # Get stop info
    stop = db.query(Stop).filter(Stop.stop_id == stop_id).first()
    if not stop:
        return {'error': f'Stop {stop_id} not found'}

    # Get vehicle positions for this route
    positions = get_vehicle_positions(db, route_id, start_time, end_time)

    if not positions:
        return {
            'route_id': route_id,
            'stop_id': stop_id,
            'stop_name': stop.stop_name,
            'arrivals_analyzed': 0,
            'on_time_percentage': None
        }

    # Find arrivals at this stop
    arrivals = []

    for pos in positions:
        # Check if vehicle is near this stop
        distance = haversine_distance(pos.latitude, pos.longitude, stop.stop_lat, stop.stop_lon)

        if distance > proximity_meters:
            continue

        # Match to scheduled trip
        match_result = find_matching_trip(db, pos)
        if not match_result or match_result[1] < min_match_confidence:
            continue

        matched_trip, confidence = match_result

        # Get scheduled time for this trip at this stop
        stop_time = db.query(StopTime).filter(
            and_(
                StopTime.trip_id == matched_trip.trip_id,
                StopTime.stop_id == stop_id
            )
        ).first()

        if not stop_time:
            continue

        # Parse scheduled time
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

            arrivals.append({
                'vehicle_id': pos.vehicle_id,
                'actual_time': pos.timestamp,
                'scheduled_time': scheduled_dt,
                'difference_seconds': diff_seconds,
                'distance_meters': distance,
                'match_confidence': confidence
            })
        except (ValueError, AttributeError):
            continue

    if not arrivals:
        return {
            'route_id': route_id,
            'stop_id': stop_id,
            'stop_name': stop.stop_name,
            'arrivals_analyzed': 0,
            'on_time_percentage': None
        }

    # Classify arrivals
    early_count = sum(1 for a in arrivals if a['difference_seconds'] < early_threshold_seconds)
    late_count = sum(1 for a in arrivals if a['difference_seconds'] > late_threshold_seconds)
    on_time_count = len(arrivals) - early_count - late_count

    total = len(arrivals)

    return {
        'route_id': route_id,
        'stop_id': stop_id,
        'stop_name': stop.stop_name,
        'stop_location': {'lat': stop.stop_lat, 'lon': stop.stop_lon},
        'arrivals_analyzed': total,
        'on_time_count': on_time_count,
        'early_count': early_count,
        'late_count': late_count,
        'on_time_percentage': round((on_time_count / total) * 100, 2) if total > 0 else None,
        'early_percentage': round((early_count / total) * 100, 2) if total > 0 else None,
        'late_percentage': round((late_count / total) * 100, 2) if total > 0 else None,
        'avg_lateness_seconds': round(sum(a['difference_seconds'] for a in arrivals) / total, 1) if total > 0 else None,
        'thresholds': {
            'proximity_meters': proximity_meters,
            'early_threshold_seconds': early_threshold_seconds,
            'late_threshold_seconds': late_threshold_seconds
        }
    }


def calculate_time_period_otp(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    early_threshold_seconds: int = -60,
    late_threshold_seconds: int = 300,
    min_match_confidence: float = 0.3
) -> Dict:
    """
    Calculate on-time performance by time period (AM Peak, Midday, PM Peak, Evening, Night).

    This shows how OTP varies throughout the day, revealing when congestion
    or other factors impact service reliability.

    Time periods:
    - AM Peak: 6:00-9:00
    - Midday: 9:00-15:00
    - PM Peak: 15:00-19:00
    - Evening: 19:00-24:00
    - Night: 0:00-6:00

    Args:
        db: Database session
        route_id: Route to analyze
        start_time: Start of analysis period
        end_time: End of analysis period
        early_threshold_seconds: Threshold for "early" (-60 = 1 min early, LA Metro)
        late_threshold_seconds: Threshold for "late" (+300 = 5 min late, LA Metro)
        min_match_confidence: Minimum confidence for trip matching

    Returns:
        Dictionary with OTP by time period
    """
    from src.trip_matching import find_matching_trip

    # Get vehicle positions
    positions = get_vehicle_positions(db, route_id, start_time, end_time)

    if not positions:
        return {
            'route_id': route_id,
            'periods': {}
        }

    # Collect arrivals with time period info
    arrivals_by_period = {
        'AM Peak (6-9)': [],
        'Midday (9-15)': [],
        'PM Peak (15-19)': [],
        'Evening (19-24)': [],
        'Night (0-6)': []
    }

    def get_period(hour: int) -> str:
        if 6 <= hour < 9:
            return 'AM Peak (6-9)'
        elif 9 <= hour < 15:
            return 'Midday (9-15)'
        elif 15 <= hour < 19:
            return 'PM Peak (15-19)'
        elif 19 <= hour < 24:
            return 'Evening (19-24)'
        else:  # 0-6
            return 'Night (0-6)'

    for pos in positions:
        # Match to trip
        match_result = find_matching_trip(db, pos)
        if not match_result or match_result[1] < min_match_confidence:
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

            # Determine time period based on actual time
            period = get_period(pos.timestamp.hour)

            arrivals_by_period[period].append({
                'difference_seconds': diff_seconds,
                'is_early': diff_seconds < early_threshold_seconds,
                'is_on_time': early_threshold_seconds <= diff_seconds <= late_threshold_seconds,
                'is_late': diff_seconds > late_threshold_seconds
            })
        except (ValueError, AttributeError):
            continue

    # Calculate statistics for each period
    period_stats = {}
    for period, arrivals in arrivals_by_period.items():
        if not arrivals:
            period_stats[period] = {
                'arrivals_analyzed': 0,
                'on_time_percentage': None
            }
            continue

        total = len(arrivals)
        on_time_count = sum(1 for a in arrivals if a['is_on_time'])
        early_count = sum(1 for a in arrivals if a['is_early'])
        late_count = sum(1 for a in arrivals if a['is_late'])

        period_stats[period] = {
            'arrivals_analyzed': total,
            'on_time_count': on_time_count,
            'early_count': early_count,
            'late_count': late_count,
            'on_time_percentage': round((on_time_count / total) * 100, 2) if total > 0 else None,
            'early_percentage': round((early_count / total) * 100, 2) if total > 0 else None,
            'late_percentage': round((late_count / total) * 100, 2) if total > 0 else None,
            'avg_lateness_seconds': round(sum(a['difference_seconds'] for a in arrivals) / total, 1) if total > 0 else None
        }

    return {
        'route_id': route_id,
        'time_range': {
            'start': start_time.isoformat() if start_time else None,
            'end': end_time.isoformat() if end_time else None
        },
        'periods': period_stats,
        'thresholds': {
            'early_threshold_seconds': early_threshold_seconds,
            'late_threshold_seconds': late_threshold_seconds
        }
    }


def calculate_line_level_otp(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    early_threshold_seconds: int = -60,
    late_threshold_seconds: int = 300,
    min_match_confidence: float = 0.3,
    sample_rate: int = 1  # Process every Nth position (3 = every 3 minutes with 60s polling)
) -> Dict:
    """
    Calculate overall line-level on-time performance for a route (HIGHLY OPTIMIZED).

    OPTIMIZATION STRATEGY:
    1. Batch-load all data upfront (stops, trips, stop_times) to minimize DB queries
    2. Use numpy for vectorized distance calculations
    3. Sample positions to reduce processing (sample_rate=3 → every 3 minutes)
    4. Match trips using RT trip_id directly (fast path)

    Args:
        db: Database session
        route_id: Route to analyze
        start_time: Start of analysis period
        end_time: End of analysis period
        early_threshold_seconds: Threshold for "early" (-60 = 1 min early, LA Metro)
        late_threshold_seconds: Threshold for "late" (+300 = 5 min late, LA Metro)
        min_match_confidence: Minimum confidence for trip matching (only used for fallback)
        sample_rate: Process every Nth position (default: 3, for 3-minute intervals)

    Returns:
        Dictionary with line-level OTP statistics
    """

    # Get vehicle positions (sampled)
    query = db.query(VehiclePosition).filter(VehiclePosition.route_id == route_id)
    if start_time:
        query = query.filter(VehiclePosition.timestamp >= start_time)
    if end_time:
        query = query.filter(VehiclePosition.timestamp <= end_time)

    positions = query.order_by(VehiclePosition.timestamp).all()

    if not positions:
        return {
            'route_id': route_id,
            'level': 'line',
            'total_observations': 0,
            'matched_observations': 0,
            'on_time_pct': None
        }

    # Track original count before deduplication
    original_count = len(positions)

    # DEDUPLICATE: Remove duplicate records (same vehicle, timestamp, location)
    # This happens when multiple collectors ran simultaneously
    seen = set()
    unique_positions = []
    for pos in positions:
        key = (pos.vehicle_id, pos.timestamp, pos.latitude, pos.longitude)
        if key not in seen:
            seen.add(key)
            unique_positions.append(pos)

    positions = unique_positions
    duplicates_removed = original_count - len(positions)

    # Sample positions
    sampled = positions[::sample_rate]
    if duplicates_removed > 0:
        print(f"Processing {len(sampled)} of {len(positions)} positions (sample_rate={sample_rate}, removed {duplicates_removed} duplicates)...")
    else:
        print(f"Processing {len(sampled)} of {len(positions)} positions (sample_rate={sample_rate})...")

    # BATCH LOAD 1: Get all route stops and create numpy arrays for vectorized distance calc
    stops = get_route_stops(db, route_id)
    stop_ids = np.array([s.stop_id for s in stops])
    stop_lats = np.array([s.stop_lat for s in stops])
    stop_lons = np.array([s.stop_lon for s in stops])
    stop_map = {s.stop_id: s for s in stops}

    # BATCH LOAD 2: Get all trips for this route
    trips = db.query(Trip).filter(Trip.route_id == route_id).all()
    trip_map = {t.trip_id: t for t in trips}

    # BATCH LOAD 3: Get ALL stop_times for this route's trips (this is the big one)
    print(f"  Loading stop_times for {len(trips)} trips...")
    trip_ids = [t.trip_id for t in trips]
    stop_times = db.query(StopTime).filter(StopTime.trip_id.in_(trip_ids)).all()

    # Index stop_times by (trip_id, stop_id) for O(1) lookup
    stop_time_map = {}
    for st in stop_times:
        key = (st.trip_id, st.stop_id)
        stop_time_map[key] = st.arrival_time

    print(f"  Loaded {len(stop_times)} stop_times, processing positions...")

    # Process positions and collect arrival data with metadata
    # We'll deduplicate later to keep only FIRST arrival at each stop
    arrival_records = []  # List of {vehicle_id, trip_id, stop_id, timestamp, diff_seconds}
    matched_count = 0
    unmatched_count = 0

    for i, pos in enumerate(sampled):
        if i % 1000 == 0 and i > 0:
            print(f"    Processed {i}/{len(sampled)} positions...")

        # FAST PATH: Use RT trip_id directly if it exists in our trip_map
        if pos.trip_id and pos.trip_id in trip_map:
            matched_trip_id = pos.trip_id
            matched_count += 1
        else:
            # No trip_id or not in our GTFS - skip
            # (Could add fallback to find_matching_trip here if needed)
            unmatched_count += 1
            continue

        # VECTORIZED: Find nearest stop using numpy
        # Haversine distance formula (vectorized)
        lat1, lon1 = np.radians(pos.latitude), np.radians(pos.longitude)
        lat2, lon2 = np.radians(stop_lats), np.radians(stop_lons)

        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        distances = 6371000 * c  # meters

        # Find nearest stop within 50m
        min_idx = np.argmin(distances)
        min_distance = distances[min_idx]

        if min_distance > 50.0:
            continue

        nearest_stop_id = stop_ids[min_idx]

        # O(1) lookup of scheduled time
        key = (matched_trip_id, nearest_stop_id)
        if key not in stop_time_map:
            continue

        scheduled_time_str = stop_time_map[key]

        # Parse scheduled time
        try:
            hours, minutes, seconds = map(int, scheduled_time_str.split(':'))
            scheduled_dt = pos.timestamp.replace(
                hour=hours % 24,
                minute=minutes,
                second=seconds,
                microsecond=0
            )
            if hours >= 24:
                scheduled_dt += timedelta(days=hours // 24)

            diff_seconds = (pos.timestamp - scheduled_dt).total_seconds()

            # Store arrival record with metadata for deduplication
            arrival_records.append({
                'vehicle_id': pos.vehicle_id,
                'trip_id': matched_trip_id,
                'stop_id': nearest_stop_id,
                'timestamp': pos.timestamp,
                'diff_seconds': diff_seconds
            })

        except (ValueError, AttributeError):
            continue

    # DEDUPLICATE: Keep only LAST observation at each stop for each vehicle/trip
    # This represents the departure time (when bus leaves the stop)
    # Rationale: If a bus arrives early but waits until scheduled time to depart,
    # passengers can still board during the wait period - this should count as on-time
    arrivals_before_dedup = len(arrival_records)
    deduplicated_arrivals = deduplicate_stop_passages(arrival_records)
    arrivals_after_dedup = len(deduplicated_arrivals)

    # Extract diff_seconds values from deduplicated records
    arrivals = [record['diff_seconds'] for record in deduplicated_arrivals]

    if not arrivals:
        return {
            'route_id': route_id,
            'level': 'line',
            'total_observations': len(positions),
            'sampled_observations': len(sampled),
            'matched_observations': 0,
            'on_time_pct': None
        }

    # VECTORIZED: Classify arrivals using numpy
    arrivals_array = np.array(arrivals)
    early_count = np.sum(arrivals_array < early_threshold_seconds)
    late_count = np.sum(arrivals_array > late_threshold_seconds)
    on_time_count = len(arrivals) - early_count - late_count

    total = len(arrivals)
    avg_lateness = float(np.mean(arrivals_array))

    if arrivals_before_dedup > arrivals_after_dedup:
        dedup_removed = arrivals_before_dedup - arrivals_after_dedup
        print(f"  Completed: {total} arrivals analyzed (removed {dedup_removed} duplicate stop arrivals)")
    else:
        print(f"  Completed: {total} arrivals analyzed")

    return {
        'route_id': route_id,
        'level': 'line',
        'description': 'Overall route performance (batch-loaded, vectorized)',
        'total_observations': len(positions),
        'sampled_observations': len(sampled),
        'matched_observations': total,
        'on_time_pct': round((on_time_count / total) * 100, 2) if total > 0 else None,
        'early_pct': round((early_count / total) * 100, 2) if total > 0 else None,
        'late_pct': round((late_count / total) * 100, 2) if total > 0 else None,
        'early_count': int(early_count),
        'on_time_count': int(on_time_count),
        'late_count': int(late_count),
        'avg_lateness_seconds': round(avg_lateness, 1),
        'sample_rate': sample_rate,
        'thresholds': {
            'early_threshold_seconds': early_threshold_seconds,
            'late_threshold_seconds': late_threshold_seconds
        }
    }


def calculate_otp_from_bus_positions(
    db: Session,
    route_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    early_threshold_minutes: float = -1.0,  # LA Metro: more than 1 min early
    late_threshold_minutes: float = 5.0     # LA Metro: more than 5 min late
) -> Dict:
    """
    Calculate on-time performance using WMATA's BusPositions API deviation data.

    ⚠️  WARNING: Use this as SUPPLEMENTARY data only, not primary OTP metrics.
    Validation shows WMATA's deviation can differ significantly from GTFS-based
    calculations (some observations differ by 7+ minutes). Possible reasons:
    - WMATA may use different/updated schedules than published GTFS
    - Different calculation methodology
    - Potential errors in WMATA's system

    RECOMMENDED: Use calculate_line_level_otp() (GTFS-based) as primary metric,
    and this function for comparison/validation.

    Benefits of this approach:
    - Much simpler (no trip matching or stop calculations)
    - Faster to compute
    - Useful for detecting schedule discrepancies

    Uses LA Metro's on-time definition:
    - Early: More than 1 minute early (deviation < -1.0)
    - On-time: Between 1 min early and 5 min late (-1.0 <= deviation <= 5.0)
    - Late: More than 5 minutes late (deviation > 5.0)

    Args:
        db: Database session
        route_id: Route to analyze
        start_time: Start of analysis period
        end_time: End of analysis period
        early_threshold_minutes: Minutes early to be considered "early" (default: -1.0, LA Metro)
        late_threshold_minutes: Minutes late to be considered "late" (default: 5.0, LA Metro)

    Returns:
        Dictionary with on-time performance statistics
    """
    # Query bus positions
    query = db.query(BusPosition).filter(BusPosition.route_id == route_id)

    if start_time:
        query = query.filter(BusPosition.timestamp >= start_time)
    if end_time:
        query = query.filter(BusPosition.timestamp <= end_time)

    positions = query.order_by(BusPosition.timestamp).all()

    if not positions:
        return {
            'route_id': route_id,
            'data_source': 'bus_positions_api',
            'on_time_percentage': None,
            'early_percentage': None,
            'late_percentage': None,
            'observations': 0,
            'early_count': 0,
            'on_time_count': 0,
            'late_count': 0,
            'unique_vehicles': 0
        }

    # Filter out positions without deviation data
    positions_with_deviation = [p for p in positions if p.deviation is not None]

    if not positions_with_deviation:
        return {
            'route_id': route_id,
            'data_source': 'bus_positions_api',
            'on_time_percentage': None,
            'early_percentage': None,
            'late_percentage': None,
            'observations': 0,
            'early_count': 0,
            'on_time_count': 0,
            'late_count': 0,
            'unique_vehicles': 0,
            'note': 'No deviation data available in collected positions'
        }

    # Classify based on deviation (already in minutes!)
    early_count = sum(1 for p in positions_with_deviation if p.deviation < early_threshold_minutes)
    late_count = sum(1 for p in positions_with_deviation if p.deviation > late_threshold_minutes)
    on_time_count = len(positions_with_deviation) - early_count - late_count

    total = len(positions_with_deviation)
    unique_vehicles = len(set(p.vehicle_id for p in positions_with_deviation))

    # Calculate average deviation
    avg_deviation = sum(p.deviation for p in positions_with_deviation) / total

    return {
        'route_id': route_id,
        'data_source': 'bus_positions_api',
        'time_range': {
            'start': start_time.isoformat() if start_time else None,
            'end': end_time.isoformat() if end_time else None
        },
        'on_time_percentage': round((on_time_count / total) * 100, 2) if total > 0 else None,
        'early_percentage': round((early_count / total) * 100, 2) if total > 0 else None,
        'late_percentage': round((late_count / total) * 100, 2) if total > 0 else None,
        'observations': total,
        'early_count': early_count,
        'on_time_count': on_time_count,
        'late_count': late_count,
        'unique_vehicles': unique_vehicles,
        'avg_deviation_minutes': round(avg_deviation, 2),
        'thresholds': {
            'early_threshold_minutes': early_threshold_minutes,
            'late_threshold_minutes': late_threshold_minutes
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
