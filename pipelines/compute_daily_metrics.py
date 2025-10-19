"""
Daily Metrics Computation Pipeline

Computes and stores daily performance metrics for all routes with sufficient data.
This script should be run daily (e.g., via cron at 2am) to populate the aggregation
tables used by the API.

Usage:
    python pipelines/compute_daily_metrics.py [--days N] [--route ROUTE_ID] [--recalculate]

Options:
    --days N         Compute metrics for last N days (default: 7)
    --route ROUTE_ID Compute for specific route only (default: all routes)
    --recalculate    Force recalculation even if metrics already exist (replaces existing data)
"""

import argparse
import time
from collections import defaultdict
from datetime import datetime, timedelta

from sqlalchemy import and_, func

from src.analytics import (
    calculate_average_speed,
    calculate_headways,
    calculate_line_level_otp,
    get_exception_service_dates,
)
from src.database import get_session
from src.models import (
    CalendarDate,
    Route,
    RouteMetricsDaily,
    RouteMetricsSummary,
    Stop,
    StopTime,
    Trip,
    VehiclePosition,
)


def compute_metrics_for_route_day(
    db,
    route_id: str,
    date: datetime.date,
    position_count: int = None,
    positions: list = None,
    trips_map: dict = None,
    stop_times_map: dict = None,
    stops_map: dict = None,
) -> dict:
    """
    Compute all metrics for a single route for a single day

    Args:
        db: Database session
        route_id: Route to analyze
        date: Date to analyze (will analyze 24 hours from midnight)
        position_count: Pre-computed position count (optional, will query if not provided)
        positions: Pre-loaded positions for this route (optional, for batch processing)

    Returns:
        Dictionary with computed metrics, or None if insufficient data
    """
    # Define time range for this day
    start_time = datetime.combine(date, datetime.min.time())
    end_time = start_time + timedelta(days=1)

    # Check if we have enough data (use pre-computed count if available)
    if position_count is None:
        position_count = (
            db.query(VehiclePosition)
            .filter(
                VehiclePosition.route_id == route_id,
                VehiclePosition.timestamp >= start_time,
                VehiclePosition.timestamp < end_time,
            )
            .count()
        )

    MIN_POSITIONS = 50
    if position_count < MIN_POSITIONS:
        return None

    # Compute OTP
    otp_start = time.time()
    try:
        otp_result = calculate_line_level_otp(
            db,
            route_id,
            start_time=start_time,
            end_time=end_time,
            sample_rate=1,  # No sampling for daily computation
            positions=positions,  # Pass pre-loaded positions
            trips=trips_map,  # Pass pre-loaded trips
            stop_times=stop_times_map,  # Pass pre-loaded stop_times
            stops=stops_map,  # Pass pre-loaded stops
        )
        otp_pct = otp_result.get("on_time_pct")
        early_pct = otp_result.get("early_pct")
        late_pct = otp_result.get("late_pct")
        total_arrivals = otp_result.get("matched_observations", 0)
        time.time() - otp_start
    except Exception:
        otp_pct = early_pct = late_pct = None
        total_arrivals = 0
        time.time() - otp_start

    # Compute headway
    headway_start = time.time()
    try:
        headway_result = calculate_headways(
            db, route_id, start_time=start_time, end_time=end_time, positions=positions
        )
        avg_headway = headway_result.get("avg_headway_minutes")
        min_headway = headway_result.get("min_headway_minutes")
        max_headway = headway_result.get("max_headway_minutes")
        time.time() - headway_start
    except Exception:
        avg_headway = min_headway = max_headway = None
        time.time() - headway_start

    # Compute speed
    speed_start = time.time()
    try:
        speed_result = calculate_average_speed(
            db, route_id, start_time=start_time, end_time=end_time, positions=positions
        )
        avg_speed = speed_result.get("avg_speed_mph")
        median_speed = speed_result.get("median_speed_mph")
        time.time() - speed_start
    except Exception:
        avg_speed = median_speed = None
        time.time() - speed_start

    # Count unique vehicles and trips
    stats = (
        db.query(
            func.count(func.distinct(VehiclePosition.vehicle_id)).label("vehicles"),
            func.count(func.distinct(VehiclePosition.trip_id)).label("trips"),
        )
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.timestamp >= start_time,
            VehiclePosition.timestamp < end_time,
        )
        .first()
    )

    # Timing breakdown (only shown in batch mode via caller)
    # print(f"    OTP: {otp_time:.2f}s, Headway: {headway_time:.2f}s, Speed: {speed_time:.2f}s")

    return {
        "route_id": route_id,
        "date": date.isoformat(),
        "otp_percentage": otp_pct,
        "early_percentage": early_pct,
        "late_percentage": late_pct,
        "avg_headway_minutes": avg_headway,
        "min_headway_minutes": min_headway,
        "max_headway_minutes": max_headway,
        "avg_speed_mph": avg_speed,
        "median_speed_mph": median_speed,
        "total_arrivals": total_arrivals,
        "unique_vehicles": stats.vehicles if stats else 0,
        "unique_trips": stats.trips if stats else 0,
    }


def compute_metrics_batch(
    db, routes: list[Route], date: datetime.date, recalculate: bool = False
) -> dict[str, dict]:
    """
    Compute metrics for ALL routes for a single day using batch processing

    This is significantly faster than processing routes one-by-one because:
    - Single database query loads all positions for all routes at once
    - Vectorized operations process data in bulk
    - No repeated database overhead

    Args:
        db: Database session
        routes: List of Route objects to analyze
        date: Date to analyze (will analyze 24 hours from midnight)
        recalculate: If True, recalculate even if metrics already exist

    Returns:
        Dictionary mapping route_id -> metrics dict (or None if insufficient data)
    """
    start_time = datetime.combine(date, datetime.min.time())
    end_time = start_time + timedelta(days=1)

    print(f"\n{'='*70}")
    print(f"Batch processing {len(routes)} routes for {date.isoformat()}")
    print(f"{'='*70}")

    batch_start = time.time()

    # Step 1: Load ALL positions for ALL routes at once (with exception filtering)
    print("  [1/5] Loading vehicle positions for all routes...")
    load_start = time.time()

    route_ids = [r.route_id for r in routes]

    # Query with exception filtering built into SQL
    query = (
        db.query(VehiclePosition)
        .join(Trip, VehiclePosition.trip_id == Trip.trip_id)
        .filter(
            VehiclePosition.route_id.in_(route_ids),
            VehiclePosition.timestamp >= start_time,
            VehiclePosition.timestamp < end_time,
            Trip.is_current,
        )
    )

    # SQL-based exception filtering - only filter out service_removed (exception_type=2)
    # Do NOT filter service_added (exception_type=1) as that's normal weekend/holiday service
    query = query.filter(
        ~db.query(CalendarDate)
        .filter(
            and_(
                CalendarDate.date == func.strftime("%Y%m%d", VehiclePosition.timestamp),
                CalendarDate.service_id == Trip.service_id,
                CalendarDate.exception_type == 2,
                CalendarDate.is_current,
            )
        )
        .exists()
    )

    all_positions = query.all()
    print(f"      ✓ Loaded {len(all_positions)} positions in {time.time() - load_start:.2f}s")

    # Step 2: Group positions by route
    print("  [2/5] Grouping positions by route...")
    group_start = time.time()

    positions_by_route = defaultdict(list)
    for pos in all_positions:
        positions_by_route[pos.route_id].append(pos)

    print(f"      ✓ Grouped into {len(positions_by_route)} routes in {time.time() - group_start:.2f}s")

    # Step 3: Filter routes with insufficient data
    MIN_POSITIONS = 50
    routes_to_process = {}
    routes_skipped = []

    for route in routes:
        pos_count = len(positions_by_route.get(route.route_id, []))
        if pos_count < MIN_POSITIONS:
            routes_skipped.append(route.route_id)
        else:
            routes_to_process[route.route_id] = route

    print(f"  [3/5] Processing {len(routes_to_process)} routes, skipping {len(routes_skipped)} (insufficient data)")

    # Step 4: Check for existing metrics if not recalculating
    if not recalculate:
        print("  [4/5] Checking for existing metrics...")
        check_start = time.time()

        existing = (
            db.query(RouteMetricsDaily.route_id)
            .filter(
                RouteMetricsDaily.route_id.in_(list(routes_to_process.keys())),
                RouteMetricsDaily.date == date.isoformat(),
            )
            .all()
        )

        existing_route_ids = {r[0] for r in existing}
        routes_with_existing = [rid for rid in routes_to_process.keys() if rid in existing_route_ids]

        # Remove from processing list
        for rid in routes_with_existing:
            del routes_to_process[rid]

        print(f"      ✓ Found {len(routes_with_existing)} existing, will process {len(routes_to_process)} in {time.time() - check_start:.2f}s")
    else:
        print("  [4/5] Recalculate mode - will replace existing metrics")

        # Delete existing metrics for routes we're processing
        if routes_to_process:
            delete_start = time.time()
            deleted = (
                db.query(RouteMetricsDaily)
                .filter(
                    RouteMetricsDaily.route_id.in_(list(routes_to_process.keys())),
                    RouteMetricsDaily.date == date.isoformat(),
                )
                .delete(synchronize_session=False)
            )
            db.commit()
            print(f"      ✓ Deleted {deleted} existing metrics in {time.time() - delete_start:.2f}s")

    # Step 5: Batch-load ALL GTFS data for all routes (avoid per-route queries)
    print("  [5/5] Loading GTFS data for all routes...")
    gtfs_start = time.time()

    route_ids_list = list(routes_to_process.keys())

    # Load all trips for these routes
    print("      Loading trips...")
    trips = db.query(Trip).filter(Trip.route_id.in_(route_ids_list), Trip.is_current).all()
    trip_map = {t.trip_id: t for t in trips}
    trips_by_route = defaultdict(list)
    for t in trips:
        trips_by_route[t.route_id].append(t)
    print(f"      ✓ Loaded {len(trips)} trips")

    # Load all stop_times - filter by date in Python, not SQL
    print("      Loading stop_times...")
    # OPTIMIZATION: Load ALL stop_times for current GTFS version, filter in Python
    # This is faster than complex SQL joins and avoids chunking large IN() clauses
    all_stop_times = db.query(StopTime).filter(StopTime.is_current).all()

    # Filter to only the trips we loaded (which are already filtered by route and date)
    trip_ids_set = {t.trip_id for t in trips}
    stop_times_by_trip = defaultdict(list)
    for st in all_stop_times:
        if st.trip_id in trip_ids_set:
            stop_times_by_trip[st.trip_id].append(st)

    filtered_count = len([st for st_list in stop_times_by_trip.values() for st in st_list])
    print(f"      ✓ Loaded {len(all_stop_times)} stop_times, using {filtered_count} for today's trips")

    # Load all stops (just get all current stops, it's not that many)
    print("      Loading stops...")
    stops = db.query(Stop).filter(Stop.is_current).all()
    stop_map = {s.stop_id: s for s in stops}
    print(f"      ✓ Loaded {len(stops)} stops")

    print(f"      ✓ GTFS data loaded in {time.time() - gtfs_start:.2f}s")

    # Step 6: VECTORIZED BATCH PROCESSING - Process ALL routes at once
    print(f"  [6/6] Computing metrics for {len(routes_to_process)} routes using vectorized batch processing...")
    compute_start = time.time()

    # Import batch functions from analytics
    from src.analytics import (
        _process_positions_batch,
        calculate_average_speed_batch,
        calculate_headways_batch,
        calculate_line_level_otp_batch,
    )

    # Get all positions for routes we're processing
    all_route_positions = []
    for route_id in routes_to_process.keys():
        all_route_positions.extend(positions_by_route[route_id])

    print(f"      [6.1] Processing {len(all_route_positions)} positions for ALL routes...")
    process_start = time.time()

    # SINGLE CALL processes ALL routes' positions at once
    positions_df = _process_positions_batch(
        positions=all_route_positions,
        trips_map=trip_map,
        stop_times_map=stop_times_by_trip,
        stops_map=stop_map,
    )

    print(f"      ✓ Processed positions in {time.time() - process_start:.2f}s ({len(positions_df)} matched arrivals)")

    # VECTORIZED METRICS CALCULATION - ALL routes computed simultaneously
    print("      [6.2] Computing OTP for ALL routes...")
    otp_start = time.time()
    otp_results = calculate_line_level_otp_batch(
        positions_df=positions_df,
        route_ids=list(routes_to_process.keys()),
    )
    print(f"      ✓ OTP computed for {len(otp_results)} routes in {time.time() - otp_start:.2f}s")

    print("      [6.3] Computing headways for ALL routes...")
    headway_start = time.time()
    headway_results = calculate_headways_batch(
        positions_df=positions_df,
        route_ids=list(routes_to_process.keys()),
    )
    print(f"      ✓ Headways computed for {len(headway_results)} routes in {time.time() - headway_start:.2f}s")

    print("      [6.4] Computing speeds for ALL routes...")
    speed_start = time.time()
    speed_results = calculate_average_speed_batch(
        positions_df=positions_df,
        route_ids=list(routes_to_process.keys()),
    )
    print(f"      ✓ Speeds computed for {len(speed_results)} routes in {time.time() - speed_start:.2f}s")

    # Combine results and save to database
    print("      [6.5] Saving metrics to database...")
    save_start = time.time()

    results = {}
    for route_id, _route in routes_to_process.items():
        # Get position count for this route
        pos_count = len(positions_by_route[route_id])

        # Combine metrics from all batch results
        otp = otp_results.get(route_id)
        headway = headway_results.get(route_id)
        speed = speed_results.get(route_id)

        # Only save if we have at least OTP metrics
        if otp and otp.get('total_arrivals', 0) > 0:
            metrics = {
                'route_id': route_id,
                'date': date.isoformat(),
                'otp_percentage': otp.get('on_time_pct'),
                'early_percentage': otp.get('early_pct'),
                'late_percentage': otp.get('late_pct'),
                'total_arrivals': otp.get('total_arrivals', 0),
                'avg_headway_minutes': headway.get('avg_headway_minutes') if headway else None,
                'headway_std_dev_minutes': headway.get('std_dev_minutes') if headway else None,
                'headway_cv': headway.get('cv') if headway else None,
                'avg_speed_mph': speed.get('avg_speed_mph') if speed else None,
                'computed_at': datetime.utcnow(),
            }

            # Save to database
            daily_record = RouteMetricsDaily(**metrics)
            db.add(daily_record)
            results[route_id] = metrics
        else:
            results[route_id] = None

    # Commit all records at once
    db.commit()
    print(f"      ✓ Saved {len([r for r in results.values() if r])} route metrics in {time.time() - save_start:.2f}s")

    print(f"      ✓ All routes computed in {time.time() - compute_start:.2f}s")

    # Add skipped routes to results
    for route_id in routes_skipped:
        results[route_id] = None

    print(f"\n{'='*70}")
    print(f"Batch complete: {len(routes_to_process)} computed, {len(routes_skipped)} skipped")
    print(f"Total time: {time.time() - batch_start:.2f}s")
    print(f"{'='*70}\n")

    return results


def compute_daily_metrics(
    days: int = 7,
    date: str = None,
    start_date: str = None,
    end_date: str = None,
    route_filter: str = None,
    recalculate: bool = False,
):
    """
    Compute daily metrics for all routes (or specific route) for the last N days, a specific date, or a date range

    Args:
        days: Number of days to compute (default: 7) - ignored if date or date range is specified
        date: Specific date to compute in YYYY-MM-DD format (e.g., '2025-10-18')
        start_date: Start date for range in YYYY-MM-DD format (requires end_date)
        end_date: End date for range in YYYY-MM-DD format (requires start_date)
        route_filter: If specified, only compute for this route_id
        recalculate: If True, recalculate even if metrics already exist (replaces existing data)
    """
    print("=" * 70)
    print("Daily Metrics Computation Pipeline")
    print("=" * 70)
    if recalculate:
        print("MODE: Recalculation (will replace existing metrics)")
    print()
    print("Connecting to database...")

    db = get_session()

    try:
        print("Loading routes...")
        # Get routes to process (current version only)
        if route_filter:
            routes = db.query(Route).filter(Route.route_id == route_filter, Route.is_current).all()
            if not routes:
                print(f"Error: Route {route_filter} not found")
                return
        else:
            routes = db.query(Route).filter(Route.is_current).order_by(Route.route_short_name).all()

        print()

        # Load exception service-dates (trip-level filtering)
        # Analytics functions will automatically filter out trips with exceptional service_ids
        print("Loading exception service dates...")
        exception_start = time.time()
        exception_service_dates = get_exception_service_dates(db)
        print(f"  ✓ Loaded {len(exception_service_dates)} exceptions in {time.time() - exception_start:.2f}s")
        if exception_service_dates:
            # Group by date to show which dates have exceptions
            dates_with_exceptions = {date for date, service_id in exception_service_dates}
            exception_dates_formatted = [
                f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in sorted(dates_with_exceptions)
            ]
            print(
                f"\nUsing trip-level filtering for {len(exception_service_dates)} exception service-dates:"
            )
            print(
                f"  ({len(dates_with_exceptions)} dates with exceptions, "
                f"excluding trips with special service_ids)"
            )
            print(f"  Example dates: {', '.join(exception_dates_formatted[:10])}")
            if len(exception_dates_formatted) > 10:
                print(f"  ... and {len(exception_dates_formatted) - 10} more")
            print()

        # Generate date range (last N days OR specific date OR date range)
        if date:
            # Parse specific date for backfill
            try:
                specific_date = datetime.strptime(date, "%Y-%m-%d").date()
                dates = [specific_date]
                print(f"Found {len(routes)} routes for date {date}")
            except ValueError:
                print(
                    f"Error: Invalid date format '{date}'. Use YYYY-MM-DD format (e.g., '2025-10-18')"
                )
                return
        elif start_date and end_date:
            # Parse date range for backfill
            try:
                start = datetime.strptime(start_date, "%Y-%m-%d").date()
                end = datetime.strptime(end_date, "%Y-%m-%d").date()

                if start > end:
                    print(f"Error: start_date ({start_date}) must be before end_date ({end_date})")
                    return

                # Generate all dates in range (inclusive)
                dates = []
                current = start
                while current <= end:
                    dates.append(current)
                    current += timedelta(days=1)

                print(f"Found {len(routes)} routes for date range {start_date} to {end_date} ({len(dates)} days)")
            except ValueError as e:
                print(
                    f"Error: Invalid date format. Use YYYY-MM-DD format (e.g., '2025-10-18'). Error: {e}"
                )
                return
        else:
            # Default: last N days
            end_date_default = datetime.now().date()
            dates = [end_date_default - timedelta(days=i) for i in range(days)]
            print(f"Found {len(routes)} routes for last {days} days")

        total_computed = 0
        total_skipped = 0

        # NEW APPROACH: Process all routes for each date using batch processing
        # This is much faster because we load all positions at once per date
        for date in dates:
            print(f"\n{'='*70}")
            print(f"Processing date: {date.isoformat()}")
            print(f"{'='*70}")

            # Use batch processing for this date
            date_results = compute_metrics_batch(db, routes, date, recalculate=recalculate)

            # Count results
            computed_count = sum(1 for result in date_results.values() if result is not None)
            skipped_count = sum(1 for result in date_results.values() if result is None)

            total_computed += computed_count
            total_skipped += skipped_count

            print(f"Date {date.isoformat()} complete: {computed_count} computed, {skipped_count} skipped")

        print("\n" + "=" * 70)
        print("Daily metrics computation complete!")
        print(f"  Computed: {total_computed} route-days")
        print(f"  Skipped (insufficient data): {total_skipped} route-days")
        print(
            f"  Note: Trip-level filtering excluded positions from {len(exception_service_dates)} "
            f"exceptional service-dates"
        )
        print("=" * 70)

        # Now compute summary metrics (rolling 7-day average)
        print("\nComputing summary metrics...")
        compute_summary_metrics(db, days)

    finally:
        db.close()


def compute_summary_metrics(db, days: int = 7):
    """
    Compute rolling summary metrics from daily metrics

    Aggregates the last N days of daily metrics into a summary record
    for each route, used by the API scorecard endpoint.
    """
    print(f"  Computing {days}-day rolling summaries...")

    # Get all routes that have daily metrics
    routes_with_data = db.query(RouteMetricsDaily.route_id).distinct().all()
    routes_with_data = [r[0] for r in routes_with_data]

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    for route_id in routes_with_data:
        # Get daily metrics for this route in the date range
        daily_metrics = (
            db.query(RouteMetricsDaily)
            .filter(
                RouteMetricsDaily.route_id == route_id,
                RouteMetricsDaily.date >= start_date.isoformat(),
                RouteMetricsDaily.date <= end_date.isoformat(),
            )
            .all()
        )

        if not daily_metrics:
            continue

        # Calculate averages
        otp_values = [m.otp_percentage for m in daily_metrics if m.otp_percentage is not None]
        headway_values = [
            m.avg_headway_minutes for m in daily_metrics if m.avg_headway_minutes is not None
        ]
        headway_std_dev_values = [
            m.headway_std_dev_minutes for m in daily_metrics if m.headway_std_dev_minutes is not None
        ]
        headway_cv_values = [
            m.headway_cv for m in daily_metrics if m.headway_cv is not None
        ]
        speed_values = [m.avg_speed_mph for m in daily_metrics if m.avg_speed_mph is not None]
        early_values = [m.early_percentage for m in daily_metrics if m.early_percentage is not None]
        late_values = [m.late_percentage for m in daily_metrics if m.late_percentage is not None]

        avg_otp = sum(otp_values) / len(otp_values) if otp_values else None
        avg_headway = sum(headway_values) / len(headway_values) if headway_values else None
        avg_headway_std_dev = sum(headway_std_dev_values) / len(headway_std_dev_values) if headway_std_dev_values else None
        avg_headway_cv = sum(headway_cv_values) / len(headway_cv_values) if headway_cv_values else None
        avg_speed = sum(speed_values) / len(speed_values) if speed_values else None
        avg_early = sum(early_values) / len(early_values) if early_values else None
        avg_late = sum(late_values) / len(late_values) if late_values else None

        total_obs = sum(m.total_arrivals for m in daily_metrics if m.total_arrivals)
        total_vehicles = sum(m.unique_vehicles for m in daily_metrics if m.unique_vehicles)

        # Get last data timestamp from vehicle_positions
        last_position = (
            db.query(VehiclePosition)
            .filter(VehiclePosition.route_id == route_id)
            .order_by(VehiclePosition.timestamp.desc())
            .first()
        )

        last_timestamp = last_position.timestamp if last_position else None

        # Upsert summary record
        summary = (
            db.query(RouteMetricsSummary).filter(RouteMetricsSummary.route_id == route_id).first()
        )

        if summary:
            # Update existing
            summary.days_analyzed = days
            summary.date_start = start_date.isoformat()
            summary.date_end = end_date.isoformat()
            summary.otp_percentage = avg_otp
            summary.early_percentage = avg_early
            summary.late_percentage = avg_late
            summary.avg_headway_minutes = avg_headway
            summary.headway_std_dev_minutes = avg_headway_std_dev
            summary.headway_cv = avg_headway_cv
            summary.avg_speed_mph = avg_speed
            summary.total_observations = total_obs
            summary.unique_vehicles = total_vehicles
            summary.last_data_timestamp = last_timestamp
            summary.computed_at = datetime.utcnow()
        else:
            # Create new
            summary = RouteMetricsSummary(
                route_id=route_id,
                days_analyzed=days,
                date_start=start_date.isoformat(),
                date_end=end_date.isoformat(),
                otp_percentage=avg_otp,
                early_percentage=avg_early,
                late_percentage=avg_late,
                avg_headway_minutes=avg_headway,
                headway_std_dev_minutes=avg_headway_std_dev,
                headway_cv=avg_headway_cv,
                avg_speed_mph=avg_speed,
                total_observations=total_obs,
                unique_vehicles=total_vehicles,
                last_data_timestamp=last_timestamp,
            )
            db.add(summary)

        db.commit()
        if avg_otp is not None:
            print(f"    ✓ {route_id}: OTP={avg_otp:.1f}% over {len(daily_metrics)} days")
        else:
            print(f"    ✓ {route_id}: OTP=N/A over {len(daily_metrics)} days")

    print(f"  ✓ Summary metrics computed for {len(routes_with_data)} routes")


def main():
    parser = argparse.ArgumentParser(
        description="Compute daily performance metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compute metrics for last 7 days (default)
  python compute_daily_metrics.py

  # Compute metrics for last 30 days
  python compute_daily_metrics.py --days 30

  # Backfill metrics for a specific date
  python compute_daily_metrics.py --date 2025-10-18

  # Backfill metrics for a date range (inclusive)
  python compute_daily_metrics.py --start-date 2025-10-15 --end-date 2025-10-18

  # Backfill for specific route and date
  python compute_daily_metrics.py --date 2025-10-15 --route C51

  # Backfill range for specific route
  python compute_daily_metrics.py --start-date 2025-10-10 --end-date 2025-10-15 --route C51

  # Force recalculation (replace existing metrics)
  python compute_daily_metrics.py --date 2025-10-18 --recalculate
        """,
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to compute (default: 7, ignored if --date or date range is used)",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to compute in YYYY-MM-DD format (e.g., '2025-10-18') - for backfilling single date",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for range in YYYY-MM-DD format (requires --end-date) - for backfilling date range",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for range in YYYY-MM-DD format (requires --start-date) - for backfilling date range",
    )
    parser.add_argument("--route", type=str, help="Specific route to compute (default: all)")
    parser.add_argument(
        "--recalculate",
        action="store_true",
        help="Force recalculation even if metrics exist (replaces existing data)",
    )

    args = parser.parse_args()

    # Validate mutually exclusive options
    options_count = sum(
        [
            args.date is not None,
            (args.start_date is not None or args.end_date is not None),
            args.days != 7,
        ]
    )

    if options_count > 1:
        parser.error(
            "Cannot specify multiple date options. Use one of: --days, --date, or --start-date/--end-date"
        )

    # Validate that start_date and end_date must be used together
    if (args.start_date is None) != (args.end_date is None):
        parser.error("--start-date and --end-date must be used together")

    compute_daily_metrics(
        days=args.days,
        date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        route_filter=args.route,
        recalculate=args.recalculate,
    )


if __name__ == "__main__":
    main()
