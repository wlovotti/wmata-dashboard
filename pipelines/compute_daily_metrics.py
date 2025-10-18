"""
Daily Metrics Computation Pipeline

Computes and stores daily performance metrics for all routes with sufficient data.
This script should be run daily (e.g., via cron at 2am) to populate the aggregation
tables used by the API.

Usage:
    python pipelines/compute_daily_metrics.py [--days N] [--route ROUTE_ID]

Options:
    --days N         Compute metrics for last N days (default: 7)
    --route ROUTE_ID Compute for specific route only (default: all routes)
"""

import argparse
from datetime import datetime, timedelta

from sqlalchemy import func

from src.analytics import (
    calculate_average_speed,
    calculate_headways,
    calculate_line_level_otp,
    get_exception_service_dates,
)
from src.database import get_session
from src.models import Route, RouteMetricsDaily, RouteMetricsSummary, VehiclePosition


def compute_metrics_for_route_day(db, route_id: str, date: datetime.date) -> dict:
    """
    Compute all metrics for a single route for a single day

    Args:
        db: Database session
        route_id: Route to analyze
        date: Date to analyze (will analyze 24 hours from midnight)

    Returns:
        Dictionary with computed metrics, or None if insufficient data
    """
    # Define time range for this day
    start_time = datetime.combine(date, datetime.min.time())
    end_time = start_time + timedelta(days=1)

    print(f"  Computing metrics for {route_id} on {date.isoformat()}...")

    # Check if we have enough data
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
        print(f"    Skipping - only {position_count} positions (need {MIN_POSITIONS})")
        return None

    # Compute OTP
    print("    Computing OTP...")
    try:
        otp_result = calculate_line_level_otp(
            db,
            route_id,
            start_time=start_time,
            end_time=end_time,
            sample_rate=1,  # No sampling for daily computation
        )
        otp_pct = otp_result.get("on_time_pct")
        early_pct = otp_result.get("early_pct")
        late_pct = otp_result.get("late_pct")
        total_arrivals = otp_result.get("matched_observations", 0)
    except Exception as e:
        print(f"    Error computing OTP: {e}")
        otp_pct = early_pct = late_pct = None
        total_arrivals = 0

    # Compute headway
    print("    Computing headway...")
    try:
        headway_result = calculate_headways(db, route_id, start_time=start_time, end_time=end_time)
        avg_headway = headway_result.get("avg_headway_minutes")
        min_headway = headway_result.get("min_headway_minutes")
        max_headway = headway_result.get("max_headway_minutes")
    except Exception as e:
        print(f"    Error computing headway: {e}")
        avg_headway = min_headway = max_headway = None

    # Compute speed
    print("    Computing speed...")
    try:
        speed_result = calculate_average_speed(
            db, route_id, start_time=start_time, end_time=end_time
        )
        avg_speed = speed_result.get("avg_speed_mph")
        median_speed = speed_result.get("median_speed_mph")
    except Exception as e:
        print(f"    Error computing speed: {e}")
        avg_speed = median_speed = None

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

    print(f"    ✓ Complete: OTP={otp_pct}%, Headway={avg_headway}min, Speed={avg_speed}mph")

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


def compute_daily_metrics(days: int = 7, route_filter: str = None):
    """
    Compute daily metrics for all routes (or specific route) for the last N days

    Args:
        days: Number of days to compute (default: 7)
        route_filter: If specified, only compute for this route_id
    """
    db = get_session()

    try:
        print("=" * 70)
        print("Daily Metrics Computation Pipeline")
        print("=" * 70)

        # Get routes to process (current version only)
        if route_filter:
            routes = (
                db.query(Route)
                .filter(Route.route_id == route_filter, Route.is_current)
                .all()
            )
            if not routes:
                print(f"Error: Route {route_filter} not found")
                return
        else:
            routes = db.query(Route).filter(Route.is_current).order_by(Route.route_short_name).all()

        print(f"\nProcessing {len(routes)} routes for last {days} days")
        print()

        # Load exception service-dates (trip-level filtering)
        # Analytics functions will automatically filter out trips with exceptional service_ids
        exception_service_dates = get_exception_service_dates(db)
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

        # Generate date range (last N days)
        end_date = datetime.now().date()
        dates = [end_date - timedelta(days=i) for i in range(days)]

        total_computed = 0
        total_skipped = 0

        # Process each route/day combination
        for route in routes:
            print(f"\nRoute: {route.route_short_name} ({route.route_id})")

            for date in dates:
                # Note: We NO LONGER skip entire exception dates at the pipeline level
                # Instead, analytics functions filter at the TRIP level (by service_id)
                # This preserves data from routes running normal service on holidays

                # Check if we already have metrics for this day
                existing = (
                    db.query(RouteMetricsDaily)
                    .filter(
                        RouteMetricsDaily.route_id == route.route_id,
                        RouteMetricsDaily.date == date.isoformat(),
                    )
                    .first()
                )

                if existing:
                    print(f"  {date.isoformat()}: Already computed, skipping")
                    continue

                # Compute metrics
                metrics = compute_metrics_for_route_day(db, route.route_id, date)

                if metrics:
                    # Store in database
                    daily_record = RouteMetricsDaily(**metrics)
                    db.add(daily_record)
                    db.commit()
                    total_computed += 1
                else:
                    total_skipped += 1

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
        speed_values = [m.avg_speed_mph for m in daily_metrics if m.avg_speed_mph is not None]
        early_values = [m.early_percentage for m in daily_metrics if m.early_percentage is not None]
        late_values = [m.late_percentage for m in daily_metrics if m.late_percentage is not None]

        avg_otp = sum(otp_values) / len(otp_values) if otp_values else None
        avg_headway = sum(headway_values) / len(headway_values) if headway_values else None
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
                avg_speed_mph=avg_speed,
                total_observations=total_obs,
                unique_vehicles=total_vehicles,
                last_data_timestamp=last_timestamp,
            )
            db.add(summary)

        db.commit()
        print(f"    ✓ {route_id}: OTP={avg_otp:.1f}% over {len(daily_metrics)} days")

    print(f"  ✓ Summary metrics computed for {len(routes_with_data)} routes")


def main():
    parser = argparse.ArgumentParser(description="Compute daily performance metrics")
    parser.add_argument(
        "--days", type=int, default=7, help="Number of days to compute (default: 7)"
    )
    parser.add_argument("--route", type=str, help="Specific route to compute (default: all)")

    args = parser.parse_args()

    compute_daily_metrics(days=args.days, route_filter=args.route)


if __name__ == "__main__":
    main()
