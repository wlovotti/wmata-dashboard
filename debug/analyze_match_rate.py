"""
Analyze vehicle position to stop match rate

This script analyzes how many collected vehicle positions successfully match
to scheduled stops, to help inform optimal data collection frequency.
"""


from sqlalchemy import func

from src.database import get_session
from src.models import RouteMetricsDaily, VehiclePosition


def analyze_match_rate():
    """Calculate match rate of vehicle positions to stop arrivals"""
    db = get_session()

    try:
        # Get date range of collected data
        date_range = (
            db.query(
                func.min(VehiclePosition.timestamp).label("min_time"),
                func.max(VehiclePosition.timestamp).label("max_time"),
            )
            .first()
        )

        print("=" * 70)
        print("Vehicle Position Match Rate Analysis")
        print("=" * 70)
        print(f"\nData Range: {date_range.min_time} to {date_range.max_time}")

        # Count total vehicle positions by date
        positions_by_date = (
            db.query(
                func.date(VehiclePosition.timestamp).label("date"),
                func.count(VehiclePosition.id).label("total_positions"),
                func.count(func.distinct(VehiclePosition.vehicle_id)).label("unique_vehicles"),
                func.count(func.distinct(VehiclePosition.route_id)).label("unique_routes"),
            )
            .group_by(func.date(VehiclePosition.timestamp))
            .order_by(func.date(VehiclePosition.timestamp))
            .all()
        )

        # Get matched arrivals from metrics
        arrivals_by_date = (
            db.query(
                RouteMetricsDaily.date,
                func.sum(RouteMetricsDaily.total_arrivals).label("total_arrivals"),
                func.count(RouteMetricsDaily.route_id).label("routes_with_data"),
            )
            .group_by(RouteMetricsDaily.date)
            .order_by(RouteMetricsDaily.date)
            .all()
        )

        # Create lookup dict for arrivals
        arrivals_dict = {a.date: a for a in arrivals_by_date}

        print("\n" + "=" * 70)
        print("Daily Breakdown:")
        print("=" * 70)
        print(
            f"{'Date':<12} {'Positions':>10} {'Arrivals':>10} {'Match %':>8} {'Routes':>7} {'Vehicles':>9}"
        )
        print("-" * 70)

        total_positions = 0
        total_arrivals = 0

        for pos in positions_by_date:
            date_str = pos.date
            arrivals_data = arrivals_dict.get(date_str)

            if arrivals_data:
                arrivals = arrivals_data.total_arrivals or 0
                match_rate = (arrivals / pos.total_positions * 100) if pos.total_positions > 0 else 0
                routes_with_data = arrivals_data.routes_with_data
            else:
                arrivals = 0
                match_rate = 0
                routes_with_data = 0

            total_positions += pos.total_positions
            total_arrivals += arrivals

            print(
                f"{date_str:<12} {pos.total_positions:>10,} {arrivals:>10,} {match_rate:>7.2f}% "
                f"{routes_with_data:>7} {pos.unique_vehicles:>9}"
            )

        print("-" * 70)
        overall_match_rate = (total_arrivals / total_positions * 100) if total_positions > 0 else 0
        print(
            f"{'TOTAL':<12} {total_positions:>10,} {total_arrivals:>10,} {overall_match_rate:>7.2f}%"
        )

        print("\n" + "=" * 70)
        print("Key Insights:")
        print("=" * 70)

        # Calculate average time between positions
        if total_positions > 0:
            time_span = date_range.max_time - date_range.min_time
            hours = time_span.total_seconds() / 3600
            avg_positions_per_hour = total_positions / hours if hours > 0 else 0
            avg_interval_seconds = 3600 / avg_positions_per_hour if avg_positions_per_hour > 0 else 0

            print("\n1. Collection Frequency:")
            print(f"   - Total collection time: {hours:.1f} hours")
            print(f"   - Total positions collected: {total_positions:,}")
            print(f"   - Average positions per hour: {avg_positions_per_hour:.0f}")
            print(
                f"   - Average interval between positions: {avg_interval_seconds:.0f} seconds"
            )

        print("\n2. Match Rate:")
        print(f"   - Overall match rate: {overall_match_rate:.2f}%")
        print(f"   - Total matched arrivals: {total_arrivals:,}")
        print(
            f"   - Positions that didn't match: {total_positions - total_arrivals:,} ({100 - overall_match_rate:.2f}%)"
        )

        # Analyze by route to see variation
        route_analysis = (
            db.query(
                RouteMetricsDaily.route_id,
                func.sum(RouteMetricsDaily.total_arrivals).label("total_arrivals"),
                func.avg(RouteMetricsDaily.otp_percentage).label("avg_otp"),
            )
            .group_by(RouteMetricsDaily.route_id)
            .having(func.sum(RouteMetricsDaily.total_arrivals) > 100)
            .order_by(func.sum(RouteMetricsDaily.total_arrivals).desc())
            .limit(10)
            .all()
        )

        print("\n3. Top 10 Routes by Matched Arrivals:")
        print(f"   {'Route':<8} {'Arrivals':>10} {'Avg OTP':>8}")
        print("   " + "-" * 28)
        for route in route_analysis:
            otp_str = f"{route.avg_otp:.1f}%" if route.avg_otp is not None else "N/A"
            print(f"   {route.route_id:<8} {route.total_arrivals:>10,} {otp_str:>8}")

        print("\n" + "=" * 70)
        print("Recommendations for Collection Frequency:")
        print("=" * 70)

        if overall_match_rate < 30:
            print("\n⚠️  Low match rate detected (<30%)")
            print("   Reasons could include:")
            print("   - Vehicles between stops (not near any stop)")
            print("   - Trips not in schedule (extra service)")
            print("   - Position accuracy issues")
            print("\n   Consider keeping 1-minute frequency to maximize data capture.")
        elif overall_match_rate < 50:
            print("\n📊 Moderate match rate (30-50%)")
            print("   - Current 1-minute frequency captures reasonable data")
            print("   - Could potentially extend to 2-minute intervals")
            print(
                f"   - Estimated arrivals at 2-min: {total_arrivals * 0.7:,.0f} ({overall_match_rate * 0.7:.1f}% match rate)"
            )
        else:
            print("\n✅ Good match rate (>50%)")
            print("   - Current 1-minute frequency is working well")
            print("   - Could consider 2-3 minute intervals if API limits are a concern")
            print(
                f"   - Estimated arrivals at 2-min: {total_arrivals * 0.8:,.0f} ({overall_match_rate * 0.8:.1f}% match rate)"
            )
            print(
                f"   - Estimated arrivals at 3-min: {total_arrivals * 0.7:,.0f} ({overall_match_rate * 0.7:.1f}% match rate)"
            )

        print("\n" + "=" * 70)

    finally:
        db.close()


if __name__ == "__main__":
    analyze_match_rate()
