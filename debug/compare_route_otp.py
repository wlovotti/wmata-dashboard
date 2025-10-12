"""
Compare OTP rates across all routes in the database
"""
from src.database import get_session
from src.analytics import calculate_line_level_otp
from src.models import VehiclePosition
from sqlalchemy import func

db = get_session()

try:
    # Get all routes with sufficient data
    print("=" * 70)
    print("Analyzing OTP Rates Across All Routes")
    print("=" * 70)

    # Find top 15 routes by position count
    # Note: find_nearest_stop() now uses caching for better performance
    routes_with_data = db.query(
        VehiclePosition.route_id,
        func.count(VehiclePosition.id).label('position_count'),
        func.count(func.distinct(VehiclePosition.vehicle_id)).label('vehicle_count')
    ).group_by(
        VehiclePosition.route_id
    ).order_by(
        func.count(VehiclePosition.id).desc()
    ).limit(15).all()

    print(f"\nAnalyzing top {len(routes_with_data)} routes by data volume (limited for performance)")
    print("-" * 70)

    # Calculate OTP for each route
    route_otp_results = []

    for i, (route_id, position_count, vehicle_count) in enumerate(routes_with_data, 1):
        print(f"  [{i}/{len(routes_with_data)}] Analyzing {route_id}...", end='', flush=True)

        otp = calculate_line_level_otp(db, route_id)

        arrivals = otp.get('arrivals_analyzed', 0)
        if arrivals >= 10:  # Only include routes with at least 10 arrivals analyzed
            print(f" {arrivals} arrivals, {otp.get('on_time_percentage', 0):.1f}% on-time")
            route_otp_results.append({
                'route_id': route_id,
                'position_count': position_count,
                'vehicle_count': vehicle_count,
                'arrivals_analyzed': arrivals,
                'on_time_pct': otp.get('on_time_percentage', 0),
                'early_pct': otp.get('early_percentage', 0),
                'late_pct': otp.get('late_percentage', 0),
                'matched_vehicles': otp.get('matched_vehicles', 0),
                'unmatched_vehicles': otp.get('unmatched_vehicles', 0)
            })
        else:
            print(f" skipped ({arrivals} arrivals, need 10+)")

    print(f"\nSuccessfully analyzed OTP for {len(route_otp_results)} routes")
    print("=" * 70)

    if not route_otp_results:
        print("No routes with sufficient data for OTP analysis")
    else:
        # Sort by on-time percentage
        route_otp_results.sort(key=lambda x: x['on_time_pct'], reverse=True)

        # Show best OTP
        print("\nHIGHEST ON-TIME PERFORMANCE")
        print("-" * 70)
        print(f"{'Route':8s} {'On-Time':>8s} {'Early':>8s} {'Late':>8s} {'Arrivals':>10s} {'Vehicles':>10s}")
        print("-" * 70)

        for route in route_otp_results[:min(5, len(route_otp_results))]:
            print(f"{route['route_id']:8s} {route['on_time_pct']:7.1f}% {route['early_pct']:7.1f}% "
                  f"{route['late_pct']:7.1f}% {route['arrivals_analyzed']:10d} "
                  f"{route['matched_vehicles']:10d}")

        # Show worst OTP
        if len(route_otp_results) > 5:
            print("\nLOWEST ON-TIME PERFORMANCE")
            print("-" * 70)
            print(f"{'Route':8s} {'On-Time':>8s} {'Early':>8s} {'Late':>8s} {'Arrivals':>10s} {'Vehicles':>10s}")
            print("-" * 70)

            for route in route_otp_results[-min(5, len(route_otp_results)):]:
                print(f"{route['route_id']:8s} {route['on_time_pct']:7.1f}% {route['early_pct']:7.1f}% "
                      f"{route['late_pct']:7.1f}% {route['arrivals_analyzed']:10d} "
                      f"{route['matched_vehicles']:10d}")

        # Summary statistics
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)

        avg_on_time = sum(r['on_time_pct'] for r in route_otp_results) / len(route_otp_results)
        avg_early = sum(r['early_pct'] for r in route_otp_results) / len(route_otp_results)
        avg_late = sum(r['late_pct'] for r in route_otp_results) / len(route_otp_results)
        total_arrivals = sum(r['arrivals_analyzed'] for r in route_otp_results)

        print(f"Routes analyzed: {len(route_otp_results)}")
        print(f"Total arrivals analyzed: {total_arrivals:,}")
        print(f"Average on-time rate: {avg_on_time:.1f}%")
        print(f"Average early rate: {avg_early:.1f}%")
        print(f"Average late rate: {avg_late:.1f}%")
        print(f"Best performing route: {route_otp_results[0]['route_id']} ({route_otp_results[0]['on_time_pct']:.1f}% on-time)")
        print(f"Worst performing route: {route_otp_results[-1]['route_id']} ({route_otp_results[-1]['on_time_pct']:.1f}% on-time)")

finally:
    db.close()
