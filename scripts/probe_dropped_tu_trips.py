"""Profile trips that appear in trip_update_snapshots but never in
vehicle_positions for the same service date.

Background: derive_stop_events_trip_updates.py needs trip_start_date for
service-date attribution but TripUpdate snapshots don't carry it (the
GTFS-RT TripDescriptor field isn't populated by WMATA). The pipeline
cross-references vehicle_positions on (trip_id, trip_start_date) to
recover it. Trips that appear in TU but never in VP for the day are
silently dropped.

The service-delivered ratio (PR #47) is sensitive to this: dropped trips
look like "not delivered" even if they ran. Re-run this periodically as
the collector accumulates clean multi-day windows; day-1 numbers are
inflated by collection-boundary artifacts (trips already in flight at
collector start, trips just-appearing at collector stop).

Usage:
    uv run python scripts/probe_dropped_tu_trips.py --date 2026-05-03
    uv run python scripts/probe_dropped_tu_trips.py --date 2026-05-03 --hours 6 18
"""

import argparse
from datetime import date, datetime, timedelta

from sqlalchemy import text

from src.database import get_session


def main() -> None:
    """CLI entry: prints summary, route breakdown, hour breakdown."""
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="Service date in YYYY-MM-DD (Eastern operational day)",
    )
    p.add_argument(
        "--hours",
        type=int,
        nargs=2,
        metavar=("START", "END"),
        help="Optional Eastern hour range (inclusive start, exclusive end) to "
        "exclude collection-boundary noise — e.g. --hours 6 22 to skip "
        "post-midnight startup and pre-shutdown trips",
    )
    args = p.parse_args()

    service_date_compact = args.date.strftime("%Y%m%d")
    day_start_utc = datetime.combine(args.date, datetime.min.time())
    day_end_utc = day_start_utc + timedelta(days=1)

    db = get_session()
    print(f"=== Service date: {args.date.isoformat()} ===\n")

    tu_total = db.execute(
        text("""
            SELECT COUNT(DISTINCT trip_id)
            FROM trip_update_snapshots
            WHERE snapshot_ts >= :start AND snapshot_ts < :end
        """),
        {"start": day_start_utc, "end": day_end_utc},
    ).scalar()
    vp_total = db.execute(
        text("""
            SELECT COUNT(DISTINCT trip_id)
            FROM vehicle_positions
            WHERE trip_start_date = :sd
        """),
        {"sd": service_date_compact},
    ).scalar()
    dropped_total = db.execute(
        text("""
            WITH tu AS (
                SELECT DISTINCT trip_id FROM trip_update_snapshots
                WHERE snapshot_ts >= :start AND snapshot_ts < :end
            )
            SELECT COUNT(*) FROM tu
            WHERE trip_id NOT IN (
                SELECT DISTINCT trip_id FROM vehicle_positions
                WHERE trip_start_date = :sd
            )
        """),
        {"start": day_start_utc, "end": day_end_utc, "sd": service_date_compact},
    ).scalar()

    print(f"TU distinct trip_ids:                          {tu_total:>6,}")
    print(f"VP distinct trip_ids (trip_start_date={service_date_compact}): {vp_total:>6,}")
    print(
        f"TU-only (dropped by B1):                       "
        f"{dropped_total:>6,}  ({dropped_total / tu_total * 100:.1f}%)\n"
    )

    print("=== Dropped trips by route (top 25) ===")
    rows = db.execute(
        text("""
            WITH tu AS (
                SELECT trip_id, MAX(route_id) AS route_id
                FROM trip_update_snapshots
                WHERE snapshot_ts >= :start AND snapshot_ts < :end
                GROUP BY trip_id
            )
            SELECT route_id, COUNT(*) AS dropped FROM tu
            WHERE trip_id NOT IN (
                SELECT DISTINCT trip_id FROM vehicle_positions
                WHERE trip_start_date = :sd
            )
            GROUP BY route_id ORDER BY dropped DESC LIMIT 25
        """),
        {"start": day_start_utc, "end": day_end_utc, "sd": service_date_compact},
    ).fetchall()
    for r in rows:
        print(f"  route={r.route_id!s:<10} dropped={r.dropped:>5}")

    print("\n=== Dropped trips by hour of first_seen (Eastern) ===")
    rows = db.execute(
        text("""
            WITH tu AS (
                SELECT trip_id, MIN(snapshot_ts) AS first_seen
                FROM trip_update_snapshots
                WHERE snapshot_ts >= :start AND snapshot_ts < :end
                GROUP BY trip_id
            )
            SELECT EXTRACT(HOUR FROM (first_seen AT TIME ZONE 'UTC'
                                      AT TIME ZONE 'America/New_York'))::int AS h,
                   COUNT(*) AS n
            FROM tu
            WHERE trip_id NOT IN (
                SELECT DISTINCT trip_id FROM vehicle_positions
                WHERE trip_start_date = :sd
            )
            GROUP BY h ORDER BY h
        """),
        {"start": day_start_utc, "end": day_end_utc, "sd": service_date_compact},
    ).fetchall()
    for r in rows:
        marker = ""
        if args.hours and not (args.hours[0] <= r.h < args.hours[1]):
            marker = "  (excluded by --hours)"
        print(f"  hour {r.h:02d}: {r.n:>4}{marker}")

    if args.hours:
        in_window = sum(r.n for r in rows if args.hours[0] <= r.h < args.hours[1])
        print(
            f"\nIn-window dropped (hours {args.hours[0]:02d}-{args.hours[1]:02d}): "
            f"{in_window:,} ({in_window / tu_total * 100:.1f}% of TU total)"
        )


if __name__ == "__main__":
    main()
