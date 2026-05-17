"""
Continuous TripUpdates collector — polls WMATA's GTFS-RT TripUpdates feed
every 30 seconds and appends raw stop_time_updates to trip_update_snapshots.

Companion to scripts/continuous_collector.py. Faster polling (30s vs. 60s)
because actual arrivals are inferred from the gap before a stop disappears
from the feed; tighter sampling shrinks that uncertainty bound. API budget:
60s positions + 30s trip_updates = 4,320 calls/day, well under WMATA's
50,000/day limit.

Run with: uv run python scripts/continuous_trip_updates_collector.py
"""

import os
import time
from datetime import datetime

from dotenv import load_dotenv

from src.database import get_session, init_db
from src.wmata_collector import WMATADataCollector

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

POLL_INTERVAL_SEC = 30


def collect_trip_updates_once(collector: WMATADataCollector) -> None:
    """Pull one TripUpdates snapshot and persist its stop_time_updates.

    The caller passes in a long-lived ``collector`` so its
    ``_tu_dedup_cache`` survives between snapshots. The DB session is
    rebound per call to keep the connection fresh on multi-day runs.
    """
    db = get_session()

    try:
        collector.db = db
        snapshot_ts, rows = collector.get_realtime_trip_updates()

        if rows:
            collector._save_trip_updates(rows)
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                f"Snapshot complete (snapshot_ts={snapshot_ts}, rows_in_feed={len(rows)})"
            )
        else:
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                f"No trip update rows in this snapshot"
            )

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error: {e}")

    finally:
        db.close()


def main():
    """Run the polling loop until interrupted."""
    print("WMATA Continuous TripUpdates Collector")
    print("=" * 50)
    print(f"Polling every {POLL_INTERVAL_SEC} seconds")
    print("Press Ctrl+C to stop")
    print("=" * 50)

    init_db()

    print("\nStarting continuous collection...")

    # Single collector instance shared across polls so its dedup cache
    # for trip_update_snapshots survives between snapshots.
    collector = WMATADataCollector(API_KEY)

    try:
        while True:
            collect_trip_updates_once(collector)
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n\nStopping continuous collection...")
        print("Trip updates collection stopped successfully!")
    finally:
        collector.close()


if __name__ == "__main__":
    main()
