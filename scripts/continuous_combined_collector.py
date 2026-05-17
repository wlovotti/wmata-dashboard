"""
Combined continuous collector — pulls both TripUpdates (every 30s) and
VehiclePositions (every 60s) from one process.

Single-process equivalent of running continuous_collector.py and
continuous_trip_updates_collector.py side by side. One log, one Ctrl+C,
one DB session per tick. Cadence is enforced by time-budgeting each
tick so a slow fetch doesn't compound into long-term drift.

API budget: 30s trip_updates + 60s positions = 4,320 calls/day, well
under WMATA's 50,000/day limit.

Run with: uv run python scripts/continuous_combined_collector.py

Keeping it running with the lid closed (macOS):
  caffeinate -dimsu uv run python scripts/continuous_combined_collector.py
  # ...still sleeps on lid-close. For lid-closed operation, use
  # `sudo pmset disablesleep 1` (and `sudo pmset disablesleep 0` to undo)
  # or run on a small cloud VM. A laptop-pegged-to-AC desk run is fine
  # if you can leave the lid open.
"""

import os
import signal
import time
from datetime import datetime

from dotenv import load_dotenv

from src.database import get_session, init_db
from src.wmata_collector import WMATADataCollector

load_dotenv()

API_KEY = os.getenv("WMATA_API_KEY")

if not API_KEY:
    raise ValueError("WMATA_API_KEY not found in environment variables")

# Trip updates poll every TICK_SEC; vehicle positions poll every
# POSITIONS_TICK_RATIO ticks (so 30s and 60s respectively).
TICK_SEC = 30
POSITIONS_TICK_RATIO = 2


def now_str() -> str:
    """Local-time stamp prefix used in console logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_one_tick(tick_idx: int, collector: WMATADataCollector) -> None:
    """Run one fetch cycle: TripUpdates always, positions every Nth tick.

    Reuses the same ``WMATADataCollector`` across ticks so its
    ``_tu_dedup_cache`` survives between snapshots. Opens a fresh DB
    session per tick so the loop survives stale connections during
    multi-day runs.
    """
    db = get_session()
    try:
        collector.db = db

        # Trip updates every tick
        try:
            _, rows = collector.get_realtime_trip_updates()
            saved = collector._save_trip_updates(rows) if rows else 0
            print(f"[{now_str()}] tick={tick_idx} trip_updates rows={saved}")
        except Exception as e:
            print(f"[{now_str()}] tick={tick_idx} trip_updates ERROR: {e}")

        # Positions every Nth tick
        if tick_idx % POSITIONS_TICK_RATIO == 0:
            try:
                vehicles = collector.get_realtime_vehicle_positions()
                if vehicles:
                    collector._save_vehicle_positions(vehicles)
                print(f"[{now_str()}] tick={tick_idx} vehicle_positions rows={len(vehicles)}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} vehicle_positions ERROR: {e}")

    finally:
        db.close()


def main() -> None:
    """Run the combined polling loop until interrupted."""
    # Force-install handlers regardless of inherited disposition. Python only
    # auto-installs default_int_handler for SIGINT when the inherited handler
    # is SIG_DFL; if the parent shell (CI, launchd, some IDEs) starts the
    # process with SIG_IGN, the script would silently ignore Ctrl+C / kill -INT
    # and skip the try/except KeyboardInterrupt cleanup (which calls
    # collector.close() to flush the JSONL archive's zstd footer). Installing
    # default_int_handler for SIGTERM gives the same graceful path for the
    # standard "kill <pid>" signal.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    print("WMATA Combined Continuous Collector")
    print("=" * 50)
    print(f"Trip updates:      every {TICK_SEC}s")
    print(f"Vehicle positions: every {TICK_SEC * POSITIONS_TICK_RATIO}s")
    print("Press Ctrl+C to stop")
    print("=" * 50)

    init_db()

    print("\nStarting continuous collection...")

    # Single collector instance shared across ticks so its dedup cache
    # for trip_update_snapshots survives between snapshots. The DB
    # session is rebound per tick inside run_one_tick.
    collector = WMATADataCollector(API_KEY)

    tick_idx = 0
    try:
        while True:
            start = time.monotonic()
            run_one_tick(tick_idx, collector)
            elapsed = time.monotonic() - start

            sleep_for = TICK_SEC - elapsed
            if sleep_for < 0:
                # Fetch took longer than the tick budget; skip sleep but warn so
                # we notice if this becomes the steady state.
                print(
                    f"[{now_str()}] tick={tick_idx} WARNING: tick took "
                    f"{elapsed:.1f}s (> {TICK_SEC}s budget)"
                )
            else:
                time.sleep(sleep_for)

            tick_idx += 1

    except KeyboardInterrupt:
        print("\n\nStopping continuous collection...")
        print("Combined collector stopped successfully!")
    finally:
        collector.close()


if __name__ == "__main__":
    main()
