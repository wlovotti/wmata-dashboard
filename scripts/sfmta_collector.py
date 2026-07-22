"""SFMTA (Muni) sidecar collector — 511.org GTFS-RT TripUpdates + VehiclePositions.

Sibling of scripts/continuous_combined_collector.py, driven by
config/agencies/sfmta.yaml. Writes to the SFMTA_DATABASE_URL database
(``sfmta_dashboard``) — never the WMATA one. Cadence budget against
511.org's default 60 req/rolling-hour token cap:

    tick 60s; TripUpdates every 2nd tick (120s, 30/hr);
    VehiclePositions every 3rd tick (180s, 20/hr); total 50/hr.

Raw TU rows archive to archive/sfmta_raw_snapshots/ (UTC-date-bucketed
jsonl.zst, same writer as WMATA). Service-date fallback uses Pacific time
(spec 2026-07-21 §2). Run with:

    uv run python scripts/sfmta_collector.py
"""

import os
import signal
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.agency_config import load_agency_config, request_kwargs
from src.database import get_session, init_db
from src.pidfile import acquire_pid_file, release_pid_file
from src.wmata_collector import WMATADataCollector

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
CFG = load_agency_config("sfmta")
PID_FILE = REPO_ROOT / CFG.pid_file


def now_str() -> str:
    """Local-time stamp prefix used in console logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_collector() -> WMATADataCollector:
    """Construct the parameterized collector for 511.org/SFMTA.

    Reads the 511 token and healthcheck URL from the env vars named in
    config/agencies/sfmta.yaml; raises early if the token is missing.
    """
    api_key = os.getenv(CFG.api_key_env)
    if not api_key:
        raise ValueError(f"{CFG.api_key_env} not found in environment variables")
    auth = request_kwargs(CFG, api_key)
    return WMATADataCollector(
        api_key,
        archive_root=REPO_ROOT / CFG.archive_dir,
        healthcheck_url=os.getenv(CFG.healthcheck_url_env),
        tu_feed_url=CFG.trip_updates_url,
        vp_feed_url=CFG.vehicle_positions_url,
        request_params=auth.get("params"),
        service_date_tz=CFG.timezone,
        heartbeat_name=CFG.heartbeat_name,
    )


def run_one_tick(tick_idx: int, collector: WMATADataCollector, db_url: str) -> None:
    """Run one 60s tick: TU on even ticks, VP on every 3rd tick.

    Opens a fresh DB session per tick (same stale-connection defense as
    the WMATA loop). Each feed's errors are caught independently so one
    failing feed never starves the other.
    """
    db = get_session(db_url=db_url)
    try:
        collector.db = db

        if tick_idx % CFG.trip_updates_every_ticks == 0:
            try:
                _, rows = collector.get_realtime_trip_updates()
                saved = collector._save_trip_updates(rows) if rows else 0
                print(f"[{now_str()}] tick={tick_idx} trip_updates rows={saved}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} trip_updates ERROR: {e}")

        if tick_idx % CFG.vehicle_positions_every_ticks == 0:
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
    """Run the SFMTA polling loop until interrupted."""
    # Force-install handlers regardless of inherited disposition (PR #129
    # lesson): a SIG_IGN-inheriting parent (systemd contexts, CI) would
    # otherwise make the process unkillable-gracefully and skip the
    # collector.close() zstd-footer flush.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    acquire_pid_file(PID_FILE)

    db_url = os.getenv(CFG.database_url_env)
    if not db_url:
        raise ValueError(f"{CFG.database_url_env} not found in environment variables")

    print(f"{CFG.display_name} Sidecar Collector")
    print("=" * 50)
    print(f"Trip updates:      every {CFG.tick_sec * CFG.trip_updates_every_ticks}s")
    print(f"Vehicle positions: every {CFG.tick_sec * CFG.vehicle_positions_every_ticks}s")
    print(f"Archive dir:       {CFG.archive_dir}")
    print(f"Pid file:          {PID_FILE}")
    print("Press Ctrl+C to stop")
    print("=" * 50)

    init_db(db_url=db_url)

    collector = build_collector()

    tick_idx = 0
    try:
        while True:
            start = time.monotonic()
            run_one_tick(tick_idx, collector, db_url)
            elapsed = time.monotonic() - start

            sleep_for = CFG.tick_sec - elapsed
            if sleep_for < 0:
                print(
                    f"[{now_str()}] tick={tick_idx} WARNING: tick took "
                    f"{elapsed:.1f}s (> {CFG.tick_sec}s budget)"
                )
            else:
                time.sleep(sleep_for)

            tick_idx += 1

    except KeyboardInterrupt:
        print("\n\nStopping SFMTA collection...")
    finally:
        collector.close()
        release_pid_file(PID_FILE)


if __name__ == "__main__":
    main()
