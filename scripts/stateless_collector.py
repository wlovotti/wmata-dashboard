"""Stateless GTFS-RT collector — poll → zstd JSONL → S3 every 15 min → hc ping.

The Path 2a endpoint (NOTES-95): no database anywhere in this process.
Reuses WMATADataCollector purely as a fetch/parse client (its internal
archive writer and DB methods are never used); archives TU and VP rows to
per-feed JSONL streams rotating every 15 minutes; ships closed files to S3
and pings healthchecks.io only while both feeds are shipping (spec §1–2).

Run with:
    uv run python scripts/stateless_collector.py --agency wmata
    uv run python scripts/stateless_collector.py --agency sfmta
"""

import argparse
import os
import signal
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.agency_config import load_agency_config, request_kwargs
from src.archive_writer import JsonlArchiveWriter
from src.pidfile import acquire_pid_file, release_pid_file
from src.s3_uploader import S3Uploader
from src.stateless_poller import PingGate, archive_tu_rows, archive_vp_rows, run_upload_cycle
from src.timezones import utcnow_naive
from src.wmata_collector import WMATADataCollector

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
ROTATE_INTERVAL_SEC = 900  # 15-minute upload cadence (spec decision 1)


def now_str() -> str:
    """Local-time stamp prefix used in console logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_fetcher(cfg) -> WMATADataCollector:
    """Construct the collector as a pure fetch client for ``cfg``'s feeds.

    The instance's internal archive writer targets the TU dir but is never
    appended to (we archive via our own rotating writers), and no DB
    session is ever attached.
    """
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        raise ValueError(f"{cfg.api_key_env} not found in environment variables")
    auth = request_kwargs(cfg, api_key)
    return WMATADataCollector(
        api_key,
        archive_root=REPO_ROOT / cfg.archive_dir,
        tu_feed_url=cfg.trip_updates_url,
        vp_feed_url=cfg.vehicle_positions_url,
        request_params=auth.get("params"),
        service_date_tz=cfg.timezone,
    )


def main(argv=None) -> None:
    """Run the stateless polling loop for one agency until interrupted.

    Parses ``argv`` (or ``sys.argv`` when ``None``, via argparse's own
    default) rather than reading ``sys.argv`` directly, so the loop is
    callable from tests/other code with an explicit argument list. Ticks
    forever at ``cfg.tick_sec``, fetching TU/VP on their configured
    cadences, archiving to rotating per-feed writers, and running an
    upload+ping cycle every tick. On SIGINT/SIGTERM (raised as
    ``KeyboardInterrupt``) both writers are closed so any partial file
    becomes shippable, a final upload pass ships it, and the pid file is
    released — but no final ping is sent, since a shutdown must not read
    as health to the dead-man gate.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", required=True, choices=("wmata", "sfmta"))
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    cfg = load_agency_config(args.agency)
    acquire_pid_file(REPO_ROOT / cfg.pid_file)

    tu_dir = REPO_ROOT / cfg.archive_dir
    vp_dir = REPO_ROOT / cfg.vp_archive_dir
    tu_writer = JsonlArchiveWriter(tu_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    vp_writer = JsonlArchiveWriter(vp_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    uploader = S3Uploader(cfg.s3_bucket)
    gate = PingGate(os.getenv(cfg.healthcheck_url_env))
    streams = [
        ("tu", tu_dir, cfg.s3_tu_prefix, tu_writer),
        ("vp", vp_dir, cfg.s3_vp_prefix, vp_writer),
    ]
    fetcher = build_fetcher(cfg)

    print(f"{cfg.display_name} Stateless Collector (no DB)")
    print(
        f"TU every {cfg.tick_sec * cfg.trip_updates_every_ticks}s -> s3://{cfg.s3_bucket}/{cfg.s3_tu_prefix}"
    )
    print(
        f"VP every {cfg.tick_sec * cfg.vehicle_positions_every_ticks}s -> s3://{cfg.s3_bucket}/{cfg.s3_vp_prefix}"
    )

    tick_idx = 0
    try:
        while True:
            start = time.monotonic()

            if tick_idx % cfg.trip_updates_every_ticks == 0:
                try:
                    _, rows = fetcher.get_realtime_trip_updates()
                    n = archive_tu_rows(tu_writer, rows)
                    print(f"[{now_str()}] tick={tick_idx} trip_updates rows={n}")
                except Exception as e:
                    print(f"[{now_str()}] tick={tick_idx} trip_updates ERROR: {e}")

            if tick_idx % cfg.vehicle_positions_every_ticks == 0:
                try:
                    vehicles = fetcher.get_realtime_vehicle_positions()
                    n = archive_vp_rows(vp_writer, vehicles, collected_at=utcnow_naive())
                    print(f"[{now_str()}] tick={tick_idx} vehicle_positions rows={n}")
                except Exception as e:
                    print(f"[{now_str()}] tick={tick_idx} vehicle_positions ERROR: {e}")

            try:
                shipped = run_upload_cycle(uploader, streams, gate, now=time.time())
                if shipped:
                    print(f"[{now_str()}] tick={tick_idx} uploaded: {', '.join(shipped)}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} upload ERROR: {e}")

            elapsed = time.monotonic() - start
            sleep_for = cfg.tick_sec - elapsed
            if sleep_for < 0:
                print(f"[{now_str()}] tick={tick_idx} WARNING: tick took {elapsed:.1f}s")
            else:
                time.sleep(sleep_for)
            tick_idx += 1

    except KeyboardInterrupt:
        print("\nStopping stateless collection...")
    finally:
        # Close both writers, then ship the final partial files (no ping —
        # a shutdown must not look like health).
        tu_writer.close()
        vp_writer.close()
        try:
            for _feed, archive_dir, key_prefix, _w in streams:
                uploader.upload_closed_files(Path(archive_dir), key_prefix, set())
        except Exception as e:
            print(f"final upload ERROR: {e}")
        fetcher.close()
        release_pid_file(REPO_ROOT / cfg.pid_file)


if __name__ == "__main__":
    main()
