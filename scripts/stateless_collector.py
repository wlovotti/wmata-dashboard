"""Stateless GTFS-RT collector — poll → zstd JSONL → S3 every 15 min → hc ping.

The Path 2a endpoint: no database anywhere in this process.
Reuses WMATADataCollector purely as a fetch/parse client (its internal
archive writer and DB methods are never used); archives TU and VP rows to
per-feed JSONL streams rotating every 15 minutes; ships closed files to S3
and pings healthchecks.io only while both feeds are shipping (spec §1–2).

WARNING: never run this on the laptop against the real config-derived
archive dirs. The laptop's local archive is the system of record's raw
GTFS-RT backing store — this process's upload cycle both ships AND prunes
(deletes local copies of) whatever it finds in its archive dirs. Local
testing must always pass BOTH --archive-root and --vp-archive-root
pointed at scratch directories, never the real ones.

Run with:
    uv run python scripts/stateless_collector.py --agency wmata
    uv run python scripts/stateless_collector.py --agency sfmta

    # Laptop/local testing — always override both archive roots so the
    # upload+prune cycle never touches the real system-of-record archive:
    uv run python scripts/stateless_collector.py --agency wmata \\
        --archive-root /tmp/scratch/tu --vp-archive-root /tmp/scratch/vp
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


def build_fetcher(cfg, archive_root: Path | None = None) -> WMATADataCollector:
    """Construct the collector as a pure fetch client for ``cfg``'s feeds.

    The instance's internal archive writer targets ``archive_root`` (the
    config-derived TU dir by default) but is never appended to (we archive
    via our own rotating writers), and no DB session is ever attached.
    ``archive_root`` should be overridden to the same directory passed as
    ``--archive-root`` when running locally, so this inert internal writer
    also points away from the real archive.
    """
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        raise ValueError(f"{cfg.api_key_env} not found in environment variables")
    auth = request_kwargs(cfg, api_key)
    return WMATADataCollector(
        api_key,
        archive_root=archive_root if archive_root is not None else REPO_ROOT / cfg.archive_dir,
        tu_feed_url=cfg.trip_updates_url,
        vp_feed_url=cfg.vehicle_positions_url,
        request_params=auth.get("params"),
        service_date_tz=cfg.timezone,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for :func:`main`.

    Factored out so tests can assert on accepted flags (``--agency``,
    ``--archive-root``, ``--vp-archive-root``) without importing anything
    that starts the polling loop.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", required=True, choices=("wmata", "sfmta"))
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help=(
            "Override the TU archive directory (default: config-derived). "
            "Required for local testing — see module docstring warning; "
            "required together with --vp-archive-root (a half-override is rejected)."
        ),
    )
    parser.add_argument(
        "--vp-archive-root",
        type=Path,
        default=None,
        help=(
            "Override the VP archive directory (default: config-derived). "
            "Required for local testing — see module docstring warning; "
            "required together with --archive-root (a half-override is rejected)."
        ),
    )
    return parser


def validate_archive_root_pairing(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    """Reject a half-override of the archive-root pair.

    ``--archive-root`` and ``--vp-archive-root`` must be passed together
    or not at all: setting only one would leave the other real archive
    tree exposed to the upload-and-prune cycle. Calls ``parser.error``
    (which prints usage and raises ``SystemExit(2)``) on a half-override;
    a separate function so tests can exercise the check without going
    through ``main``'s full argv-to-loop path.
    """
    if (args.archive_root is None) != (args.vp_archive_root is None):
        parser.error(
            "--archive-root and --vp-archive-root must be passed together — "
            "a half-override leaves the other real archive tree exposed to "
            "the upload-and-prune cycle"
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
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    validate_archive_root_pairing(args, parser)

    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    cfg = load_agency_config(args.agency)
    acquire_pid_file(REPO_ROOT / cfg.pid_file)

    tu_dir = args.archive_root if args.archive_root is not None else REPO_ROOT / cfg.archive_dir
    vp_dir = (
        args.vp_archive_root if args.vp_archive_root is not None else REPO_ROOT / cfg.vp_archive_dir
    )
    tu_writer = JsonlArchiveWriter(tu_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    vp_writer = JsonlArchiveWriter(vp_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    uploader = S3Uploader(cfg.s3_bucket)
    gate = PingGate(os.getenv(cfg.healthcheck_url_env))
    streams = [
        ("tu", tu_dir, cfg.s3_tu_prefix, tu_writer),
        ("vp", vp_dir, cfg.s3_vp_prefix, vp_writer),
    ]
    # Pass the (possibly overridden) tu_dir so the fetcher's inert internal
    # archive writer also points away from the real archive during local
    # testing (see build_fetcher docstring).
    fetcher = build_fetcher(cfg, archive_root=tu_dir)

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
