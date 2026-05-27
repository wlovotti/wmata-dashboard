"""
One-shot health check for the combined continuous collector.

Prints a tight, human-scannable status report covering: process liveness,
macOS sleep state, disk free, recent log errors, and per-table row
counts / cadence / latest-data freshness for collector_heartbeats and
vehicle_positions. Exits 0 if everything is healthy; exits 1 if any
hard check fails (process down, stale data, recent errors).

Usage: uv run python scripts/collector_status.py
       (or via the /collector-status slash command)
"""

import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import func

from src.database import get_session
from src.models import CollectorHeartbeat, VehiclePosition
from src.timezones import utcnow_naive

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
PID_FILE = REPO_ROOT / "logs" / "collector.pid"
LOG_FILE = REPO_ROOT / "logs" / "collector.log"

# Thresholds for "healthy" — tuned to the 30s/60s collection cadence with
# some headroom for the WMATA feed's own jitter.
MAX_HEARTBEAT_AGE_SEC = 90
MAX_POSITION_AGE_SEC = 180
MAX_DRIFT_SEC = 5
LOG_TAIL_LINES = 100


def _check_process():
    """Return (alive, pid, uptime_str, rss_mb) for the collector PID file."""
    if not PID_FILE.exists():
        return False, None, None, None
    pid = PID_FILE.read_text().strip()
    if not pid.isdigit():
        return False, None, None, None
    result = subprocess.run(
        ["ps", "-p", pid, "-o", "pid=,etime=,rss="],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return False, int(pid), None, None
    parts = result.stdout.strip().split()
    return True, int(parts[0]), parts[1], int(parts[2]) // 1024


def _check_sleep_disabled():
    """Return True if macOS pmset reports SleepDisabled=1, else False."""
    result = subprocess.run(["pmset", "-g"], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    match = re.search(r"SleepDisabled\s+(\d)", result.stdout)
    return match.group(1) == "1" if match else None


def _disk_free_gb():
    """Return free GB on the partition holding the repo."""
    usage = shutil.disk_usage(REPO_ROOT)
    return usage.free // (1024**3)


def _recent_log_errors():
    """Return list of error/warning lines from the last LOG_TAIL_LINES of the log."""
    if not LOG_FILE.exists():
        return []
    lines = LOG_FILE.read_text(errors="replace").splitlines()[-LOG_TAIL_LINES:]
    pattern = re.compile(r"\b(ERROR|WARNING|Traceback|Exception)\b", re.IGNORECASE)
    return [line.strip() for line in lines if pattern.search(line)]


def main() -> int:
    """Print the status report and exit 0 if healthy, 1 if not."""
    db = get_session()
    issues = []

    print(f"=== collector status — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} local ===")

    # Process
    alive, pid, etime, rss = _check_process()
    if alive:
        print(f"process     PID {pid}  up {etime}  RSS {rss}MB")
    else:
        print(f"process     NOT RUNNING (pid file: {PID_FILE})")
        issues.append("collector process is not running")

    # Sleep
    sleep_disabled = _check_sleep_disabled()
    if sleep_disabled is True:
        print("sleep       disabled (lid-safe)")
    elif sleep_disabled is False:
        print("sleep       ENABLED — laptop will sleep on lid close")
    else:
        print("sleep       (could not query pmset)")

    # Disk
    free_gb = _disk_free_gb()
    print(f"disk        {free_gb}G free")
    if free_gb < 20:
        issues.append(f"disk only {free_gb}G free")

    # Log errors
    errors = _recent_log_errors()
    if errors:
        print(f"log         {len(errors)} error/warning line(s) in last {LOG_TAIL_LINES}")
        for line in errors[-3:]:
            print(f"              {line[:120]}")
        issues.append(f"{len(errors)} log error/warning(s)")
    else:
        print("log         no errors")
    print()

    # Collector heartbeats (Phase E.2: replaced trip_update_snapshots as coverage signal)
    now_utc = utcnow_naive()
    hb_total = db.query(CollectorHeartbeat).count()
    hb_24h = (
        db.query(CollectorHeartbeat)
        .filter(CollectorHeartbeat.ts >= now_utc - timedelta(hours=24))
        .count()
    )
    hb_latest = db.query(func.max(CollectorHeartbeat.ts)).scalar()

    # Median of inter-heartbeat gaps over the last 20 rows — robust to
    # historical interruptions (e.g., collector restarts).
    recent_hbs = (
        db.query(CollectorHeartbeat.ts).order_by(CollectorHeartbeat.ts.desc()).limit(20).all()
    )
    recent_ts = sorted([r[0] for r in recent_hbs])
    gaps = [(recent_ts[i] - recent_ts[i - 1]).total_seconds() for i in range(1, len(recent_ts))]
    cadence_median = sorted(gaps)[len(gaps) // 2] if gaps else None

    if hb_latest:
        age_sec = (now_utc - hb_latest).total_seconds()
        cadence_str = f"{cadence_median:.1f}s" if cadence_median else "n/a"
        drift_flag = ""
        if cadence_median and abs(cadence_median - 30) > MAX_DRIFT_SEC:
            drift_flag = " ⚠"
            issues.append(f"heartbeat cadence drift: median {cadence_median:.1f}s vs 30s target")
        age_flag = ""
        if age_sec > MAX_HEARTBEAT_AGE_SEC:
            age_flag = " ⚠"
            issues.append(f"collector_heartbeats stale: latest {age_sec:.0f}s ago")
        print(
            f"heartbeats    {hb_total:>10,} rows  "
            f"({hb_24h:>10,} last 24h, "
            f"cadence {cadence_str}{drift_flag}, latest {age_sec:.0f}s ago{age_flag})"
        )
    else:
        print(f"heartbeats    {hb_total:>10,} rows  (no heartbeats yet)")
        issues.append("no collector_heartbeats collected")

    # Vehicle positions
    vp_total = db.query(VehiclePosition).count()
    vp_24h = (
        db.query(VehiclePosition)
        .filter(VehiclePosition.collected_at >= now_utc - timedelta(hours=24))
        .count()
    )
    vp_latest = db.query(func.max(VehiclePosition.timestamp)).scalar()
    vp_active_fleet = (
        db.query(func.count(func.distinct(VehiclePosition.vehicle_id)))
        .filter(VehiclePosition.collected_at >= now_utc - timedelta(minutes=2))
        .scalar()
    )

    if vp_latest:
        age_sec = (now_utc - vp_latest).total_seconds()
        age_flag = ""
        if age_sec > MAX_POSITION_AGE_SEC:
            age_flag = " ⚠"
            issues.append(f"vehicle_positions stale: latest {age_sec:.0f}s ago")
        print(
            f"positions     {vp_total:>10,} rows  "
            f"({vp_24h:>10,} last 24h, latest {age_sec:.0f}s ago{age_flag}, "
            f"fleet {vp_active_fleet})"
        )
    else:
        print(f"positions     {vp_total:>10,} rows  (no positions yet)")
        issues.append("no vehicle_positions collected")

    print()
    if issues:
        print(f"✗ {len(issues)} issue(s):")
        for issue in issues:
            print(f"  - {issue}")
        return 1
    print("✓ healthy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
