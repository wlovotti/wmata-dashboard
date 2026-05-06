"""
launchd-scheduled wrapper for `scripts/reload_gtfs_complete.py`.

Closes NOTES-23. Before this, the GTFS reload was a manual script —
the failure mode wasn't a broken reload (PR #48 made the script
transactional and FK-safe) but forgetting to run it. The schedule
went 6 months stale before someone noticed.

This wrapper is what `com.wmata-dashboard.gtfs-reload.plist` invokes
weekly. Its job is small:

1. Spawn `reload_gtfs_complete.py` as a subprocess and capture its
   stdout/stderr into a per-run log file at
   `logs/gtfs_reload_<YYYY-MM-DD>.log`.
2. Time the run, record the exit code.
3. On non-zero exit, fire a macOS desktop notification via
   `osascript` so the user actually sees the failure when they next
   look at the laptop. Silent staleness is the failure mode this
   whole feature exists to prevent — the notification is the alert.
4. Propagate the subprocess exit code so launchd's own
   stdout/stderr capture (logs/launchd_gtfs_reload.{out,err}.log)
   also records a failure.

Subprocess invocation uses `sys.executable` rather than
`["uv", "run", ...]`. The plist's outer `ProgramArguments` already
runs `/opt/homebrew/bin/uv run python scripts/run_gtfs_reload.py`,
so by the time we're inside this wrapper the venv is active and
`sys.executable` is the venv Python. Re-resolving via `uv` here
would also fail under launchd, which strips PATH down to a minimal
set that doesn't include Homebrew (this lesson cost a debugging
loop in PR #62 — see pipelines/run_daily_batch.py for the same
pattern).

Usage:
  uv run python scripts/run_gtfs_reload.py
  uv run python scripts/run_gtfs_reload.py --dry-run    # log the plan, skip the reload
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"
RELOAD_SCRIPT = REPO_ROOT / "scripts" / "reload_gtfs_complete.py"
FAILURE_MARKER = LOGS_DIR / "gtfs_reload_LAST_FAILURE.json"


def fire_failure_notification(exit_code: int, elapsed: float, log_path: Path) -> None:
    """Surface a GTFS reload failure as a macOS desktop notification.

    The whole reason this feature exists is that silent staleness
    went unnoticed for 6 months. A non-zero exit from the reload is
    the alertable event — `osascript` shows it in the user's
    notification center the next time the laptop is open. If
    `osascript` itself isn't on PATH (unlikely on macOS), we fall
    back to a stderr message; either way the wrapper still exits
    non-zero so launchd records the failure.
    """
    osascript = shutil.which("osascript") or "/usr/bin/osascript"
    title = "WMATA dashboard"
    message = f"GTFS reload failed (exit {exit_code}, {elapsed:.0f}s). See {log_path.name}."
    try:
        subprocess.run(
            [
                osascript,
                "-e",
                f'display notification "{message}" with title "{title}"',
            ],
            check=False,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        # Notification is best-effort — the non-zero exit + log file
        # are the durable signal. Don't let a notification problem
        # mask the underlying failure.
        print(f"run_gtfs_reload: failed to fire notification: {exc}", file=sys.stderr)


def write_failure_marker(exit_code: int, elapsed: float, log_path: Path) -> None:
    """Drop a JSON marker file the dashboard could surface (NOTES-24).

    This is intentionally tiny and side-effect-free — the dashboard
    doesn't read it yet, but if NOTES-24 ever wants a "last reload
    failed" badge, the data is already on disk in a stable shape.
    """
    import json
    from datetime import UTC, datetime

    payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "exit_code": exit_code,
        "elapsed_seconds": round(elapsed, 1),
        "log_file": str(log_path),
    }
    try:
        FAILURE_MARKER.write_text(json.dumps(payload, indent=2) + "\n")
    except OSError as exc:
        print(f"run_gtfs_reload: failed to write failure marker: {exc}", file=sys.stderr)


def clear_failure_marker() -> None:
    """Remove the failure marker on a successful run.

    Keeping a stale failure marker after a recovery would be
    actively misleading — the dashboard or operator might think the
    reload is still broken. Best-effort: a missing file is fine, a
    permissions error gets logged but doesn't fail the run.
    """
    try:
        FAILURE_MARKER.unlink(missing_ok=True)
    except OSError as exc:
        print(f"run_gtfs_reload: failed to clear failure marker: {exc}", file=sys.stderr)


def run_reload(log_handle, dry_run: bool) -> tuple[int, float]:
    """Spawn reload_gtfs_complete.py and stream its output into log_handle.

    Returns (exit_code, elapsed_seconds). On dry-run, returns
    (0, 0.0) without spawning anything — used by the smoke test to
    validate wiring without hitting the WMATA API or the DB.
    """
    cmd = [sys.executable, str(RELOAD_SCRIPT)]
    log_handle.write(f"$ {' '.join(cmd)}\n")
    log_handle.flush()
    if dry_run:
        log_handle.write("(dry-run — subprocess not invoked)\n")
        log_handle.flush()
        return 0, 0.0
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        check=False,
    )
    elapsed = time.time() - start
    log_handle.write(f"\n[exit {proc.returncode}, {elapsed:.1f}s]\n")
    log_handle.flush()
    return proc.returncode, elapsed


def main() -> int:
    """CLI entry point — opens the day's log file and drives the reload."""
    parser = argparse.ArgumentParser(
        description=(
            "launchd wrapper for the weekly GTFS reload. Logs to "
            "logs/gtfs_reload_<date>.log and fires a macOS notification "
            "on failure."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the plan and exit 0 without invoking the reload subprocess.",
    )
    args = parser.parse_args()

    LOGS_DIR.mkdir(exist_ok=True)
    # Eastern is the project's service-date timezone, but the wrapper
    # runs at 04:00 local on the user's laptop and the log file just
    # needs a per-run-day name — local date is fine and matches the
    # daily-batch wrapper's convention.
    from datetime import date

    today = date.today()
    log_path = LOGS_DIR / f"gtfs_reload_{today.isoformat()}.log"

    with log_path.open("a") as log_handle:
        log_handle.write(f"\n========== run_gtfs_reload start {today.isoformat()} ==========\n")
        if args.dry_run:
            log_handle.write("(dry-run mode — subprocess will not be invoked)\n")
        log_handle.flush()
        print(f"run_gtfs_reload: log={log_path}")

        exit_code, elapsed = run_reload(log_handle, dry_run=args.dry_run)

        if exit_code == 0:
            log_handle.write(f"========== run_gtfs_reload OK ({elapsed:.1f}s) ==========\n")
            if not args.dry_run:
                clear_failure_marker()
        else:
            log_handle.write(
                f"========== run_gtfs_reload FAILED (exit {exit_code}, {elapsed:.1f}s) ==========\n"
            )
            log_handle.flush()
            if not args.dry_run:
                fire_failure_notification(exit_code, elapsed, log_path)
                write_failure_marker(exit_code, elapsed, log_path)

    print(f"run_gtfs_reload: exit={exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
