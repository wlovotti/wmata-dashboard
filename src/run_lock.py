"""Single-instance lock for freshness runs (``bin/pull-and-derive.sh`` / ``refresh_by_day``).

Two refreshes at once can prune raw files under the other's replay, delete
its in-flight ``trip_update_state`` via cleanup, and clobber the progress
JSON (PR #264 review). Every non-dry-run refresh therefore holds one lock:
the directory ``logs/.pull-and-derive.lock.d`` (``mkdir`` is atomic on
every filesystem and needs no ``flock(1)``, which macOS lacks — it is the
same directory the nightly wrapper's fallback used). A ``holder.json``
inside names the holder (pid, host, start time, command) so a refusal can
say who has it, and a lock whose holder pid is gone on this host is
treated as stale and taken over.

``bin/pull-and-derive.sh`` acquires it for its whole run (GTFS reload +
refresh) with ``python -m src.run_lock acquire --pid $$`` and exports
``PULL_AND_DERIVE_LOCK_PID=$$``; ``refresh_by_day`` then sees its parent
already holds the lock and does not re-acquire. Run directly,
``refresh_by_day`` acquires and releases it itself.

CLI (exit 0 = acquired/released, 75 = held by someone else):

    python -m src.run_lock acquire --pid 12345 --label "bin/pull-and-derive.sh ..."
    python -m src.run_lock release --pid 12345
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sys
from pathlib import Path

from src.timezones import utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_DIR = REPO_ROOT / "logs" / ".pull-and-derive.lock.d"
LOCK_ENV = "PULL_AND_DERIVE_LOCK_PID"
EXIT_LOCKED = 75  # EX_TEMPFAIL


class LockHeldError(RuntimeError):
    """Another live process holds the refresh lock."""


def _pid_alive(pid: int) -> bool:
    """True when ``pid`` exists on this host (signal 0 probe)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_holder(lock_dir: Path = LOCK_DIR) -> dict | None:
    """Return the holder record, ``{}`` for a lock dir without one, or None if unlocked."""
    if not lock_dir.is_dir():
        return None
    try:
        return json.loads((lock_dir / "holder.json").read_text())
    except (OSError, ValueError):
        return {}


def describe(holder: dict | None) -> str:
    """One-line human description of a holder record."""
    if not holder:
        return "an unknown process (lock dir has no readable holder.json)"
    return (
        f"pid {holder.get('pid')} on {holder.get('host')} since {holder.get('started')}Z "
        f"({holder.get('label', '?')})"
    )


def acquire(pid: int, label: str, lock_dir: Path = LOCK_DIR) -> None:
    """Take the lock for ``pid`` or raise :class:`LockHeldError` naming the holder.

    A lock held by a dead pid on this host is stale and is taken over (one
    retry). A lock dir without a readable holder file is NOT assumed stale
    — it may be mid-creation by another process — and is refused.
    """
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            lock_dir.mkdir()
        except FileExistsError:
            holder = read_holder(lock_dir)
            if (
                holder
                and holder.get("host") == socket.gethostname()
                and isinstance(holder.get("pid"), int)
                and not _pid_alive(holder["pid"])
            ):
                print(f"run_lock: removing stale lock held by {describe(holder)}", file=sys.stderr)
                shutil.rmtree(lock_dir, ignore_errors=True)
                continue
            raise LockHeldError(f"another refresh holds {lock_dir}: {describe(holder)}") from None
        record = {
            "pid": pid,
            "host": socket.gethostname(),
            "started": utcnow_naive().isoformat(timespec="seconds"),
            "label": label,
        }
        (lock_dir / "holder.json").write_text(json.dumps(record))
        return
    raise LockHeldError(f"could not take {lock_dir} after removing a stale lock")


def release(pid: int, lock_dir: Path = LOCK_DIR) -> bool:
    """Remove the lock if ``pid`` holds it; return whether it was removed."""
    holder = read_holder(lock_dir)
    if holder is None or holder.get("pid") != pid:
        return False
    shutil.rmtree(lock_dir, ignore_errors=True)
    return True


def held_by_parent(lock_dir: Path = LOCK_DIR) -> bool:
    """True when ``PULL_AND_DERIVE_LOCK_PID`` names the current lock holder."""
    env_pid = os.environ.get(LOCK_ENV)
    holder = read_holder(lock_dir)
    return bool(env_pid and holder and str(holder.get("pid")) == env_pid)


def main(argv=None) -> int:
    """CLI: ``acquire``/``release`` for shell callers."""
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("action", choices=("acquire", "release"))
    p.add_argument("--pid", type=int, required=True)
    p.add_argument("--label", default="bin/pull-and-derive.sh")
    p.add_argument("--lock-dir", type=Path, default=LOCK_DIR)
    args = p.parse_args(argv)
    if args.action == "release":
        release(args.pid, args.lock_dir)
        return 0
    try:
        acquire(args.pid, args.label, args.lock_dir)
    except LockHeldError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_LOCKED
    return 0


if __name__ == "__main__":
    sys.exit(main())
