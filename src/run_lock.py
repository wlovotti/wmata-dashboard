"""Single-instance lock for freshness runs (``bin/pull-and-derive.sh`` / ``refresh_by_day``).

Two refreshes at once can prune raw files under the other's replay, delete
its in-flight ``trip_update_state`` via cleanup, and clobber the progress
JSON (PR #264 review). Every non-dry-run refresh therefore holds one lock:
the directory ``<main checkout>/logs/.pull-and-derive.lock.d``. ``mkdir``
is atomic on every filesystem and needs no ``flock(1)``, which macOS lacks.

**Shared by every worktree of this repo.** The lock lives under the MAIN
checkout's ``logs/`` (resolved through a worktree's ``.git`` file), so a
run from any ``git worktree`` of this clone excludes a run from any other
— they all target the same ``.env`` databases. A separate *clone* of the
repo is not excluded; don't run refreshes from two clones.

A ``holder.json`` inside (written atomically: temp file + rename) names the
holder (pid, host, start time, command) so a refusal can say who has it.
Stale locks are taken over:

- a holder pid that no longer exists on this host, or
- a lock dir with no readable ``holder.json`` older than ``GRACE_SEC``
  (a crash between ``mkdir`` and writing the holder); younger than that it
  may be mid-creation and is refused.

Takeover is atomic: the taker first creates ``<lock>/takeover`` (``mkdir``
— only one starter can), re-checks that the lock dir is the same
directory (inode) with the same holder record, renames the whole lock dir
aside to a unique name, removes it, and then competes for a fresh
``mkdir`` like any other starter. A starter that loses any of those steps
re-evaluates from the top. A ``takeover`` marker older than ``GRACE_SEC``
(its taker crashed) is expired so it can't block every later start.

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
import time
import uuid
from pathlib import Path

from src.timezones import utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_ENV = "PULL_AND_DERIVE_LOCK_PID"
EXIT_LOCKED = 75  # EX_TEMPFAIL
GRACE_SEC = 60


def main_checkout_root(repo_root: Path) -> Path:
    """Return the main checkout for ``repo_root`` (itself unless it is a git worktree).

    A worktree's ``.git`` is a file ``gitdir: <main>/.git/worktrees/<name>``.
    """
    git = repo_root / ".git"
    if git.is_file():
        text = git.read_text().strip()
        if text.startswith("gitdir:"):
            gitdir = Path(text.split(":", 1)[1].strip())
            if not gitdir.is_absolute():
                gitdir = (repo_root / gitdir).resolve()
            if gitdir.parent.name == "worktrees" and gitdir.parent.parent.name == ".git":
                return gitdir.parent.parent.parent
    return repo_root


LOCK_DIR = main_checkout_root(REPO_ROOT) / "logs" / ".pull-and-derive.lock.d"


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


def _is_stale(lock_dir: Path, holder: dict | None) -> bool:
    """True for a dead holder on this host, or a holder-less dir older than the grace period."""
    if holder is None:
        return False
    if not holder:
        try:
            return time.time() - lock_dir.stat().st_mtime > GRACE_SEC
        except FileNotFoundError:
            return False
    return (
        holder.get("host") == socket.gethostname()
        and isinstance(holder.get("pid"), int)
        and not _pid_alive(holder["pid"])
    )


def _write_holder(lock_dir: Path, record: dict) -> None:
    """Write ``holder.json`` atomically (temp file + rename) so readers never see half a file."""
    tmp = lock_dir / f".holder.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(record))
    os.replace(tmp, lock_dir / "holder.json")


def _dir_identity(path: Path) -> tuple[int, int] | None:
    """``(st_dev, st_ino)`` of ``path`` — distinguishes a recreated lock dir — or None."""
    try:
        st = path.stat()
    except FileNotFoundError:
        return None
    return (st.st_dev, st.st_ino)


def _expire_takeover(takeover: Path) -> None:
    """Remove a ``takeover`` dir older than ``GRACE_SEC`` (left by a taker that crashed).

    Renamed aside first and removed only if the renamed dir is the very
    one judged old (same inode); otherwise it is someone's fresh takeover
    and is put back if its name is still free. (A residual race — a third
    starter recreating ``takeover`` inside that microsecond window — would
    only make one starter retry; the lock-dir inode check guards the
    actual takeover.)
    """
    try:
        st = takeover.stat()
    except FileNotFoundError:
        return
    if time.time() - st.st_mtime <= GRACE_SEC:
        return
    aside = takeover.with_name(f"takeover.expired-{uuid.uuid4().hex}")
    try:
        os.rename(takeover, aside)
    except FileNotFoundError:
        return
    if _dir_identity(aside) == (st.st_dev, st.st_ino):
        shutil.rmtree(aside, ignore_errors=True)
        print("run_lock: removed a stale takeover marker", file=sys.stderr)
        return
    if not takeover.exists():
        os.rename(aside, takeover)
    else:
        shutil.rmtree(aside, ignore_errors=True)


def acquire(pid: int, label: str, lock_dir: Path = LOCK_DIR, attempts: int = 5) -> None:
    """Take the lock for ``pid`` or raise :class:`LockHeldError` naming the holder."""
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(attempts):
        try:
            lock_dir.mkdir()
        except FileExistsError:
            pass
        else:
            _write_holder(
                lock_dir,
                {
                    "pid": pid,
                    "host": socket.gethostname(),
                    "started": utcnow_naive().isoformat(timespec="seconds"),
                    "label": label,
                },
            )
            return
        identity = _dir_identity(lock_dir)
        holder = read_holder(lock_dir)
        if holder is None or identity is None:
            continue  # released between our mkdir and the read — try again
        if not _is_stale(lock_dir, holder):
            raise LockHeldError(f"another refresh holds {lock_dir}: {describe(holder)}")
        takeover = lock_dir / "takeover"
        try:
            takeover.mkdir()
        except FileNotFoundError:
            continue  # already gone
        except FileExistsError:
            _expire_takeover(takeover)  # a crashed taker's leftover, if old enough
            time.sleep(0.05)
            continue
        # Compare the directory itself (inode), not just the holder record:
        # a holder-less stale lock and another starter's fresh, not-yet-
        # written lock both read as {}.
        if _dir_identity(lock_dir) != identity or read_holder(lock_dir) != holder:
            shutil.rmtree(takeover, ignore_errors=True)
            continue
        aside = lock_dir.with_name(f"{lock_dir.name}.stale-{os.getpid()}-{uuid.uuid4().hex}")
        try:
            os.rename(lock_dir, aside)
        except FileNotFoundError:
            continue
        print(f"run_lock: removed stale lock held by {describe(holder)}", file=sys.stderr)
        shutil.rmtree(aside, ignore_errors=True)
    raise LockHeldError(f"could not take {lock_dir} after {attempts} attempts")


def release(pid: int, lock_dir: Path = LOCK_DIR) -> bool:
    """Remove the lock if ``pid`` holds it; return whether it was removed."""
    holder = read_holder(lock_dir)
    if holder is None or holder.get("pid") != pid:
        return False
    aside = lock_dir.with_name(f"{lock_dir.name}.released-{uuid.uuid4().hex}")
    try:
        os.rename(lock_dir, aside)
    except FileNotFoundError:
        return False
    shutil.rmtree(aside, ignore_errors=True)
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
