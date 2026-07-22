"""Pid-file guard shared by all collector processes.

Extracted from scripts/continuous_combined_collector.py (which hardcoded
logs/collector.pid) so sidecar collectors (SFMTA) can guard their own
pid path with identical semantics: stale/malformed files are silently
overwritten; a live foreign pid aborts startup with SystemExit(1).
"""

import atexit
import os
from functools import partial
from pathlib import Path


def _is_pid_alive(pid: int) -> bool:
    """Return True if *pid* refers to a live process on this machine.

    Uses ``os.kill(pid, 0)`` which sends no signal but raises if the
    process does not exist or if we lack permission to signal it.

    - ``ProcessLookupError`` (ESRCH) — process does not exist; return False.
    - ``PermissionError`` (EPERM) — process exists but is owned by another
      user; treat as live (return True) to avoid clobbering.
    - Any other ``OSError`` — treated conservatively as live.
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True


def release_pid_file(pid_file: Path) -> None:
    """Remove *pid_file* if it still contains this process's pid (idempotent)."""
    if pid_file.exists():
        try:
            if pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink(missing_ok=True)
        except OSError:
            pass


def acquire_pid_file(pid_file: Path) -> None:
    """Write the current pid to *pid_file*, refusing if a live collector holds it.

    Raises ``SystemExit(1)`` if the file points to a live process that is
    not this process. Stale or malformed files are silently overwritten.
    Registers ``release_pid_file`` via ``atexit`` for cleanup on any
    normal exit.
    """
    my_pid = os.getpid()
    if pid_file.exists():
        raw = pid_file.read_text().strip()
        try:
            existing_pid = int(raw)
        except ValueError:
            existing_pid = None

        if existing_pid is not None and existing_pid != my_pid and _is_pid_alive(existing_pid):
            print(
                f"ERROR: collector already running as pid {existing_pid}. "
                f"Refusing to start a second instance. "
                f"If you are sure no collector is running, remove {pid_file} and retry."
            )
            raise SystemExit(1)

    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(my_pid))
    atexit.register(partial(release_pid_file, pid_file))
