"""Tests for src/run_lock.py — the single-instance refresh lock (PR #264 review)."""

import json
import os
import socket
import subprocess
import sys
import time

import pytest

from src import run_lock


@pytest.mark.smoke
def test_acquire_refuses_a_live_holder_and_names_it(tmp_path):
    """A second acquire fails while the first holder's pid is alive."""
    lock = tmp_path / "lock.d"
    run_lock.acquire(os.getpid(), "first run", lock)
    with pytest.raises(run_lock.LockHeldError, match="first run"):
        run_lock.acquire(os.getpid() + 1, "second run", lock)
    assert run_lock.release(os.getpid(), lock)
    assert not lock.exists()


@pytest.mark.smoke
def test_stale_lock_from_dead_pid_is_taken_over(tmp_path):
    """A lock whose holder pid no longer exists on this host is replaced."""
    lock = tmp_path / "lock.d"
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True
    )
    dead_pid = int(proc.stdout)
    lock.mkdir()
    (lock / "holder.json").write_text(
        json.dumps({"pid": dead_pid, "host": socket.gethostname(), "label": "crashed"})
    )
    run_lock.acquire(os.getpid(), "new run", lock)
    assert run_lock.read_holder(lock)["label"] == "new run"


@pytest.mark.smoke
def test_lock_dir_without_holder_is_stale_only_after_grace(tmp_path):
    """No holder.json: refused while young (maybe mid-creation), taken over once old."""
    lock = tmp_path / "lock.d"
    lock.mkdir()
    with pytest.raises(run_lock.LockHeldError, match="unknown process"):
        run_lock.acquire(os.getpid(), "run", lock)
    old = time.time() - run_lock.GRACE_SEC - 5
    os.utime(lock, (old, old))
    run_lock.acquire(os.getpid(), "run", lock)
    assert run_lock.read_holder(lock)["label"] == "run"


@pytest.mark.smoke
def test_takeover_in_progress_by_another_starter_is_not_duplicated(tmp_path):
    """A stale lock someone else is already taking over (takeover dir present) is not stolen."""
    lock = tmp_path / "lock.d"
    lock.mkdir()
    (lock / "holder.json").write_text(
        json.dumps({"pid": _dead_pid(), "host": socket.gethostname(), "label": "crashed"})
    )
    (lock / "takeover").mkdir()
    with pytest.raises(run_lock.LockHeldError, match="could not take"):
        run_lock.acquire(os.getpid(), "run", lock, attempts=3)
    assert run_lock.read_holder(lock)["label"] == "crashed"


@pytest.mark.smoke
def test_takeover_aborts_if_holder_changed_meanwhile(tmp_path, monkeypatch):
    """If the holder changes between the staleness check and the takeover, don't steal it."""
    lock = tmp_path / "lock.d"
    lock.mkdir()
    stale = {"pid": _dead_pid(), "host": socket.gethostname(), "label": "crashed"}
    (lock / "holder.json").write_text(json.dumps(stale))
    fresh = {"pid": os.getpid(), "host": socket.gethostname(), "label": "winner"}
    reads = iter([stale, fresh, fresh, fresh, fresh, fresh, fresh, fresh])
    monkeypatch.setattr(run_lock, "read_holder", lambda d=None: next(reads))
    with pytest.raises(run_lock.LockHeldError, match="winner"):
        run_lock.acquire(os.getpid() + 1, "loser", lock)
    assert lock.is_dir() and not (lock / "takeover").exists()


@pytest.mark.smoke
def test_lock_is_shared_by_worktrees(tmp_path):
    """A worktree's lock resolves to the main checkout (via its .git file)."""
    main = tmp_path / "main"
    (main / ".git" / "worktrees" / "wt1").mkdir(parents=True)
    wt = tmp_path / "wt1"
    wt.mkdir()
    (wt / ".git").write_text(f"gitdir: {main / '.git' / 'worktrees' / 'wt1'}\n")
    assert run_lock.main_checkout_root(wt) == main
    assert run_lock.main_checkout_root(main) == main


def _dead_pid() -> int:
    """A pid that has already exited."""
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True
    )
    return int(proc.stdout)


@pytest.mark.smoke
def test_release_only_by_holder_and_parent_detection(tmp_path, monkeypatch):
    """release() ignores non-holders; held_by_parent() matches the env pid to the holder."""
    lock = tmp_path / "lock.d"
    run_lock.acquire(4242, "shell", lock)
    assert not run_lock.release(os.getpid(), lock)
    monkeypatch.setenv(run_lock.LOCK_ENV, "4242")
    assert run_lock.held_by_parent(lock)
    monkeypatch.setenv(run_lock.LOCK_ENV, "1")
    assert not run_lock.held_by_parent(lock)


@pytest.mark.smoke
def test_cli_exit_codes(tmp_path):
    """CLI: 0 when acquired, 75 when held by a live process."""
    lock = str(tmp_path / "lock.d")
    assert run_lock.main(["acquire", "--pid", str(os.getpid()), "--lock-dir", lock]) == 0
    assert run_lock.main(["acquire", "--pid", "1", "--lock-dir", lock]) == run_lock.EXIT_LOCKED
    assert run_lock.main(["release", "--pid", str(os.getpid()), "--lock-dir", lock]) == 0
