"""Tests for src/run_lock.py — the single-instance refresh lock (PR #264 review)."""

import json
import os
import socket
import subprocess
import sys

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
def test_lock_dir_without_holder_is_not_assumed_stale(tmp_path):
    """No readable holder.json (maybe mid-creation) → refuse rather than steal."""
    lock = tmp_path / "lock.d"
    lock.mkdir()
    with pytest.raises(run_lock.LockHeldError, match="unknown process"):
        run_lock.acquire(os.getpid(), "run", lock)


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
