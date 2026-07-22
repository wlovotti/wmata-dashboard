"""Path-parameterized pid-file guard (extracted from the WMATA collector)."""

import os

import pytest

from src.pidfile import acquire_pid_file, release_pid_file


def test_acquire_writes_own_pid(tmp_path):
    """Fresh acquire writes this process's pid."""
    pf = tmp_path / "x.pid"
    acquire_pid_file(pf)
    assert pf.read_text().strip() == str(os.getpid())
    release_pid_file(pf)
    assert not pf.exists()


def test_acquire_overwrites_stale_pid(tmp_path):
    """A pid file pointing at a dead process is silently overwritten."""
    pf = tmp_path / "x.pid"
    pf.write_text("999999999")  # certainly dead
    acquire_pid_file(pf)
    assert pf.read_text().strip() == str(os.getpid())
    release_pid_file(pf)


def test_acquire_refuses_live_other_process(tmp_path):
    """A pid file pointing at a live foreign process aborts startup."""
    pf = tmp_path / "x.pid"
    pf.write_text("1")  # pid 1 is always alive and never us
    with pytest.raises(SystemExit):
        acquire_pid_file(pf)


def test_release_leaves_foreign_pid_file(tmp_path):
    """release only deletes a file containing OUR pid."""
    pf = tmp_path / "x.pid"
    pf.write_text("1")
    release_pid_file(pf)
    assert pf.exists()
