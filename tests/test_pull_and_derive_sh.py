"""Argument handling of bin/pull-and-derive.sh, with a fake ``uv`` on PATH (PR #264 review).

The fake ``uv`` records every invocation and exits 0 (or 2 for a
``--check-args`` call containing ``--dryrun``, standing in for argparse
rejecting a typo), so these run the real script without touching GTFS,
S3, the lock directory, or any DB.
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "bin" / "pull-and-derive.sh"
CHECK = "run python -m pipelines.refresh_by_day --check-args"


def _run_script(tmp_path, *args, expect_rc=0) -> list[str]:
    """Run the script with a recording fake ``uv``; return the recorded uv argument lines."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    record = tmp_path / "uv_calls.txt"
    fake = bindir / "uv"
    fake.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> "{record}"\n'
        'case "$*" in *--check-args*--dryrun*) exit 2 ;; esac\n'
        "exit 0\n"
    )
    fake.chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    env.pop("PULL_AND_DERIVE_LOCK_PID", None)
    proc = subprocess.run(
        ["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, check=False
    )
    assert proc.returncode == expect_rc, proc.stderr
    return record.read_text().splitlines() if record.exists() else []


@pytest.mark.smoke
def test_script_parses():
    """bash -n: the script is syntactically valid."""
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


@pytest.mark.smoke
def test_dry_run_skips_reload_and_lock(tmp_path):
    """--dry-run validates, then only runs the planner: no GTFS reload, no lock."""
    calls = _run_script(tmp_path, "--dry-run", "--agency", "wmata")
    assert calls == [
        f"{CHECK} --dry-run --agency wmata",
        "run python -m pipelines.refresh_by_day --dry-run --agency wmata",
    ]


@pytest.mark.smoke
def test_bad_arguments_fail_before_lock_or_reload(tmp_path):
    """A typo exits 2 after the parse-only check, before the lock or any GTFS reload."""
    calls = _run_script(tmp_path, "--dryrun", expect_rc=2)
    assert calls == [f"{CHECK} --dryrun"]


@pytest.mark.smoke
def test_abandon_skips_reload_and_shell_lock(tmp_path):
    """--abandon-date is bookkeeping: no GTFS reload; the driver takes the lock itself."""
    calls = _run_script(tmp_path, "--agency", "wmata", "--abandon-date", "2026-10-07")
    assert calls == [
        f"{CHECK} --agency wmata --abandon-date 2026-10-07",
        "run python -m pipelines.refresh_by_day --agency wmata --abandon-date 2026-10-07",
    ]


@pytest.mark.smoke
@pytest.mark.parametrize(
    ("args", "reloaded"),
    [
        ((), {"wmata", "sfmta"}),
        (("--agency", "wmata", "--date", "2026-09-29"), {"wmata"}),
        (("--agency=sfmta",), {"sfmta"}),
    ],
)
def test_reload_only_selected_agency_and_lock_taken(tmp_path, args, reloaded):
    """A real run validates, takes the lock, reloads only the selected agency, then refreshes."""
    calls = _run_script(tmp_path, *args)
    assert calls[0].startswith(CHECK)
    assert calls[1].startswith("run python -m src.run_lock acquire --pid ")
    got = {c.split("--agency ")[1].split()[0] for c in calls if "run_gtfs_reload.py" in c}
    assert got == reloaded
    assert any(
        c.startswith("run python -m pipelines.refresh_by_day") and "--check-args" not in c
        for c in calls
    )
    assert calls[-1].startswith("run python -m src.run_lock release")


@pytest.mark.smoke
def test_legacy_numeric_argument(tmp_path):
    """A lone number still means --lookback-days N."""
    calls = _run_script(tmp_path, "35")
    assert "run python -m pipelines.refresh_by_day --lookback-days 35" in calls
