"""Per-agency GTFS reload gate coverage (NOTES-134).

Before this, `run_gtfs_reload.py` only ever reloaded WMATA's static
GTFS, gated on the WMATA `gtfs_snapshots` table — SFMTA's static GTFS
had no staleness gate at all. These tests cover the pure, DB-free
plumbing that makes the gate agency-aware: building the
`reload_gtfs_complete.py` subprocess command and the per-agency log /
failure-marker filenames (mirroring `run_daily_batch.py`'s
`log_suffix` convention so a concurrent SFMTA run doesn't collide with
WMATA's).
"""

import sys
from datetime import date
from pathlib import Path

from scripts.run_gtfs_reload import RELOAD_SCRIPT, agency_paths, build_reload_cmd


def test_build_reload_cmd_default_agency():
    """Default agency omits nothing — --agency wmata is passed explicitly."""
    cmd = build_reload_cmd("wmata")
    assert cmd == [sys.executable, str(RELOAD_SCRIPT), "--agency", "wmata"]


def test_build_reload_cmd_sfmta():
    """A non-default agency is forwarded the same way."""
    cmd = build_reload_cmd("sfmta")
    assert cmd == [sys.executable, str(RELOAD_SCRIPT), "--agency", "sfmta"]


def test_agency_paths_default_agency_unsuffixed():
    """WMATA (the default) keeps the pre-existing unsuffixed filenames."""
    log_path, marker_path = agency_paths("wmata", date(2026, 8, 31))
    assert log_path.name == "gtfs_reload_2026-08-31.log"
    assert marker_path.name == "gtfs_reload_LAST_FAILURE.json"


def test_agency_paths_sfmta_suffixed():
    """A non-default agency gets its own log + failure marker, never WMATA's."""
    log_path, marker_path = agency_paths("sfmta", date(2026, 8, 31))
    assert log_path.name == "gtfs_reload_sfmta_2026-08-31.log"
    assert marker_path.name == "gtfs_reload_LAST_FAILURE_sfmta.json"


def test_agency_paths_are_distinct_across_agencies():
    """WMATA and SFMTA runs on the same day never write the same files."""
    wmata_log, wmata_marker = agency_paths("wmata", date(2026, 8, 31))
    sfmta_log, sfmta_marker = agency_paths("sfmta", date(2026, 8, 31))
    assert wmata_log != sfmta_log
    assert wmata_marker != sfmta_marker
    assert isinstance(wmata_log, Path)
