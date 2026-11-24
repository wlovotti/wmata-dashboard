"""GTFS snapshot de-duplication (scripts/dedup_gtfs_snapshots.py) against scratch Postgres.

Seeds four snapshots:

- 1 and 2: identical content (2 loaded later) — 1 is a duplicate of 2;
- 3: same feed_version and the SAME row counts in every table as 1/2, but
  its calendar ends a week later (WMATA snapshots 11/12) — distinct
  content, must never be merged;
- 4: the current, different feed.

Uses ``pg_session`` (savepoint-wrapped; the tool's commits release
savepoints, the teardown rolls everything back).
"""

import zipfile
from datetime import datetime

import pytest
from sqlalchemy import text

from src.models import (
    Calendar,
    CalendarDate,
    Corridor,
    GTFSSnapshot,
    Route,
    Stop,
    StopTime,
    Trip,
)

pytestmark = pytest.mark.pg

FEED = "S1"


def _feed(session, sid, *, loaded, end_date="20261231", is_current=False, variant=0):
    """Insert one snapshot's full versioned content."""
    session.add(
        GTFSSnapshot(
            snapshot_id=sid, snapshot_date=loaded, feed_version=FEED if variant == 0 else "S2"
        )
    )
    session.flush()
    common = {"snapshot_id": sid, "is_current": is_current, "valid_from": loaded}
    session.add(Route(route_id="R1", route_short_name="R1", **common))
    session.add(Stop(stop_id="S1", stop_name="One", stop_lat=38.9, stop_lon=-77.0, **common))
    session.add(Stop(stop_id="S2", stop_name="Two", stop_lat=38.91, stop_lon=-77.01, **common))
    for t in range(12):
        trip_id = f"T{t:02d}{'x' if variant else ''}"
        session.add(Trip(trip_id=trip_id, route_id="R1", service_id="WK", direction_id=0, **common))
        for seq, stop in ((1, "S1"), (2, "S2")):
            session.add(
                StopTime(
                    trip_id=trip_id,
                    stop_id=stop,
                    arrival_time=f"{8 + t}:0{seq}:00",
                    departure_time=f"{8 + t}:0{seq}:00",
                    stop_sequence=seq,
                    shape_dist_traveled=None if seq == 1 else 1.5,
                    **common,
                )
            )
    session.add(
        Calendar(
            service_id="WK",
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            friday=1,
            saturday=1,
            sunday=1,
            start_date="20260101",
            end_date=end_date,
            **common,
        )
    )
    session.add(CalendarDate(service_id="WK", date="20260704", exception_type=2, **common))
    session.commit()


@pytest.fixture
def seeded(pg_session):
    """Four snapshots as described in the module docstring, plus a corridor on snapshot 1.

    Wipes the GTFS, corridor and system_metrics_daily tables (inside the
    rolled-back test transaction), so it refuses — unconditionally, even
    with PYTEST_ALLOW_PROD_DB=1 — any database that is a production name or
    does not look like a scratch test DB.
    """
    from tests._prod_db_guard import PRODUCTION_DB_NAMES

    dbname = pg_session.execute(text("SELECT current_database()")).scalar()
    if dbname in PRODUCTION_DB_NAMES or "test" not in dbname:
        pytest.fail(f"refusing to wipe tables in {dbname!r}: scratch test databases only")
    for table in (
        "corridor_slip_rollup",
        "corridor_route_membership",
        "corridors",
        "route_service_profile",
        "stop_times",
        "trips",
        "stops",
        "routes",
        "calendar_dates",
        "calendar",
        "gtfs_snapshots",
        "system_metrics_daily",
    ):
        pg_session.execute(text(f"DELETE FROM {table}"))
    pg_session.commit()
    _feed(pg_session, 1, loaded=datetime(2026, 5, 1))
    _feed(pg_session, 2, loaded=datetime(2026, 5, 8))
    _feed(pg_session, 3, loaded=datetime(2026, 5, 15), end_date="20270107")
    _feed(pg_session, 4, loaded=datetime(2026, 9, 1), is_current=True, variant=1)
    pg_session.add(
        Corridor(
            direction_bearing_deg=0.0,
            direction_cardinal="N",
            start_stop_id="S1",
            end_stop_id="S2",
            length_m=10.0,
            n_routes=1,
            route_set="R1",
            display_name="c",
            geometry_wkt="LINESTRING(0 0, 1 1)",
            gtfs_snapshot_id=1,
        )
    )
    pg_session.execute(
        text(
            "INSERT INTO system_metrics_daily (service_date, data_quality, computed_at) "
            "VALUES ('2026-11-01', 'complete', now())"
        )
    )
    pg_session.commit()
    return pg_session


def _args(*argv):
    """Parse CLI args for the tool."""
    from scripts.dedup_gtfs_snapshots import build_parser

    return build_parser().parse_args(["--agency", "wmata", *argv])


def _ids(session):
    """Surviving snapshot ids."""
    return [
        r[0] for r in session.execute(text("SELECT snapshot_id FROM gtfs_snapshots ORDER BY 1"))
    ]


def test_fingerprint_groups_by_content_not_version_and_counts(seeded):
    """1 and 2 group; 3 (same version + counts, longer calendar) stays distinct."""
    from scripts.dedup_gtfs_snapshots import fingerprint, group_snapshots, load_snapshots

    snaps = load_snapshots(seeded)
    for s in snaps:
        s.fingerprint = fingerprint(seeded, s.snapshot_id, lambda m: None)
    by_id = {s.snapshot_id: s for s in snaps}
    assert [by_id[i].counts for i in (1, 2)] == [by_id[3].counts] * 2  # identical counts
    groups = group_snapshots(snaps, lambda s: s.fingerprint)
    dup_map = {g.survivor.snapshot_id: [d.snapshot_id for d in g.duplicates] for g in groups}
    assert dup_map == {2: [1], 3: [], 4: []}


def test_metadata_only_grouping_would_wrongly_merge_11_and_12(seeded):
    """Version+counts grouping lumps 1, 2, 3 together — why it is never used to delete."""
    from scripts.dedup_gtfs_snapshots import group_snapshots, load_snapshots, metadata_key

    groups = group_snapshots(load_snapshots(seeded), metadata_key)
    assert any(len(g.duplicates) == 2 for g in groups)


def test_prove_identical_is_exact_and_pieced(seeded):
    """Proof passes for 1 vs 2 in several trip_id pieces and fails for 3 vs 2."""
    from scripts.dedup_gtfs_snapshots import load_snapshots, prove_identical

    by_id = {s.snapshot_id: s for s in load_snapshots(seeded)}
    lines = []
    assert prove_identical(seeded, by_id[1], by_id[2], piece_trips=5, log=lines.append)
    assert any("3 piece(s)" in line for line in lines)
    assert not prove_identical(seeded, by_id[3], by_id[2], piece_trips=5, log=lambda m: None)


def test_dry_run_deletes_nothing(seeded):
    """Default run reports the duplicate and deletes nothing."""
    from scripts.dedup_gtfs_snapshots import run

    out = []
    assert run(_args(), seeded, log=out.append) == 0
    assert _ids(seeded) == [1, 2, 3, 4]
    assert any("duplicate 1" in line for line in out)
    assert any("DRY RUN" in line for line in out)


def test_apply_deletes_only_the_proven_duplicate_and_repoints_corridors(seeded):
    """--apply removes snapshot 1 entirely, keeps 2/3/4 and moves the corridor to 2."""
    from scripts.dedup_gtfs_snapshots import run

    assert (
        run(_args("--apply", "--batch-size", "7", "--piece-trips", "5"), seeded, log=lambda m: None)
        == 0
    )
    assert _ids(seeded) == [2, 3, 4]
    for table in ("routes", "stops", "trips", "stop_times", "calendar", "calendar_dates"):
        assert (
            seeded.execute(text(f"SELECT count(*) FROM {table} WHERE snapshot_id = 1")).scalar()
            == 0
        )
    assert seeded.execute(text("SELECT gtfs_snapshot_id FROM corridors")).scalar() == 2
    assert (
        seeded.execute(text("SELECT count(*) FROM stop_times WHERE snapshot_id = 3")).scalar() == 24
    )


def test_apply_refuses_when_proof_fails_despite_equal_fingerprints(seeded, monkeypatch):
    """A fingerprint collision (simulated) cannot delete: the exact proof says no."""
    import scripts.dedup_gtfs_snapshots as mod

    real = mod.fingerprint
    # Snapshot 3 "collides" with 2 (and 1 gets its own fingerprint), so the
    # only candidate pair is 3 vs 2 — whose calendars differ.
    monkeypatch.setattr(
        mod,
        "fingerprint",
        lambda session, sid, log: real(session, 2 if sid == 3 else sid, log)
        if sid != 1
        else tuple((t, n, h0 + 1, h1) for t, n, h0, h1 in real(session, 1, log)),
    )
    assert mod.run(_args("--apply"), seeded, log=lambda m: None) == 1
    assert _ids(seeded) == [1, 2, 3, 4]


def test_resume_finishes_a_tagged_half_deleted_duplicate(seeded):
    """A duplicate interrupted after tagging is finished on the next --apply."""
    from scripts.dedup_gtfs_snapshots import load_snapshots, run

    seeded.execute(text("DELETE FROM calendar WHERE snapshot_id = 1"))
    seeded.execute(text("DELETE FROM calendar_dates WHERE snapshot_id = 1"))
    seeded.execute(
        text("UPDATE gtfs_snapshots SET notes = 'x; dedup_into=2' WHERE snapshot_id = 1")
    )
    seeded.commit()
    assert {s.snapshot_id: s.dedup_into for s in load_snapshots(seeded)}[1] == 2
    assert run(_args("--apply"), seeded, log=lambda m: None) == 0
    assert _ids(seeded) == [2, 3, 4]


def test_export_writes_csvs_and_manifest(seeded, tmp_path):
    """export writes one zip per distinct un-archived content with per-file sha256."""
    import json

    from scripts.dedup_gtfs_snapshots import run

    args = _args("export", "--out-dir", str(tmp_path), "--snapshot", "3")
    assert run(args, seeded, log=lambda m: None) == 0
    (path,) = tmp_path.glob("*.zip")
    with zipfile.ZipFile(path) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        assert manifest["snapshot_id"] == 3
        assert manifest["files"]["stop_times.txt"]["rows"] == 24
        cal = zf.read("calendar.txt").decode()
        assert "20270107" in cal and cal.startswith("service_id,")


def test_export_default_writes_one_zip_per_distinct_unarchived_content(seeded, tmp_path):
    """Without --snapshot: fingerprint survivors only (2, 3, 4) — never duplicate 1."""
    from scripts.dedup_gtfs_snapshots import run

    assert run(_args("export", "--out-dir", str(tmp_path)), seeded, log=lambda m: None) == 0
    names = sorted(p.name for p in tmp_path.glob("*.zip"))
    assert [n.split("_")[1] for n in names] == ["snapshot2", "snapshot3", "snapshot4"]


def test_group_refused_when_dedup_would_change_a_fallback_resolution(seeded):
    """Load order matters to the fallback rules: refuse rather than shift a date's schedule.

    10 and 12 are identical, 11 (distinct) was loaded between them, and no
    calendar covers 2026-05-02. Today 5/2 falls back to "newest loaded by
    then" = 10; without 10 it would fall back to the oldest left = 11 —
    different content, so the group is refused and nothing is deleted.
    """
    from scripts.dedup_gtfs_snapshots import run

    for table in ("stop_times", "trips", "stops", "routes", "calendar_dates", "calendar"):
        seeded.execute(text(f"DELETE FROM {table}"))
    seeded.execute(text("DELETE FROM corridors"))
    seeded.execute(text("DELETE FROM gtfs_snapshots"))
    seeded.commit()
    _feed(seeded, 10, loaded=datetime(2026, 5, 1, 12), end_date="20271231")
    _feed(seeded, 11, loaded=datetime(2026, 5, 4), end_date="20271231", variant=1)
    _feed(seeded, 12, loaded=datetime(2026, 5, 8), end_date="20271231", is_current=True)
    seeded.execute(text("UPDATE calendar SET start_date = '20261201'"))
    seeded.execute(text("UPDATE system_metrics_daily SET service_date = '2026-05-02'"))
    seeded.commit()
    out = []
    assert run(_args("--apply"), seeded, log=out.append) == 0
    assert any("REFUSED: 2026-05-02 resolves to snapshot 10" in line for line in out)
    assert _ids(seeded) == [10, 11, 12]


def test_content_columns_match_schema_and_last_scheduled_date(seeded):
    """Review N10/N1: CONTENT_COLUMNS covers every content column; resolution checks
    run through the last scheduled date, not just today."""
    from datetime import date

    from scripts.dedup_gtfs_snapshots import content_columns_problem, last_scheduled_date

    assert content_columns_problem(seeded) is None
    assert last_scheduled_date(seeded) == date(2027, 1, 7)
