"""Tests for ``pipelines.replay_archive_to_state``.

The replay tool reads ``archive/raw_snapshots/<date>.*.jsonl.zst`` files
and replays each line into ``trip_update_state`` via the same UPSERT
helper the live collector uses. Two contracts under test:

1. **Idempotency** — re-running the replay for the same date produces
   the same end state. Required so partial failures can be retried
   without producing inconsistent data.
2. **Cross-date isolation** — replaying day D never modifies rows for
   any other service_date. Required so backfilling 5/18 cannot
   silently corrupt the 5/19 rows the running collector wrote.

Uses ``pg_session`` (real Postgres) because the replay path exercises
``upsert_trip_update_state`` which uses ``pg_insert`` with conditional
``ON CONFLICT`` clauses (CASE expressions not supported by SQLite's
upsert).
"""

import json
from datetime import date, datetime
from pathlib import Path

import pytest
import zstandard as zstd

from src.models import TripUpdateState


def _write_jsonl_zst(path: Path, rows: list[dict]) -> Path:
    """Helper: write a list of dicts as a single-frame zstd-compressed JSONL file.

    Matches the on-disk format of the live ``JsonlArchiveWriter`` — one
    JSON object per line, datetime fields serialised via ``str()`` (so
    "2026-05-18 18:00:00", not ISO ``T``).
    """
    line_bytes = b""
    for r in rows:
        line_bytes += (json.dumps(r, default=str) + "\n").encode("utf-8")
    cctx = zstd.ZstdCompressor(level=3)
    path.write_bytes(cctx.compress(line_bytes))
    return path


def _row(
    snapshot_ts: str,
    trip_id: str,
    stop_sequence: int,
    predicted_arrival_ts: str | None = None,
    vehicle_id: str = "V1",
    schedule_relationship: str = "SCHEDULED",
    trip_start_date: str | None = "20260518",
) -> dict:
    """Build a JSONL-archive-shaped row dict for tests."""
    return {
        "snapshot_ts": snapshot_ts,
        "trip_id": trip_id,
        "route_id": "TEST_ROUTE",
        "vehicle_id": vehicle_id,
        "stop_id": "S1",
        "stop_sequence": stop_sequence,
        "predicted_arrival_ts": predicted_arrival_ts,
        "predicted_departure_ts": None,
        "schedule_relationship": schedule_relationship,
        "trip_start_date": trip_start_date,
    }


@pytest.mark.integration
def test_replay_writes_final_state(tmp_path, pg_session):
    """Two snapshots for the same (trip, stop) collapse to one state row.

    Final values follow the most recent snapshot. ``last_predicted_*``
    overwrites only when the new predicted_arrival_ts is non-null
    (matching the collector's UPSERT semantics in upsert_trip_update_state).
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [
            _row("2026-05-18 22:00:00", "T_REPLAY", 1, "2026-05-18 22:05:00"),
            _row("2026-05-18 22:01:00", "T_REPLAY", 1, "2026-05-18 22:06:00"),
        ],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_REPLAY")
    )
    pg_session.commit()

    count = replay_archive_for_date(
        pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir
    )
    pg_session.commit()
    assert count == 2  # two snapshot lines processed

    state = pg_session.query(TripUpdateState).filter(TripUpdateState.trip_id == "T_REPLAY").all()
    assert len(state) == 1
    assert state[0].service_date == date(2026, 5, 18)
    assert state[0].last_predicted_arrival_ts == datetime(2026, 5, 18, 22, 6)
    assert state[0].final_snapshot_ts == datetime(2026, 5, 18, 22, 1)


@pytest.mark.integration
def test_replay_is_idempotent(tmp_path, pg_session):
    """Replaying the same date twice produces the same end state.

    Required for restart-after-crash semantics: a partial replay that
    crashed halfway through must be safely re-runnable.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [_row("2026-05-18 22:00:00", "T_IDEM", 1, "2026-05-18 22:05:00")],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_IDEM")
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()
    state1 = pg_session.query(TripUpdateState).filter_by(trip_id="T_IDEM").one()
    snap1 = state1.final_snapshot_ts
    pred1 = state1.last_predicted_arrival_ts

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()
    state2 = pg_session.query(TripUpdateState).filter_by(trip_id="T_IDEM").one()

    assert state2.final_snapshot_ts == snap1
    assert state2.last_predicted_arrival_ts == pred1


@pytest.mark.integration
def test_replay_does_not_touch_other_dates(tmp_path, pg_session):
    """Replaying 2026-05-18 must not modify rows for 2026-05-19.

    The cross-date isolation guarantee that makes per-day backfill safe
    to run while the collector keeps writing today's data.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [_row("2026-05-18 22:00:00", "T_SAME", 1, "2026-05-18 22:05:00", vehicle_id="V_18")],
    )

    # Pre-seed a 5/19 row for the same (trip_id, stop_sequence). The
    # replay must leave it untouched.
    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_SAME")
    )
    pg_session.add(
        TripUpdateState(
            trip_id="T_SAME",
            stop_sequence=1,
            service_date=date(2026, 5, 19),
            stop_id="S1",
            vehicle_id="V_19",
            final_snapshot_ts=datetime(2026, 5, 19, 18, 0),
        )
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    on_19 = (
        pg_session.query(TripUpdateState)
        .filter_by(trip_id="T_SAME", service_date=date(2026, 5, 19))
        .one()
    )
    assert on_19.vehicle_id == "V_19"  # untouched

    on_18 = (
        pg_session.query(TripUpdateState)
        .filter_by(trip_id="T_SAME", service_date=date(2026, 5, 18))
        .one()
    )
    assert on_18.vehicle_id == "V_18"  # newly created


@pytest.mark.integration
def test_replay_preserves_last_prediction_when_feed_nullifies(tmp_path, pg_session):
    """A trailing null-prediction snapshot must not erase the last estimate.

    WMATA nullifies predictions right at arrival. The fold must keep the
    last non-null predicted_arrival_ts (and its snapshot_ts) while still
    advancing final_snapshot_ts / final_schedule_relationship to the very
    last snapshot — the same conditional semantics the live collector's
    ON CONFLICT CASE expressions implement per-poll.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [
            _row("2026-05-18 22:00:00", "T_NULLPRED", 1, "2026-05-18 22:05:00"),
            _row("2026-05-18 22:04:30", "T_NULLPRED", 1, None),  # arrival nullification
        ],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_NULLPRED")
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_NULLPRED").one()
    assert state.final_snapshot_ts == datetime(2026, 5, 18, 22, 4, 30)
    assert state.last_predicted_arrival_ts == datetime(2026, 5, 18, 22, 5)
    assert state.last_pred_snapshot_ts == datetime(2026, 5, 18, 22, 0)


@pytest.mark.integration
def test_replay_keeps_last_non_null_vehicle_id(tmp_path, pg_session):
    """vehicle_id follows last-non-null-wins across the snapshot sequence."""
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [
            _row("2026-05-18 22:00:00", "T_VEH", 1, "2026-05-18 22:05:00", vehicle_id="V_A"),
            _row("2026-05-18 22:01:00", "T_VEH", 1, "2026-05-18 22:06:00", vehicle_id=None),
        ],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_VEH"))
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_VEH").one()
    assert state.vehicle_id == "V_A"  # null did not clobber
    assert state.final_snapshot_ts == datetime(2026, 5, 18, 22, 1)


@pytest.mark.integration
def test_replay_orders_by_snapshot_ts_across_files(tmp_path, pg_session):
    """Overlapping per-process files fold by snapshot_ts, not filename order.

    A collector restart produces two per-process files whose name sort
    order does not match chronology. The fold must let the latest
    snapshot_ts win regardless of which file it came from.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    # Later-named file carries the EARLIER snapshot.
    _write_jsonl_zst(
        archive_dir / "2026-05-18.111.9.jsonl.zst",
        [_row("2026-05-18 22:00:00", "T_ORDER", 1, "2026-05-18 22:05:00")],
    )
    _write_jsonl_zst(
        archive_dir / "2026-05-18.222.1.jsonl.zst",
        [_row("2026-05-18 23:30:00", "T_ORDER", 1, "2026-05-18 23:35:00")],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_ORDER")
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_ORDER").one()
    assert state.final_snapshot_ts == datetime(2026, 5, 18, 23, 30)
    assert state.last_predicted_arrival_ts == datetime(2026, 5, 18, 23, 35)


@pytest.mark.integration
def test_replay_conflict_with_existing_row_preserves_prediction(tmp_path, pg_session):
    """Folding respects ON CONFLICT semantics against pre-existing DB rows.

    If the DB already holds a row with a non-null prediction and the
    replayed sequence for that key contains only null predictions, the
    existing prediction must survive (CASE keeps table value when
    excluded.last_predicted_arrival_ts is null).
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [_row("2026-05-18 23:00:00", "T_EXIST", 1, None)],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_EXIST")
    )
    pg_session.add(
        TripUpdateState(
            trip_id="T_EXIST",
            stop_sequence=1,
            service_date=date(2026, 5, 18),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 18, 22, 0),
            last_pred_snapshot_ts=datetime(2026, 5, 18, 22, 0),
            last_predicted_arrival_ts=datetime(2026, 5, 18, 22, 5),
        )
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_EXIST").one()
    assert state.final_snapshot_ts == datetime(2026, 5, 18, 23, 0)  # advanced
    assert state.last_predicted_arrival_ts == datetime(2026, 5, 18, 22, 5)  # preserved


@pytest.mark.smoke
def test_replay_raises_on_zero_files(tmp_path, db_session):
    """Zero matching archive files must raise, not silently return 0.

    See ``NoArchiveFilesFoundError`` (NOTES-93). No DB write happens on
    this path, so the in-memory SQLite ``db_session`` fixture is enough
    — this never touches Postgres.
    """
    from pipelines.replay_archive_to_state import NoArchiveFilesFoundError, replay_archive_for_date

    empty_dir = tmp_path / "raw_snapshots"
    empty_dir.mkdir()

    with pytest.raises(NoArchiveFilesFoundError):
        replay_archive_for_date(db_session, target_date=date(2026, 5, 18), archive_root=empty_dir)


@pytest.mark.smoke
def test_replay_allow_empty_returns_zero_without_raising(tmp_path, db_session):
    """``allow_empty=True`` is the documented opt-out for a genuinely-empty date."""
    from pipelines.replay_archive_to_state import replay_archive_for_date

    empty_dir = tmp_path / "raw_snapshots"
    empty_dir.mkdir()

    count = replay_archive_for_date(
        db_session, target_date=date(2026, 5, 18), archive_root=empty_dir, allow_empty=True
    )
    assert count == 0


@pytest.mark.integration
def test_replay_uses_agency_timezone_for_service_date(tmp_path, pg_session):
    """``--agency`` threads the agency's IANA tz into service-date inference.

    A 2026-05-19 05:30 UTC snapshot with no ``trip_start_date`` falls back
    to the snapshot's local calendar day: 2026-05-19 Eastern (WMATA's
    default) but 2026-05-18 Pacific (SFMTA, ``config/agencies/sfmta.yaml``).
    ``agency="sfmta"`` must resolve the row against 2026-05-18. NOTES-96.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "sfmta_raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.0.jsonl.zst",
        [_row("2026-05-19 05:30:00", "T_SFMTA_TZ", 1, trip_start_date=None)],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_SFMTA_TZ")
    )
    pg_session.commit()

    count = replay_archive_for_date(
        pg_session,
        target_date=date(2026, 5, 18),
        archive_root=archive_dir,
        agency="sfmta",
    )
    pg_session.commit()
    assert count == 1

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_SFMTA_TZ").one()
    assert state.service_date == date(2026, 5, 18)


@pytest.mark.integration
def test_replay_default_agency_stays_eastern(tmp_path, pg_session):
    """Omitting ``--agency`` keeps today's WMATA/Eastern behavior (regression guard)."""
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-19.0.jsonl.zst",
        [_row("2026-05-19 05:30:00", "T_WMATA_TZ", 1, trip_start_date=None)],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_WMATA_TZ")
    )
    pg_session.commit()

    count = replay_archive_for_date(
        pg_session, target_date=date(2026, 5, 19), archive_root=archive_dir
    )
    pg_session.commit()
    assert count == 1

    state = pg_session.query(TripUpdateState).filter_by(trip_id="T_WMATA_TZ").one()
    assert state.service_date == date(2026, 5, 19)


@pytest.mark.integration
def test_replay_dedupes_duplicate_stop_sequence_within_one_poll(tmp_path, pg_session):
    """Two archive rows sharing (trip_id, stop_sequence) at one snapshot_ts must
    not crash the fold/upsert, and must resolve via the same shared
    ``dedupe_trip_update_rows`` semantics the live collector uses (last row
    in feed order wins outright) -- not the fold's own per-field
    non-null-preserving tie-break. Mirrors
    ``test_save_trip_updates_dedupes_duplicate_key_within_one_poll`` in
    tests/test_collector_dual_write.py. NOTES-96 (SFMTA/511.org repeats
    stop_sequence within a trip in ~0.24% of rows).
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    row_a = _row("2026-05-18 22:00:00", "T_DUPSEQ", 44, "2026-05-18 22:05:00", vehicle_id="V_A")
    row_b = _row("2026-05-18 22:00:00", "T_DUPSEQ", 44, "2026-05-18 22:06:00", vehicle_id=None)
    row_b["stop_id"] = "S_SECOND"
    _write_jsonl_zst(archive_dir / "2026-05-18.0.jsonl.zst", [row_a, row_b])

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_DUPSEQ")
    )
    pg_session.commit()

    replay_archive_for_date(  # must not raise CardinalityViolation
        pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir
    )
    pg_session.commit()

    states = pg_session.query(TripUpdateState).filter_by(trip_id="T_DUPSEQ", stop_sequence=44).all()
    assert len(states) == 1
    # Same-poll collision: last row in feed order wins outright, including
    # a null vehicle_id overwriting the earlier row's non-null value.
    assert states[0].stop_id == "S_SECOND"
    assert states[0].vehicle_id is None


@pytest.mark.integration
def test_replay_finds_legacy_single_daily_filename(tmp_path, pg_session):
    """Older archive files use ``YYYY-MM-DD.jsonl.zst`` (no pid suffix).

    The early collector runs (pre-PR #132) used a single daily file
    rather than per-process. The replay tool must handle both layouts so
    backfill works across the transition.
    """
    from pipelines.replay_archive_to_state import replay_archive_for_date

    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir / "2026-05-18.jsonl.zst",  # legacy single-file pattern
        [_row("2026-05-18 22:00:00", "T_LEGACY", 1, "2026-05-18 22:05:00")],
    )

    # Scoped reset (not whole-table): avoids deadlocking the live collector. Matches PR #144.
    pg_session.execute(
        TripUpdateState.__table__.delete().where(TripUpdateState.trip_id == "T_LEGACY")
    )
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    assert pg_session.query(TripUpdateState).filter_by(trip_id="T_LEGACY").one()
