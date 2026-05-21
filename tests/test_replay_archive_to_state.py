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

    pg_session.execute(TripUpdateState.__table__.delete())
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

    pg_session.execute(TripUpdateState.__table__.delete())
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
    pg_session.execute(TripUpdateState.__table__.delete())
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

    pg_session.execute(TripUpdateState.__table__.delete())
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    pg_session.commit()

    assert pg_session.query(TripUpdateState).filter_by(trip_id="T_LEGACY").one()
