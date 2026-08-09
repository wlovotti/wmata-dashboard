"""Tests for ``dedupe_trip_update_rows``, the shared same-poll dedup helper.

NOTES-96: extracted from ``WMATADataCollector._save_trip_updates`` so both
the live collector and ``pipelines/replay_archive_to_state.py`` enforce the
same ``(trip_id, stop_sequence, service_date)`` upsert-key uniqueness
against ``trip_update_state`` -- Postgres raises CardinalityViolation if
that conflict target repeats within one INSERT's VALUES list, which
SFMTA/511.org triggers in ~0.24% of rows (a single trip reporting one
stop_sequence twice for two different physical stops within one poll).
"""

from datetime import date, datetime

import pytest

from src.wmata_collector import dedupe_trip_update_rows


def _raw_row(
    stop_id: str,
    vehicle_id: str | None,
    predicted_arrival_ts: datetime | None,
    snapshot_ts: datetime = datetime(2026, 5, 17, 14, 0, 0),
    trip_id: str = "T1",
    stop_sequence: int | None = 44,
    trip_start_date: str | None = "20260517",
) -> dict:
    """Build a raw trip-update row dict as ``get_realtime_trip_updates`` produces it."""
    return {
        "trip_id": trip_id,
        "stop_id": stop_id,
        "stop_sequence": stop_sequence,
        "vehicle_id": vehicle_id,
        "snapshot_ts": snapshot_ts,
        "predicted_arrival_ts": predicted_arrival_ts,
        "schedule_relationship": "UNSET",
        "trip_start_date": trip_start_date,
    }


@pytest.mark.smoke
def test_dedupe_keeps_last_row_for_colliding_key():
    """Two rows sharing (trip_id, stop_sequence, service_date) collapse to one; last wins outright."""
    rows = [
        _raw_row("S_FIRST", "V_A", datetime(2026, 5, 17, 14, 5, 0)),
        _raw_row("S_SECOND", "V_B", datetime(2026, 5, 17, 14, 6, 0)),
    ]
    out = dedupe_trip_update_rows(rows)
    assert len(out) == 1
    assert out[0]["stop_id"] == "S_SECOND"
    assert out[0]["vehicle_id"] == "V_B"
    assert out[0]["service_date"] == date(2026, 5, 17)


@pytest.mark.smoke
def test_dedupe_last_row_wins_even_with_null_field():
    """Last-row-wins is a full overwrite, not a per-field non-null merge.

    Distinguishes this helper from the richer cross-poll fold in
    ``replay_archive_to_state.py`` (which preserves the last *non-null*
    vehicle_id/prediction across snapshots) -- within one poll, the last
    row simply wins, including a null field clobbering an earlier
    non-null value. Matches ``_save_trip_updates``'s documented
    "keep last row in feed order" semantics.
    """
    rows = [
        _raw_row("S_FIRST", "V_A", datetime(2026, 5, 17, 14, 5, 0)),
        _raw_row("S_SECOND", None, datetime(2026, 5, 17, 14, 6, 0)),
    ]
    out = dedupe_trip_update_rows(rows)
    assert len(out) == 1
    assert out[0]["vehicle_id"] is None


@pytest.mark.smoke
def test_dedupe_drops_rows_missing_stop_sequence():
    """A row with no stop_sequence can't be keyed into trip_update_state's PK -- dropped."""
    rows = [_raw_row("S1", "V1", None, stop_sequence=None)]
    assert dedupe_trip_update_rows(rows) == []


@pytest.mark.smoke
def test_dedupe_uses_agency_timezone_for_service_date():
    """``tz_name`` threads into the per-row service_date fallback (NOTES-96)."""
    rows = [
        _raw_row(
            "S1",
            "V1",
            None,
            snapshot_ts=datetime(2026, 5, 19, 5, 30, 0),  # naive UTC
            trip_start_date=None,
        )
    ]
    out = dedupe_trip_update_rows(rows, tz_name="America/Los_Angeles")
    assert out[0]["service_date"] == date(2026, 5, 18)  # still 5/18 on the US west coast


@pytest.mark.smoke
def test_dedupe_distinct_keys_all_survive():
    """Rows with distinct (trip_id, stop_sequence) keys are untouched."""
    rows = [
        _raw_row("S1", "V1", None, stop_sequence=1),
        _raw_row("S2", "V1", None, stop_sequence=2),
    ]
    out = dedupe_trip_update_rows(rows)
    assert len(out) == 2
