"""Tests for the ``vp_archive_loaded_files`` manifest model (NOTES-95).

Started here (Task 6: model + migration); extended in Task 7 with the
loader function that consumes this manifest for idempotency.
"""

from datetime import datetime

from src.models import VpArchiveLoadedFile


def test_manifest_model_round_trip(db_session):
    """VpArchiveLoadedFile persists filename/counts/loaded_at.

    ``filename`` is the primary key the loader uses to look up whether a
    file has already been loaded; omitting ``dropped_count`` must default
    to 0 rather than requiring every caller to pass it explicitly.
    """
    db_session.add(
        VpArchiveLoadedFile(filename="2026-08-22.1.100.jsonl.zst", row_count=10, dropped_count=1)
    )
    db_session.commit()
    row = db_session.query(VpArchiveLoadedFile).one()
    assert row.filename == "2026-08-22.1.100.jsonl.zst"
    assert row.row_count == 10
    assert row.loaded_at is not None


def test_manifest_model_dropped_count_defaults_to_zero(db_session):
    """Omitting ``dropped_count`` at construction defaults it to 0."""
    db_session.add(VpArchiveLoadedFile(filename="2026-08-23.1.200.jsonl.zst", row_count=5))
    db_session.commit()
    row = db_session.query(VpArchiveLoadedFile).one()
    assert row.dropped_count == 0


def _write_vp_file(dirpath, vehicles, collected_at):
    """Produce a VP archive file exactly as the stateless collector would."""
    from src.archive_writer import JsonlArchiveWriter
    from src.stateless_poller import archive_vp_rows

    w = JsonlArchiveWriter(dirpath)
    archive_vp_rows(w, vehicles, collected_at=collected_at)
    return w.close()


VEHICLE = {
    "vehicle_id": "42",
    "route_id": "D72",
    "trip_id": "t1",
    "direction_id": 0,
    "trip_start_date": "20260822",
    "latitude": 38.9,
    "longitude": -77.0,
    "speed": 5.5,
    "current_stop_sequence": 3,
    "stop_id": "1001",
    "current_status": 2,
    "timestamp": 1787400015,  # ~5 s after COLLECTED below (verified via from_epoch_naive_utc)
}
COLLECTED = datetime(2026, 8, 22, 12, 0, 10)  # ~5 s before the fix timestamp


def test_load_vp_file_inserts_rows_and_manifest(db_session, tmp_path):
    """A single well-formed vehicle row loads into vehicle_positions + the manifest."""
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (1, 0)
    vp = db_session.query(VehiclePosition).one()
    assert vp.vehicle_id == "42" and vp.route_id == "D72"
    from src.timezones import from_epoch_naive_utc

    assert vp.timestamp == from_epoch_naive_utc(1787400015)
    assert db_session.query(VpArchiveLoadedFile).one().filename == path.name


def test_double_load_is_idempotent(db_session, tmp_path):
    """Loading the same archive file twice skips the second pass entirely."""
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    load_vp_file(db_session, path)
    assert load_vp_file(db_session, path) == (0, 0)
    assert db_session.query(VehiclePosition).count() == 1


def test_phantom_timestamp_dropped_and_counted(db_session, tmp_path):
    """NOTES-81: a +24h vehicle-reported timestamp never reaches the table."""
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    phantom = dict(VEHICLE, vehicle_id="99", timestamp=1787400015 + 86_400)
    path = _write_vp_file(tmp_path, [VEHICLE, phantom], COLLECTED)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (1, 1)
    assert db_session.query(VehiclePosition).one().vehicle_id == "42"
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1


def test_missing_timestamp_falls_back_to_collected_at(db_session, tmp_path):
    """A null/missing ``timestamp`` field falls back to the poll's collected_at."""
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition

    no_ts = dict(VEHICLE, timestamp=None)
    path = _write_vp_file(tmp_path, [no_ts], COLLECTED)
    assert load_vp_file(db_session, path) == (1, 0)
    assert db_session.query(VehiclePosition).one().timestamp == COLLECTED


def test_null_lat_lon_dropped_and_counted(db_session, tmp_path):
    """A vehicle with no reported position (lat/lon null) is dropped, not inserted.

    ``VehiclePosition.latitude``/``longitude`` are NOT NULL columns, but the
    feed can emit vehicles mid-assignment with no fix yet; ``parse_vp_line``
    must filter these out before the insert rather than let the DB reject
    the whole batch.
    """
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    no_position = dict(VEHICLE, vehicle_id="7", latitude=None)
    path = _write_vp_file(tmp_path, [no_position], COLLECTED)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (0, 1)
    assert db_session.query(VehiclePosition).count() == 0
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1
