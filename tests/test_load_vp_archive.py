"""Tests for the ``vp_archive_loaded_files`` manifest model (NOTES-95).

Started here (Task 6: model + migration); extended in Task 7 with the
loader function that consumes this manifest for idempotency.
"""

from datetime import datetime

import zstandard as zstd

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


def test_null_vehicle_id_dropped_and_counted(db_session, tmp_path):
    """A vehicle_id-less row is dropped rather than violating the NOT NULL column.

    ``VehiclePosition.vehicle_id`` is NOT NULL; passing a null value through
    to the insert would raise an IntegrityError at chunk-flush time and
    abort the whole file (the same poison-file failure mode as null
    lat/lon). ``parse_vp_line`` must filter it out up front instead.
    """
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    no_vehicle_id = dict(VEHICLE, vehicle_id=None)
    path = _write_vp_file(tmp_path, [no_vehicle_id], COLLECTED)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (0, 1)
    assert db_session.query(VehiclePosition).count() == 0
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1


def _append_garbage_line(path, garbage=b"this is not json\n"):
    """Decompress a closed archive file, append one bad raw line, recompress.

    Simulates a single corrupt/truncated line landing mid-file (a partial
    write racing a crash, or bit rot in transit) without needing to
    hand-roll a second zstd frame: read the whole stream back via
    ``stream_reader`` (tolerant of a missing frame footer, same as the
    loader), append the raw ``garbage`` bytes, and recompress as one fresh
    frame. ``garbage`` defaults to valid-UTF-8-but-invalid-JSON bytes;
    pass invalid UTF-8 bytes to exercise the decode-error path instead.
    """
    decompressor = zstd.ZstdDecompressor()
    with open(path, "rb") as fh:
        raw = decompressor.stream_reader(fh).read()
    raw += garbage
    path.write_bytes(zstd.ZstdCompressor(level=3).compress(raw))


def test_corrupt_line_dropped_and_good_rows_still_load(db_session, tmp_path):
    """A malformed JSON line is dropped and counted, not fatal to the file.

    Before this fix, ``json.loads`` had no try/except around the per-line
    decode, so one bad line raised out of ``load_vp_file`` entirely — no
    manifest row was ever written, and the whole file (including its good
    rows) re-failed identically on every re-run.
    """
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    _append_garbage_line(path)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (1, 1)
    assert db_session.query(VehiclePosition).one().vehicle_id == "42"
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1


def test_invalid_utf8_line_dropped_and_good_rows_still_load(db_session, tmp_path):
    """A line with genuinely invalid UTF-8 bytes is dropped, not fatal to the file.

    Regression test for review round 2: the original fix caught
    ``UnicodeDecodeError`` around ``json.loads(line)``, but ``line`` came
    from an ``io.TextIOWrapper`` that had *already* decoded bytes to
    ``str`` before ``json.loads`` ever ran — the real bytes-to-str decode
    raised from the ``for line in text_stream:`` statement itself,
    outside that try block, so actual byte corruption escaped
    ``load_vp_file`` entirely (no manifest row written, poison file at
    file granularity). This appends a line with an invalid UTF-8 byte
    sequence (``\\xff\\xfe``, not any valid encoding) directly into the
    archive to prove that path is now caught too.
    """
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    _append_garbage_line(path, garbage=b"\xff\xfe not utf8\n")
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (1, 1)
    assert db_session.query(VehiclePosition).one().vehicle_id == "42"
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1


def test_absurd_epoch_dropped_and_good_rows_still_load(db_session, tmp_path):
    """A well-formed but absurd epoch (out of datetime range) is dropped, not fatal.

    Regression test for review round 3: the per-line try only wrapped
    ``json.loads(raw.decode())`` — ``parse_vp_line(obj)`` ran OUTSIDE that
    try, so a syntactically valid JSON line with a garbage ``timestamp``
    value (e.g. exactly the NOTES-81 phantom pattern, just more extreme)
    raised straight out of ``from_epoch_naive_utc`` (``ValueError``:
    "year ... is out of range") and poisoned the whole file at file
    granularity — no manifest row written, re-failing identically forever.
    """
    from pipelines.load_vp_archive import load_vp_file
    from src.models import VehiclePosition, VpArchiveLoadedFile

    absurd = dict(VEHICLE, vehicle_id="66", timestamp=9999999999999)
    path = _write_vp_file(tmp_path, [VEHICLE, absurd], COLLECTED)
    inserted, dropped = load_vp_file(db_session, path)

    assert (inserted, dropped) == (1, 1)
    assert db_session.query(VehiclePosition).one().vehicle_id == "42"
    assert db_session.query(VpArchiveLoadedFile).one().dropped_count == 1


def test_main_continues_past_a_failing_file(db_session, tmp_path, monkeypatch):
    """main() isolates a per-file failure: it logs, continues, and reports nonzero exit.

    Regression guard for the fix to finding 1(b): previously a single
    exception from ``load_vp_file`` (e.g. an IntegrityError that slips
    past the line-level guards) would abort the entire run, leaving every
    later file — good ones included — unloaded.
    """
    import pipelines.load_vp_archive as mod

    good_path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    bad_vehicle = dict(VEHICLE, vehicle_id="bad")
    bad_path = _write_vp_file(tmp_path, [bad_vehicle], COLLECTED)
    # Force the "bad" file to blow up inside load_vp_file despite passing
    # the line-level guards, simulating an error class those guards don't
    # cover (e.g. a DB-level constraint the guards don't fully anticipate).
    real_load_vp_file = mod.load_vp_file

    def _flaky_load_vp_file(session, path):
        """Delegate to the real loader, except raise for ``bad_path``.

        Stands in for an error class the line-level guards inside
        ``load_vp_file`` don't cover (e.g. a DB-level failure), so this
        test can exercise ``main()``'s per-file isolation in isolation
        from the loader's own internals.
        """
        if path == bad_path:
            raise RuntimeError("simulated DB failure")
        return real_load_vp_file(session, path)

    monkeypatch.setattr(mod, "load_vp_file", _flaky_load_vp_file)
    monkeypatch.setattr(mod, "get_session", lambda db_url=None: db_session)
    monkeypatch.setattr(
        mod,
        "load_agency_config",
        lambda name: type("Cfg", (), {"vp_archive_dir": ""})(),
    )
    monkeypatch.setattr(mod, "resolve_agency_db_url", lambda cfg: None)

    exit_code = mod.main(["--agency", "wmata", "--archive-root", str(tmp_path)])

    assert exit_code == 1
    from src.models import VehiclePosition

    assert db_session.query(VehiclePosition).count() == 1
    assert good_path.exists() and bad_path.exists()
