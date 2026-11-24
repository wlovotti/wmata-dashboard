"""Tests for the ``vp_archive_loaded_files`` manifest model.

Started here (Task 6: model + migration); extended in Task 7 with the
loader function that consumes this manifest for idempotency.
"""

from datetime import datetime

import pytest
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
    """A +24h phantom vehicle-reported timestamp never reaches the table."""
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
    value (e.g. exactly the documented phantom-timestamp pattern, just more extreme)
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


def test_complete_file_logs_no_truncation_warning(db_session, tmp_path, caplog):
    """A normally-closed archive file (valid zstd footer) must not trip the
    truncation warning (PR #235 — truncated-file manifest hardening) —
    only a crash-truncated file should.
    """
    import logging

    from pipelines.load_vp_archive import load_vp_file

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    with caplog.at_level(logging.WARNING, logger="pipelines.load_vp_archive"):
        load_vp_file(db_session, path)

    assert "crash-truncated" not in caplog.text


def test_crash_truncated_file_loads_readable_prefix_and_logs_warning(db_session, tmp_path, caplog):
    """A crash-truncated file (flushed compression blocks present, but no
    closing zstd footer — exactly what a process death mid-write leaves
    behind) still loads its readable prefix and is still marked loaded in
    the manifest (files are immutable once uploaded, per the module
    docstring), but now logs a visible warning instead of completing
    silently (PR #235, closing a previously-theoretical crash-truncation
    visibility gap in the manifest loader).
    """
    import logging

    from pipelines.load_vp_archive import load_vp_file
    from src.archive_writer import JsonlArchiveWriter
    from src.models import VehiclePosition, VpArchiveLoadedFile
    from src.stateless_poller import archive_vp_rows

    writer = JsonlArchiveWriter(tmp_path)
    for i in range(50):
        archive_vp_rows(writer, [dict(VEHICLE, vehicle_id=str(i))], collected_at=COLLECTED)
    path = writer._open_path
    writer._raw_fh.flush()  # simulate a crash: flushed blocks, no frame footer
    # Deliberately never call writer.close() — that would write the footer.

    with caplog.at_level(logging.WARNING, logger="pipelines.load_vp_archive"):
        inserted, dropped = load_vp_file(db_session, path)

    assert inserted > 0  # readable prefix still loads
    assert dropped == 0
    assert db_session.query(VehiclePosition).count() == inserted
    assert db_session.query(VpArchiveLoadedFile).one().filename == path.name  # still marked loaded
    assert "crash-truncated" in caplog.text


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


# --- issue #261: UTC-day-scoped (re)loading -----------------------------------

RELOAD_DAY = datetime(2031, 3, 5).date()  # far from any other test's rows


def _write_day_files(dirpath, n_files=2, per_file=3):
    """Write ``n_files`` VP archive files for RELOAD_DAY, ``per_file`` vehicles each.

    Each file gets a distinct collected_at hour and a distinct writer
    (so a distinct open-epoch filename); ``timestamp`` is omitted so the
    loader falls back to collected_at (no guard interaction).
    """
    from src.archive_writer import JsonlArchiveWriter
    from src.stateless_poller import archive_vp_rows

    paths = []
    for f in range(n_files):
        vehicles = [
            dict(VEHICLE, vehicle_id=f"v{f}-{i}", timestamp=None, trip_start_date="20310305")
            for i in range(per_file)
        ]
        w = JsonlArchiveWriter(dirpath)
        w._pid = 1000 + f  # distinct filename per file
        archive_vp_rows(w, vehicles, collected_at=datetime(2031, 3, 5, 10 + f, 0, 0))
        paths.append(w.close())
    return paths


def _day_state(session):
    """(rows on RELOAD_DAY, manifest rows for RELOAD_DAY, manifest row_count sum)."""
    from pipelines.load_vp_archive import day_row_count, manifested_day

    manifest = manifested_day(session, RELOAD_DAY)
    return day_row_count(session, RELOAD_DAY), len(manifest), sum(manifest.values())


@pytest.mark.integration
def test_scoped_load_then_reload_never_duplicates(pg_session, tmp_path):
    """Loading a day twice, or force-reloading it, leaves exactly one copy of each row."""
    from pipelines.load_vp_archive import load_utc_days

    _write_day_files(tmp_path)
    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False) == []
    assert _day_state(pg_session) == (6, 2, 6)

    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False) == []
    assert _day_state(pg_session) == (6, 2, 6)

    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=True) == []
    assert _day_state(pg_session) == (6, 2, 6)


@pytest.mark.integration
def test_pruned_day_is_reloaded_from_its_files(pg_session, tmp_path):
    """Rows deleted after loading (retention) come back on a scoped load, manifest intact."""
    from sqlalchemy import delete

    from pipelines.load_vp_archive import load_utc_days
    from src.models import VehiclePosition

    _write_day_files(tmp_path)
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)

    # Partial prune (as a timestamp-cutoff retention could do mid-day) ...
    pg_session.execute(delete(VehiclePosition).where(VehiclePosition.vehicle_id == "v0-0"))
    pg_session.commit()
    assert _day_state(pg_session) == (5, 2, 6)
    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False) == []
    assert _day_state(pg_session) == (6, 2, 6)

    # ... and a full prune.
    pg_session.execute(delete(VehiclePosition).where(VehiclePosition.trip_start_date == "20310305"))
    pg_session.commit()
    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False) == []
    assert _day_state(pg_session) == (6, 2, 6)


@pytest.mark.integration
def test_new_file_for_intact_day_loads_incrementally(pg_session, tmp_path):
    """A day whose rows are intact only loads its not-yet-manifested files."""
    from pipelines.load_vp_archive import load_utc_days

    first, second = _write_day_files(tmp_path)
    second.rename(tmp_path.parent / second.name)  # second file "not synced yet"
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    assert _day_state(pg_session) == (3, 1, 3)
    (tmp_path.parent / second.name).rename(second)
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    assert _day_state(pg_session) == (6, 2, 6)


@pytest.mark.integration
def test_reload_refuses_when_a_manifested_file_is_missing(pg_session, tmp_path):
    """A needed full-day reload with a manifested file absent changes nothing."""
    from sqlalchemy import delete

    from pipelines.load_vp_archive import load_utc_days
    from src.models import VehiclePosition

    first, second = _write_day_files(tmp_path)
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    pg_session.execute(delete(VehiclePosition).where(VehiclePosition.vehicle_id == "v1-0"))
    pg_session.commit()
    second.unlink()
    failures = load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    assert len(failures) == 1 and "not local" in failures[0]
    assert _day_state(pg_session) == (5, 2, 6)


@pytest.mark.integration
def _add_legacy_row(pg_session):
    """Insert one RELOAD_DAY row no archive file accounts for (VM-era shape)."""
    from src.models import VehiclePosition

    pg_session.add(
        VehiclePosition(
            vehicle_id="legacy",
            latitude=1.0,
            longitude=1.0,
            timestamp=datetime(2031, 3, 5, 12),
            collected_at=datetime(2031, 3, 5, 12),
        )
    )
    pg_session.commit()


@pytest.mark.integration
def test_unknown_provenance_day_with_nothing_new_is_a_noop(pg_session, tmp_path):
    """More rows than manifested but no unmanifested local file → untouched, not an error.

    This is every VM-era day (through 2026-08-22); failing here would stop
    the agency for any ``--date`` that old (PR #264 review).
    """
    from pipelines.load_vp_archive import load_utc_days

    _write_day_files(tmp_path)
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    _add_legacy_row(pg_session)
    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False) == []
    assert _day_state(pg_session) == (7, 2, 6)
    # with no local files at all, too
    assert load_utc_days(pg_session, tmp_path / "empty", [RELOAD_DAY], force=False) == []


@pytest.mark.integration
def test_unknown_provenance_day_with_new_files_or_force_is_refused(pg_session, tmp_path):
    """More rows than manifested AND a file to load (or --force) → refuse, change nothing."""
    from pipelines.load_vp_archive import load_utc_days

    first, second = _write_day_files(tmp_path)
    second.rename(tmp_path.parent / second.name)
    load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    _add_legacy_row(pg_session)
    assert load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=True)[0].count("unknown")
    (tmp_path.parent / second.name).rename(second)
    failures = load_utc_days(pg_session, tmp_path, [RELOAD_DAY], force=False)
    assert len(failures) == 1 and "unknown provenance" in failures[0]
    assert _day_state(pg_session) == (4, 1, 3)


def test_main_force_requires_utc_date():
    """--force without --utc-date is a usage error (reloads are whole UTC days)."""
    import pipelines.load_vp_archive as mod

    with pytest.raises(SystemExit):
        mod.main(["--agency", "wmata", "--force"])
