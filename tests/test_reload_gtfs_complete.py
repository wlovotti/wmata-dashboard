"""Tests for the agency-aware GTFS static download/reload path (NOTES-100).

`apply_gtfs_to_db` itself (the DB-side logic) is already covered by
tests/test_smoke.py's GTFS-reload regression block and is unchanged by
this PR. These tests cover the two things NOTES-100 item 3 adds:

  - `_download_and_parse_gtfs` becomes agency-aware (URL, auth style,
    query params all come from `AgencyConfig` instead of a hardcoded
    WMATA constant).
  - SFMTA's real GTFS zip (from 511.org) has no GTFS-Plus extension
    files (`timepoints.txt` / `timepoint_times.txt`, WMATA-only) — the
    parser must tolerate their absence instead of raising `KeyError`.

No network calls: `requests.get` is monkeypatched to return an
in-memory zip built from small fixtures.
"""

import io
import zipfile

import pytest

from src.agency_config import load_agency_config

MINIMAL_FILES = {
    "agency.txt": "agency_id,agency_name\n1,Test Agency\n",
    "calendar.txt": "service_id,monday\nWK,1\n",
    "calendar_dates.txt": "service_id,date,exception_type\n",
    "feed_info.txt": "feed_publisher_name\nTest\n",
    "routes.txt": "route_id,route_short_name\nR1,R1\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,38.9,-77.0\n",
    "trips.txt": "trip_id,route_id\nT1,R1\n",
    "stop_times.txt": "trip_id,stop_id,arrival_time,departure_time,stop_sequence\nT1,S1,08:00:00,08:00:00,1\n",
    "shapes.txt": "shape_id\n",
}

WMATA_PLUS_FILES = {
    "timepoints.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,38.9,-77.0\n",
    "timepoint_times.txt": "trip_id,stop_id,arrival_time,departure_time,stop_sequence\n",
}


def _build_zip_bytes(files: dict[str, str]) -> bytes:
    """Build an in-memory GTFS zip from {filename: csv_text}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


@pytest.mark.smoke
def test_download_and_parse_gtfs_wmata_uses_header_auth(monkeypatch):
    """WMATA (auth_style='header') sends its key in a header, hits its own URL."""
    from scripts.reload_gtfs_complete import _download_and_parse_gtfs

    monkeypatch.setenv("WMATA_API_KEY", "SECRET")
    cfg = load_agency_config("wmata")
    captured = {}

    def _fake_get(url, headers=None, params=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return _FakeResponse(_build_zip_bytes({**MINIMAL_FILES, **WMATA_PLUS_FILES}))

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", _fake_get)

    gtfs_data = _download_and_parse_gtfs(cfg)

    assert captured["url"] == cfg.static_gtfs_url
    assert captured["headers"] == {"api_key": "SECRET"}
    assert len(gtfs_data["routes"]) == 1
    assert len(gtfs_data["timepoints"]) == 1
    assert len(gtfs_data["timepoint_times"]) == 0


@pytest.mark.smoke
def test_download_and_parse_gtfs_sfmta_uses_query_auth_and_static_params(monkeypatch):
    """SFMTA (auth_style='query') sends its key + operator_id as query params,
    hits the 511.org datafeeds URL -- a DIFFERENT param name than the
    real-time feeds' `agency=SF` (see config/agencies/sfmta.yaml comment)."""
    from scripts.reload_gtfs_complete import _download_and_parse_gtfs

    monkeypatch.setenv("SFMTA_API_KEY", "SECRET")
    cfg = load_agency_config("sfmta")
    captured = {}

    def _fake_get(url, headers=None, params=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return _FakeResponse(_build_zip_bytes(MINIMAL_FILES))  # no GTFS-Plus files

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", _fake_get)

    _download_and_parse_gtfs(cfg)

    assert captured["url"] == "https://api.511.org/transit/datafeeds"
    assert captured["params"] == {"api_key": "SECRET", "operator_id": "SF"}
    assert captured["headers"] is None


def test_download_and_parse_gtfs_tolerates_missing_gtfs_plus_files(monkeypatch):
    """SFMTA's real zip has no timepoints.txt/timepoint_times.txt (a
    WMATA-only GTFS-Plus extension) -- these must come back as empty
    lists instead of raising KeyError, matching the design spec's "GTFS-
    Plus extras... stay empty" for non-WMATA agencies."""
    from scripts.reload_gtfs_complete import _download_and_parse_gtfs

    monkeypatch.setenv("SFMTA_API_KEY", "SECRET")
    cfg = load_agency_config("sfmta")

    def _fake_get(url, headers=None, params=None, timeout=None):
        return _FakeResponse(_build_zip_bytes(MINIMAL_FILES))

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", _fake_get)

    gtfs_data = _download_and_parse_gtfs(cfg)

    assert gtfs_data["timepoints"] == []
    assert gtfs_data["timepoint_times"] == []
    # Required files still parse normally.
    assert len(gtfs_data["routes"]) == 1
    assert len(gtfs_data["stops"]) == 1


def test_download_and_parse_gtfs_missing_required_file_still_raises(monkeypatch):
    """A required (non-GTFS-Plus) file missing from the zip is still a hard
    error -- only the two GTFS-Plus files are tolerant of absence."""
    from scripts.reload_gtfs_complete import _download_and_parse_gtfs

    monkeypatch.setenv("SFMTA_API_KEY", "SECRET")
    cfg = load_agency_config("sfmta")
    broken_files = {k: v for k, v in MINIMAL_FILES.items() if k != "routes.txt"}

    def _fake_get(url, headers=None, params=None, timeout=None):
        return _FakeResponse(_build_zip_bytes(broken_files))

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", _fake_get)

    with pytest.raises(KeyError):
        _download_and_parse_gtfs(cfg)


def test_download_and_parse_gtfs_missing_api_key_raises(monkeypatch):
    """Agency's configured api_key_env unset -> clear RuntimeError, not a
    confusing downstream 401 from requests."""
    from scripts.reload_gtfs_complete import _download_and_parse_gtfs

    monkeypatch.delenv("SFMTA_API_KEY", raising=False)
    cfg = load_agency_config("sfmta")

    with pytest.raises(RuntimeError, match="SFMTA_API_KEY"):
        _download_and_parse_gtfs(cfg)


@pytest.mark.smoke
def test_reload_complete_gtfs_targets_agency_database(db_session, monkeypatch):
    """--agency threads through to the DB session resolution, mirroring
    every other NOTES-100 pipeline entry point."""
    import scripts.reload_gtfs_complete as reload_module

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")

    stub_gtfs_data = {
        "agency": [],
        "routes": [],
        "stops": [],
        "stop_times": [],
        "calendar": [],
        "calendar_dates": [],
        "timepoints": [],
        "timepoint_times": [],
    }
    monkeypatch.setattr(reload_module, "_download_gtfs_zip", lambda cfg: _build_zip_bytes({}))
    monkeypatch.setattr(reload_module, "parse_gtfs_zip", lambda zip_bytes, cfg: stub_gtfs_data)
    monkeypatch.setattr(reload_module, "apply_gtfs_to_db", lambda db, gtfs_data, **kwargs: 1)

    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(reload_module, "get_session", _fake_get_session)

    reload_module.reload_complete_gtfs(agency="sfmta", archive=False)

    assert seen_db_urls == ["postgresql:///sfmta_test"]


# ---------------------------------------------------------------------------
# Raw-zip archive + unchanged-feed skip
# ---------------------------------------------------------------------------


def _build_zip_bytes_with(files: dict[str, str], date_time: tuple, reverse: bool = False) -> bytes:
    """Build a GTFS zip with a fixed member timestamp and optional reversed member order."""
    buf = io.BytesIO()
    names = sorted(files, reverse=reverse)
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in names:
            zf.writestr(zipfile.ZipInfo(name, date_time=date_time), files[name])
    return buf.getvalue()


class _FakeS3:
    """Records put_object/head_object calls; can simulate failures or a short write."""

    def __init__(self, fail_put: bool = False, stored_size_delta: int = 0):
        """Configure the fake's failure mode."""
        self.fail_put = fail_put
        self.stored_size_delta = stored_size_delta
        self.objects: dict[tuple[str, str], dict] = {}
        self.calls: list[str] = []

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        """Store the object (or raise when configured to fail)."""
        self.calls.append("put_object")
        if self.fail_put:
            raise RuntimeError("simulated S3 outage")
        self.objects[(Bucket, Key)] = {"Body": Body, "Metadata": Metadata}

    def head_object(self, Bucket, Key):
        """Report the stored object's size (optionally skewed) and metadata."""
        self.calls.append("head_object")
        obj = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(obj["Body"]) + self.stored_size_delta,
            "Metadata": obj["Metadata"] or {},
        }

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        """List stored keys under ``Prefix`` (single page)."""
        self.calls.append("list_objects_v2")
        return {
            "Contents": [
                {"Key": k} for (b, k) in self.objects if b == Bucket and k.startswith(Prefix)
            ],
            "IsTruncated": False,
        }


@pytest.mark.smoke
def test_content_hash_ignores_zip_packaging_but_not_content():
    """Same member bytes re-zipped with other timestamps/order -> same hash; any
    content change -> different hash."""
    from scripts.reload_gtfs_complete import gtfs_content_sha256

    a = _build_zip_bytes_with(MINIMAL_FILES, (2026, 1, 1, 0, 0, 0))
    b = _build_zip_bytes_with(MINIMAL_FILES, (2026, 9, 13, 4, 0, 0), reverse=True)
    assert a != b
    assert gtfs_content_sha256(a) == gtfs_content_sha256(b)

    changed = dict(MINIMAL_FILES)
    changed["calendar.txt"] = "service_id,monday\nWK,0\n"
    assert gtfs_content_sha256(_build_zip_bytes(changed)) != gtfs_content_sha256(a)


@pytest.mark.smoke
def test_archive_key_scheme():
    """gtfs-archive/<agency>/<UTC ts>_<feed_version>_<sha12>.zip, feed_version sanitised."""
    from datetime import datetime

    from scripts.reload_gtfs_complete import gtfs_archive_key

    sha = "ab" * 32
    when = datetime(2026, 9, 13, 8, 15, 38)
    assert (
        gtfs_archive_key("wmata", when, "S1000251", sha)
        == "gtfs-archive/wmata/20260913T081538Z_S1000251_abababababab.zip"
    )
    assert gtfs_archive_key("sfmta", when, None, sha).endswith("_unknown_abababababab.zip")
    assert "/" not in gtfs_archive_key("sfmta", when, "a/b c", sha).split("sfmta/")[1]


@pytest.mark.smoke
def test_archive_gtfs_zip_verifies_stored_size():
    """A short write reported by head_object raises GTFSArchiveError."""
    from scripts.reload_gtfs_complete import GTFSArchiveError, archive_gtfs_zip

    ok = _FakeS3()
    assert archive_gtfs_zip(ok, "bkt", "k.zip", b"1234", {}) == "s3://bkt/k.zip"
    with pytest.raises(GTFSArchiveError, match="expected 4"):
        archive_gtfs_zip(_FakeS3(stored_size_delta=-1), "bkt", "k.zip", b"1234", {})
    with pytest.raises(GTFSArchiveError, match="simulated S3 outage"):
        archive_gtfs_zip(_FakeS3(fail_put=True), "bkt", "k.zip", b"1234", {})


def _stub_reload(monkeypatch, db_session, current_notes: str | None):
    """Wire reload_complete_gtfs to an in-memory zip and the test session.

    Seeds a current snapshot (id 7) whose notes are ``current_notes`` and
    records apply_gtfs_to_db calls. Returns (module, zip_bytes, applied).
    """
    from datetime import datetime

    import scripts.reload_gtfs_complete as reload_module
    from src.models import GTFSSnapshot, Route

    zip_bytes = _build_zip_bytes({**MINIMAL_FILES, **WMATA_PLUS_FILES})
    if current_notes is not None:
        db_session.add(
            GTFSSnapshot(snapshot_id=7, snapshot_date=datetime(2026, 9, 13), notes=current_notes)
        )
        db_session.flush()
        db_session.add(Route(route_id="R1", route_short_name="R1", snapshot_id=7, is_current=True))
        db_session.commit()

    applied = []

    def _fake_apply(db, gtfs_data, content_sha256=None, archive_uri=None):
        """Record the apply call instead of loading."""
        applied.append({"content_sha256": content_sha256, "archive_uri": archive_uri})
        return 8

    class _NoCloseSession:
        """Proxy that keeps the test session open across reload's close()."""

        def __getattr__(self, name):
            """Delegate everything to the real test session."""
            return getattr(db_session, name)

        def close(self):
            """No-op so the fixture can still roll back."""

        def rollback(self):
            """No-op: the fixture owns the outer transaction's rollback."""

    monkeypatch.setattr(reload_module, "_download_gtfs_zip", lambda cfg: zip_bytes)
    monkeypatch.setattr(reload_module, "apply_gtfs_to_db", _fake_apply)
    monkeypatch.setattr(reload_module, "get_session", lambda db_url=None: _NoCloseSession())
    return reload_module, zip_bytes, applied


@pytest.mark.smoke
def test_reload_skips_unchanged_feed(db_session, monkeypatch):
    """Current snapshot already carries this feed's content hash AND an archive URI
    -> no snapshot, no S3 traffic at all."""
    from scripts.reload_gtfs_complete import gtfs_content_sha256

    sha = gtfs_content_sha256(_build_zip_bytes({**MINIMAL_FILES, **WMATA_PLUS_FILES}))
    reload_module, _zip, applied = _stub_reload(
        monkeypatch,
        db_session,
        current_notes=(
            f"Auto-reload at x; content_sha256={sha}; archive=s3://wmata-dashboard-backups/k.zip"
        ),
    )
    s3 = _FakeS3()

    assert reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3) is None
    assert applied == []
    assert s3.calls == []


@pytest.mark.smoke
def test_reload_force_loads_unchanged_feed(db_session, monkeypatch):
    """--force loads a new snapshot even when the hash matches."""
    from scripts.reload_gtfs_complete import gtfs_content_sha256

    sha = gtfs_content_sha256(_build_zip_bytes({**MINIMAL_FILES, **WMATA_PLUS_FILES}))
    reload_module, _zip, applied = _stub_reload(
        monkeypatch, db_session, current_notes=f"content_sha256={sha}"
    )

    assert reload_module.reload_complete_gtfs(agency="wmata", force=True, s3_client=_FakeS3()) == 8
    assert len(applied) == 1


@pytest.mark.smoke
def test_reload_archives_exact_bytes_before_loading_a_changed_feed(db_session, monkeypatch):
    """Legacy current snapshot (no hash) -> archive the exact zip bytes to the
    agency bucket under gtfs-archive/, then load with hash + URI in the notes."""
    reload_module, zip_bytes, applied = _stub_reload(
        monkeypatch, db_session, current_notes="Auto-reload at 2026-09-29T13:27:23"
    )
    s3 = _FakeS3()

    assert reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3) == 8

    [(bucket, key)] = list(s3.objects)
    assert bucket == "wmata-dashboard-backups"
    assert key.startswith("gtfs-archive/wmata/")
    assert s3.objects[(bucket, key)]["Body"] == zip_bytes
    meta = s3.objects[(bucket, key)]["Metadata"]
    assert meta["content-sha256"] == applied[0]["content_sha256"]
    assert applied[0]["archive_uri"] == f"s3://{bucket}/{key}"


@pytest.mark.smoke
def test_reload_fails_without_touching_db_when_archive_fails(db_session, monkeypatch):
    """S3 upload failure aborts the reload before apply_gtfs_to_db runs."""
    from scripts.reload_gtfs_complete import GTFSArchiveError

    reload_module, _zip, applied = _stub_reload(monkeypatch, db_session, current_notes=None)

    with pytest.raises(GTFSArchiveError):
        reload_module.reload_complete_gtfs(agency="wmata", s3_client=_FakeS3(fail_put=True))
    assert applied == []


@pytest.mark.smoke
def test_reload_no_archive_loads_without_upload(db_session, monkeypatch, capsys):
    """archive=False (--no-archive) skips S3 and warns loudly on stderr."""
    reload_module, _zip, applied = _stub_reload(monkeypatch, db_session, current_notes=None)
    s3 = _FakeS3()

    assert reload_module.reload_complete_gtfs(agency="wmata", archive=False, s3_client=s3) == 8
    assert s3.calls == []
    assert applied[0]["archive_uri"] is None
    assert "--no-archive" in capsys.readouterr().err


@pytest.mark.smoke
def test_unchanged_feed_loaded_with_no_archive_gets_archived_not_skipped(db_session, monkeypatch):
    """Hash matches but the current snapshot has no archive= token (it was loaded
    with --no-archive) -> upload the zip, record the URI on that snapshot, create
    no new snapshot. The next unchanged run then skips with zero S3 traffic."""
    from scripts.reload_gtfs_complete import current_snapshot_hash_info, gtfs_content_sha256

    sha = gtfs_content_sha256(_build_zip_bytes({**MINIMAL_FILES, **WMATA_PLUS_FILES}))
    reload_module, zip_bytes, applied = _stub_reload(
        monkeypatch, db_session, current_notes=f"Auto-reload at x; content_sha256={sha}"
    )
    s3 = _FakeS3()

    assert reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3) is None
    assert applied == []
    [(bucket, key)] = list(s3.objects)
    assert s3.objects[(bucket, key)]["Body"] == zip_bytes
    snap_id, recorded_sha, recorded_uri = current_snapshot_hash_info(db_session)
    assert (snap_id, recorded_sha, recorded_uri) == (7, sha, f"s3://{bucket}/{key}")

    s3.calls.clear()
    assert reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3) is None
    assert s3.calls == []


@pytest.mark.smoke
def test_retry_after_failed_load_reuses_the_archived_zip(db_session, monkeypatch):
    """Archive succeeded, DB load failed; the retry finds the same-content object
    under the agency prefix and does not upload a second copy."""
    reload_module, _zip, applied = _stub_reload(monkeypatch, db_session, current_notes=None)
    s3 = _FakeS3()

    def _failing_apply(db, gtfs_data, content_sha256=None, archive_uri=None):
        """Simulate the transactional load blowing up after the upload."""
        raise RuntimeError("load failed")

    monkeypatch.setattr(reload_module, "apply_gtfs_to_db", _failing_apply)
    with pytest.raises(RuntimeError, match="load failed"):
        reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3)
    assert len(s3.objects) == 1
    [(bucket, key)] = list(s3.objects)

    recorded = []

    def _ok_apply(db, gtfs_data, content_sha256=None, archive_uri=None):
        """Record the archive URI the retry loads with."""
        recorded.append(archive_uri)
        return 9

    monkeypatch.setattr(reload_module, "apply_gtfs_to_db", _ok_apply)
    assert reload_module.reload_complete_gtfs(agency="wmata", s3_client=s3) == 9
    assert len(s3.objects) == 1
    assert s3.calls.count("put_object") == 1
    assert recorded == [f"s3://{bucket}/{key}"]


@pytest.mark.smoke
def test_download_error_never_leaks_the_query_param_api_key(monkeypatch):
    """SFMTA query-param auth: an HTTPError's message carries the full URL incl.
    api_key=...; the re-raised error must redact it and suppress the original."""
    import requests

    from scripts.reload_gtfs_complete import _download_gtfs_zip

    monkeypatch.setenv("SFMTA_API_KEY", "SUPERSECRET123")
    cfg = load_agency_config("sfmta")

    class _ErrResponse:
        """A 401 whose raise_for_status embeds the keyed URL, like requests does."""

        content = b""

        def raise_for_status(self):
            """Raise the way requests.Response does for a 4xx."""
            raise requests.HTTPError(
                "401 Client Error: Unauthorized for url: "
                "https://api.511.org/transit/datafeeds?api_key=SUPERSECRET123&operator_id=SF"
            )

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", lambda *a, **k: _ErrResponse())
    with pytest.raises(RuntimeError) as excinfo:
        _download_gtfs_zip(cfg)
    assert "SUPERSECRET123" not in str(excinfo.value)
    assert "<redacted>" in str(excinfo.value)
    assert excinfo.value.__suppress_context__

    def _conn_error(*args, **kwargs):
        """Connection failures also embed the URL."""
        raise requests.ConnectionError("Max retries exceeded with url: /x?api_key=SUPERSECRET123")

    monkeypatch.setattr("scripts.reload_gtfs_complete.requests.get", _conn_error)
    with pytest.raises(RuntimeError) as excinfo:
        _download_gtfs_zip(cfg)
    assert "SUPERSECRET123" not in str(excinfo.value)
