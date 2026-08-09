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
    monkeypatch.setattr(reload_module, "_download_and_parse_gtfs", lambda cfg: stub_gtfs_data)
    monkeypatch.setattr(reload_module, "apply_gtfs_to_db", lambda db, gtfs_data: 1)

    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(reload_module, "get_session", _fake_get_session)

    reload_module.reload_complete_gtfs(agency="sfmta")

    assert seen_db_urls == ["postgresql:///sfmta_test"]
