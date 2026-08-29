"""Unit tests for the stateless poller's gate, VP serialization, and upload cycle."""

from datetime import datetime
from pathlib import Path

from src.archive_writer import JsonlArchiveWriter
from src.stateless_poller import PingGate, archive_vp_rows, run_upload_cycle


class RecordingUploader:
    """Stands in for S3Uploader; reports the pending files as shipped."""

    def __init__(self):
        """Initialize with empty upload and prune call logs."""
        self.calls = []
        self.prune_calls = []

    def upload_closed_files(self, archive_dir, key_prefix, skip):
        """Report every ``*.jsonl.zst`` file under ``archive_dir`` not in ``skip`` as shipped.

        Records the ``(archive_dir, key_prefix, skip)`` triple it was called
        with so tests can assert on call shape, including exactly which
        paths the caller told us to skip; performs no actual upload.
        """
        shipped = [p.name for p in sorted(Path(archive_dir).glob("*.jsonl.zst")) if p not in skip]
        self.calls.append((archive_dir, key_prefix, skip))
        return shipped

    def prune_uploaded(self, archive_dir, max_age_sec=48 * 3600):
        """No-op stand-in for S3Uploader.prune_uploaded; records the call and reports 0 pruned."""
        self.prune_calls.append(archive_dir)
        return 0


def test_ping_gate_requires_both_feeds_fresh(monkeypatch):
    """maybe_ping only fires once both "tu" and "vp" have shipped recently, rate-limited.

    Also covers the one-feed-wedge case: once "vp" stops shipping while
    "tu" keeps going, the gate must fall silent even though "tu" is fresh
    and the min-gap window has long since passed — this is the behavior
    the module docstring cites as covering a VP-only collector failure:
    a single gate, not a per-feed one, is what actually detects a wedge
    on either feed.
    """
    pings = []
    monkeypatch.setattr("src.stateless_poller.ping_healthcheck", lambda url: pings.append(url))
    gate = PingGate("http://hc/x", freshness_sec=1200, min_gap_sec=300)

    gate.record_ship("tu", now=1000.0)
    assert gate.maybe_ping(now=1001.0) is False  # vp never shipped
    gate.record_ship("vp", now=1002.0)
    assert gate.maybe_ping(now=1003.0) is True
    assert pings == ["http://hc/x"]
    assert gate.maybe_ping(now=1100.0) is False  # inside min_gap_sec
    assert gate.maybe_ping(now=1400.0) is True  # gap passed, both still fresh
    assert gate.maybe_ping(now=9999.0) is False  # both feeds stale now

    # One-feed wedge after a healthy period: vp goes quiet (e.g. wedged)
    # while tu keeps shipping right up to `now`. Even though tu is fresh
    # and min_gap_sec has elapsed since the last ping, the gate must stay
    # silent because vp alone is stale.
    gate.record_ship("tu", now=2400.0)
    assert gate.maybe_ping(now=2401.0) is False


def test_archive_vp_rows_round_trip(tmp_path):
    """VP dicts serialize with collected_at and read back losslessly."""
    import io
    import json

    import zstandard as zstd

    w = JsonlArchiveWriter(tmp_path)
    vehicles = [
        {
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
            "timestamp": 1787740800,
        }
    ]
    n = archive_vp_rows(w, vehicles, collected_at=datetime(2026, 8, 22, 12, 0, 5))
    closed = w.close()
    assert n == 1

    with open(closed, "rb") as fh:
        text = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        rows = [json.loads(line) for line in text]
    assert rows[0]["vehicle_id"] == "42"
    assert rows[0]["timestamp"] == 1787740800
    assert rows[0]["collected_at"] == "2026-08-22T12:00:05"


def test_run_upload_cycle_records_ships_per_feed(tmp_path, monkeypatch):
    """run_upload_cycle records a ship only for feeds that actually shipped a file.

    Also covers the open-file skip arm: the tu writer has an open (not yet
    rotated) file at call time, and that open file must (a) never appear in
    the returned ``shipped`` list and (b) be present in the ``skip`` set
    that ``run_upload_cycle`` passes to the uploader — proving the
    open-path exclusion is actually wired through, not just true by
    accident because no open file existed.

    Also asserts prune_uploaded is called once per stream dir (not just
    upload_closed_files), and that the healthcheck ping fires only when
    BOTH feeds shipped — here only "tu" shipped, so the ping must not
    fire, and we record real calls to the monkeypatched
    ping_healthcheck instead of trusting a bare ``lambda url: True``.
    """
    pings = []
    monkeypatch.setattr("src.stateless_poller.ping_healthcheck", lambda url: pings.append(url))
    tu_dir, vp_dir = tmp_path / "tu", tmp_path / "vp"
    tu_dir.mkdir()
    vp_dir.mkdir()
    (tu_dir / "2026-08-22.1.100.jsonl.zst").write_bytes(b"x")
    tu_w, vp_w = JsonlArchiveWriter(tu_dir), JsonlArchiveWriter(vp_dir)
    tu_w.append({"trip_id": "t1"}, snapshot_ts=datetime(2026, 8, 22, 12, 0, 0))
    assert tu_w.open_path is not None  # arm the skip case: tu has an open file
    gate = PingGate("http://hc/x")
    uploader = RecordingUploader()
    streams = [
        ("tu", tu_dir, "raw-jsonl-archive/", tu_w),
        ("vp", vp_dir, "raw-jsonl-archive/vp/", vp_w),
    ]

    shipped = run_upload_cycle(uploader, streams, gate, now=1000.0)

    assert shipped == ["2026-08-22.1.100.jsonl.zst"]
    assert gate._last_ship["tu"] == 1000.0
    assert "vp" not in gate._last_ship  # nothing shipped for vp
    assert tu_w.open_path.name not in shipped  # the open file was never shipped
    tu_call = next(c for c in uploader.calls if c[1] == "raw-jsonl-archive/")
    assert tu_w.open_path in tu_call[2]  # ...because it was passed in `skip`

    # prune_uploaded runs for every stream dir, regardless of whether it shipped.
    assert uploader.prune_calls == [Path(tu_dir), Path(vp_dir)]

    # Only "tu" shipped, so the ping must not fire even though it's fresh.
    assert pings == []
