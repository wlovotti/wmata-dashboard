"""Tests for src.archive_writer.JsonlArchiveWriter."""

import os
from datetime import datetime
from pathlib import Path

import zstandard as zstd


def _list_jsonl_files(directory: Path) -> list[Path]:
    """Return sorted list of .jsonl.zst files in ``directory``."""
    return sorted(directory.glob("*.jsonl.zst"))


def test_writer_creates_per_process_file(tmp_path: Path):
    """append() creates a per-process JSONL file under archive_dir.

    The filename must contain the date, the current PID, and a startup
    timestamp — not just the bare date.
    """
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append(
        {"trip_id": "T1", "stop_id": "S1"},
        snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
    )
    writer.close()

    files = _list_jsonl_files(tmp_path)
    assert len(files) == 1
    name = files[0].name
    # Must start with the date.
    assert name.startswith("2026-05-17.")
    # Must embed the current PID.
    pid = str(os.getpid())
    assert f".{pid}." in name
    # Must end with the expected extension.
    assert name.endswith(".jsonl.zst")


def test_writer_filename_format(tmp_path: Path):
    """The filename produced by _filename_for matches YYYY-MM-DD.<pid>.<open_ts>.jsonl.zst.

    The third token is now captured at call time (per-open), not at
    writer-construction time — so we bracket it with a before/after
    wall-clock read instead of comparing to a stored startup timestamp.
    """
    import time
    from datetime import date

    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    before = int(time.time())
    fname = writer._filename_for(date(2026, 5, 17))
    after = int(time.time())
    parts = fname.split(".")
    # Expected: ['2026-05-17', '<pid>', '<open_unix_ts>', 'jsonl', 'zst']
    assert len(parts) == 5
    assert parts[0] == "2026-05-17"
    assert parts[1] == str(writer._pid)
    assert before <= int(parts[2]) <= after
    assert parts[3] == "jsonl"
    assert parts[4] == "zst"


def test_writer_appends_multiple_rows(tmp_path: Path):
    """Multiple append() calls add separate lines to the same per-process file."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    for i in range(3):
        writer.append(
            {"trip_id": f"T{i}", "stop_id": "S1"},
            snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
        )
    writer.close()

    files = _list_jsonl_files(tmp_path)
    assert len(files) == 1

    dctx = zstd.ZstdDecompressor()
    with open(files[0], "rb") as f:
        # Use stream_reader because the streaming compressor does not embed
        # a content-size header; decompress() requires one.
        with dctx.stream_reader(f) as reader:
            decompressed = reader.read().decode()
    lines = [line for line in decompressed.splitlines() if line]
    assert len(lines) == 3


def test_writer_rotates_at_utc_midnight(tmp_path: Path):
    """Crossing a UTC date boundary writes to a new per-process file.

    Also verifies each file contains exactly the expected row — guards
    against the failure mode where rotation accidentally writes both rows
    to one file.
    """
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append({"trip_id": "T1"}, snapshot_ts=datetime(2026, 5, 17, 23, 59, 59))
    writer.append({"trip_id": "T2"}, snapshot_ts=datetime(2026, 5, 18, 0, 0, 1))
    writer.close()

    files = _list_jsonl_files(tmp_path)
    assert len(files) == 2

    # Files sorted by name; 2026-05-17.* sorts before 2026-05-18.*
    file_17, file_18 = files
    assert file_17.name.startswith("2026-05-17.")
    assert file_18.name.startswith("2026-05-18.")

    dctx = zstd.ZstdDecompressor()

    # 2026-05-17 file must contain exactly "T1" (not "T2")
    with open(file_17, "rb") as f:
        with dctx.stream_reader(f) as reader:
            content_17 = reader.read().decode()
    assert "T1" in content_17
    assert "T2" not in content_17

    # 2026-05-18 file must contain exactly "T2" (not "T1")
    with open(file_18, "rb") as f:
        with dctx.stream_reader(f) as reader:
            content_18 = reader.read().decode()
    assert "T2" in content_18
    assert "T1" not in content_18


def test_two_distinct_writers_produce_separate_files(tmp_path: Path):
    """Two JsonlArchiveWriter instances with different pid/open-ts produce separate files.

    Simulates two sequential collector runs on the same date — confirms
    that per-open filenames never collide and each run's data is
    independently readable. Timestamps are now captured at open time
    (inside append()), so the patch must span the append call, not just
    construction.
    """
    from unittest.mock import patch

    from src.archive_writer import JsonlArchiveWriter

    # Simulate two different processes by patching os.getpid and time.time
    # across construction and the first append (which opens the file).
    with patch("os.getpid", return_value=1001), patch("time.time", return_value=1_000_000.0):
        writer_a = JsonlArchiveWriter(archive_dir=tmp_path)
        writer_a.append({"trip_id": "A1"}, snapshot_ts=datetime(2026, 5, 17, 12, 0, 0))
        writer_a.close()

    with patch("os.getpid", return_value=1002), patch("time.time", return_value=1_000_060.0):
        writer_b = JsonlArchiveWriter(archive_dir=tmp_path)
        writer_b.append({"trip_id": "B1"}, snapshot_ts=datetime(2026, 5, 17, 12, 1, 0))
        writer_b.close()

    files = _list_jsonl_files(tmp_path)
    assert len(files) == 2, f"Expected 2 files, got: {[f.name for f in files]}"

    dctx = zstd.ZstdDecompressor()
    contents = []
    for p in files:
        with open(p, "rb") as f:
            with dctx.stream_reader(f) as reader:
                contents.append(reader.read().decode())

    combined = "".join(contents)
    assert "A1" in combined
    assert "B1" in combined


def _read_rows(path):
    """Decode every JSON line from a .jsonl.zst file (tolerates no footer)."""
    import io
    import json

    import zstandard as zstd

    with open(path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh)
        text = io.TextIOWrapper(reader, encoding="utf-8")
        return [json.loads(line) for line in text]


def test_interval_rotation_opens_new_file(tmp_path):
    """Crossing rotate_interval_sec closes the open file and starts a new one."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path, rotate_interval_sec=900)
    ts = datetime(2026, 8, 22, 12, 0, 0)
    w.append({"a": 1}, snapshot_ts=ts)
    # Simulate 15 minutes of wall clock passing.
    w._open_wall_ts -= 901
    w.append({"a": 2}, snapshot_ts=ts)
    w.close()

    files = sorted(tmp_path.glob("2026-08-22.*.jsonl.zst"))
    assert len(files) == 2
    assert [_read_rows(f)[0]["a"] for f in files] == [1, 2]


def test_no_interval_keeps_single_file(tmp_path):
    """Default rotate_interval_sec=None preserves the current one-file behavior."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path)
    ts = datetime(2026, 8, 22, 12, 0, 0)
    w.append({"a": 1}, snapshot_ts=ts)
    w._open_wall_ts -= 10_000
    w.append({"a": 2}, snapshot_ts=ts)
    w.close()
    assert len(list(tmp_path.glob("*.jsonl.zst"))) == 1


def test_close_returns_closed_path(tmp_path):
    """close() reports which file it closed so the uploader can ship it."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path, rotate_interval_sec=900)
    assert w.close() is None  # nothing open yet
    w.append({"a": 1}, snapshot_ts=datetime(2026, 8, 22, 12, 0, 0))
    open_path = w.open_path
    assert open_path is not None and open_path.exists()
    assert w.close() == open_path
    assert w.open_path is None
