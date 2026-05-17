"""Tests for src.archive_writer.JsonlArchiveWriter."""

from datetime import datetime
from pathlib import Path

import zstandard as zstd


def test_writer_creates_daily_file(tmp_path: Path):
    """append() creates a daily JSONL file under archive_dir."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append(
        {"trip_id": "T1", "stop_id": "S1"},
        snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
    )
    writer.close()

    expected = tmp_path / "2026-05-17.jsonl.zst"
    assert expected.exists()


def test_writer_appends_multiple_rows(tmp_path: Path):
    """Multiple append() calls add separate lines to the same daily file."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    for i in range(3):
        writer.append(
            {"trip_id": f"T{i}", "stop_id": "S1"},
            snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
        )
    writer.close()

    path = tmp_path / "2026-05-17.jsonl.zst"
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as f:
        # Use stream_reader because the streaming compressor does not embed
        # a content-size header; decompress() requires one.
        with dctx.stream_reader(f) as reader:
            decompressed = reader.read().decode()
    lines = [line for line in decompressed.splitlines() if line]
    assert len(lines) == 3


def test_writer_rotates_at_utc_midnight(tmp_path: Path):
    """Crossing a UTC date boundary writes to a new file."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append({"trip_id": "T1"}, snapshot_ts=datetime(2026, 5, 17, 23, 59, 59))
    writer.append({"trip_id": "T2"}, snapshot_ts=datetime(2026, 5, 18, 0, 0, 1))
    writer.close()

    assert (tmp_path / "2026-05-17.jsonl.zst").exists()
    assert (tmp_path / "2026-05-18.jsonl.zst").exists()
