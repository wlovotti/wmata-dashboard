"""Tests for pipelines.rotate_archive."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import zstandard as zstd


def _write_jsonl_zst(path: Path, rows: list[dict]) -> None:
    """Helper: write rows as ZSTD-compressed JSONL using STREAMING compression,
    matching the production JsonlArchiveWriter (which produces unframed output)."""
    cctx = zstd.ZstdCompressor(level=3)
    with open(path, "wb") as raw_fh:
        with cctx.stream_writer(raw_fh) as stream:
            for r in rows:
                stream.write((json.dumps(r, default=str) + "\n").encode("utf-8"))


def test_jsonl_to_parquet_round_trip(tmp_path: Path):
    """jsonl_to_parquet preserves all rows and columns."""
    from pipelines.rotate_archive import jsonl_to_parquet

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    parquet_path = tmp_path / "2026-05-17.parquet"
    _write_jsonl_zst(
        jsonl_path,
        [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1},
            {"trip_id": "T2", "stop_id": "S2", "stop_sequence": 2},
        ],
    )

    row_count = jsonl_to_parquet(jsonl_path, parquet_path)
    assert row_count == 2

    df = pl.read_parquet(parquet_path)
    assert df.height == 2
    assert df["trip_id"].to_list() == ["T1", "T2"]


def test_rotate_uploads_and_cleans_up(tmp_path: Path):
    """End-to-end: rotate reads JSONL, uploads parquet, deletes both on success."""
    from pipelines import rotate_archive

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    _write_jsonl_zst(
        jsonl_path,
        [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1}],
    )

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {"ContentLength": 9999}

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        with patch("pathlib.Path.stat") as stat_mock:
            stat_mock.return_value = MagicMock(st_size=9999)
            rotate_archive.rotate_one_day(
                archive_dir=tmp_path,
                target_date=date(2026, 5, 17),
                bucket="test-bucket",
                key_prefix="raw_snapshots",
            )

    fake_s3.upload_file.assert_called_once()
    args, kwargs = fake_s3.upload_file.call_args
    assert kwargs.get("Bucket") == "test-bucket" or args[1] == "test-bucket"

    assert not jsonl_path.exists()
    assert not (tmp_path / "2026-05-17.parquet").exists()


def test_rotate_keeps_local_on_upload_failure(tmp_path: Path):
    """If upload fails (head_object size mismatch), local files are NOT deleted."""
    from pipelines import rotate_archive

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    _write_jsonl_zst(
        jsonl_path,
        [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1}],
    )

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {"ContentLength": 1}  # mismatch

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError, match="size mismatch"):
            rotate_archive.rotate_one_day(
                archive_dir=tmp_path,
                target_date=date(2026, 5, 17),
                bucket="test-bucket",
                key_prefix="raw_snapshots",
            )

    assert jsonl_path.exists()
