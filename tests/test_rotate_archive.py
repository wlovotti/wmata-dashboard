"""Tests for pipelines.rotate_archive."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import zstandard as zstd


def _write_jsonl_zst(path: Path, rows: list[dict]) -> None:
    """Write rows as ZSTD-compressed JSONL using STREAMING compression,
    matching the production JsonlArchiveWriter (which produces unframed output)."""
    cctx = zstd.ZstdCompressor(level=3)
    with open(path, "wb") as raw_fh:
        with cctx.stream_writer(raw_fh) as stream:
            for r in rows:
                stream.write((json.dumps(r, default=str) + "\n").encode("utf-8"))


def _make_fake_s3(parquet_path_ref: list):
    """Return a mock S3 client that reports the on-disk parquet size on head_object.

    ``parquet_path_ref`` is a one-element list updated by the upload side-effect
    so the head side-effect can find the file without relying on a closure over
    a mutable outer variable.
    """
    fake_s3 = MagicMock()
    captured = {}

    def capture_upload(local_path, bucket, key):
        """Record upload path for the head_object side-effect."""
        captured["local_path"] = local_path

    def fake_head(Bucket, Key):
        """Return the actual on-disk size of the uploaded parquet file."""
        return {"ContentLength": Path(captured["local_path"]).stat().st_size}

    fake_s3.upload_file.side_effect = capture_upload
    fake_s3.head_object.side_effect = fake_head
    return fake_s3, captured


# ---------------------------------------------------------------------------
# _find_jsonl_files_for_date
# ---------------------------------------------------------------------------


def test_find_jsonl_files_discovers_legacy_name(tmp_path: Path):
    """Legacy ``YYYY-MM-DD.jsonl.zst`` is discovered by _find_jsonl_files_for_date."""
    from pipelines.rotate_archive import _find_jsonl_files_for_date

    legacy = tmp_path / "2026-05-17.jsonl.zst"
    legacy.touch()
    found = _find_jsonl_files_for_date(tmp_path, date(2026, 5, 17))
    assert found == [legacy]


def test_find_jsonl_files_discovers_per_process_names(tmp_path: Path):
    """Per-process files ``YYYY-MM-DD.<pid>.<ts>.jsonl.zst`` are discovered."""
    from pipelines.rotate_archive import _find_jsonl_files_for_date

    f1 = tmp_path / "2026-05-17.1001.1000000.jsonl.zst"
    f2 = tmp_path / "2026-05-17.1002.1000060.jsonl.zst"
    f1.touch()
    f2.touch()
    found = _find_jsonl_files_for_date(tmp_path, date(2026, 5, 17))
    assert found == [f1, f2]


def test_find_jsonl_files_excludes_other_dates(tmp_path: Path):
    """Files from a different date are not returned."""
    from pipelines.rotate_archive import _find_jsonl_files_for_date

    (tmp_path / "2026-05-17.jsonl.zst").touch()
    (tmp_path / "2026-05-18.jsonl.zst").touch()
    found = _find_jsonl_files_for_date(tmp_path, date(2026, 5, 17))
    assert len(found) == 1
    assert found[0].name == "2026-05-17.jsonl.zst"


# ---------------------------------------------------------------------------
# jsonl_to_parquet (single-file path, kept for backward compat)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _merge_jsonl_files_to_parquet
# ---------------------------------------------------------------------------


def test_merge_jsonl_files_combines_rows(tmp_path: Path):
    """_merge_jsonl_files_to_parquet concatenates rows from multiple source files."""
    from pipelines.rotate_archive import _merge_jsonl_files_to_parquet

    f1 = tmp_path / "2026-05-17.1001.1000000.jsonl.zst"
    f2 = tmp_path / "2026-05-17.1002.1000060.jsonl.zst"
    _write_jsonl_zst(f1, [{"trip_id": "T1", "stop_id": "S1"}])
    _write_jsonl_zst(f2, [{"trip_id": "T2", "stop_id": "S2"}])

    parquet_path = tmp_path / "2026-05-17.parquet"
    row_count = _merge_jsonl_files_to_parquet([f1, f2], parquet_path)
    assert row_count == 2

    df = pl.read_parquet(parquet_path)
    assert set(df["trip_id"].to_list()) == {"T1", "T2"}


# ---------------------------------------------------------------------------
# rotate_one_day — single legacy file
# ---------------------------------------------------------------------------


def test_rotate_uploads_and_cleans_up_legacy(tmp_path: Path):
    """End-to-end: rotate handles a single legacy JSONL, uploads parquet, cleans up."""
    from pipelines import rotate_archive

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    _write_jsonl_zst(
        jsonl_path,
        [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1}],
    )

    fake_s3, _captured = _make_fake_s3([])

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        rotate_archive.rotate_one_day(
            archive_dir=tmp_path,
            target_date=date(2026, 5, 17),
            bucket="test-bucket",
            key_prefix="raw_snapshots",
        )

    fake_s3.upload_file.assert_called_once()
    assert _captured["local_path"].endswith("2026-05-17.parquet")

    # Local files deleted after successful upload + verification.
    assert not jsonl_path.exists()
    assert not (tmp_path / "2026-05-17.parquet").exists()


# ---------------------------------------------------------------------------
# rotate_one_day — multiple per-process files
# ---------------------------------------------------------------------------


def test_rotate_merges_multiple_per_process_files(tmp_path: Path):
    """rotate_one_day merges multiple per-process JSONL files into one parquet."""
    from pipelines import rotate_archive

    f1 = tmp_path / "2026-05-17.1001.1000000.jsonl.zst"
    f2 = tmp_path / "2026-05-17.1002.1000060.jsonl.zst"
    _write_jsonl_zst(f1, [{"trip_id": "T1", "stop_id": "S1"}])
    _write_jsonl_zst(f2, [{"trip_id": "T2", "stop_id": "S2"}])

    fake_s3, _captured = _make_fake_s3([])

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        rotate_archive.rotate_one_day(
            archive_dir=tmp_path,
            target_date=date(2026, 5, 17),
            bucket="test-bucket",
            key_prefix="raw_snapshots",
        )

    # Single upload with merged parquet.
    fake_s3.upload_file.assert_called_once()
    assert _captured["local_path"].endswith("2026-05-17.parquet")

    # Both source files deleted.
    assert not f1.exists()
    assert not f2.exists()
    assert not (tmp_path / "2026-05-17.parquet").exists()

    # Confirm row count via the upload path is not available post-delete,
    # but we can verify the upload key.
    _, _, upload_key = fake_s3.upload_file.call_args[0]
    assert upload_key == "raw_snapshots/2026-05-17.parquet"


# ---------------------------------------------------------------------------
# rotate_one_day — upload failure keeps files
# ---------------------------------------------------------------------------


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

    # Files retained for retry.
    assert jsonl_path.exists()
    assert (tmp_path / "2026-05-17.parquet").exists()


# ---------------------------------------------------------------------------
# rotate_one_day — missing JSONL
# ---------------------------------------------------------------------------


def test_rotate_skips_when_jsonl_missing(tmp_path: Path):
    """rotate_one_day exits cleanly when no JSONL exists for the date."""
    from pipelines import rotate_archive

    fake_s3 = MagicMock()
    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        rotate_archive.rotate_one_day(
            archive_dir=tmp_path,
            target_date=date(2026, 5, 17),
            bucket="test-bucket",
            key_prefix="raw_snapshots",
        )

    # No upload should happen — the function returns before touching S3.
    fake_s3.upload_file.assert_not_called()
