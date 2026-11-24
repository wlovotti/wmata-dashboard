"""Tests for src/raw_archive.py — per-date file selection, S3 fetch, verify-then-prune (#261).

All S3 access goes through ``FakeS3`` (an in-memory ``list_objects_v2`` /
``download_file`` double); all files live in ``tmp_path``.
"""

import hashlib
from datetime import date
from pathlib import Path

import pytest

import src.raw_archive as ra
from src.raw_archive import (
    DELETE,
    KEEP,
    REFUSE,
    ArchiveStream,
    S3Object,
    archive_file_date,
    execute_prune,
    fetch_day_files,
    last_unneeded_utc_date,
    list_s3_day,
    plan_prune,
    tu_utc_dates,
    utc_dates_needed,
    verify_local_against_s3,
    vp_utc_dates,
)

BUCKET = "bucket"
EASTERN = "America/New_York"
PACIFIC = "America/Los_Angeles"


def _etag(data: bytes, chunk: int | None = None) -> str:
    """S3-style ETag for ``data`` (single-part MD5, or multipart with ``chunk``-byte parts)."""
    if chunk is None or len(data) <= chunk:
        return hashlib.md5(data).hexdigest()
    parts = [hashlib.md5(data[i : i + chunk]).digest() for i in range(0, len(data), chunk)]
    return f"{hashlib.md5(b''.join(parts)).hexdigest()}-{len(parts)}"


class FakeS3:
    """In-memory stand-in for the two boto3 S3 calls raw_archive uses."""

    def __init__(self, objects: dict[str, bytes], page_size: int = 1000, etags=None):
        """``objects`` maps full key → content; ``etags`` overrides per-key ETags."""
        self.objects = dict(objects)
        self.page_size = page_size
        self.etags = etags or {}
        self.downloads: list[str] = []
        self.corrupt_downloads: set[str] = set()

    def list_objects_v2(self, Bucket, Prefix, Delimiter=None, ContinuationToken=None):
        """Paginated listing honoring ``Delimiter='/'`` like S3."""
        assert Bucket == BUCKET
        keys = sorted(
            k
            for k in self.objects
            if k.startswith(Prefix) and not (Delimiter and Delimiter in k[len(Prefix) :])
        )
        start = int(ContinuationToken or 0)
        page = keys[start : start + self.page_size]
        resp = {
            "Contents": [
                {
                    "Key": k,
                    "Size": len(self.objects[k]),
                    "ETag": f'"{self.etags.get(k, _etag(self.objects[k]))}"',
                }
                for k in page
            ],
            "IsTruncated": start + self.page_size < len(keys),
        }
        if resp["IsTruncated"]:
            resp["NextContinuationToken"] = str(start + self.page_size)
        return resp

    def download_file(self, bucket, key, filename):
        """Write the object's bytes to ``filename`` (optionally corrupted)."""
        self.downloads.append(key)
        data = self.objects[key]
        if key in self.corrupt_downloads:
            data = data[:-1] + bytes([data[-1] ^ 0xFF])
        Path(filename).write_bytes(data)


def _stream(tmp_path, kind="tu", prefix="raw-jsonl-archive/", tz=EASTERN) -> ArchiveStream:
    """An ArchiveStream rooted in ``tmp_path``."""
    return ArchiveStream("wmata", kind, tmp_path / kind, BUCKET, prefix, tz)


# --- file naming / per-date selection ----------------------------------------


@pytest.mark.smoke
def test_archive_file_date_parses_both_naming_schemes():
    """Per-process and legacy single-file names parse; anything else is None."""
    assert archive_file_date("2026-10-03.182378.1790687037.jsonl.zst") == date(2026, 10, 3)
    assert archive_file_date("2026-10-03.jsonl.zst") == date(2026, 10, 3)
    assert archive_file_date("2026-10-03.182378.1790687037.jsonl.zst.part") is None
    assert archive_file_date("2026-05-03.parquet") is None
    assert archive_file_date("2026-13-40.1.2.jsonl.zst") is None
    assert archive_file_date("notes.txt") is None


@pytest.mark.smoke
def test_tu_dates_are_d_and_utc_next_day_across_month_and_year():
    """TU needs UTC D (primary) and D+1 (supplement), rolling months and years."""
    assert tu_utc_dates(date(2026, 10, 3)) == [date(2026, 10, 3), date(2026, 10, 4)]
    assert tu_utc_dates(date(2026, 10, 31)) == [date(2026, 10, 31), date(2026, 11, 1)]
    assert tu_utc_dates(date(2026, 12, 31)) == [date(2026, 12, 31), date(2027, 1, 1)]


@pytest.mark.smoke
@pytest.mark.parametrize("tz", [EASTERN, PACIFIC])
@pytest.mark.parametrize(
    "service_date",
    [
        date(2026, 10, 3),
        date(2026, 9, 30),  # month boundary
        date(2026, 12, 31),  # year boundary
        date(2026, 10, 31),  # window spans the 11/1 fall-back
        date(2027, 3, 13),  # window spans the 3/14 spring-forward
    ],
)
def test_vp_dates_are_d_through_d_plus_2(tz, service_date):
    """The guard-widened VP window [local midnight D, local midnight D+2) is UTC D..D+2.

    Both agencies sit 4-8 h behind UTC, so local midnight D minus the
    30-min guard is still UTC date D, and local midnight D+2 plus the
    guard is still UTC date D+2 — including across DST transitions.
    """
    from datetime import timedelta

    expected = [service_date + timedelta(days=i) for i in range(3)]
    assert vp_utc_dates(service_date, tz) == expected


@pytest.mark.smoke
def test_next_date_frees_files_dated_through_previous_day(tmp_path):
    """After D, the next date needs nothing dated <= D in either stream."""
    tu = _stream(tmp_path, "tu")
    vp = _stream(tmp_path, "vp")
    d = date(2026, 12, 31)
    assert utc_dates_needed(tu, d)[0] == d
    assert last_unneeded_utc_date(tu, date(2027, 1, 1)) == d
    assert last_unneeded_utc_date(vp, date(2027, 1, 1)) == d


# --- S3 listing / fetch -------------------------------------------------------


@pytest.mark.smoke
def test_list_s3_day_excludes_sub_prefixes_and_paginates(tmp_path):
    """Root-prefix listing for one day skips vp/... keys and follows continuation tokens."""
    objs = {f"raw-jsonl-archive/2026-10-03.1.{i}.jsonl.zst": b"x" * i for i in range(1, 6)}
    objs["raw-jsonl-archive/vp/2026-10-03.1.1.jsonl.zst"] = b"vp"
    objs["raw-jsonl-archive/2026-10-04.1.1.jsonl.zst"] = b"next day"
    objs["raw-jsonl-archive/2026-10-03.jsonl.zst"] = b"legacy"
    s3 = FakeS3(objs, page_size=2)
    listing = list_s3_day(s3, _stream(tmp_path), date(2026, 10, 3))
    assert sorted(listing) == sorted(
        [f"2026-10-03.1.{i}.jsonl.zst" for i in range(1, 6)] + ["2026-10-03.jsonl.zst"]
    )
    assert listing["2026-10-03.1.3.jsonl.zst"].size == 3


@pytest.mark.smoke
def test_fetch_downloads_missing_and_mismatched_skips_present(tmp_path):
    """Only files absent locally or of a different size are downloaded."""
    stream = _stream(tmp_path)
    objs = {
        "raw-jsonl-archive/2026-10-03.1.1.jsonl.zst": b"aaaa",
        "raw-jsonl-archive/2026-10-03.1.2.jsonl.zst": b"bbbb",
        "raw-jsonl-archive/2026-10-03.1.3.jsonl.zst": b"cccc",
    }
    s3 = FakeS3(objs)
    stream.local_dir.mkdir(parents=True)
    (stream.local_dir / "2026-10-03.1.1.jsonl.zst").write_bytes(b"aaaa")  # present
    (stream.local_dir / "2026-10-03.1.2.jsonl.zst").write_bytes(b"bb")  # truncated
    result = fetch_day_files(s3, stream, date(2026, 10, 3), log=lambda m: None)
    assert result.already_present == ["2026-10-03.1.1.jsonl.zst"]
    assert sorted(result.downloaded) == ["2026-10-03.1.2.jsonl.zst", "2026-10-03.1.3.jsonl.zst"]
    assert (stream.local_dir / "2026-10-03.1.2.jsonl.zst").read_bytes() == b"bbbb"
    assert not list(stream.local_dir.glob("*.part"))


@pytest.mark.smoke
def test_fetch_refetches_same_size_local_file_with_wrong_checksum(tmp_path):
    """A same-size but corrupt local file is not reused — it is re-downloaded."""
    stream = _stream(tmp_path)
    s3 = FakeS3({"raw-jsonl-archive/2026-10-03.1.1.jsonl.zst": b"good"})
    stream.local_dir.mkdir(parents=True)
    (stream.local_dir / "2026-10-03.1.1.jsonl.zst").write_bytes(b"bad!")
    result = fetch_day_files(s3, stream, date(2026, 10, 3), log=lambda m: None)
    assert result.downloaded == ["2026-10-03.1.1.jsonl.zst"]
    assert (stream.local_dir / "2026-10-03.1.1.jsonl.zst").read_bytes() == b"good"


@pytest.mark.smoke
def test_fetch_rejects_corrupt_download_and_leaves_no_partial(tmp_path):
    """A download whose checksum doesn't match S3 raises and leaves nothing behind."""
    stream = _stream(tmp_path)
    key = "raw-jsonl-archive/2026-10-03.1.1.jsonl.zst"
    s3 = FakeS3({key: b"payload"})
    s3.corrupt_downloads.add(key)
    with pytest.raises(RuntimeError, match="failed verification"):
        fetch_day_files(s3, stream, date(2026, 10, 3), log=lambda m: None)
    assert list(stream.local_dir.iterdir()) == []


@pytest.mark.smoke
def test_fetch_of_empty_day_is_not_an_error(tmp_path):
    """A UTC date with no objects (future / gap) returns an empty result."""
    result = fetch_day_files(FakeS3({}), _stream(tmp_path), date(2026, 10, 3), log=lambda m: None)
    assert result.downloaded == [] and result.s3_objects == {}


# --- checksum verification ----------------------------------------------------


@pytest.mark.smoke
def test_verify_single_part_and_multipart_etags(tmp_path, monkeypatch):
    """Matching MD5 / multipart ETags verify; content or part-count mismatches don't."""
    monkeypatch.setattr(ra, "MULTIPART_CHUNK_BYTES", 4)
    p = tmp_path / "2026-10-03.1.1.jsonl.zst"
    data = b"0123456789"  # 3 parts of 4 bytes
    p.write_bytes(data)
    assert verify_local_against_s3(p, S3Object(p.name, 10, _etag(data))) is None
    assert verify_local_against_s3(p, S3Object(p.name, 10, _etag(data, 4))) is None
    assert "checksum mismatch" in verify_local_against_s3(p, S3Object(p.name, 10, _etag(b"x" * 10)))
    assert "unverifiable" in verify_local_against_s3(p, S3Object(p.name, 10, _etag(data, 5)))
    assert "size mismatch" in verify_local_against_s3(p, S3Object(p.name, 11, _etag(data)))


# --- prune --------------------------------------------------------------------


def _files(tmp_path, names_to_bytes: dict[str, bytes]) -> list[Path]:
    """Write files into ``tmp_path/tu`` and return their paths sorted."""
    d = tmp_path / "tu"
    d.mkdir(exist_ok=True)
    for name, data in names_to_bytes.items():
        (d / name).write_bytes(data)
    return sorted(d.iterdir())


@pytest.mark.smoke
def test_plan_prune_refuses_everything_it_cannot_verify(tmp_path):
    """Only a processed, S3-identical, no-longer-needed file is deleted."""
    files = {
        "2026-10-01.1.1.jsonl.zst": b"ok",  # deletable
        "2026-10-01.1.2.jsonl.zst": b"not in s3",
        "2026-10-01.1.3.jsonl.zst": b"size differs",
        "2026-10-01.1.4.jsonl.zst": b"bytes differ",
        "2026-10-01.1.5.jsonl.zst": b"never replayed",
        "2026-10-03.1.1.jsonl.zst": b"still needed",
        "stray.txt": b"?",
    }
    paths = _files(tmp_path, files)
    s3_index = {
        n: S3Object(n, len(b), _etag(b))
        for n, b in files.items()
        if n not in ("2026-10-01.1.2.jsonl.zst", "stray.txt")
    }
    s3_index["2026-10-01.1.3.jsonl.zst"] = S3Object("2026-10-01.1.3.jsonl.zst", 999, "x")
    s3_index["2026-10-01.1.4.jsonl.zst"] = S3Object(
        "2026-10-01.1.4.jsonl.zst", len(files["2026-10-01.1.4.jsonl.zst"]), _etag(b"other bytes!")
    )
    processed = set(files) - {"2026-10-01.1.5.jsonl.zst"}
    decisions = {d.path.name: d for d in plan_prune(paths, s3_index, processed, date(2026, 10, 2))}
    assert decisions["2026-10-01.1.1.jsonl.zst"].action == DELETE
    assert decisions["2026-10-01.1.2.jsonl.zst"].reason == "missing from S3"
    assert "size mismatch" in decisions["2026-10-01.1.3.jsonl.zst"].reason
    assert "checksum mismatch" in decisions["2026-10-01.1.4.jsonl.zst"].reason
    assert "manifest" in decisions["2026-10-01.1.5.jsonl.zst"].reason
    assert decisions["2026-10-03.1.1.jsonl.zst"].action == KEEP
    assert decisions["stray.txt"].action == REFUSE
    assert sum(1 for d in decisions.values() if d.action == DELETE) == 1


@pytest.mark.smoke
def test_execute_prune_dry_run_deletes_nothing(tmp_path):
    """Dry run reports what it would free; a real run deletes only DELETE decisions."""
    paths = _files(
        tmp_path, {"2026-10-01.1.1.jsonl.zst": b"aa", "2026-10-01.1.2.jsonl.zst": b"bbb"}
    )
    s3_index = {p.name: S3Object(p.name, p.stat().st_size, _etag(p.read_bytes())) for p in paths}
    processed = {paths[0].name}  # second one is unprocessed → refused
    decisions = plan_prune(paths, s3_index, processed, None)

    assert execute_prune(decisions, dry_run=True) == (1, 2)
    assert all(p.exists() for p in paths)

    assert execute_prune(decisions, dry_run=False) == (1, 2)
    assert not paths[0].exists() and paths[1].exists()


@pytest.mark.smoke
def test_prune_stream_uses_manifest_and_s3_listing(tmp_path, db_session):
    """End-to-end: manifests from the DB, sizes/ETags from (fake) S3, dry run is inert."""
    from datetime import datetime

    from src.models import TuArchiveReplayedFile

    stream = _stream(tmp_path)
    files = {
        "2026-10-01.1.1.jsonl.zst": b"replayed",
        "2026-10-01.1.2.jsonl.zst": b"not replayed",
        "2026-10-02.1.1.jsonl.zst": b"replayed, needed",
    }
    _files(tmp_path, files)
    s3 = FakeS3({f"raw-jsonl-archive/{n}": b for n, b in files.items()})
    for name in ("2026-10-01.1.1.jsonl.zst", "2026-10-02.1.1.jsonl.zst"):
        db_session.add(
            TuArchiveReplayedFile(
                filename=name,
                target_service_date=date(2026, 10, 1),
                row_count=1,
                replayed_at=datetime(2026, 10, 2),
            )
        )
    db_session.flush()

    decisions, count, nbytes = ra.prune_stream(s3, db_session, stream, date(2026, 10, 1), True)
    assert (count, nbytes) == (1, len(b"replayed"))
    assert all((stream.local_dir / n).exists() for n in files)

    decisions, count, _ = ra.prune_stream(s3, db_session, stream, date(2026, 10, 1), False)
    assert count == 1
    assert sorted(p.name for p in stream.local_dir.iterdir()) == [
        "2026-10-01.1.2.jsonl.zst",
        "2026-10-02.1.1.jsonl.zst",
    ]
