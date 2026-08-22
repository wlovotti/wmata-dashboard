"""S3Uploader unit tests — a fake boto3 client, no network."""

from pathlib import Path

import pytest

from src.s3_uploader import S3Uploader, UploadVerificationError


class FakeS3:
    """Records upload_file calls; head_object reports the stored size."""

    def __init__(self, corrupt: bool = False):
        self.uploads: list[tuple[str, str, str]] = []  # (local, bucket, key)
        self._sizes: dict[str, int] = {}
        self._corrupt = corrupt

    def upload_file(self, filename, bucket, key):
        size = Path(filename).stat().st_size
        self._sizes[key] = size - 1 if self._corrupt else size
        self.uploads.append((filename, bucket, key))

    def head_object(self, Bucket, Key):
        return {"ContentLength": self._sizes[Key]}


def _mk(dirpath: Path, name: str, content: bytes = b"x" * 64) -> Path:
    """Create a fake closed archive file."""
    p = dirpath / name
    p.write_bytes(content)
    return p


def test_uploads_closed_files_and_moves_to_uploaded(tmp_path):
    fake = FakeS3()
    up = S3Uploader("bkt", s3_client=fake)
    closed = _mk(tmp_path, "2026-08-22.1.100.jsonl.zst")
    open_file = _mk(tmp_path, "2026-08-22.1.200.jsonl.zst")

    shipped = up.upload_closed_files(tmp_path, "raw-jsonl-archive/vp/", skip={open_file})

    assert shipped == ["2026-08-22.1.100.jsonl.zst"]
    assert fake.uploads[0][2] == "raw-jsonl-archive/vp/2026-08-22.1.100.jsonl.zst"
    assert not closed.exists()
    assert (tmp_path / "uploaded" / closed.name).exists()
    assert open_file.exists()  # skipped


def test_verification_failure_leaves_file_in_place(tmp_path):
    up = S3Uploader("bkt", s3_client=FakeS3(corrupt=True))
    f = _mk(tmp_path, "2026-08-22.1.100.jsonl.zst")
    with pytest.raises(UploadVerificationError):
        up.upload_closed_files(tmp_path, "p/", skip=set())
    assert f.exists()  # still pending — retried next cycle


def test_upload_order_is_by_mtime_not_lexicographic_pid(tmp_path):
    """Filenames are ``YYYY-MM-DD.<pid>.<open_ts>.jsonl.zst`` with an
    unpadded pid token, so a lexicographic path sort orders pid "10"
    before pid "9" even when pid 9's file is older. Regression for the
    two-crashed-processes case: upload order must follow mtime, oldest
    first, not string order.
    """
    import os
    import time

    fake = FakeS3()
    up = S3Uploader("bkt", s3_client=fake)
    older = _mk(tmp_path, "2026-08-22.9.100.jsonl.zst")
    newer = _mk(tmp_path, "2026-08-22.10.200.jsonl.zst")

    now = time.time()
    os.utime(older, (now - 100, now - 100))
    os.utime(newer, (now, now))

    shipped = up.upload_closed_files(tmp_path, "p/", skip=set())

    assert shipped == [older.name, newer.name]


def test_upload_resets_mtime_so_buffer_counts_from_upload_time(tmp_path):
    """A file that sat pending for a while (old content mtime, e.g. after a
    crash) must still get the full 48-hour local retention window once it
    ships — ``upload_closed_files`` resets mtime to upload time, not
    content time, so an immediate prune must not delete it.
    """
    import os
    import time

    fake = FakeS3()
    up = S3Uploader("bkt", s3_client=fake)
    pending = _mk(tmp_path, "2026-08-22.1.100.jsonl.zst")
    old_ts = time.time() - 172_801  # more than 48h old, before upload
    os.utime(pending, (old_ts, old_ts))

    test_start = time.time()
    shipped = up.upload_closed_files(tmp_path, "p/", skip=set())

    assert shipped == [pending.name]
    uploaded_path = tmp_path / "uploaded" / pending.name
    assert uploaded_path.exists()
    assert uploaded_path.stat().st_mtime >= test_start

    # Immediate prune must delete nothing: the file just shipped, so its
    # (reset) mtime is nowhere near the 48-hour cutoff.
    assert up.prune_uploaded(tmp_path) == 0
    assert uploaded_path.exists()


def test_prune_uploaded_deletes_only_old_files(tmp_path):
    import os
    import time

    up = S3Uploader("bkt", s3_client=FakeS3())
    updir = tmp_path / "uploaded"
    updir.mkdir()
    old = _mk(updir, "2026-08-20.1.100.jsonl.zst")
    fresh = _mk(updir, "2026-08-22.1.100.jsonl.zst")
    two_days_ago = time.time() - 172_801
    os.utime(old, (two_days_ago, two_days_ago))

    assert up.prune_uploaded(tmp_path) == 1
    assert not old.exists() and fresh.exists()
