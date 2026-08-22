"""Upload closed JSONL archive files to S3 with verify-then-buffer semantics.

Lifecycle per file (spec 2026-08-22 §1): the collector's writer closes a
file every 15 minutes; this uploader ships every closed file it finds,
verifies the stored byte count, then moves the file into an ``uploaded/``
subdirectory that is pruned after 48 hours. Idempotency is filesystem
state: pending files sit next to the writer's open file; a crash between
upload and the rename re-uploads identical bytes to the same key (a
harmless overwrite); a leftover file from a crashed process is shipped on
the next scan because it is closed and therefore not in ``skip``.
"""

import time
from pathlib import Path

import boto3


class UploadVerificationError(RuntimeError):
    """Raised when S3 reports a different byte count than the local file."""


class S3Uploader:
    """Ship closed ``*.jsonl.zst`` files from an archive dir to one S3 prefix."""

    def __init__(self, bucket: str, s3_client=None):
        """Build an uploader targeting ``bucket``.

        ``s3_client=None`` builds a real boto3 client; tests inject a fake.
        """
        self.bucket = bucket
        self._s3 = s3_client if s3_client is not None else boto3.client("s3")

    def upload_closed_files(self, archive_dir: Path, key_prefix: str, skip: set[Path]) -> list[str]:
        """Upload every closed archive file directly under ``archive_dir``.

        ``skip`` holds the writer's currently-open path(s). Each verified
        file moves to ``archive_dir/uploaded/``; a verification mismatch
        raises and leaves the file pending so the next cycle retries it.
        Returns the uploaded filenames, oldest first by mtime. Filenames
        embed an unpadded pid token (``YYYY-MM-DD.<pid>.<open_ts>.jsonl.zst``)
        that a lexicographic sort would order incorrectly across pids
        (e.g. "10" before "9") — sorting by mtime instead keeps upload
        order correct when files from more than one crashed process are
        pending at once.
        """
        uploaded_dir = archive_dir / "uploaded"
        shipped: list[str] = []
        candidates = sorted(archive_dir.glob("*.jsonl.zst"), key=lambda p: p.stat().st_mtime)
        for path in candidates:
            if path in skip:
                continue
            size = path.stat().st_size
            if size == 0:
                continue
            key = f"{key_prefix}{path.name}"
            self._s3.upload_file(str(path), self.bucket, key)
            head = self._s3.head_object(Bucket=self.bucket, Key=key)
            if head["ContentLength"] != size:
                raise UploadVerificationError(
                    f"{key}: S3 has {head['ContentLength']} bytes, local file has {size}"
                )
            uploaded_dir.mkdir(exist_ok=True)
            path.rename(uploaded_dir / path.name)
            shipped.append(path.name)
        return shipped

    def prune_uploaded(self, archive_dir: Path, max_age_sec: int = 48 * 3600) -> int:
        """Delete verified-uploaded files older than ``max_age_sec`` (mtime)."""
        uploaded_dir = archive_dir / "uploaded"
        if not uploaded_dir.exists():
            return 0
        cutoff = time.time() - max_age_sec
        pruned = 0
        for path in uploaded_dir.glob("*.jsonl.zst"):
            if path.stat().st_mtime < cutoff:
                path.unlink()
                pruned += 1
        return pruned
