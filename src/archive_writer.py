"""Streaming JSONL writer for raw WMATA TripUpdate snapshots.

The writer appends one JSON line per (trip, stop) per snapshot to a
ZSTD-compressed file named by UTC date, process identity, and open time.
Files rotate automatically when the snapshot timestamp crosses a UTC
midnight boundary, and optionally on a fixed wall-clock interval as well
(``rotate_interval_sec``).

Each open file gets its own name:
``YYYY-MM-DD.<pid>.<open_unix_ts>.jsonl.zst``

This eliminates the multi-frame hazard from the old ``YYYY-MM-DD.jsonl.zst``
scheme: a mid-day restart previously appended a new zstd frame onto the
existing file; if the prior process exited ungracefully (no ``close()`` → no
frame footer), the result was an un-decodable abandoned-frame boundary.
Per-open filenames give each rotation window a clean, single-frame file —
the same property the earlier per-process scheme introduced, now also
achieved within a single long-running process via interval rotation.
``rotate_archive.py`` discovers all per-open files for a day via glob and
merges them into a single daily parquet.

Designed for the cold-archive path: writes are append-only. Each call to
``append()`` flushes the underlying file descriptor after the line write,
so a crash loses at most the bytes still buffered inside zstd's current
compression block (up to ~128 KiB worth of pending writes). Already-flushed
blocks remain readable via ``zstandard.ZstdDecompressor().stream_reader``,
which tolerates a missing zstd frame footer.
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd


class JsonlArchiveWriter:
    """Append rows to per-open-window ZSTD-compressed JSONL files.

    Each currently-open file is named:
    ``<archive_dir>/YYYY-MM-DD.<pid>.<open_unix_ts>.jsonl.zst``

    Rotation rules: a new file is opened when (a) the snapshot_ts UTC date
    changes vs the currently-open file's date, or (b) ``rotate_interval_sec``
    is set and that many wall-clock seconds have elapsed since the current
    file was opened. The collector calls ``close()`` on shutdown (and a
    later S3 uploader task calls it after each rotation) to flush the
    active file.

    Per-open filenames eliminate the multi-frame zstd hazard from the
    original single-daily-file scheme: each rotation window owns its file
    exclusively, so no abandoned-frame boundary can form — whether that
    window boundary comes from a process restart, a UTC-midnight crossing,
    or a fixed rotation interval.
    """

    def __init__(self, archive_dir: Path | str, rotate_interval_sec: int | None = None):
        """Create or reopen an archive directory; no file is opened yet.

        Captures the process ID at construction time; it is embedded in
        every filename this instance writes so that files from different
        collector processes never collide. The open-time timestamp token
        is captured per-file, when each file is actually opened (see
        ``_rotate_to``), not once at construction.

        ``rotate_interval_sec``, when set, additionally rotates the open
        file once that many wall-clock seconds have elapsed since it was
        opened — independent of the UTC-midnight rule. ``None`` (the
        default) preserves the original midnight-only rotation behavior.

        The first call to ``append()`` opens the file for that snapshot's
        UTC date.
        """
        self._archive_dir = Path(archive_dir)
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._pid = os.getpid()
        self._rotate_interval_sec = rotate_interval_sec
        self._open_date: date | None = None
        self._open_wall_ts: float | None = None
        self._open_path: Path | None = None
        self._open_fh = None
        self._raw_fh = None
        self._compressor = None

    def _filename_for(self, target_date: date) -> str:
        """Return a filename stem for ``target_date``, unique within ``archive_dir``.

        Format: ``YYYY-MM-DD.<pid>.<open_unix_ts>.jsonl.zst``. The
        timestamp token is the current wall-clock time (open time, not
        process-startup time), so distinct rotation windows within the
        same process get distinct filenames. Ties are only possible at a
        date-boundary-plus-interval race; ``ts`` is incremented until the
        candidate path doesn't already exist.
        """
        ts = int(time.time())
        while True:
            candidate = f"{target_date.isoformat()}.{self._pid}.{ts}.jsonl.zst"
            if not (self._archive_dir / candidate).exists():
                return candidate
            ts += 1

    def append(self, row: dict[str, Any], snapshot_ts: datetime) -> None:
        """Write one JSON line for ``row`` to the file for snapshot_ts's UTC date.

        ``snapshot_ts`` MUST be naive UTC (project-wide convention; see
        CLAUDE.md). Rotates the open file if the date has changed, or if
        ``rotate_interval_sec`` is set and that many wall-clock seconds
        have elapsed since the current file was opened.

        The underlying file is flushed after each line. A crash loses at
        most the bytes still buffered inside zstd's in-progress block;
        all earlier blocks are readable via ``ZstdDecompressor().stream_reader``.
        """
        target_date = snapshot_ts.date()
        needs_rotation = self._open_date != target_date
        if (
            not needs_rotation
            and self._rotate_interval_sec is not None
            and self._open_wall_ts is not None
            and time.time() - self._open_wall_ts >= self._rotate_interval_sec
        ):
            needs_rotation = True
        if needs_rotation:
            self._rotate_to(target_date)

        line = json.dumps(row, default=str) + "\n"
        self._open_fh.write(line.encode("utf-8"))
        self._open_fh.flush()

    def _rotate_to(self, target_date: date) -> None:
        """Close any open file and open a fresh file for ``target_date``."""
        self.close()
        path = self._archive_dir / self._filename_for(target_date)
        # Open in write-binary mode — each rotation window's file is written
        # exactly once from scratch; there is no mid-window append.
        self._raw_fh = open(path, "wb")
        # zstd streaming compressor; level 3 = good balance of
        # compression ratio vs CPU on a steady ingest workload.
        self._compressor = zstd.ZstdCompressor(level=3)
        self._open_fh = self._compressor.stream_writer(self._raw_fh)
        self._open_date = target_date
        self._open_wall_ts = time.time()
        self._open_path = path

    @property
    def open_path(self) -> Path | None:
        """Return the path of the currently-open file, or ``None`` if none is open.

        Used by the S3 uploader to skip the file that's still being
        written to — only CLOSED files are safe to ship.
        """
        return self._open_path

    def close(self) -> Path | None:
        """Flush the zstd footer and close both the writer and the underlying file.

        Two separate handles to close: the stream_writer flushes the zstd
        compression footer; the underlying raw file holds the OS handle.
        Closing the stream_writer alone leaks the raw handle.

        Returns the path of the file that was just closed, or ``None`` if
        no file was open — so callers (e.g. an upload loop) know exactly
        which file just became safe to ship.
        """
        closed_path = self._open_path
        if self._open_fh is not None:
            self._open_fh.close()  # flushes zstd footer to self._raw_fh
            self._open_fh = None
        if self._raw_fh is not None:
            self._raw_fh.close()
            self._raw_fh = None
        self._open_date = None
        self._open_wall_ts = None
        self._open_path = None
        self._compressor = None
        return closed_path
