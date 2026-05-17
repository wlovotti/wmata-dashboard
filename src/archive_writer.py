"""Streaming JSONL writer for raw WMATA TripUpdate snapshots.

The writer appends one JSON line per (trip, stop) per snapshot to a
ZSTD-compressed file named by UTC date. Files rotate automatically when
the snapshot timestamp crosses a UTC midnight boundary.

Designed for the cold-archive path: writes are append-only. Each call to
``append()`` flushes the underlying file descriptor after the line write,
so a crash loses at most the bytes still buffered inside zstd's current
compression block (up to ~128 KiB worth of pending writes). Already-flushed
blocks remain readable via ``zstandard.ZstdDecompressor().stream_reader``,
which tolerates a missing zstd frame footer.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd


class JsonlArchiveWriter:
    """Append rows to daily ZSTD-compressed JSONL files.

    Rotation rule: a new file is opened when the snapshot_ts UTC date
    changes vs the currently-open file's date. The collector calls
    ``close()`` on shutdown to flush the active file.
    """

    def __init__(self, archive_dir: Path | str):
        """Create or reopen an archive directory; no file is opened yet.

        The first call to ``append()`` opens the file for that snapshot's
        UTC date.
        """
        self._archive_dir = Path(archive_dir)
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._open_date: date | None = None
        self._open_fh = None
        self._raw_fh = None
        self._compressor = None

    def append(self, row: dict[str, Any], snapshot_ts: datetime) -> None:
        """Write one JSON line for ``row`` to the file for snapshot_ts's UTC date.

        ``snapshot_ts`` MUST be naive UTC (project-wide convention; see
        CLAUDE.md). Rotates the open file if the date has changed.

        The underlying file is flushed after each line. A crash loses at
        most the bytes still buffered inside zstd's in-progress block;
        all earlier blocks are readable via ``ZstdDecompressor().stream_reader``.
        """
        target_date = snapshot_ts.date()
        if self._open_date != target_date:
            self._rotate_to(target_date)

        line = json.dumps(row, default=str) + "\n"
        self._open_fh.write(line.encode("utf-8"))
        self._open_fh.flush()

    def _rotate_to(self, target_date: date) -> None:
        """Close any open file and open the file for ``target_date``."""
        self.close()
        path = self._archive_dir / f"{target_date.isoformat()}.jsonl.zst"
        # Open in append-binary mode so reopening today's file preserves
        # earlier rows (collector restart mid-day).
        self._raw_fh = open(path, "ab")
        # zstd streaming compressor; level 3 = good balance of
        # compression ratio vs CPU on a steady ingest workload.
        self._compressor = zstd.ZstdCompressor(level=3)
        self._open_fh = self._compressor.stream_writer(self._raw_fh)
        self._open_date = target_date

    def close(self) -> None:
        """Flush the zstd footer and close both the writer and the underlying file.

        Two separate handles to close: the stream_writer flushes the zstd
        compression footer; the underlying raw file holds the OS handle.
        Closing the stream_writer alone leaks the raw handle.
        """
        if self._open_fh is not None:
            self._open_fh.close()  # flushes zstd footer to self._raw_fh
            self._open_fh = None
        if self._raw_fh is not None:
            self._raw_fh.close()
            self._raw_fh = None
        self._open_date = None
        self._compressor = None
