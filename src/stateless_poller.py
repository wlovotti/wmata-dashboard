"""Pure logic for the stateless collector: ping gating, VP archiving, upload cycles.

Kept import-safe (no boto3 client construction, no network at import) so
`scripts/stateless_collector.py` stays a thin loop and everything here is
unit-testable. Spec: docs/superpowers/specs/2026-08-22-stateless-collector-design.md §1–2.

Rotation reminder (Task 1): ``JsonlArchiveWriter`` rotation is
append-driven only — if a feed goes quiet, no ``append()`` call means no
rotation, so the currently-open file never closes on its own. The loop's
shutdown path (``scripts/stateless_collector.py``) explicitly calls
``close()`` on both writers so the final partial file becomes shippable;
during a quiet period nothing ships and ``PingGate`` correctly stops
pinging — that silence is the intended failure signal, not a bug.
"""

from datetime import datetime
from pathlib import Path

from src.archive_writer import JsonlArchiveWriter
from src.deadman import ping_healthcheck


class PingGate:
    """Dead-man gate: ping only while BOTH feeds are shipping fresh data.

    ``record_ship(feed, now)`` marks a feed as having shipped an S3 object.
    ``maybe_ping(now)`` pings when every recorded feed shipped within
    ``freshness_sec`` AND both "tu" and "vp" have shipped at least once,
    rate-limited to one ping per ``min_gap_sec``. A wedged feed therefore
    silences the check within ~freshness_sec — this single signal subsumes
    the VP-path dead-man coverage item (NOTES-94; see spec §2).
    """

    def __init__(self, url: str | None, freshness_sec: int = 1200, min_gap_sec: int = 300):
        """Configure the gate for healthcheck ``url`` with freshness/rate-limit windows.

        ``url`` is passed straight through to ``ping_healthcheck`` on every
        ping, including ``None`` (which disables pinging there).
        """
        self._url = url
        self._freshness_sec = freshness_sec
        self._min_gap_sec = min_gap_sec
        self._last_ship: dict[str, float] = {}
        self._last_ping: float | None = None

    def record_ship(self, feed: str, now: float) -> None:
        """Mark ``feed`` ("tu" | "vp") as having shipped an object at ``now``."""
        self._last_ship[feed] = now

    def maybe_ping(self, now: float) -> bool:
        """Ping the healthcheck if both feeds are fresh; return whether we pinged."""
        both_fresh = all(
            now - self._last_ship.get(feed, float("-inf")) <= self._freshness_sec
            for feed in ("tu", "vp")
        )
        gap_ok = self._last_ping is None or now - self._last_ping >= self._min_gap_sec
        if both_fresh and gap_ok:
            ping_healthcheck(self._url)
            self._last_ping = now
            return True
        return False


def archive_tu_rows(writer: JsonlArchiveWriter, rows: list[dict]) -> int:
    """Append TU rows in the exact format `_save_trip_updates` archives them.

    One line per (trip, stop) row, keyed by the row's own ``snapshot_ts`` —
    byte-format compatibility with pipelines/replay_archive_to_state.py is
    an acceptance test (tests/test_stateless_replay_compat.py).
    """
    for row in rows:
        writer.append(row, snapshot_ts=row["snapshot_ts"])
    return len(rows)


def archive_vp_rows(
    writer: JsonlArchiveWriter, vehicles: list[dict], collected_at: datetime
) -> int:
    """Append one line per vehicle, adding the poll's ``collected_at`` (naive UTC).

    The raw GTFS-RT epoch ``timestamp`` is preserved untouched — the
    NOTES-81 phantom-timestamp guard is applied by the laptop-side loader,
    never at collection (raw stays raw; spec §1).
    """
    for vehicle in vehicles:
        row = dict(vehicle)
        row["collected_at"] = collected_at.isoformat()
        writer.append(row, snapshot_ts=collected_at)
    return len(vehicles)


def run_upload_cycle(uploader, streams, gate: PingGate, now: float) -> list[str]:
    """Ship every closed file across all feed streams, then maybe ping.

    ``streams`` is a list of ``(feed, archive_dir, key_prefix, writer)``.
    The writer's currently-open file is skipped; everything else directly
    under the dir is closed by construction. Prunes the 48-hour uploaded/
    buffer as it goes.
    """
    shipped_all: list[str] = []
    for feed, archive_dir, key_prefix, writer in streams:
        skip = {writer.open_path} if writer.open_path is not None else set()
        shipped = uploader.upload_closed_files(Path(archive_dir), key_prefix, skip)
        if shipped:
            gate.record_ship(feed, now)
            shipped_all.extend(shipped)
        uploader.prune_uploaded(Path(archive_dir))
    gate.maybe_ping(now)
    return shipped_all
