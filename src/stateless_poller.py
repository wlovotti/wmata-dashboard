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

import logging
import time
from datetime import datetime
from pathlib import Path

from src.archive_writer import JsonlArchiveWriter
from src.deadman import ping_healthcheck

logger = logging.getLogger(__name__)

# Module-level indirection so tests can monkeypatch this specific reference
# (PR #235 review) instead of `src.stateless_poller.time.monotonic`,
# which is the SAME object as the process-wide `time` stdlib module — patching
# it there would mutate `time.monotonic` for every other consumer in the test
# process, not just this module.
_monotonic_clock = time.monotonic


class PingGate:
    """Dead-man gate: ping only while BOTH feeds are shipping fresh data.

    ``record_ship(feed, now)`` marks a feed as having shipped an S3 object.
    ``maybe_ping(now)`` pings when every recorded feed shipped within
    ``freshness_sec`` AND both "tu" and "vp" have shipped at least once,
    rate-limited to one ping per ``min_gap_sec``. A wedged feed therefore
    silences the check within ~freshness_sec — this single signal covers a
    VP-only collector failure just as well as a whole-process death (see
    spec §2), which a TU-only ping historically missed.
    """

    def __init__(self, url: str | None, freshness_sec: int = 1200, min_gap_sec: int = 300):
        """Configure the gate for healthcheck ``url`` with freshness/rate-limit windows.

        ``url`` is passed straight through to ``ping_healthcheck`` on every
        ping, including ``None`` (which disables pinging there).

        ``freshness_sec``'s default (1200s = 20 min) is intentionally
        looser than ``scripts/stateless_collector.py``'s
        ``ROTATE_INTERVAL_SEC`` (900s = 15 min): the ~5-minute margin is
        the slack docs/DEPLOYMENT.md §13.5's grace-period math already
        assumes. If either constant changes, re-check that margin rather
        than letting it silently erode or balloon (PR #235 — named the
        freshness/cadence slack as a tunable).

        ``record_ship``/``maybe_ping`` take a caller-supplied ``now`` —
        see ``run_upload_cycle``'s docstring for why that clock must be
        monotonic (PR #235 — monotonic PingGate clock).
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
    phantom-timestamp guard (stale AVL clocks reporting fixes hours or
    months in the past) is applied by the laptop-side loader, never at
    collection (raw stays raw; spec §1).
    """
    for vehicle in vehicles:
        row = dict(vehicle)
        row["collected_at"] = collected_at.isoformat()
        writer.append(row, snapshot_ts=collected_at)
    return len(vehicles)


def run_upload_cycle(uploader, streams, gate: PingGate, now: float | None = None) -> list[str]:
    """Ship every closed file across all feed streams, then maybe ping.

    ``streams`` is a list of ``(feed, archive_dir, key_prefix, writer)``.
    The writer's currently-open file is skipped; everything else directly
    under the dir is closed by construction. Prunes the 48-hour uploaded/
    buffer as it goes.

    ``now`` defaults to ``time.monotonic()`` (PR #235 — monotonic PingGate
    clock): it drives
    only ``PingGate``'s freshness/rate-limit window math, which must be
    immune to an NTP step or manual wall-clock change mid-run. Nothing
    that lands in persisted artifacts or S3 key names (row timestamps,
    ``JsonlArchiveWriter``'s filename tokens) goes through this value —
    those still use wall-clock time elsewhere, untouched by this default.
    Tests pass an explicit ``now`` to control the gate deterministically.

    Each stream's upload+prune is isolated in its own try/except
    (PR #235 — per-stream upload error isolation): a failure on one feed (e.g. an
    ``UploadVerificationError`` on "tu") is logged and does not skip the
    other, otherwise-independent feed's upload or prune.

    ``gate.maybe_ping`` is only called when NO stream raised this cycle
    (PR #235 review): ``PingGate``'s freshness check alone is
    not enough to withhold the ping on a total outage, because
    ``_last_ship`` still holds each feed's last *successful* ship from a
    prior, healthy cycle — a revoked IAM key (or any other error on
    every stream) would otherwise still read as "fresh" for up to
    ``freshness_sec`` and keep the dead-man pinged, silently regressing
    docs/DEPLOYMENT.md §13.5's outage-detection math. Withholding the
    ping on ANY stream error (not just a total one) also matches
    pre-per-stream-isolation behavior, where a single try/except around
    the whole cycle skipped ``maybe_ping`` on any exception. The first
    exception encountered is re-raised after every stream has had a
    chance to run, so it remains visible to the caller (the collector
    loop's per-cycle try/except already logs and continues).
    """
    if now is None:
        now = _monotonic_clock()
    shipped_all: list[str] = []
    first_error: Exception | None = None
    for feed, archive_dir, key_prefix, writer in streams:
        try:
            skip = {writer.open_path} if writer.open_path is not None else set()
            shipped = uploader.upload_closed_files(Path(archive_dir), key_prefix, skip)
            if shipped:
                gate.record_ship(feed, now)
                shipped_all.extend(shipped)
            uploader.prune_uploaded(Path(archive_dir))
        except Exception as exc:
            logger.error("upload cycle: %s stream failed: %r", feed, exc)
            if first_error is None:
                first_error = exc
    if first_error is None:
        gate.maybe_ping(now)
    else:
        raise first_error
    return shipped_all
