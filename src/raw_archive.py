"""Per-date raw-archive file selection, S3 fetch, and verify-then-prune.

The raw GTFS-RT archive lives permanently in S3
(``s3://<bucket>/raw-jsonl-archive/`` — root = WMATA trip updates, ``vp/``,
``sfmta/``, ``sfmta_vp/``); the laptop only needs the files one service
date's derive reads, for as long as it takes to process that date. This
module answers three questions for the day-at-a-time refresh
(``pipelines/refresh_by_day.py``) and the standalone prune tool
(``scripts/prune_local_archive.py``):

1. **Which files does service date D need?** Archive files are named by
   the UTC calendar date of the row timestamp the collector keyed them on
   (``JsonlArchiveWriter.append``: ``snapshot_ts.date()``), never by the
   agency-local service date:

   - trip updates (keyed on ``snapshot_ts``): ``replay_archive_to_state``
     reads UTC date D (primary) plus UTC date D+1 (the late-evening
     supplement) — :func:`tu_utc_dates`.
   - vehicle positions (keyed on the poll's ``collected_at``): every VP
     reader (``derive_stop_events*``, ``src/trip_match_rate.py``) bounds
     ``vehicle_positions.timestamp`` by
     ``local_service_date_position_window_utc`` = [local midnight D, local
     midnight D+2), and the loader only keeps rows whose ``timestamp`` is
     within ``VP_TIMESTAMP_GUARD_SEC`` of ``collected_at`` — so the
     needed files are the UTC dates that window, widened by the guard,
     touches. For both agencies (UTC-4..-8) that is UTC D, D+1, D+2 —
     :func:`vp_utc_dates`.

2. **Fetch exactly those files** (:func:`fetch_day_files`): one
   ``list_objects_v2`` per UTC date with prefix ``<stream prefix><date>.``
   (which cannot match a sibling sub-prefix's keys), then download
   whatever is missing locally or a different size, verifying size and
   ETag before the file is renamed into place.

3. **Is a local file safe to delete?** (:func:`plan_prune`) Only when the
   same key exists in S3 with the identical byte size AND identical
   content checksum (the S3 ETag: plain MD5 for single-part uploads, the
   MD5-of-part-MD5s for multipart uploads at the 8 MiB part size boto3 /
   the AWS CLI use — anything else is "unverifiable" and refused), AND
   the file is recorded as processed in its manifest
   (``tu_archive_replayed_files`` / ``vp_archive_loaded_files``), AND its
   UTC date is not needed by any date still to be processed.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from src.timezones import local_service_date_position_window_utc

TU = "tu"
VP = "vp"

# Rows whose vehicle-reported timestamp is further than this from the poll's
# collected_at are dropped at load (phantom-timestamp guard). Single source
# for pipelines/load_vp_archive.py's GUARD_SEC and vp_utc_dates below.
VP_TIMESTAMP_GUARD_SEC = 1800

# boto3's managed transfer and the AWS CLI both default to an 8 MiB
# multipart threshold and part size; every multipart object observed in
# the raw-jsonl-archive prefixes has a part count consistent with it.
MULTIPART_CHUNK_BYTES = 8 * 1024 * 1024

# ``YYYY-MM-DD.<pid>.<open_epoch>.jsonl.zst`` (PR #132 onward) or the legacy
# single-file ``YYYY-MM-DD.jsonl.zst``.
_ARCHIVE_NAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:\.\d+\.\d+)?\.jsonl\.zst$")


@dataclass(frozen=True)
class ArchiveStream:
    """One agency's raw feed archive: a local directory mirroring one S3 prefix."""

    agency: str
    kind: str  # TU or VP
    local_dir: Path
    bucket: str
    prefix: str  # S3 key prefix, ends with "/"
    tz_name: str

    @property
    def label(self) -> str:
        """Short human label, e.g. ``wmata/tu``."""
        return f"{self.agency}/{self.kind}"


def streams_for_agency(
    cfg, repo_root: Path, tu_dir: Path | None = None, vp_dir: Path | None = None
) -> tuple[ArchiveStream, ArchiveStream]:
    """Return ``(tu_stream, vp_stream)`` for one agency config.

    Local directories default to the agency config's ``archive_dir`` /
    ``vp_archive_dir`` under ``repo_root``; ``tu_dir`` / ``vp_dir`` override
    them (``bin/pull-and-derive.sh``'s ``LOCAL_ARCHIVE*`` variables).
    """
    tu = ArchiveStream(
        agency=cfg.name,
        kind=TU,
        local_dir=Path(tu_dir) if tu_dir else repo_root / cfg.archive_dir,
        bucket=cfg.s3_bucket,
        prefix=cfg.s3_tu_prefix,
        tz_name=cfg.timezone,
    )
    vp = ArchiveStream(
        agency=cfg.name,
        kind=VP,
        local_dir=Path(vp_dir) if vp_dir else repo_root / cfg.vp_archive_dir,
        bucket=cfg.s3_bucket,
        prefix=cfg.s3_vp_prefix,
        tz_name=cfg.timezone,
    )
    return tu, vp


def archive_file_date(name: str) -> date | None:
    """Return the UTC date encoded in an archive filename, or None if unrecognized."""
    m = _ARCHIVE_NAME_RE.match(name)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def tu_utc_dates(service_date: date) -> list[date]:
    """UTC-dated TU files ``replay_archive_to_state`` reads for ``service_date``.

    UTC date D (primary) and D+1 (supplement) — mirrors the two globs in
    ``replay_archive_for_date``, which are valid only for agencies west of
    UTC (enforced there by ``_assert_west_of_utc``).
    """
    return [service_date, service_date + timedelta(days=1)]


def vp_utc_dates(service_date: date, tz_name: str) -> list[date]:
    """UTC-dated VP files whose rows can fall in ``service_date``'s VP window.

    The derive/guard readers filter ``timestamp`` to
    ``local_service_date_position_window_utc(service_date, tz_name)``
    (half-open); a loaded row's ``collected_at`` (which names its file) is
    within ``VP_TIMESTAMP_GUARD_SEC`` of its ``timestamp``. Every UTC date
    the guard-widened window touches is returned, ascending.
    """
    start, end = local_service_date_position_window_utc(service_date, tz_name)
    guard = timedelta(seconds=VP_TIMESTAMP_GUARD_SEC)
    first = (start - guard).date()
    last = (end + guard - timedelta(microseconds=1)).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def utc_dates_needed(stream: ArchiveStream, service_date: date) -> list[date]:
    """UTC file dates ``stream`` must have locally to process ``service_date``."""
    if stream.kind == TU:
        return tu_utc_dates(service_date)
    return vp_utc_dates(service_date, stream.tz_name)


def last_unneeded_utc_date(stream: ArchiveStream, next_service_date: date) -> date:
    """Latest UTC file date that ``next_service_date`` (and later dates) no longer need.

    Files dated strictly before the first UTC date the next service date
    reads are done with once every earlier service date has been processed.
    """
    return min(utc_dates_needed(stream, next_service_date)) - timedelta(days=1)


@dataclass(frozen=True)
class S3Object:
    """One raw-archive object in S3 (basename, byte size, ETag without quotes)."""

    name: str
    size: int
    etag: str


def _list_objects(s3, bucket: str, key_prefix: str) -> Iterable[dict]:
    """Yield every ``list_objects_v2`` entry under ``key_prefix`` (paginated, no recursion).

    ``Delimiter="/"`` keeps keys under a deeper sub-prefix (``vp/...`` when
    listing the WMATA root prefix) out of the result.
    """
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": key_prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        yield from resp.get("Contents", []) or []
        if not resp.get("IsTruncated"):
            return
        token = resp.get("NextContinuationToken")


def list_s3_objects(s3, stream: ArchiveStream, name_prefix: str) -> dict[str, S3Object]:
    """Return ``{basename: S3Object}`` for ``stream`` keys starting with ``name_prefix``.

    ``name_prefix`` is a filename prefix such as ``"2026-10-03."`` (one UTC
    day) or ``"2026-10-"`` (one month).
    """
    out: dict[str, S3Object] = {}
    for entry in _list_objects(s3, stream.bucket, stream.prefix + name_prefix):
        key = entry["Key"]
        name = key[len(stream.prefix) :]
        if "/" in name or archive_file_date(name) is None:
            continue
        out[name] = S3Object(name=name, size=int(entry["Size"]), etag=entry["ETag"].strip('"'))
    return out


def list_s3_day(s3, stream: ArchiveStream, utc_date: date) -> dict[str, S3Object]:
    """Return every ``stream`` object for one UTC file date (both naming schemes)."""
    return list_s3_objects(s3, stream, f"{utc_date.isoformat()}.")


def compute_local_etag(path: Path, part_count: int | None) -> str:
    """Compute the S3-style ETag of a local file.

    ``part_count=None`` → plain MD5 hex (single-part upload). Otherwise the
    multipart form ``md5(concat(md5(part_i)))-N`` with
    ``MULTIPART_CHUNK_BYTES`` parts.
    """
    if part_count is None:
        h = hashlib.md5()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    digests = []
    with open(path, "rb") as fh:
        while True:
            part = fh.read(MULTIPART_CHUNK_BYTES)
            if not part:
                break
            digests.append(hashlib.md5(part).digest())
    return f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(digests)}"


def verify_local_against_s3(path: Path, obj: S3Object) -> str | None:
    """Return None when ``path`` matches ``obj`` byte-for-byte, else the reason it doesn't.

    Size first (free), then the ETag checksum. A multipart ETag whose part
    count is inconsistent with 8 MiB parts cannot be recomputed locally and
    is reported as unverifiable rather than accepted on size alone.
    """
    size = path.stat().st_size
    if size != obj.size:
        return f"size mismatch (local={size} s3={obj.size})"
    etag = obj.etag
    if "-" in etag:
        _, _, suffix = etag.partition("-")
        try:
            parts = int(suffix)
        except ValueError:
            return f"unverifiable S3 ETag {etag!r}"
        expected = max(1, math.ceil(size / MULTIPART_CHUNK_BYTES))
        if parts != expected:
            return (
                f"unverifiable checksum (S3 ETag has {parts} parts; 8 MiB parts would give "
                f"{expected})"
            )
        local = compute_local_etag(path, parts)
    else:
        local = compute_local_etag(path, None)
    if local != etag:
        return f"checksum mismatch (local ETag {local} != S3 {etag})"
    return None


@dataclass
class FetchResult:
    """Outcome of fetching one stream's files for one UTC date."""

    utc_date: date
    s3_objects: dict[str, S3Object]
    downloaded: list[str]
    already_present: list[str]
    bytes_downloaded: int


def fetch_day_files(s3, stream: ArchiveStream, utc_date: date, log=print) -> FetchResult:
    """Make ``stream.local_dir`` hold every S3 object for ``utc_date``.

    A local file already matching the S3 size is kept as-is (the cheap
    check; the full checksum runs before anything is pruned). Anything
    missing or a different size is downloaded to ``<name>.part``, verified
    (size + ETag), and atomically renamed into place, so an interrupted
    download never leaves a truncated ``*.jsonl.zst`` for replay or the
    loader to read. A UTC date with no objects (a future date, or an
    archive gap) is not an error here — replay / the loader decide whether
    an empty date is acceptable.

    Raises:
        RuntimeError: if a downloaded file fails verification.
    """
    stream.local_dir.mkdir(parents=True, exist_ok=True)
    objects = list_s3_day(s3, stream, utc_date)
    downloaded: list[str] = []
    present: list[str] = []
    nbytes = 0
    for name in sorted(objects):
        obj = objects[name]
        dest = stream.local_dir / name
        if dest.exists() and dest.stat().st_size == obj.size:
            present.append(name)
            continue
        part = stream.local_dir / f"{name}.part"
        try:
            s3.download_file(stream.bucket, stream.prefix + name, str(part))
            problem = verify_local_against_s3(part, obj)
            if problem is not None:
                raise RuntimeError(
                    f"{stream.label} {name}: downloaded copy failed verification: {problem}"
                )
            os.replace(part, dest)
        finally:
            if part.exists():
                part.unlink()
        downloaded.append(name)
        nbytes += obj.size
    log(
        f"  fetch {stream.label} {utc_date}: {len(objects)} object(s) in S3, "
        f"{len(downloaded)} downloaded ({nbytes / 1e9:.2f} GB), {len(present)} already local"
    )
    return FetchResult(utc_date, objects, downloaded, present, nbytes)


# --- prune -------------------------------------------------------------------

DELETE = "delete"
KEEP = "keep"  # still needed by a date not yet processed — not a refusal
REFUSE = "refuse"


@dataclass(frozen=True)
class PruneDecision:
    """What the prune logic decided for one local file, and why."""

    path: Path
    size: int
    action: str  # DELETE / KEEP / REFUSE
    reason: str


def local_archive_files(local_dir: Path) -> list[Path]:
    """Every regular file directly under ``local_dir`` (sorted), or [] if absent."""
    if not local_dir.is_dir():
        return []
    return sorted(p for p in local_dir.iterdir() if p.is_file())


def plan_prune(
    paths: Iterable[Path],
    s3_index: dict[str, S3Object],
    processed: set[str],
    max_utc_date: date | None,
    verify: Callable[[Path, S3Object], str | None] = verify_local_against_s3,
) -> list[PruneDecision]:
    """Decide, per local file, whether it may be deleted.

    A file is deleted only when ALL hold: its name is a recognized archive
    filename; its UTC date is ``<= max_utc_date`` (otherwise it is KEPT —
    a date still to be processed reads it; ``None`` disables this check);
    it is recorded in ``processed`` (the stream's manifest); the same key
    is in ``s3_index``; and ``verify`` reports a byte-identical S3 copy.
    Every other file is REFUSED with the first failing reason. Checks run
    cheapest-first so the checksum is only computed for otherwise-eligible
    files.
    """
    decisions: list[PruneDecision] = []
    for path in paths:
        size = path.stat().st_size
        d = archive_file_date(path.name)
        if d is None:
            decisions.append(PruneDecision(path, size, REFUSE, "unrecognized filename"))
            continue
        if max_utc_date is not None and d > max_utc_date:
            decisions.append(
                PruneDecision(path, size, KEEP, f"UTC date {d} still needed (> {max_utc_date})")
            )
            continue
        if path.name not in processed:
            decisions.append(
                PruneDecision(path, size, REFUSE, "not recorded as processed in manifest")
            )
            continue
        obj = s3_index.get(path.name)
        if obj is None:
            decisions.append(PruneDecision(path, size, REFUSE, "missing from S3"))
            continue
        problem = verify(path, obj)
        if problem is not None:
            decisions.append(PruneDecision(path, size, REFUSE, problem))
            continue
        decisions.append(PruneDecision(path, size, DELETE, "verified in S3 + processed"))
    return decisions


def execute_prune(decisions: Iterable[PruneDecision], dry_run: bool) -> tuple[int, int]:
    """Delete every DELETE decision's file unless ``dry_run``; return ``(count, bytes)``.

    Under ``dry_run`` nothing is touched and the return value is what would
    be freed.
    """
    count = nbytes = 0
    for d in decisions:
        if d.action != DELETE:
            continue
        if not dry_run:
            d.path.unlink()
        count += 1
        nbytes += d.size
    return count, nbytes


def summarize_prune(label: str, decisions: list[PruneDecision], dry_run: bool) -> list[str]:
    """Human-readable summary lines: per-action counts/bytes, then every refusal."""
    by_action: dict[str, list[PruneDecision]] = {DELETE: [], KEEP: [], REFUSE: []}
    for d in decisions:
        by_action[d.action].append(d)
    verb = "would delete" if dry_run else "deleted"
    lines = [
        f"{label}: {verb} {len(by_action[DELETE])} file(s) "
        f"({sum(d.size for d in by_action[DELETE]) / 1e9:.2f} GB); "
        f"kept {len(by_action[KEEP])} still-needed; refused {len(by_action[REFUSE])} "
        f"({sum(d.size for d in by_action[REFUSE]) / 1e9:.2f} GB)"
    ]
    for d in by_action[REFUSE]:
        lines.append(f"  REFUSE {d.path.name} ({d.size} bytes): {d.reason}")
    return lines


def processed_names(session, stream: ArchiveStream, names: list[str]) -> set[str]:
    """Return the subset of ``names`` recorded in ``stream``'s manifest table.

    TU: ``tu_archive_replayed_files`` (any target date — a file folded as a
    supplement for D-1 and as a primary for D appears twice). VP:
    ``vp_archive_loaded_files``.
    """
    from src.models import TuArchiveReplayedFile, VpArchiveLoadedFile

    col = TuArchiveReplayedFile.filename if stream.kind == TU else VpArchiveLoadedFile.filename
    found: set[str] = set()
    for i in range(0, len(names), 1000):
        chunk = names[i : i + 1000]
        found.update(row[0] for row in session.query(col).filter(col.in_(chunk)).distinct())
    return found


def prune_stream(
    s3,
    session,
    stream: ArchiveStream,
    max_utc_date: date | None,
    dry_run: bool,
    min_utc_date: date | None = None,
) -> tuple[list[PruneDecision], int, int]:
    """Plan and (unless ``dry_run``) execute the prune for one stream.

    Lists S3 once per month present among the candidate files, looks the
    candidates up in the manifest, and applies :func:`plan_prune`.
    ``min_utc_date`` restricts the candidates to recognized archive files
    dated on/after it (the per-day refresh passes the range it just
    processed so a run never re-verifies the whole directory; the
    standalone tool passes None and so also reports unrecognized files). Returns ``(decisions, count, bytes)``.
    """
    paths = local_archive_files(stream.local_dir)
    if min_utc_date is not None:
        paths = [
            p for p in paths if (d := archive_file_date(p.name)) is not None and d >= min_utc_date
        ]
    months = sorted(
        {d.strftime("%Y-%m-") for p in paths if (d := archive_file_date(p.name)) is not None}
    )
    s3_index: dict[str, S3Object] = {}
    for m in months:
        s3_index.update(list_s3_objects(s3, stream, m))
    processed = processed_names(session, stream, [p.name for p in paths])
    decisions = plan_prune(paths, s3_index, processed, max_utc_date)
    count, nbytes = execute_prune(decisions, dry_run)
    return decisions, count, nbytes
