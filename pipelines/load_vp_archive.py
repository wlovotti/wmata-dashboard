"""Load raw VP JSONL archives into vehicle_positions (spec 2026-08-22 §3).

Replaces the retired \\copy-over-tunnel VP delta: the stateless collector
archives VehiclePositions as jsonl.zst in S3; this loader parses each
synced file once (manifest-table idempotency) and applies the
phantom-timestamp guard at load time — raw files stay raw.

    uv run python pipelines/load_vp_archive.py --agency wmata
    uv run python pipelines/load_vp_archive.py --agency sfmta
    uv run python pipelines/load_vp_archive.py --agency wmata \\
        --utc-date 2026-10-03 --utc-date 2026-10-04          # scoped
    uv run python pipelines/load_vp_archive.py --agency wmata \\
        --utc-date 2026-10-03 --force                         # reload a day

Re-loadability (issue #261): ``vehicle_positions`` has no natural unique
key, so "load each file once" (the manifest) is the only thing preventing
duplicates — and a date whose VP rows were later pruned could never be
re-derived, because its files stayed manifested. ``--utc-date`` scopes a
run to the files named for those UTC dates and reconciles each day first
(see :func:`reconcile_utc_day`): a day whose rows are all still present
loads only its new files; a day whose rows were pruned (fully or
partially) is reloaded whole — delete exactly that day's rows, delete its
manifest rows, re-insert every file, re-manifest — in ONE transaction, so
a crash leaves the old state intact. The day is the unit because files
are named by the UTC date of each row's ``collected_at``
(``JsonlArchiveWriter``), so "rows with ``collected_at`` on day U" and
"rows loaded from files ``U.*``" are the same set — provided every row
came from those files, which the reconcile step checks (DB rows <=
manifested rows) before it will delete anything. A day with more rows
than its manifest (the VM era, through 2026-08-22) is left untouched when
there is nothing new to load, and refused when there is.

VP retention (``pipelines/apply_retention.py``, ``src/retention.py``)
deletes whole UTC days of ``collected_at`` and keeps their manifest rows, so
a pruned day reconciles as "fewer rows than manifested" and reloads whole.
It never deletes a day holding rows the manifest cannot account for unless
told to (``--include-unmanifested-vp``).
"""

import argparse
import io
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy import delete, func, insert, select

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import VehiclePosition, VpArchiveLoadedFile
from src.raw_archive import VP_TIMESTAMP_GUARD_SEC, archive_file_date
from src.timezones import from_epoch_naive_utc, utcnow_naive

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent

# Drop rows whose vehicle-reported timestamp is >30 min from the row's own
# poll time: the spec's "±15 min outside the 15-min rotation window" with
# row-level precision. Catches the documented +20-24h phantom-timestamp
# rows (stale AVL clocks); tolerates ordinary AVL lag.
GUARD_SEC = VP_TIMESTAMP_GUARD_SEC
INSERT_CHUNK = 5000


def parse_vp_line(obj: dict) -> dict | None:
    """Map one archived VP JSON line to VehiclePosition insert kwargs.

    Returns None when the row should be dropped: either the
    phantom-timestamp guard fires, or the row is missing a value one of
    ``VehiclePosition``'s NOT NULL columns requires (``vehicle_id``,
    ``latitude``, ``longitude`` — the feed can emit vehicles mid-assignment
    with no ID or fix yet). Passing a null through to the insert would
    raise an IntegrityError at chunk-flush time and abort the whole file,
    so these are filtered here instead. A missing/null ``timestamp`` falls
    back to ``collected_at`` (legacy collector behavior for
    un-timestamped vehicles).
    """
    if obj.get("vehicle_id") is None or obj.get("latitude") is None or obj.get("longitude") is None:
        return None

    collected_at = datetime.fromisoformat(obj["collected_at"])
    epoch = obj.get("timestamp")
    if epoch:
        ts = from_epoch_naive_utc(epoch)
        if abs((ts - collected_at).total_seconds()) > GUARD_SEC:
            return None
    else:
        ts = collected_at
    return {
        "vehicle_id": obj.get("vehicle_id"),
        "route_id": obj.get("route_id"),
        "trip_id": obj.get("trip_id"),
        "direction_id": obj.get("direction_id"),
        "trip_start_date": obj.get("trip_start_date"),
        "latitude": obj.get("latitude"),
        "longitude": obj.get("longitude"),
        "speed": obj.get("speed"),
        "current_stop_sequence": obj.get("current_stop_sequence"),
        "stop_id": obj.get("stop_id"),
        "current_status": obj.get("current_status"),
        "timestamp": ts,
        "collected_at": collected_at,
    }


def _zstd_frame_is_complete(path: Path) -> bool:
    """Return whether ``path``'s zstd stream ended with a valid frame footer.

    PR #235 (truncated-file manifest hardening): ``load_vp_file`` reads via ``stream_reader``, which
    (per ``archive_writer.py``'s docstring) tolerates a missing footer and
    silently stops at whatever full blocks were flushed before a crash —
    exactly the behavior that lets a crash-truncated file's readable
    prefix still load. That tolerance also means the read itself never
    signals truncation, so this is a second, cheap pass over the same
    (already-small, already-compressed) bytes using the lower-level
    ``decompressobj`` API, whose ``.eof`` flag is False when the frame
    epilogue never arrived — the zstandard-library equivalent of zlib's
    ``decompressobj().eof``/``unused_data`` pattern. Any decompression
    error while probing (corrupt bytes, not just a missing footer) also
    reports incomplete rather than propagating — this is a best-effort
    diagnostic, not a correctness gate.

    This only validates the FIRST zstd frame: ``decompressobj().eof``
    goes true as soon as one complete frame has been consumed, even if
    more (possibly truncated) frames follow in ``unused_data``. That is
    fine for the VP corpus specifically — every archive file is opened
    once with ``"wb"`` by ``JsonlArchiveWriter`` and written as a single
    zstd frame, so "first frame" and "whole file" coincide here. This
    helper is not safe to reuse as-is on a multi-frame stream.
    """
    try:
        obj = zstd.ZstdDecompressor().decompressobj()
        with open(path, "rb") as fh:
            while not obj.eof:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                obj.decompress(chunk)
        return obj.eof
    except (zstd.ZstdError, OSError):
        return False


def load_vp_file(session, path: Path) -> tuple[int, int]:
    """Load one archive file exactly once; returns (inserted, dropped).

    Rows + the manifest row commit in a single transaction, so a crash
    mid-file rolls back cleanly and the file re-loads next run. The zstd
    stream_reader tolerates a missing frame footer (crash-cut files). A
    line that fails to decode (JSON or UTF-8) OR that fails to *parse*
    into a row (e.g. ``parse_vp_line`` raising on an absurd epoch or a
    missing ``collected_at`` key) is dropped and counted rather than
    raised — one bad line must not poison the whole file.

    The stream is read as raw bytes (``io.BufferedReader`` over the zstd
    stream_reader), split into lines, and decoded by hand per line —
    NOT via ``io.TextIOWrapper``, whose ``for line in text_stream:``
    would raise ``UnicodeDecodeError`` from the iteration itself, outside
    any per-line try/except, escaping this function on real byte
    corruption (review round 2 caught this: the wrapper decodes eagerly
    before ``json.loads`` ever runs, so a try/except around only
    ``json.loads`` never actually saw a ``UnicodeDecodeError``).

    ``parse_vp_line(obj)`` runs INSIDE the same try as the decode (review
    round 3): well-formed JSON can still raise there —
    ``from_epoch_naive_utc`` on an out-of-range epoch raises ``ValueError``
    / ``OverflowError`` / ``OSError`` (exactly the documented phantom
    pattern, just extreme enough to overflow ``datetime`` instead of only
    tripping ``GUARD_SEC``), and ``datetime.fromisoformat`` or a missing
    ``collected_at`` key can raise ``ValueError`` / ``TypeError`` /
    ``KeyError``. Any of those must drop the one bad line, not the file.

    PR #235: the file is still marked loaded (row count and all) even
    when its zstd frame never got a proper footer — files are immutable
    once uploaded (verify-then-buffer in ``S3Uploader``), so a corrected
    re-upload under the same name is not expected in practice. A
    ``_zstd_frame_is_complete`` check runs anyway and logs a warning when
    the frame looks crash-truncated, purely so the (currently
    theoretical) gap is visible in pipeline/cron logs instead of silent —
    it does not change what gets inserted or manifested.
    """
    already = session.get(VpArchiveLoadedFile, path.name)
    if already is not None:
        print(f"  {path.name}: already loaded ({already.row_count} rows), skipping")
        return 0, 0

    inserted, dropped = _insert_vp_file(session, path)
    session.add(
        VpArchiveLoadedFile(
            filename=path.name,
            row_count=inserted,
            dropped_count=dropped,
            loaded_at=utcnow_naive(),
        )
    )
    session.commit()
    print(f"  {path.name}: inserted {inserted}, dropped {dropped}")
    return inserted, dropped


def _insert_vp_file(session, path: Path) -> tuple[int, int]:
    """Insert one archive file's rows (no manifest row, no commit); return (inserted, dropped).

    The shared body of :func:`load_vp_file` and :func:`reload_utc_day` —
    see ``load_vp_file`` for the per-line drop rules and the
    crash-truncated-frame warning.
    """
    inserted = dropped = 0
    batch: list[dict] = []
    with open(path, "rb") as fh:
        buffered = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(fh))
        for raw in buffered:
            try:
                obj = json.loads(raw.decode("utf-8"))
                row = parse_vp_line(obj)
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValueError,
                TypeError,
                KeyError,
                OverflowError,
                OSError,
            ):
                dropped += 1
                continue
            if row is None:
                dropped += 1
                continue
            batch.append(row)
            if len(batch) >= INSERT_CHUNK:
                session.execute(insert(VehiclePosition), batch)
                inserted += len(batch)
                batch = []
    if batch:
        session.execute(insert(VehiclePosition), batch)
        inserted += len(batch)

    if not _zstd_frame_is_complete(path):
        logger.warning(
            "%s: zstd frame did not end cleanly (crash-truncated mid-upload?) — "
            "loaded %d rows from the readable prefix only; marking loaded anyway "
            "since archive files are immutable once uploaded",
            path.name,
            inserted,
        )
    return inserted, dropped


class UnknownProvenanceError(RuntimeError):
    """A UTC day holds more VP rows than its manifested files account for.

    Those extra rows came from somewhere other than this loader (e.g. the
    retired direct-to-DB collector), so neither deleting the day to reload
    it nor inserting archive rows on top of it is safe: the first could
    destroy data the archive doesn't hold, the second could duplicate rows.
    """


class MissingDayFilesError(RuntimeError):
    """A full-day reload was needed but a manifested file for that day is not local."""


def _day_bounds(utc_date: date) -> tuple[datetime, datetime]:
    """Half-open naive-UTC ``collected_at`` bounds for one UTC calendar day."""
    start = datetime(utc_date.year, utc_date.month, utc_date.day)
    return start, start + timedelta(days=1)


def day_row_count(session, utc_date: date) -> int:
    """Count ``vehicle_positions`` rows whose ``collected_at`` falls on ``utc_date``."""
    start, end = _day_bounds(utc_date)
    return session.execute(
        select(func.count())
        .select_from(VehiclePosition)
        .where(VehiclePosition.collected_at >= start, VehiclePosition.collected_at < end)
    ).scalar_one()


def manifested_day(session, utc_date: date) -> dict[str, int]:
    """Return ``{filename: row_count}`` for manifest rows naming ``utc_date``."""
    prefix = f"{utc_date.isoformat()}."
    return {
        name: count
        for name, count in session.query(
            VpArchiveLoadedFile.filename, VpArchiveLoadedFile.row_count
        ).filter(VpArchiveLoadedFile.filename.startswith(prefix))
        if archive_file_date(name) == utc_date
    }


def reconcile_utc_day(
    session, utc_date: date, force: bool = False, local_names: set[str] | None = None
) -> str:
    """Decide how to load one UTC day: ``"incremental"``, ``"reload"`` or ``"noop"``.

    Compares the rows present for the day (by ``collected_at``) with the
    rows its manifested files inserted (each file's rows and manifest row
    commit together, so after any sequence of loads the two are equal):

    - equal (and not ``force``) → ``"incremental"``: everything already
      loaded is still there; load only not-yet-manifested files.
    - fewer rows than manifested (something deleted some or all of them —
      ``pipelines/apply_retention.py`` deletes whole days and keeps the
      manifest, or a manual delete), or ``force`` → ``"reload"``: rebuild
      the whole day.
    - MORE rows than manifested: rows the manifest can't account for
      (every day through 2026-08-22 holds rows written straight to the DB
      by the old VM collector) are never deleted or loaded on top of.
      Without ``force`` and with no unmanifested file among
      ``local_names`` there is nothing to do → ``"noop"`` (logged);
      otherwise :class:`UnknownProvenanceError`.
    """
    present = day_row_count(session, utc_date)
    manifest = manifested_day(session, utc_date)
    manifested = sum(manifest.values())
    if present > manifested:
        unmanifested = sorted(set(local_names or ()) - set(manifest))
        if not force and not unmanifested:
            print(
                f"  {utc_date}: {present} rows vs {manifested} manifested (rows of unknown "
                "provenance) and no new local files — leaving the day untouched"
            )
            return "noop"
        raise UnknownProvenanceError(
            f"{utc_date}: vehicle_positions holds {present} rows with collected_at on this "
            f"UTC day but its manifested archive files account for only {manifested}; "
            "refusing to delete or add rows for a day with rows of unknown provenance"
            + (f" ({len(unmanifested)} unmanifested local file(s))" if unmanifested else "")
        )
    if force or present < manifested:
        return "reload"
    return "incremental"


def reload_utc_day(session, utc_date: date, paths: list[Path]) -> tuple[int, int]:
    """Rebuild one UTC day of ``vehicle_positions`` from its archive files, atomically.

    In one transaction: delete every row with ``collected_at`` on
    ``utc_date``, delete the day's manifest rows, insert every file in
    ``paths``, add a manifest row per file, commit. Any exception rolls
    the whole day back to its prior state. Refuses (before touching
    anything) if a file the manifest says was loaded for this day is not
    in ``paths`` — reloading a subset would silently lose its rows.
    Returns ``(inserted, dropped)`` across the day.
    """
    names = {p.name for p in paths}
    missing = sorted(set(manifested_day(session, utc_date)) - names)
    if missing:
        raise MissingDayFilesError(
            f"{utc_date}: full-day reload needs every file loaded for that day, but "
            f"{len(missing)} manifested file(s) are not local (e.g. {missing[0]}); "
            "fetch them from S3 first"
        )
    start, end = _day_bounds(utc_date)
    try:
        deleted = session.execute(
            delete(VehiclePosition).where(
                VehiclePosition.collected_at >= start, VehiclePosition.collected_at < end
            )
        ).rowcount
        session.execute(
            delete(VpArchiveLoadedFile).where(
                VpArchiveLoadedFile.filename.startswith(f"{utc_date.isoformat()}.")
            )
        )
        total_ins = total_drop = 0
        now = utcnow_naive()
        for path in sorted(paths):
            ins, drop = _insert_vp_file(session, path)
            session.add(
                VpArchiveLoadedFile(
                    filename=path.name, row_count=ins, dropped_count=drop, loaded_at=now
                )
            )
            total_ins += ins
            total_drop += drop
        session.commit()
    except Exception:
        session.rollback()
        raise
    print(
        f"  {utc_date}: reloaded {len(paths)} file(s) — deleted {deleted} row(s), "
        f"inserted {total_ins}, dropped {total_drop}"
    )
    return total_ins, total_drop


def day_paths(archive_root: Path, utc_date: date) -> list[Path]:
    """Local archive files named for ``utc_date`` (both naming schemes), sorted."""
    return sorted(
        p
        for p in archive_root.glob(f"{utc_date.isoformat()}*.jsonl.zst")
        if archive_file_date(p.name) == utc_date
    )


def load_utc_days(session, archive_root: Path, utc_dates: list[date], force: bool) -> list[str]:
    """Load (or reload) the files named for each of ``utc_dates``; return failure messages.

    Each day is reconciled (:func:`reconcile_utc_day`) and then either
    loaded incrementally file-by-file (:func:`load_vp_file`, each file its
    own transaction) or rebuilt whole (:func:`reload_utc_day`). A day with
    no local files and nothing to reload is a notice, not an error (a
    future date, an archive gap). One day's failure does not stop the
    others.
    """
    failures: list[str] = []
    for utc_date in sorted(set(utc_dates)):
        paths = day_paths(archive_root, utc_date)
        try:
            mode = reconcile_utc_day(
                session, utc_date, force=force, local_names={p.name for p in paths}
            )
            session.rollback()  # end the read-only transaction the counts opened
            if mode == "noop":
                continue
            if mode == "reload":
                reload_utc_day(session, utc_date, paths)
                continue
            if not paths:
                print(f"  {utc_date}: no local VP files, nothing to load")
                continue
            for path in paths:
                try:
                    load_vp_file(session, path)
                except Exception as exc:  # isolate one bad file, keep going
                    session.rollback()
                    failures.append(f"{path.name}: {exc!r}")
                    print(f"  {path.name}: FAILED to load ({exc!r}), continuing")
        except Exception as exc:
            session.rollback()
            failures.append(f"{utc_date}: {exc}")
            print(f"  {utc_date}: FAILED ({exc})")
    return failures


def main(argv=None) -> int:
    """Load VP archive files for one agency — all of them, or scoped to UTC dates.

    Without ``--utc-date``: every not-yet-loaded file in the archive dir
    (the original behavior). Each file is loaded in isolation: an
    exception from ``load_vp_file`` (any DB- or file-level failure a
    line-level guard didn't already catch) is logged and the run continues
    to the next file rather than aborting the whole batch — one bad file
    must not block every other file behind it.

    With ``--utc-date`` (repeatable): only files named for those UTC
    dates, each day reconciled first so a pruned day is reloaded — see
    :func:`load_utc_days`. ``--force`` (requires ``--utc-date``) reloads
    every named day whole even when its rows look intact.

    Returns 1 if any file/day failed, so callers can detect a partial run;
    the manifest table is the source of truth for exactly which files
    still need attention.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    parser.add_argument("--archive-root", type=Path, default=None)
    parser.add_argument(
        "--utc-date",
        action="append",
        type=date.fromisoformat,
        default=None,
        help="Only load files named for this UTC date (repeatable); reconciles the day first.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="With --utc-date: delete and reload each named day from its files.",
    )
    args = parser.parse_args(argv)
    if args.force and not args.utc_date:
        parser.error("--force requires --utc-date (reloads are scoped to whole UTC days)")
    load_dotenv()

    cfg = load_agency_config(args.agency)
    archive_root = args.archive_root or REPO_ROOT / cfg.vp_archive_dir
    if not archive_root.is_dir():
        print(f"Error: --archive-root {archive_root} is not a directory.")
        return 1

    session = get_session(db_url=resolve_agency_db_url(cfg))
    if args.utc_date:
        try:
            failures = load_utc_days(session, archive_root, args.utc_date, args.force)
        finally:
            session.close()
        if failures:
            print(f"  {len(failures)} failure(s): " + "; ".join(failures))
        return 1 if failures else 0
    try:
        total_ins = total_drop = 0
        failed_files: list[str] = []
        for path in sorted(archive_root.glob("*.jsonl.zst")):
            try:
                ins, drop = load_vp_file(session, path)
            except Exception as exc:  # isolate one bad file, keep going
                print(f"  {path.name}: FAILED to load ({exc!r}), continuing")
                session.rollback()
                failed_files.append(path.name)
                continue
            total_ins += ins
            total_drop += drop
        print(f"Done: {total_ins} rows inserted, {total_drop} dropped (guard + unusable rows).")
        if failed_files:
            print(f"  {len(failed_files)} file(s) failed to load: {', '.join(failed_files)}")
    finally:
        session.close()
    return 1 if failed_files else 0


if __name__ == "__main__":
    sys.exit(main())
