"""Replay archived JSONL snapshots into ``trip_update_state``.

The JSONL archive (``archive/raw_snapshots/<date>.*.jsonl.zst``) is the
only source of truth for any historical service_date once the
``trip_update_state`` row for the trip has been overwritten by a later
day's snapshot. This tool reads those files and replays each line
through the same UPSERT helper (``upsert_trip_update_state``) the live
collector uses.

The tool is idempotent: re-running for the same date produces the same
end state, because the UPSERT formulas are deterministic functions of
the input sequence.

The tool is cross-date safe: rows for service_dates other than the
target are silently skipped, so backfilling 2026-05-18 cannot corrupt
the 2026-05-19 rows the running collector is writing.

Multi-agency (NOTES-96): pass ``--agency`` (matching a
``config/agencies/<agency>.yaml``) to replay a non-WMATA archive with the
correct service-date timezone, default archive directory, and target
database. Omitting it keeps the WMATA/Eastern default.

Usage:
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18 \\
        --archive-root /path/to/archive/raw_snapshots
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18 \\
        --agency sfmta
"""

import argparse
import itertools
import json
import sys
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.agency_config import MissingAgencyDatabaseUrlError, load_agency_config
from src.agency_config import resolve_agency_db_url as _resolve_agency_db_url
from src.database import get_session
from src.upsert_helpers import upsert_trip_update_state
from src.wmata_collector import dedupe_trip_update_rows

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_AGENCY = "wmata"

# Consecutive past-target-date polls required before _fold_archive_file's
# early exit fires on the UTC-next-day supplement file. >1 guards against
# a single flapping-entity poll masquerading as "we're done" -- see
# _fold_archive_file's docstring.
_SUPPLEMENT_EARLY_EXIT_STREAK = 3


class NoArchiveFilesFoundError(RuntimeError):
    """Raised when the archive glob for a date matches zero files.

    A replay with no input is almost always an operator error (the
    JSONL archive hasn't been synced from S3 yet, a typo'd date, a wrong
    ``--archive-root``) rather than an intentional no-op. NOTES-93:
    this used to return 0 silently, which during the recovery driver's
    fold-in phase turned "the archive isn't here yet" into a
    clean-looking success — the failure guard never tripped, and
    derivation ran against empty state. Callers that genuinely expect
    an empty date must pass ``allow_empty=True`` explicitly.
    """


def _assert_west_of_utc(tz_name: str) -> None:
    """Raise if ``tz_name`` is not west of (or exactly at) UTC.

    ``replay_archive_for_date`` globs the target service date's UTC-named
    archive file AND the *following* UTC date's, never the *previous*
    one -- correct only because every agency wired up so far
    (``America/New_York``, ``America/Los_Angeles``) sits behind UTC, so
    an agency-local service day's snapshots can only spill into the
    following UTC calendar date, never the preceding one. An east-of-UTC
    agency would invert that: its late-night local snapshots spill into
    the *preceding* UTC date instead, and this glob would silently miss
    them the same way the pre-fix D-only glob missed WMATA's evening
    tail. Fail loudly here instead, at agency-config resolution time, so
    a future eastern-hemisphere agency can't reach that silent-truncation
    failure mode.

    A single fixed reference instant is NOT enough: some zones (the
    British/EU "Western European Time" family -- ``Europe/London``,
    ``Europe/Dublin``, ``Europe/Lisbon``) sit at UTC+0 in winter but
    UTC+1 for roughly seven months of summer DST, so a January-only
    sample would wrongly accept them. Checking both a January and a
    July instant catches that: an agency must be at or west of UTC
    (offset <= 0) at BOTH, since ``Atlantic/Reykjavik``-style zones with
    no DST at all stay at UTC+0 year-round and must still pass.
    """
    offsets = [
        datetime(2026, 1, 15, tzinfo=ZoneInfo(tz_name)).utcoffset(),
        datetime(2026, 7, 15, tzinfo=ZoneInfo(tz_name)).utcoffset(),
    ]
    if any(offset is None or offset > timedelta(0) for offset in offsets):
        raise NotImplementedError(
            f"Agency timezone {tz_name!r} is not west of UTC year-round "
            f"(January/July offsets: {offsets}). "
            "replay_archive_for_date's D+1-not-D-1 archive glob "
            "assumption does not hold for east-of-UTC (or DST-boundary) "
            "agencies -- extend the glob to also check target_date - 1 "
            "day before wiring up an agency in this timezone."
        )


def _parse_dt(s: str | None) -> datetime | None:
    """Parse an archive datetime string ("2026-05-18 22:00:00") to a naive datetime.

    The live ``JsonlArchiveWriter`` serialises datetimes via ``str()``,
    producing a space-separated form that Python's ``fromisoformat``
    accepts since 3.11. Returns ``None`` for ``None`` or empty input
    (some snapshots have null ``predicted_arrival_ts``).
    """
    if not s:
        return None
    return datetime.fromisoformat(s)


def _iter_jsonl_zst(path: Path):
    """Yield decoded dicts from a zstd-compressed JSONL file.

    Uses streaming decompression so multi-frame files (one frame per
    collector flush) decode correctly — see PR #131 for the analogous
    fix on the parquet reader side. Lines split on ``\\n``; the final
    partial-line is yielded only if it has non-whitespace content.
    """
    dctx = zstd.ZstdDecompressor()
    with path.open("rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buf = b""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if line:
                        yield json.loads(line)
            if buf.strip():
                yield json.loads(buf)


def _fold_archive_file(
    path: Path,
    *,
    tz_name: str,
    target_date: date_type,
    folded: dict[tuple[str, int], dict],
    early_exit: bool,
) -> int:
    """Read one archive file's polls and fold matching rows into ``folded``.

    Groups consecutive lines sharing one snapshot_ts (one archived poll
    — the live archive writer appends a poll's rows contiguously, one
    call per tick) and dedupes same-poll (trip_id, stop_sequence)
    collisions before folding, exactly as
    ``WMATADataCollector._save_trip_updates`` does for live polls —
    NOTES-96. Parsing snapshot_ts to a datetime up front lets
    ``dedupe_trip_update_rows``' service_date fallback (no
    trip_start_date) work the same way it does for live rows.

    ``early_exit``: when ``True`` (used only for the UTC-next-day
    supplement file — never the primary date's own files, which must
    always be read in full), stops reading once
    ``_SUPPLEMENT_EARLY_EXIT_STREAK`` CONSECUTIVE polls have each
    resolved entirely past ``target_date`` — see the early-exit comment
    at the ``replay_archive_for_date`` call site for why that's safe.
    Requiring a streak (not just one such poll) guards against a
    flapping GTFS-RT entity producing one transient poll with no
    service-day-``target_date`` trips (e.g. every currently-tracked trip
    happens to have already rolled to the next service day for that one
    poll) sandwiched between polls that still have one — exiting on the
    single transient poll would silently drop the later, still-in-scope
    poll. The streak counter resets to zero on any poll containing at
    least one row whose service_date == ``target_date`` (unambiguous
    proof we're still in scope); a poll with rows on neither side of
    that boundary (empty after filtering, or a rare mix that's past
    target_date but not resolved to it either) neither resets nor
    advances the streak.

    Mutates ``folded`` in place (so multiple files, primary and
    supplement, accumulate into one shared fold) and returns the number
    of rows counted (post same-poll dedup, pre-fold-collapse) from this
    file alone.
    """
    total = 0
    consecutive_past_target_polls = 0
    for _snapshot_key, poll_lines in itertools.groupby(
        _iter_jsonl_zst(path), key=lambda r: r.get("snapshot_ts")
    ):
        poll_rows = []
        for raw in poll_lines:
            if raw.get("stop_sequence") is None:
                continue
            normalized = dict(raw)
            normalized["snapshot_ts"] = _parse_dt(raw["snapshot_ts"])
            poll_rows.append(normalized)

        deduped = dedupe_trip_update_rows(poll_rows, tz_name)

        if early_exit and deduped:
            if any(row["service_date"] == target_date for row in deduped):
                consecutive_past_target_polls = 0
            elif min(row["service_date"] for row in deduped) > target_date:
                consecutive_past_target_polls += 1
                if consecutive_past_target_polls >= _SUPPLEMENT_EARLY_EXIT_STREAK:
                    break

        for row in deduped:
            service_date = row["service_date"]
            if service_date != target_date:
                continue

            snapshot_ts = row["snapshot_ts"]
            vehicle_id = row.get("vehicle_id")
            predicted_arrival_ts = _parse_dt(row.get("predicted_arrival_ts"))
            key = (row["trip_id"], row["stop_sequence"])
            cur = folded.get(key)
            if cur is None:
                cur = {
                    "trip_id": row["trip_id"],
                    "stop_sequence": row["stop_sequence"],
                    "service_date": target_date,
                    "stop_id": row["stop_id"],
                    "vehicle_id": vehicle_id,
                    "snapshot_ts": snapshot_ts,
                    "schedule_relationship": row.get("schedule_relationship"),
                    "predicted_arrival_ts": None,
                    "last_pred_snapshot_ts": None,
                    "_vehicle_ts": snapshot_ts if vehicle_id is not None else None,
                }
                folded[key] = cur
            else:
                if snapshot_ts >= cur["snapshot_ts"]:
                    cur["snapshot_ts"] = snapshot_ts
                    cur["stop_id"] = row["stop_id"]
                    cur["schedule_relationship"] = row.get("schedule_relationship")
                if vehicle_id is not None and (
                    cur["_vehicle_ts"] is None or snapshot_ts >= cur["_vehicle_ts"]
                ):
                    cur["vehicle_id"] = vehicle_id
                    cur["_vehicle_ts"] = snapshot_ts
            if predicted_arrival_ts is not None and (
                cur["last_pred_snapshot_ts"] is None or snapshot_ts >= cur["last_pred_snapshot_ts"]
            ):
                cur["predicted_arrival_ts"] = predicted_arrival_ts
                cur["last_pred_snapshot_ts"] = snapshot_ts
            total += 1

    return total


def replay_archive_for_date(
    db: Session,
    target_date: date_type,
    archive_root: Path | None = None,
    chunk_size: int = 5000,
    allow_empty: bool = False,
    agency: str = DEFAULT_AGENCY,
) -> int:
    """Replay all archive files for ``target_date`` into trip_update_state.

    Globs both the per-process pattern (``{date}.*.jsonl.zst`` — current,
    from PR #132) and the legacy single-file pattern
    (``{date}.jsonl.zst`` — pre-PR #132).

    **Fold-then-upsert:** a full day of the WMATA TripUpdates feed is
    ~10M raw lines (every future stop republished on every ~30s poll),
    but only ~150k distinct (trip_id, stop_sequence) keys. Replaying
    line-by-line through per-snapshot UPSERT statements takes hours per
    date (measured ~7h on the production Lightsail box, 2026-07-12).
    Instead, the accumulation semantics — which the live collector
    expresses as ON CONFLICT CASE expressions applied poll-by-poll —
    are a deterministic fold, so this replays them in memory:

    - ``final_snapshot_ts`` / ``final_schedule_relationship`` /
      ``stop_id``: from the row with the greatest snapshot_ts.
    - ``last_pred_snapshot_ts`` / ``last_predicted_arrival_ts``: from
      the greatest-snapshot_ts row whose predicted_arrival_ts is
      non-null (WMATA nullifies predictions at arrival; the last
      meaningful estimate must survive).
    - ``vehicle_id``: last non-null across the sequence.

    Rows are ordered by snapshot_ts (not file order) so overlapping
    per-process files from a collector restart fold correctly. The
    folded rows (one per key) then go through the same
    ``upsert_trip_update_state`` helper in chunks, preserving the
    conditional ON CONFLICT semantics against any pre-existing DB rows.

    Rows whose computed service_date doesn't match ``target_date`` are
    silently skipped — defensive against midnight-crossing files that
    might contain a few rows belonging to the adjacent service-day.

    **Agency-aware service date (NOTES-96):** ``agency`` selects the
    IANA timezone (via ``config/agencies/<agency>.yaml``,
    ``src.agency_config.load_agency_config``) used to infer
    ``service_date`` when a row's ``trip_start_date`` is absent.
    Defaults to ``"wmata"`` (Eastern), matching prior behavior. SFMTA
    service dates run on Pacific time, so replaying an SFMTA archive
    requires ``agency="sfmta"`` or rows near midnight UTC silently
    resolve to the wrong calendar day.

    **Same-poll dedup (NOTES-96):** before folding, rows sharing one
    ``snapshot_ts`` (one archived poll — assumed contiguous within a
    file, matching how the live archive writer appends them) run
    through ``dedupe_trip_update_rows``, the same helper
    ``WMATADataCollector._save_trip_updates`` uses. WMATA never repeats
    a ``stop_sequence`` within one poll, but SFMTA/511.org does (~0.24%
    of rows) — without this, two same-poll rows sharing
    (trip_id, stop_sequence) could both survive into one folded output
    row's inputs with an undefined tie-break; with it, the collision
    resolves exactly as the live collector would (last row in feed
    order wins outright, including a null field clobbering an earlier
    non-null one).

    Args:
        db: Active SQLAlchemy session bound to PostgreSQL. Caller is
            responsible for committing or rolling back.
        target_date: The service date to replay (agency-local calendar
            day; see ``agency``).
        archive_root: Directory holding the JSONL.zst files. ``None``
            (the default) resolves to ``REPO_ROOT / <agency's
            collector.archive_dir>`` — e.g. ``archive/raw_snapshots`` for
            wmata, ``archive/sfmta_raw_snapshots`` for sfmta — so the
            right directory is picked even when ``agency`` isn't
            ``"wmata"``.
        chunk_size: Folded rows per UPSERT statement.
        allow_empty: When ``True``, a zero-file glob match returns 0
            instead of raising — see ``NoArchiveFilesFoundError``
            (NOTES-93).
        agency: Name matching ``config/agencies/<agency>.yaml``, e.g.
            ``"wmata"`` or ``"sfmta"``. Selects the service-date
            timezone (NOTES-96). Defaults to ``"wmata"``.

    Returns:
        The number of snapshot lines that matched ``target_date`` after
        same-poll dedup and were folded (note: not the row-count in
        state — many snapshots collapse to one state row by design).

    Raises:
        NoArchiveFilesFoundError: if the glob matches zero files and
            ``allow_empty`` is ``False``.
    """
    cfg = load_agency_config(agency)
    tz_name = cfg.timezone
    _assert_west_of_utc(tz_name)
    if archive_root is None:
        archive_root = REPO_ROOT / cfg.archive_dir

    # JsonlArchiveWriter names files by the snapshot's UTC calendar date,
    # never by the agency-local service date. Every agency here is west
    # of UTC (behind it, enforced by _assert_west_of_utc above), so an
    # agency-local service day D's snapshots fall on UTC date D or UTC
    # date D+1 (late-evening/night local time rolls the UTC date
    # forward) -- never D-1. Globbing only D silently dropped that D+1
    # tail (WMATA: everything after ~20:00 ET; SFMTA: ~17:00-24:00 PT)
    # -- confirmed empirically as zero trip_update_state rows past UTC
    # midnight on every recent day. NOTES-96 follow-up.
    #
    # The D+1 file is purely a content SUPPLEMENT, never a substitute
    # for the primary (target) date's own file(s): the NOTES-93
    # empty-archive guard exists because a genuinely-missing archive
    # must fail loudly, not "succeed" on whatever unrelated sliver
    # happens to land in a same-named neighboring file. A contiguous
    # daily archive always has a D+1 file once D has ended, so checking
    # the union for emptiness would make the guard nearly never fire.
    next_date = target_date + timedelta(days=1)
    primary_paths = set(archive_root.glob(f"{target_date.isoformat()}.*.jsonl.zst")) | set(
        archive_root.glob(f"{target_date.isoformat()}.jsonl.zst")
    )
    supplement_paths = set(archive_root.glob(f"{next_date.isoformat()}.*.jsonl.zst")) | set(
        archive_root.glob(f"{next_date.isoformat()}.jsonl.zst")
    )
    if not primary_paths:
        if allow_empty:
            print(
                f"No archive files found for {target_date} under {archive_root} "
                f"(also checked the UTC-next-day supplement, {next_date}) "
                "(--allow-empty set, continuing)"
            )
            return 0
        raise NoArchiveFilesFoundError(
            f"No archive files found for {target_date} under {archive_root} "
            f"(also checked the UTC-next-day supplement, {next_date}). "
            "This usually means the JSONL archive hasn't been synced yet — "
            "check the source before assuming the date is genuinely empty. "
            "Pass --allow-empty to proceed anyway."
        )

    paths = sorted(primary_paths)
    supplement_paths = sorted(supplement_paths)
    print(f"Replaying {len(paths) + len(supplement_paths)} archive file(s) for {target_date}:")
    for p in paths:
        print(f"  - {p.name}")
    for p in supplement_paths:
        print(f"  - {p.name} (UTC-next-day supplement)")

    # Fold state per (trip_id, stop_sequence); service_date is fixed to
    # target_date by the filter inside _fold_archive_file. "_vehicle_ts"
    # / "_pred_ts" track which snapshot each conditional field last came
    # from so ordering is by snapshot_ts, not file iteration order.
    folded: dict[tuple[str, int], dict] = {}
    total = 0
    for p in paths:
        total += _fold_archive_file(
            p, tz_name=tz_name, target_date=target_date, folded=folded, early_exit=False
        )
    for p in supplement_paths:
        # Rows are chronological within a file (the collector writes
        # each poll in real time), so once a whole poll's rows have all
        # moved past target_date, every later poll in this same
        # supplement file is guaranteed to have moved past it too --
        # stop reading rather than decompressing the rest of a
        # multi-hundred-MB file for rows that will only be filtered out.
        # Only ever applied to the D+1 supplement, never the primary
        # date's own file(s), which are always read in full.
        total += _fold_archive_file(
            p, tz_name=tz_name, target_date=target_date, folded=folded, early_exit=True
        )

    rows_out = [{k: v for k, v in cur.items() if not k.startswith("_")} for cur in folded.values()]
    for i in range(0, len(rows_out), chunk_size):
        upsert_trip_update_state(db, rows_out[i : i + chunk_size])

    print(f"Replayed {total} snapshot rows for {target_date} ({len(rows_out)} state rows).")
    return total


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Service date (YYYY-MM-DD)")
    parser.add_argument(
        "--agency",
        default=DEFAULT_AGENCY,
        help=(
            f"Agency name matching config/agencies/<agency>.yaml (default: "
            f"{DEFAULT_AGENCY!r}). Selects the service-date timezone and the "
            "target database (unconditionally), and — unless --archive-root "
            "is given explicitly — the default archive directory too."
        ),
    )
    parser.add_argument(
        "--archive-root",
        default=None,
        help=(
            "Archive directory (default: the agency config's "
            "collector.archive_dir, e.g. archive/raw_snapshots for wmata or "
            "archive/sfmta_raw_snapshots for sfmta)"
        ),
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Exit 0 instead of erroring when the archive glob matches zero "
            "files for --date. Off by default — see NoArchiveFilesFoundError "
            "(NOTES-93)."
        ),
    )
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    cfg = load_agency_config(args.agency)
    archive_root = Path(args.archive_root) if args.archive_root else None

    try:
        db_url = _resolve_agency_db_url(cfg)
    except MissingAgencyDatabaseUrlError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    db = get_session(db_url=db_url)
    try:
        replay_archive_for_date(
            db, target_date, archive_root, allow_empty=args.allow_empty, agency=args.agency
        )
        db.commit()
    except NoArchiveFilesFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
