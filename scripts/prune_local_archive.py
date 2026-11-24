"""Delete local raw-archive files that are safely in S3 and already processed (issue #261).

Dry-run by default — prints, per directory, how many files and bytes
would be freed, then every file it refuses to delete and why. ``--apply``
deletes. A file is deleted only when ALL hold (``src/raw_archive.py:
plan_prune``):

- its name is a recognized archive filename (``YYYY-MM-DD[.<pid>.<epoch>].jsonl.zst``);
- its UTC date is no later than the agency's latest derived service date
  L (``max(runs.service_date)``) — the next date after L reads only files
  dated after L. This is not a guarantee that nothing older will ever be
  read: a hole, held or failed date at or before L re-derives later and
  needs its files again; the refresh then re-fetches them from S3 (the
  cost of a re-download, never data loss — every deleted file is verified
  in S3 first);
- it is recorded in its manifest (``tu_archive_replayed_files`` for trip
  updates, ``vp_archive_loaded_files`` for vehicle positions);
- the same key exists in S3 with the same byte size AND the same
  checksum (S3 ETag: MD5 for single-part uploads, MD5-of-8-MiB-part-MD5s
  for multipart — a file whose ETag can't be recomputed is refused).

Directories under ``archive/`` that are not one of the four managed
streams (e.g. ``archive/trip_update_snapshots``, the retired
2026-05 parquet export, which has no S3 copy) are never touched; they are
listed so the owner can decide.

    uv run python scripts/prune_local_archive.py               # dry run, both agencies
    uv run python scripts/prune_local_archive.py --agency sfmta
    uv run python scripts/prune_local_archive.py --apply       # really delete

The day-at-a-time refresh (``pipelines/refresh_by_day.py``) applies the
same ``prune_stream`` logic to the files it just processed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agency_config import load_agency_config, resolve_agency_db_url  # noqa: E402
from src.raw_archive import (  # noqa: E402
    REFUSE,
    prune_stream,
    streams_for_agency,
    summarize_prune,
)

AGENCIES = ("wmata", "sfmta")


def latest_derived_date(session) -> date | None:
    """Return the agency DB's latest ``runs.service_date`` (None when empty)."""
    from sqlalchemy import func

    from src.models import Run

    latest = session.query(func.max(Run.service_date)).scalar()
    return date.fromisoformat(latest) if latest else None


def dir_size(path: Path) -> tuple[int, int]:
    """Return ``(file_count, total_bytes)`` for every file under ``path``."""
    files = [p for p in path.rglob("*") if p.is_file()]
    return len(files), sum(p.stat().st_size for p in files)


def main(argv=None) -> int:
    """CLI entry point; returns 0 (refusals are reported, not errors)."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--agency", choices=(*AGENCIES, "both"), default="both")
    parser.add_argument("--apply", action="store_true", help="delete (default: dry run)")
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT,
        help="checkout whose archive/ dirs to prune (default: this repo)",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    archive_dir = root / "archive"
    load_dotenv()

    import boto3

    from src.database import get_session

    dry_run = not args.apply
    s3 = boto3.client("s3")
    managed: set[Path] = set()
    total_count = total_bytes = refused = 0
    print(f"{'DRY RUN — nothing will be deleted' if dry_run else 'APPLY — deleting'}\n")
    agencies = AGENCIES if args.agency == "both" else (args.agency,)
    # Every agency's dirs count as managed even when only one is selected,
    # so the other's dirs aren't reported as unmanaged.
    for name in AGENCIES:
        managed.update(
            s.local_dir.resolve() for s in streams_for_agency(load_agency_config(name), root)
        )
    for name in agencies:
        cfg = load_agency_config(name)
        session = get_session(db_url=resolve_agency_db_url(cfg))
        try:
            latest = latest_derived_date(session)
            if latest is None:
                print(f"{name}: runs is empty — refusing to prune (no derived date to anchor on)")
                continue
            print(f"{name}: latest derived service date {latest}; files dated after it are kept")
            for stream in streams_for_agency(cfg, root):
                decisions, count, nbytes = prune_stream(
                    s3, session, stream, max_utc_date=latest, dry_run=dry_run
                )
                for line in summarize_prune(f"  {stream.local_dir}", decisions, dry_run):
                    print(line)
                total_count += count
                total_bytes += nbytes
                refused += sum(1 for d in decisions if d.action == REFUSE)
        finally:
            session.close()

    if archive_dir.is_dir():
        for sub in sorted(p for p in archive_dir.iterdir() if p.is_dir()):
            if sub.resolve() in managed:
                continue
            n, b = dir_size(sub)
            print(
                f"  {sub}: NOT MANAGED — {n} file(s), {b / 1e9:.2f} GB left untouched "
                "(no S3 prefix/manifest to verify against; delete by hand if unwanted)"
            )

    verb = "Would free" if dry_run else "Freed"
    print(f"\n{verb} {total_count} file(s), {total_bytes / 1e9:.2f} GB; refused {refused} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
