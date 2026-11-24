"""Apply the local-DB retention tiers for one agency (dry-run by default).

Replaces ``pipelines/window_derived_tables.py`` (a flat 365-day,
today-anchored ``stop_events``/``runs`` delete whose ``--dry-run`` actually
ran the DELETE and rolled it back). The tiers, the window anchor and every
guard are documented in ``src/retention.py``; the numbers live in
``config/agencies/<agency>.yaml`` → ``retention:``.

Default (no ``--apply``): read-only. Prints, per table, the rows / date
range / estimated bytes it would delete, then every date or UTC day it
would keep inside the deletion range and why. Counting reads indexes only
(``service_date`` / ``collected_at``), one bounded query per date or day.

``--apply``: takes the refresh run lock (``logs/.pull-and-derive.lock.d``
in the main checkout — so it never runs beside ``bin/pull-and-derive.sh``;
when invoked by ``refresh_by_day`` the parent's lock is inherited),
re-checks each guard immediately before each date/day, and deletes in
batches of ``--batch-size`` rows, committing each batch. ``--vacuum``
then runs plain ``VACUUM (ANALYZE)`` on the touched tables (no exclusive
lock; makes the space reusable but does not shrink files — see
``docs/DEPLOYMENT.md`` §13 for when ``VACUUM FULL`` is worth it).

Usage:
    uv run python -m pipelines.apply_retention --agency wmata              # dry run
    uv run python -m pipelines.apply_retention --agency sfmta --apply
    uv run python -m pipelines.apply_retention --agency wmata --apply --allow-unrederivable

Exit codes: 0 ok; 1 error; 2 usage; 75 the run lock is held.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from src import run_lock
from src.agency_config import load_agency_config, resolve_agency_db_url
from src.retention import (
    DEFAULT_BATCH_SIZE,
    RetentionPlan,
    apply_retention,
    plan_retention,
    read_progress_statuses,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TOUCHED_TABLES = ("runs", "stop_events", "vehicle_positions", "trip_update_state")


def _fmt_bytes(n: float | None) -> str:
    """Human-readable byte estimate (``~1.2 GB``) or ``?`` when unknown."""
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1000 or unit == "TB":
            return f"~{n:.1f} {unit}"
        n /= 1000
    return "?"


def _est(plan: RetentionPlan, table: str, rows: int) -> str:
    """Estimated bytes for ``rows`` rows of ``table`` using the plan's per-row average."""
    per = plan.bytes_per_row.get(table)
    return _fmt_bytes(per * rows if per is not None else None)


def format_plan(plan: RetentionPlan) -> list[str]:
    """Render a plan as report lines (what goes, what stays inside the range, and why)."""
    p = plan.policy
    lines = [
        f"retention plan — {plan.agency}: stop_level_days={p.stop_level_days}, "
        f"raw_days={p.raw_days}",
    ]
    if plan.anchor is None:
        lines.append(
            "  no fully derived service date (complete, non-null system_metrics_daily row "
            "with runs) — nothing would be deleted"
        )
        return lines
    lines.append(
        f"  anchor (latest fully derived date): {plan.anchor}; stop-level keeps "
        f">= {plan.stop_level_first_kept}; raw keeps service dates >= {plan.raw_first_kept} "
        f"(VP UTC days >= {plan.vp_first_kept_utc_day})"
    )
    dd = plan.deletable_dates()
    se = sum(d.stop_events for d in dd)
    rn = sum(d.runs for d in dd)
    span = f"{dd[0].service_date} .. {dd[-1].service_date}" if dd else "none"
    lines.append(
        f"  stop_events: {se:,} rows over {len(dd)} date(s) [{span}] {_est(plan, 'stop_events', se)}"
    )
    lines.append(f"  runs:        {rn:,} rows {_est(plan, 'runs', rn)}")
    vd = plan.deletable_vp_days()
    vr = sum(v.rows for v in vd)
    vspan = f"{vd[0].utc_day} .. {vd[-1].utc_day}" if vd else "none"
    lines.append(
        f"  vehicle_positions: {vr:,} rows over {len(vd)} UTC day(s) [{vspan}] "
        f"{_est(plan, 'vehicle_positions', vr)} (manifest rows kept; days reload on demand)"
    )
    lines.append(
        f"  trip_update_state: {plan.tus_rows:,} rows with service_date < {plan.tus_cutoff} "
        f"{_est(plan, 'trip_update_state', plan.tus_rows)}"
    )
    kept = [d for d in plan.dates if not d.deletable]
    if kept:
        se_k = sum(d.stop_events for d in kept)
        lines.append(
            f"  KEPT inside the stop-level deletion range: {len(kept)} date(s), "
            f"{se_k:,} stop_events {_est(plan, 'stop_events', se_k)}"
        )
        for d in kept:
            lines.append(f"    {d.service_date}: {d.stop_events:,} stop_events — {d.reason}")
    kept_vp = [v for v in plan.vp_days if not v.deletable]
    if kept_vp:
        vr_k = sum(v.rows for v in kept_vp)
        lines.append(
            f"  KEPT inside the VP deletion range: {len(kept_vp)} UTC day(s), {vr_k:,} rows "
            f"{_est(plan, 'vehicle_positions', vr_k)}"
        )
        for v in kept_vp:
            lines.append(f"    {v.utc_day}: {v.rows:,} rows — {v.reason}")
    return lines


def build_parser() -> argparse.ArgumentParser:
    """CLI definition."""
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--agency", required=True, choices=("wmata", "sfmta"))
    p.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    p.add_argument(
        "--allow-unrederivable",
        action="store_true",
        help=(
            "also prune stop-level rows of dates whose VP window predates the S3 VP archive "
            "(their aggregates stay, but the dates can never be re-derived)"
        ),
    )
    p.add_argument(
        "--include-unmanifested-vp",
        action="store_true",
        help=(
            "also prune VP UTC days holding rows no archive file accounts for (VM era; the DB "
            "may be the only copy)"
        ),
    )
    p.add_argument(
        "--allow-partial-date",
        action="append",
        type=date.fromisoformat,
        default=[],
        help="prune this date (repeatable) even though its aggregates are stamped partial",
    )
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument(
        "--vacuum",
        action="store_true",
        help="with --apply: plain VACUUM (ANALYZE) the touched tables afterwards",
    )
    p.add_argument(
        "--progress-dir",
        type=Path,
        default=run_lock.main_checkout_root(REPO_ROOT) / "logs",
        help="where refresh_progress_<agency>.json lives (read-only)",
    )
    return p


def vacuum_tables(engine, tables=TOUCHED_TABLES) -> None:
    """Plain ``VACUUM (ANALYZE)`` each table on an autocommit connection (never FULL)."""
    from sqlalchemy import text

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for t in tables:
            print(f"VACUUM (ANALYZE) {t}", flush=True)
            conn.execute(text(f"VACUUM (ANALYZE) {t}"))


def run(args, session_factory=None) -> int:
    """Plan (and with ``--apply``, apply) retention; ``session_factory`` is for tests."""
    from src.database import get_session

    cfg = load_agency_config(args.agency)
    if session_factory is None:

        def session_factory():
            """Open a session on the agency DB."""
            return get_session(db_url=resolve_agency_db_url(cfg))

    try:
        progress = read_progress_statuses(
            args.progress_dir / f"refresh_progress_{args.agency}.json"
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    options = {
        "progress": progress,
        "allow_unrederivable": args.allow_unrederivable,
        "include_unmanifested_vp": args.include_unmanifested_vp,
        "allow_partial_dates": set(args.allow_partial_date),
    }
    session = session_factory()
    try:
        plan = plan_retention(session, args.agency, cfg.timezone, cfg.retention, **options)
        session.rollback()
        for line in format_plan(plan):
            print(line, flush=True)
        if not args.apply:
            print("DRY RUN — nothing deleted. Re-run with --apply to delete the rows above.")
            return 0
        result = apply_retention(session, plan, cfg.timezone, batch_size=args.batch_size, **options)
        print(
            f"deleted: stop_events={result.stop_events:,} runs={result.runs:,} "
            f"vehicle_positions={result.vehicle_positions:,} "
            f"trip_update_state={result.trip_update_state:,}"
            + (f"; skipped {len(result.skipped)}" if result.skipped else ""),
            flush=True,
        )
        engine = session.get_bind()
    finally:
        session.close()
    if args.vacuum and engine.dialect.name == "postgresql":
        vacuum_tables(engine)
    return 0


def main(argv=None) -> int:
    """CLI entry point (takes the run lock for ``--apply``)."""
    args = build_parser().parse_args(argv)
    if args.vacuum and not args.apply:
        print("ERROR: --vacuum needs --apply", file=sys.stderr)
        return 2
    load_dotenv()
    if not args.apply:
        return run(args)
    lock_dir = run_lock.LOCK_DIR
    own_lock = not run_lock.held_by_parent(lock_dir)
    if own_lock:
        try:
            run_lock.acquire(
                os.getpid(), "pipelines.apply_retention " + " ".join(argv or sys.argv[1:]), lock_dir
            )
        except run_lock.LockHeldError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return run_lock.EXIT_LOCKED
    try:
        return run(args)
    finally:
        if own_lock:
            run_lock.release(os.getpid(), lock_dir)


if __name__ == "__main__":
    sys.exit(main())
