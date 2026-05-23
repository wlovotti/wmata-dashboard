"""
Daily metrics batch with catch-up. The launchd-scheduled entry point that
keeps `stop_events`, `runs`, and `route_bunching_periods` populated for
every active route on every recent service date.

Closes NOTES-28. Before this, the metrics-redesign pipelines —
`derive_stop_events`, `derive_stop_events_trip_updates`, `aggregate_runs`,
`compute_bunching` — were manual CLIs with no orchestrator. NOTES-26
showed the failure mode: only 6 routes had been aggregated for
2026-05-03, and the headline service-delivered metric silently read 0%
for everything else. This wrapper closes that gap by:

1. Always processing yesterday's Eastern service date (the freshly-
   complete day).
2. Looking back ~7 days and re-processing any service date that has
   zero rows in `runs`. Catches scheduler outages without manual
   intervention.
3. Running all per-date pipelines in dependency order:
   derive_stop_events + derive_stop_events_trip_updates first (both
   write `stop_events`, both independent), then aggregate_runs (reads
   stop_events → writes runs), then compute_bunching (reads runs).
4. Failing soft per (pipeline, date): if one of the derivation steps
   crashes for one date, the wrapper logs and continues with the next
   date. If a hard-dependency pipeline fails for a date
   (stop_events → aggregate_runs, runs → compute_bunching), the
   downstream pipelines for that date are skipped.

The pipelines themselves use Postgres ON CONFLICT upserts, so re-running
this wrapper is safe — the catch-up branch will replace any partial rows
with a clean re-derivation.

Logs land in `logs/daily_batch_<YYYY-MM-DD>.log` (Eastern date the batch
ran on, not the service date). The `logs/` dir is gitignored (see
.gitignore line 41) and is the same convention the continuous collector
uses.

Usage:
  uv run python pipelines/run_daily_batch.py
  uv run python pipelines/run_daily_batch.py --lookback-days 14   # wider catch-up
  uv run python pipelines/run_daily_batch.py --dry-run            # print plan, don't execute
"""

import argparse
import re
import subprocess
import sys
import time
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.database import get_session
from src.date_ranges import iter_eastern_dates
from src.models import Route, Run
from src.timezones import eastern_today

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"


_COMPARE_LINE_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}): "
    r"(?P<pct>[\d.]+)% agreement "
    r"\((?P<matched>[\d,]+)/(?P<total>[\d,]+)\), "
    r"(?P<v2_only>[\d,]+) v2-only rows, "
    r"(?P<diverging>\d+) routes with >1% disagreement"
)


def _check_comparison_thresholds(line: str) -> str | None:
    """Return a WARN reason string if the comparison line exceeds thresholds, else None.

    Thresholds (from 2026-05-23 cutover design spec):
    - agreement_pct >= 99.5
    - 0 diverging routes
    - v2-only rows <= 2% of total

    Args:
        line: A single output line from compare_old_vs_new_derivation.py.

    Returns:
        A short human-readable reason describing the violated threshold,
        or None if all thresholds pass.
    """
    m = _COMPARE_LINE_RE.match(line.strip())
    if not m:
        return f"unparseable comparison output: {line.strip()[:80]!r}"
    pct = float(m.group("pct"))
    total = int(m.group("total").replace(",", ""))
    v2_only = int(m.group("v2_only").replace(",", ""))
    diverging = int(m.group("diverging"))
    if pct < 99.5:
        return f"agreement {pct}% below 99.5% bar"
    if diverging > 0:
        return f"{diverging} route(s) with >1% disagreement"
    if total > 0 and (v2_only / total) > 0.02:
        return f"v2-only fraction {v2_only / total * 100:.2f}% above 2% bar"
    return None


# Order matters. The first two are independent (both write stop_events from
# different sources). aggregate_runs reads stop_events. compute_bunching
# reads runs. The "depends_on" key is the pipeline whose successful run is
# a precondition; None means independent.
PIPELINES: list[dict] = [
    {
        "name": "derive_stop_events",
        "module": "pipelines.derive_stop_events",
        "depends_on": None,
    },
    {
        "name": "derive_stop_events_trip_updates",
        "module": "pipelines.derive_stop_events_trip_updates",
        "depends_on": None,
    },
    {
        # Phase D side-by-side validation: same logic as
        # derive_stop_events_trip_updates but reads trip_update_state
        # directly and writes to stop_events_v2. The comparison script
        # (pipelines/compare_old_vs_new_derivation.py) diffs the two
        # tables nightly. Remove after Phase E cutover.
        "name": "derive_stop_events_from_state_v2",
        "module": "pipelines.derive_stop_events_from_state",
        "depends_on": None,
        "extra_args": ["--target-table", "stop_events_v2"],
    },
    {
        "name": "aggregate_runs",
        "module": "pipelines.aggregate_runs",
        # Either of the two derivation pipelines is sufficient — aggregate_runs
        # works off whichever stop_events rows exist.
        "depends_on": "derive_stop_events",
    },
    {
        "name": "compute_bunching",
        "module": "pipelines.compute_bunching",
        "depends_on": "aggregate_runs",
    },
    {
        # Per-date system-level rollup written to `system_metrics_daily`.
        # Depends on aggregate_runs (for service_delivered) and stop_events
        # (for OTP / EWT / bunching). compute_bunching is the strongest
        # transitive dep — if it succeeded, the inputs this pipeline needs
        # are all current.
        "name": "upsert_system_metrics_daily",
        "module": "pipelines.upsert_system_metrics_daily",
        "depends_on": "compute_bunching",
    },
    {
        # Per-(route, service_date) sufficient-statistics overlay written
        # to `route_metrics_daily_overlay`. Lets the windowed scorecard
        # endpoint read materialized rows instead of recomputing ~35s of
        # live aggregation on every cold cache hit. Same upstream deps as
        # `upsert_system_metrics_daily` — both consume the per-date
        # stop_events + runs the prior pipelines produced.
        "name": "upsert_route_metrics_overlay",
        "module": "pipelines.upsert_route_metrics_overlay",
        "depends_on": "compute_bunching",
    },
]

# Housekeeping pipelines that aren't date-scoped — they operate on the global
# table (or other shared state) rather than a per-(route, date) subset. Run
# ONCE per batch invocation, after all per-date pipelines have completed.
# Failures here log but don't block the run — these are housekeeping, not
# the metrics critical path.
HOUSEKEEPING_PIPELINES: list[dict] = [
    {
        "name": "archive_trip_update_snapshots",
        "module": "pipelines.archive_trip_update_snapshots",
    },
    {
        # Refresh `route_diagnostic_segment/_timepoint/_direction` from the
        # last 30 days of stop_events (NOTES-57). The diagnostic surfaces
        # are windowed pooled samples — re-running them per service date
        # would re-compute the same window N times. Once per batch is
        # right; runs after the per-date pipelines have committed so the
        # latest day is included.
        "name": "refresh_route_diagnostic_profile",
        "module": "pipelines.refresh_route_diagnostic_profile",
    },
    {
        # Two-pass cleanup of trip_update_state: deletes rows that were
        # derived more than 2 days ago (normal lifecycle), and as a
        # safety net deletes any un-derived rows older than 7 days.
        # Keeps the state table bounded so it can't grow unbounded
        # if a trip is never anchored to a service_date.
        "name": "cleanup_trip_update_state",
        "module": "pipelines.cleanup_trip_update_state",
    },
    {
        # Refresh `cross_route_segment_rollup` from `route_diagnostic_segment`
        # (NOTES-59). Must run AFTER `refresh_route_diagnostic_profile` so the
        # source table is current. Aggregates per-stop-pair slip across all
        # routes for the cross-route segment diagnostic /segments page.
        "name": "refresh_cross_route_segments",
        "module": "pipelines.refresh_cross_route_segments",
    },
]


def determine_target_dates(lookback_days: int = 7) -> list[date_type]:
    """Return the service dates this batch should process.

    Always includes yesterday (Eastern). Additionally scans the prior
    `lookback_days` service dates and includes any that have ZERO rows in
    `runs` — those are catch-up targets where the scheduler missed a day.

    Service dates are returned in ascending order (oldest first) so the
    catch-up pipelines see data in chronological order, matching how a
    human would re-run them.
    """
    today = eastern_today()
    yesterday = today - timedelta(days=1)

    catch_up_window: list[date_type] = []
    db = get_session()
    try:
        # Scan from (today - lookback_days) up to (today - 2 == yesterday - 1).
        # Yesterday itself is always processed; skip it in the catch-up
        # branch to avoid double-listing. iter_eastern_dates yields
        # ascending; that matches the existing oldest-first sort below.
        catch_up_start = today - timedelta(days=lookback_days)
        catch_up_end = today - timedelta(days=2)
        for candidate in iter_eastern_dates(catch_up_start, catch_up_end):
            count = db.query(Run).filter(Run.service_date == candidate.isoformat()).count()
            if count == 0:
                catch_up_window.append(candidate)
    finally:
        db.close()

    targets = sorted(set(catch_up_window + [yesterday]))
    return targets


def list_active_route_ids() -> list[str]:
    """Return the route_ids of every current GTFS route, sorted.

    `is_current=True` is the project-wide GTFS versioning filter.
    """
    db = get_session()
    try:
        rows = db.query(Route.route_id).filter(Route.is_current).order_by(Route.route_id).all()
        return [r[0] for r in rows]
    finally:
        db.close()


def run_housekeeping_pipeline(
    module: str,
    log_handle,
) -> tuple[int, float]:
    """Run a single non-date-scoped housekeeping pipeline.

    Same subprocess pattern as `run_pipeline`, minus `--all-routes`/`--date`
    args — housekeeping pipelines like `archive_trip_update_snapshots`
    operate on the global table, not a per-(route, date) subset.
    """
    cmd = [sys.executable, "-m", module]
    log_handle.write(f"\n$ {' '.join(cmd)}\n")
    log_handle.flush()
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        check=False,
    )
    elapsed = time.time() - start
    log_handle.write(f"[exit {proc.returncode}, {elapsed:.1f}s]\n")
    log_handle.flush()
    return proc.returncode, elapsed


def _v2_row_count_guard(service_date: date_type, log_handle) -> bool:
    """Return True if stop_events_v2 has rows for ``service_date``.

    Closes the silent-failure mode that broke Phase D validation for 4
    consecutive nightly batches (2026-05-16 → 19): the v2 derivation
    crashed at the first non-empty route, exited 0 for the "no rows to
    upsert" path on the remaining routes, and the wrapper had no way to
    distinguish "ran successfully and produced rows" from "ran
    successfully and produced nothing." This guard flips the latter into
    a logged failure.

    The check uses a fresh session because the long-lived batch process
    doesn't hold a Postgres connection. Reports via ``log_handle`` and
    returns True/False rather than raising — the caller treats it as a
    soft failure (counts toward exit-non-zero, doesn't block downstream).
    """
    from sqlalchemy import text

    db = get_session()
    try:
        n = db.execute(
            text("SELECT COUNT(*) FROM stop_events_v2 WHERE service_date = :d"),
            {"d": service_date.isoformat()},
        ).scalar_one()
    finally:
        db.close()
    if not n:
        log_handle.write(
            f"GUARD stop_events_v2 has 0 rows for {service_date.isoformat()} "
            "after v2 derivation exited 0 — silent-failure guard tripped\n"
        )
        log_handle.flush()
        return False
    log_handle.write(f"GUARD stop_events_v2 has {n} rows for {service_date.isoformat()}\n")
    log_handle.flush()
    return True


def _run_comparison_check(service_date: date_type, log_handle) -> None:
    """Invoke compare_old_vs_new_derivation.py and write a WARN if thresholds exceeded.

    Informational only — never fails the batch. Output (one line) is captured
    and parsed via _check_comparison_thresholds. A violation writes a
    grep-able ``WARN compare_old_vs_new_derivation: <reason>`` line.

    Temporary: removed at NOTES-72 Phase E cutover when stop_events_v2 is
    dropped. See docs/superpowers/specs/2026-05-23-stop-events-v2-cutover-design.md.

    Args:
        service_date: The service date to compare.
        log_handle: Open file handle for the daily batch log.
    """
    cmd = [
        sys.executable,
        "-m",
        "pipelines.compare_old_vs_new_derivation",
        "--date",
        service_date.isoformat(),
    ]
    log_handle.write(f"\n$ {' '.join(cmd)}\n")
    log_handle.flush()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=300)
    except subprocess.TimeoutExpired:
        log_handle.write("WARN compare_old_vs_new_derivation: timed out after 300s\n")
        log_handle.flush()
        return
    log_handle.write(proc.stdout)
    if proc.stderr:
        log_handle.write(proc.stderr)
    if proc.returncode != 0:
        log_handle.write(f"WARN compare_old_vs_new_derivation: exited {proc.returncode}\n")
        log_handle.flush()
        return
    last_line = (proc.stdout.strip().splitlines() or [""])[-1]
    reason = _check_comparison_thresholds(last_line)
    if reason:
        log_handle.write(f"WARN compare_old_vs_new_derivation: {reason}\n")
    log_handle.flush()


def run_pipeline(
    module: str,
    service_date: date_type,
    log_handle,
    extra_args: list[str] | None = None,
) -> tuple[int, float]:
    """Run a single pipeline module via `python -m ...` for one service date.

    Uses `--all-routes` (the wrapper's design contract is "cover everything")
    and `--date YYYY-MM-DD`. Pipeline stdout/stderr is appended to
    `log_handle`; the return code and elapsed wall time are returned.

    ``extra_args`` is appended after ``--all-routes --date X`` for pipelines
    that need additional flags (e.g., derive_stop_events_from_state's
    ``--target-table stop_events_v2`` for Phase D validation).

    Subprocess is the chosen integration mechanism: the four pipelines are
    already CLI scripts and the user's manual workflow is `uv run python
    pipelines/...`. Wrapping them as a library would be a bigger change
    out of scope for this orchestration PR.

    Invokes `sys.executable` directly rather than `uv run python` because the
    outer entry (the plist or a manual `uv run python pipelines/run_daily_batch.py`)
    has already activated the venv — `sys.executable` points at the venv's
    Python. Re-resolving via `uv` per subprocess would also fail under launchd,
    which strips PATH down to a minimal set that doesn't include Homebrew.
    """
    cmd = [
        sys.executable,
        "-m",
        module,
        "--all-routes",
        "--date",
        service_date.isoformat(),
    ]
    if extra_args:
        cmd.extend(extra_args)
    log_handle.write(f"\n$ {' '.join(cmd)}\n")
    log_handle.flush()
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        check=False,
    )
    elapsed = time.time() - start
    log_handle.write(f"[exit {proc.returncode}, {elapsed:.1f}s]\n")
    log_handle.flush()
    return proc.returncode, elapsed


def run_batch(
    target_dates: list[date_type],
    log_handle,
    dry_run: bool = False,
) -> int:
    """Drive all per-date pipelines across every (pipeline, target_date) cell.

    Returns the number of (pipeline, date) combinations that failed —
    callers turn a non-zero into a non-zero process exit so launchd can
    surface it.
    """
    failure_count = 0
    # results[date][pipeline_name] = exit_code, used for skipping downstream
    # pipelines whose hard dependency just failed.
    results: dict[date_type, dict[str, int]] = {d: {} for d in target_dates}

    for service_date in target_dates:
        log_handle.write(f"\n=== service_date={service_date.isoformat()} ===\n")
        log_handle.flush()
        for pipeline in PIPELINES:
            dep = pipeline["depends_on"]
            if dep is not None and results[service_date].get(dep, 0) != 0:
                msg = (
                    f"SKIP {pipeline['name']} for {service_date.isoformat()} — "
                    f"hard dependency {dep} failed (exit "
                    f"{results[service_date].get(dep)})\n"
                )
                log_handle.write(msg)
                log_handle.flush()
                failure_count += 1
                results[service_date][pipeline["name"]] = -1
                continue

            if dry_run:
                extra_args = pipeline.get("extra_args", []) or []
                extra_str = " ".join(extra_args)
                log_handle.write(
                    f"DRY-RUN would run {pipeline['module']} "
                    f"--all-routes --date {service_date.isoformat()}"
                    f"{(' ' + extra_str) if extra_str else ''}\n"
                )
                results[service_date][pipeline["name"]] = 0
                continue

            rc, elapsed = run_pipeline(
                pipeline["module"],
                service_date,
                log_handle,
                extra_args=pipeline.get("extra_args"),
            )
            results[service_date][pipeline["name"]] = rc
            if rc != 0:
                failure_count += 1
                log_handle.write(
                    f"FAIL {pipeline['name']} for {service_date.isoformat()}: "
                    f"exit {rc} after {elapsed:.1f}s\n"
                )
            else:
                log_handle.write(
                    f"OK   {pipeline['name']} for {service_date.isoformat()}: {elapsed:.1f}s\n"
                )
                # Post-step row-count guard for the v2 derivation. Catches the
                # silent-zero failure mode (process exits 0 but writes 0 rows).
                if pipeline["name"] == "derive_stop_events_from_state_v2":
                    if not _v2_row_count_guard(service_date, log_handle):
                        # Mark as failed for the rest of this run so the
                        # process exits non-zero. We don't downgrade the
                        # original rc=0 in `results` (no downstream depends
                        # on v2), but the wrapper-level failure count is
                        # what launchd surfaces.
                        failure_count += 1
                    else:
                        # NOTES-72 Phase D monitoring: run the comparison
                        # and log a WARN line if any threshold is exceeded.
                        # Informational only; does NOT fail the batch.
                        # Removed at cutover (see spec
                        # docs/superpowers/specs/2026-05-23-stop-events-v2-cutover-design.md).
                        _run_comparison_check(service_date, log_handle)
            log_handle.flush()

    # Housekeeping runs ONCE per batch, after all per-date pipelines. Failures
    # log but do not increment the metrics-critical failure count; the caller
    # surfaces housekeeping outcomes through the same exit code so launchd
    # still notices, but a metrics-redesign pipeline failure is the priority
    # signal.
    for hk in HOUSEKEEPING_PIPELINES:
        log_handle.write(f"\n=== housekeeping: {hk['name']} ===\n")
        log_handle.flush()
        if dry_run:
            log_handle.write(f"DRY-RUN would run {hk['module']}\n")
            continue
        rc, elapsed = run_housekeeping_pipeline(hk["module"], log_handle)
        if rc != 0:
            # Soft-failure: log it, count it (so the wrapper exits non-zero),
            # but don't let it block anything (no downstream depends on it).
            failure_count += 1
            log_handle.write(
                f"FAIL housekeeping {hk['name']}: exit {rc} after {elapsed:.1f}s (non-blocking)\n"
            )
        else:
            log_handle.write(f"OK   housekeeping {hk['name']}: {elapsed:.1f}s\n")
        log_handle.flush()

    return failure_count


def main() -> int:
    """CLI entry point — opens the day's log file and drives the batch."""
    parser = argparse.ArgumentParser(
        description=(
            "Run all per-date metrics pipelines for yesterday plus any "
            "recent service date that's missing from `runs`."
        )
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="How far back to scan for catch-up dates (default: 7).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without invoking subprocesses.",
    )
    args = parser.parse_args()

    load_dotenv()
    LOGS_DIR.mkdir(exist_ok=True)
    today = eastern_today()
    log_path = LOGS_DIR / f"daily_batch_{today.isoformat()}.log"

    target_dates = determine_target_dates(lookback_days=args.lookback_days)
    route_ids = list_active_route_ids()

    with log_path.open("a") as log_handle:
        log_handle.write(
            f"\n========== run_daily_batch start {today.isoformat()} "
            f"({len(route_ids)} active routes) ==========\n"
        )
        log_handle.write(f"target_dates: {[d.isoformat() for d in target_dates]}\n")
        if args.dry_run:
            log_handle.write("(dry-run mode — no subprocesses will be invoked)\n")
        log_handle.flush()
        # Mirror the header to stdout too — useful when launchd captures stdout
        # to a separate file or when running by hand.
        print(f"run_daily_batch: log={log_path}")
        print(f"run_daily_batch: target_dates={[d.isoformat() for d in target_dates]}")

        failure_count = run_batch(target_dates, log_handle, dry_run=args.dry_run)

        log_handle.write(
            f"\n========== run_daily_batch done — "
            f"{failure_count} (pipeline, date) failures ==========\n"
        )

    print(f"run_daily_batch: {failure_count} (pipeline, date) failures")
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
