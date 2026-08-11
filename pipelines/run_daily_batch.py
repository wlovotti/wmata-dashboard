"""
Daily metrics batch with catch-up. The launchd-scheduled entry point that
keeps `stop_events`, `runs`, and `route_bunching_periods` populated for
every active route on every recent service date.

Closes NOTES-28. Before this, the metrics-redesign pipelines —
`derive_stop_events`, `derive_stop_events_from_state`, `aggregate_runs`,
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
   derive_stop_events (proximity source) + derive_stop_events_from_state
   (trip_update source, reads `trip_update_state`) first — both write
   `stop_events`, both independent — then aggregate_runs (reads
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
ran on, not the service date) — or `logs/daily_batch_<agency>_<date>.log`
for a non-default `--agency`, so a concurrent SFMTA run doesn't
interleave with the WMATA log. The `logs/` dir is gitignored (see
.gitignore line 41) and is the same convention the continuous collector
uses.

Multi-agency (NOTES-100): pass `--agency` (matching a
`config/agencies/<agency>.yaml`) to run the per-date pipeline chain
(derive_stop_events*, aggregate_runs, compute_bunching,
upsert_system_metrics_daily, upsert_route_metrics_overlay) against a
non-WMATA database with the correct service-date timezone. The
non-date-scoped HOUSEKEEPING_PIPELINES below are skipped entirely for
any agency other than the default: three of the four
(refresh_route_diagnostic_profile, refresh_cross_route_segments,
refresh_corridor_slip) are NOT agency-aware and always target
`DATABASE_URL` (WMATA); the fourth, `cleanup_trip_update_state.py`, IS
agency-aware but needs its own separate invocation/schedule per agency
(`cleanup_trip_update_state.py --agency sfmta`) rather than running
here, so `run_batch` skips the whole group rather than a non-WMATA run
silently touching the WMATA database. Omitting `--agency` keeps
today's WMATA/Eastern behavior, including housekeeping, unchanged.

The six per-date pipelines above bucket `ewt_seconds` and `bunching_rate`
by the agency's own local hour-of-day (the agency-local hour bucketing
fix, PR #190) — `otp_percentage` and `service_delivered_ratio` never
needed this since they don't bucket by hour. `run_daily_batch.py --agency
sfmta` now writes correct EWT/bunching numbers alongside OTP/service-
delivered.

Usage:
  uv run python pipelines/run_daily_batch.py
  uv run python pipelines/run_daily_batch.py --lookback-days 14   # wider catch-up
  uv run python pipelines/run_daily_batch.py --dry-run            # print plan, don't execute
  uv run python pipelines/run_daily_batch.py --agency sfmta       # SFMTA sidecar DB
"""

import argparse
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.date_ranges import iter_eastern_dates
from src.models import Route, Run
from src.timezones import eastern_today, local_today

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"
DEFAULT_AGENCY = "wmata"

# PR #193 review: the derive_stop_events* pipelines print this marker (see
# their main()s) reporting every distinct service_date they actually wrote —
# usually just the requested date, but the resolver's owl-route anchor
# correction (NOTES-110) can additionally write service_date - 1. Matched
# against the log file slice this run's subprocess appended (see
# `run_pipeline`).
_SERVICE_DATES_WRITTEN_RE = re.compile(r"^SERVICE_DATES_WRITTEN:(\S*)$", re.MULTILINE)


def _parse_service_dates_written(output: str) -> set[date_type]:
    """Parse a derive pipeline's "SERVICE_DATES_WRITTEN: ..." marker line.

    Returns the set of service dates the subprocess reported writing
    stop_events rows under. Only the derive_stop_events* pipelines print
    this marker; other pipelines' output has no match and this returns an
    empty set. Malformed tokens (should never happen — the marker is
    machine-generated) are skipped rather than raising, since a parse
    hiccup here shouldn't take down the whole batch.
    """
    match = _SERVICE_DATES_WRITTEN_RE.search(output or "")
    if not match or not match.group(1):
        return set()
    dates: set[date_type] = set()
    for token in match.group(1).split(","):
        token = token.strip()
        if not token:
            continue
        try:
            dates.add(date_type.fromisoformat(token))
        except ValueError:
            continue
    return dates


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
        # NOTES-72 Phase E.1 switched the primary trip_update derivation to
        # derive_stop_events_from_state (reads trip_update_state). Phase E.2
        # stopped the collector's snapshot dual-write. Phase F (the
        # trip-update-state retirement, PR #155) deleted the old pipeline and
        # its archive job; trip_update_snapshots is dropped via the manual
        # runbook in migrate_drop_phase_f.py.
        "name": "derive_stop_events_from_state",
        "module": "pipelines.derive_stop_events_from_state",
        "depends_on": None,
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
    {
        # Refresh `corridor_slip_rollup` from `route_diagnostic_segment` via
        # `corridor_route_membership` (NOTES-62). Must also run AFTER
        # `refresh_route_diagnostic_profile` (shared source). Aggregates
        # per-corridor slip — the shape-aware V2 of the stop-pair rollup,
        # backing the level=corridor view on /segments.
        "name": "refresh_corridor_slip",
        "module": "pipelines.refresh_corridor_slip",
    },
]


def determine_target_dates(lookback_days: int = 7, agency: str = DEFAULT_AGENCY) -> list[date_type]:
    """Return the service dates this batch should process.

    Always includes yesterday (agency-local; Eastern for the default
    `wmata` agency). Additionally scans the prior `lookback_days` service
    dates and includes any that have ZERO rows in `runs` — those are
    catch-up targets where the scheduler missed a day.

    Service dates are returned in ascending order (oldest first) so the
    catch-up pipelines see data in chronological order, matching how a
    human would re-run them.

    Args:
        lookback_days: How far back to scan for catch-up dates.
        agency: Agency name matching `config/agencies/<agency>.yaml`
            (NOTES-100 multi-agency; default `"wmata"`). Selects the
            timezone used for "today"/"yesterday" and the database
            queried for the catch-up scan.
    """
    cfg = load_agency_config(agency)
    today = local_today(cfg.timezone)
    yesterday = today - timedelta(days=1)

    catch_up_window: list[date_type] = []
    db = get_session(db_url=resolve_agency_db_url(cfg))
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


def list_active_route_ids(agency: str = DEFAULT_AGENCY) -> list[str]:
    """Return the route_ids of every current GTFS route, sorted.

    `is_current=True` is the project-wide GTFS versioning filter.

    Args:
        agency: Agency name matching `config/agencies/<agency>.yaml`
            (NOTES-100 multi-agency; default `"wmata"`). Selects the
            database queried.
    """
    cfg = load_agency_config(agency)
    db = get_session(db_url=resolve_agency_db_url(cfg))
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
    args — housekeeping pipelines like `retain_trip_update_state`
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


def run_pipeline(
    module: str,
    service_date: date_type,
    log_handle,
    extra_args: list[str] | None = None,
    agency: str = DEFAULT_AGENCY,
) -> tuple[int, float, set[date_type]]:
    """Run a single pipeline module via `python -m ...` for one service date.

    Uses `--all-routes` (the wrapper's design contract is "cover everything")
    and `--date YYYY-MM-DD`. Pipeline stdout/stderr is appended to
    `log_handle`; the return code, elapsed wall time, and (PR #193 review)
    the set of service_dates the pipeline reported writing (parsed from its
    SERVICE_DATES_WRITTEN marker — empty for pipelines that don't print
    one) are returned.

    ``extra_args`` is appended after ``--all-routes --date X`` for pipelines
    that need additional flags. None of the currently-configured pipelines
    use it; the hook remains for future per-pipeline overrides.

    ``agency`` (NOTES-100 multi-agency) is always appended as
    `--agency <agency>` — every per-date pipeline this wrapper drives
    (derive_stop_events*, aggregate_runs, compute_bunching,
    upsert_system_metrics_daily, upsert_route_metrics_overlay) accepts
    it and defaults to `"wmata"` itself, so this is explicit rather than
    a behavior change for the default case.

    Subprocess is the chosen integration mechanism: the four pipelines are
    already CLI scripts and the user's manual workflow is `uv run python
    pipelines/...`. Wrapping them as a library would be a bigger change
    out of scope for this orchestration PR.

    Invokes `sys.executable` directly rather than `uv run python` because the
    outer entry (the plist or a manual `uv run python pipelines/run_daily_batch.py`)
    has already activated the venv — `sys.executable` points at the venv's
    Python. Re-resolving via `uv` per subprocess would also fail under launchd,
    which strips PATH down to a minimal set that doesn't include Homebrew.

    Output streaming (PR #193 review round 2, finding 2): stdout/stderr are
    redirected straight to `log_handle` (same real-time-tailing pattern as
    `run_housekeeping_pipeline`), not captured via `subprocess.PIPE` — a
    prior revision buffered the whole subprocess's output and wrote it only
    after exit, which meant a hung derive emitted nothing at all to the log
    while it hung. Per-route progress lines streaming live is exactly what
    diagnosed the 2026-06/07 nightly-batch slow-seq-scan outage referenced
    below. The SERVICE_DATES_WRITTEN marker is recovered by re-reading the
    slice of the log file this call appended (from the byte offset recorded
    just before the subprocess starts, to EOF) once the subprocess exits,
    rather than from captured stdout.
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
    cmd.extend(["--agency", agency])
    log_handle.write(f"\n$ {' '.join(cmd)}\n")
    log_handle.flush()
    log_path = Path(log_handle.name)
    marker_start_pos = log_path.stat().st_size
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
    with log_path.open("r") as f:
        f.seek(marker_start_pos)
        output = f.read()
    return proc.returncode, elapsed, _parse_service_dates_written(output)


def run_batch(
    target_dates: list[date_type],
    log_handle,
    dry_run: bool = False,
    agency: str = DEFAULT_AGENCY,
) -> int:
    """Drive all per-date pipelines across every (pipeline, target_date) cell.

    Returns the number of (pipeline, date) combinations that failed —
    callers turn a non-zero into a non-zero process exit so launchd can
    surface it.

    ``agency`` (NOTES-100 multi-agency) is forwarded to every per-date
    pipeline invocation as `--agency <agency>`. HOUSEKEEPING_PIPELINES
    are skipped entirely for any non-default agency: they're global
    (not per-route/per-date); three of the four aren't agency-aware and
    always target `DATABASE_URL` (WMATA) — running them under `--agency
    sfmta` would silently operate on the wrong database rather than the
    SFMTA sidecar the caller intended. The fourth,
    `cleanup_trip_update_state.py`, IS agency-aware but is deliberately
    not invoked from this loop for a non-default agency either — see the
    skip-message text this function emits for why. This isn't counted
    as a failure.

    PR #193 review: derive and aggregate are run in two separate phases rather
    than interleaved per date. Phase 1 runs both derive_stop_events*
    pipelines across every target date, collecting the union of
    `target_dates` and any adjacent service_date the resolver's
    owl-route anchor correction (NOTES-110) wrote rows under (via each
    subprocess's SERVICE_DATES_WRITTEN marker — see `run_pipeline`).
    Phase 2 runs aggregate_runs/compute_bunching/the rollups across that
    full union, in ascending date order. Without this split, a date-N
    derive that corrects a row onto date N-1 would run after N-1's own
    aggregation already completed (target_dates are processed oldest
    first) and the row would never be aggregated.
    """
    failure_count = 0
    # results[date][pipeline_name] = exit_code, used for skipping downstream
    # pipelines whose hard dependency just failed. defaultdict so a date
    # that only appears via a day-shifted write (not in target_dates) gets
    # an empty dict on first access — dep-check below then treats it as
    # "no failure recorded" (matching the pre-existing default-0 semantics)
    # rather than raising KeyError.
    results: dict[date_type, dict[str, int]] = defaultdict(dict)

    derive_pipelines = [p for p in PIPELINES if p["depends_on"] is None]
    downstream_pipelines = [p for p in PIPELINES if p["depends_on"] is not None]

    # Phase 1: derive_stop_events* for every target date, collecting the
    # full set of service dates (target dates plus any day-shifted dates)
    # before any aggregation runs.
    all_service_dates: set[date_type] = set(target_dates)
    for service_date in target_dates:
        log_handle.write(f"\n=== derive: service_date={service_date.isoformat()} ===\n")
        log_handle.flush()
        for pipeline in derive_pipelines:
            if dry_run:
                extra_args = pipeline.get("extra_args", []) or []
                extra_str = " ".join(extra_args)
                log_handle.write(
                    f"DRY-RUN would run {pipeline['module']} "
                    f"--all-routes --date {service_date.isoformat()}"
                    f"{(' ' + extra_str) if extra_str else ''} --agency {agency}\n"
                )
                results[service_date][pipeline["name"]] = 0
                continue

            rc, elapsed, extra_dates = run_pipeline(
                pipeline["module"],
                service_date,
                log_handle,
                extra_args=pipeline.get("extra_args"),
                agency=agency,
            )
            results[service_date][pipeline["name"]] = rc
            all_service_dates.update(extra_dates)
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
            log_handle.flush()

    # Phase 2: aggregate_runs + downstream across the full union, ascending.
    for service_date in sorted(all_service_dates):
        log_handle.write(f"\n=== aggregate: service_date={service_date.isoformat()} ===\n")
        log_handle.flush()
        for pipeline in downstream_pipelines:
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
                    f"{(' ' + extra_str) if extra_str else ''} --agency {agency}\n"
                )
                results[service_date][pipeline["name"]] = 0
                continue

            rc, elapsed, _extra_dates = run_pipeline(
                pipeline["module"],
                service_date,
                log_handle,
                extra_args=pipeline.get("extra_args"),
                agency=agency,
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
            log_handle.flush()

    # Housekeeping runs ONCE per batch, after all per-date pipelines. Failures
    # log but do not increment the metrics-critical failure count; the caller
    # surfaces housekeeping outcomes through the same exit code so launchd
    # still notices, but a metrics-redesign pipeline failure is the priority
    # signal.
    #
    # NOTES-100: the WHOLE loop below is skipped for non-default agencies,
    # not run with a substituted --agency. Three of the four housekeeping
    # pipelines (refresh_route_diagnostic_profile,
    # refresh_cross_route_segments, refresh_corridor_slip) aren't
    # agency-aware and always target DATABASE_URL. The fourth,
    # cleanup_trip_update_state.py, IS agency-aware (accepts --agency) --
    # for the default agency it still runs below via
    # run_housekeeping_pipeline (which passes no --agency flag, so it
    # keeps defaulting to wmata, unchanged). For a non-default agency it's
    # NOT invoked here at all: it needs its own schedule/invocation with
    # `--agency <agency>` (never the default -- that would prune WMATA's
    # table, not the agency's own) so that agency's trip_update_state
    # table stays bounded.
    if agency != DEFAULT_AGENCY:
        log_handle.write(
            f"\nSKIP housekeeping for agency={agency!r} — refresh_route_diagnostic_profile / "
            "refresh_cross_route_segments / refresh_corridor_slip are not agency-aware yet "
            f"(would target DATABASE_URL/{DEFAULT_AGENCY} regardless of --agency). "
            "cleanup_trip_update_state.py IS agency-aware but is not invoked from here for a "
            f"non-default agency -- run `cleanup_trip_update_state.py --agency {agency}` "
            "on its own schedule to keep this agency's trip_update_state table bounded.\n"
        )
        log_handle.flush()
        return failure_count

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
    parser.add_argument(
        "--agency",
        default=DEFAULT_AGENCY,
        help=(
            f"Agency name matching config/agencies/<agency>.yaml (default: "
            f"{DEFAULT_AGENCY!r}). Runs the per-date pipeline chain against "
            "that agency's database and timezone; HOUSEKEEPING_PIPELINES are "
            "skipped for any non-default agency — see module docstring."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    LOGS_DIR.mkdir(exist_ok=True)
    # Log filename uses wall-clock Eastern regardless of --agency (it's when
    # the operator ran the batch, not a service-date concept) but is
    # agency-suffixed so a concurrent SFMTA run doesn't interleave with the
    # WMATA log in the same file.
    today = eastern_today()
    log_suffix = "" if args.agency == DEFAULT_AGENCY else f"_{args.agency}"
    log_path = LOGS_DIR / f"daily_batch{log_suffix}_{today.isoformat()}.log"

    target_dates = determine_target_dates(lookback_days=args.lookback_days, agency=args.agency)
    route_ids = list_active_route_ids(agency=args.agency)

    with log_path.open("a") as log_handle:
        log_handle.write(
            f"\n========== run_daily_batch start {today.isoformat()} "
            f"agency={args.agency} ({len(route_ids)} active routes) ==========\n"
        )
        log_handle.write(f"target_dates: {[d.isoformat() for d in target_dates]}\n")
        if args.dry_run:
            log_handle.write("(dry-run mode — no subprocesses will be invoked)\n")
        log_handle.flush()
        # Mirror the header to stdout too — useful when launchd captures stdout
        # to a separate file or when running by hand.
        print(f"run_daily_batch: log={log_path}")
        print(f"run_daily_batch: target_dates={[d.isoformat() for d in target_dates]}")

        failure_count = run_batch(
            target_dates, log_handle, dry_run=args.dry_run, agency=args.agency
        )

        log_handle.write(
            f"\n========== run_daily_batch done — "
            f"{failure_count} (pipeline, date) failures ==========\n"
        )

    print(f"run_daily_batch: {failure_count} (pipeline, date) failures")
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
