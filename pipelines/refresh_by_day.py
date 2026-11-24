"""Day-at-a-time refresh: fetch one service date's raw files, load, replay, derive, prune.

The laptop can't hold the raw archive (S3 has it all; ~1.7 GB/day for
both agencies), so freshness — whether yesterday or an eight-week
catch-up — processes ONE service date at a time, oldest first, per agency:

1. **fetch** exactly the raw files that date reads from S3
   (``src/raw_archive.py``: TU UTC D and D+1; VP UTC D..D+2),
2. **load VP** for those UTC days (``load_vp_archive.py --utc-date``,
   which reloads a day whose rows were deleted),
3. **replay** trip updates for D (``replay_archive_to_state.py``),
4. **derive** D (``run_daily_batch.py --date D --skip-housekeeping
   --exclude-date <held/failed dates>``),
5. **clean state**: delete ``trip_update_state`` for service dates before
   D (``cleanup_trip_update_state.py --before-date D --vacuum``),
6. **prune** local raw files no later date needs, once each is verified
   byte-identical in S3 and recorded in its manifest.

Date selection (default, per agency):

- every date after the latest ``runs.service_date`` through the latest
  date whose raw window is COMPLETE (below),
- zero-``runs`` dates in the last ``--lookback-days`` (default 14),
- every date the progress file has as ``failed``, ``derived`` or
  ``deferred`` (whatever its age — a date can fail after ``aggregate_runs``
  wrote runs, which would otherwise hide it from the two rules above), and
  ``held`` dates inside the lookback window. Older held dates are listed
  in the plan as "not retried" each run — rerun them with ``--date D``
  after reloading GTFS.
- never an ``abandoned`` date (``--abandon-date D --agency X``): the escape
  for a date that fails every time and would otherwise block the agency's
  default runs forever. It is excluded from aggregation like held/failed
  dates and listed in every summary; ``--date D`` still processes it.

``--start``/``--end`` (inclusive) or ``--date`` (repeatable) select exactly
those dates instead.

Complete raw window: a date D is eligible by default only once now is past
agency-local midnight after D plus ``WINDOW_MARGIN`` (WMATA 5 h, SFMTA
9 h) — the last scheduled arrival of a trip on service day D (current
GTFS, measured 2026-11-24: WMATA 26:58, SFMTA 30:53 — Muni owl service runs
to ~07:00 PT) plus ~2 h. S3 object timestamps show rows land in S3 at most
~16 min after their file opens (15-min rotation + upload), so ~1 h 45 min
is left for late-running trips. Deriving earlier writes runs for a partial
day that is then never revisited. Deferred dates are printed and recorded
as ``deferred`` so default selection always returns to them. An explicit
date inside that margin is processed with a loud warning.

Retry re-folds: a date previously ``failed``/``held``/``abandoned`` is
replayed with ``--force``. ``aggregate_runs`` depends only on the
proximity derive, so a date that failed in the trip-update derive already
has runs; if a later run's cleanup then deleted its state, a plain replay
(manifest + runs present) would not re-fold and the retry would derive
from empty state.

"Done" means BOTH the progress file says ``done`` AND ``runs`` has rows for
the date; the DB wins over the file (a done date with zero runs is
re-processed, a ``derived`` date with no runs is re-run in full, a corrupt
progress file is set aside with a warning). ``--force`` re-processes done
dates and forces the replay.

Failure policy: a hard failure (fetch, VP load, replay, or derive exit 1)
STOPS that agency for the rest of the run — the other agency continues —
because failures are almost always systemic and stopping keeps resume
exact (the failed date is re-selected next run). Its local raw files are
kept. A held date (``run_daily_batch`` exit 3) is reported and the run
continues. A date whose match-rate guard could not run (exit 4) is derived
and reported as a warning; the run exits non-zero.

trip_update_state: after each derived (or held) date D, state for service
dates before ``min(D, local today - 7)`` is deleted and the table is
vacuumed, so a backfill reuses the same space instead of growing a day of
state per date. D itself survives because D+1's completeness rollup reads
state polled inside local day D+1, which includes D's owl trips; nothing
reads older state (derive reads only ``service_date == D``). End of run:
WMATA housekeeping once (skipped if a WMATA date failed this run, or with
``--skip-housekeeping``) and SFMTA's 7-day cleanup once (skipped if an
SFMTA date failed). A date whose state was deleted before it was derived
self-heals: replay re-folds a date with neither state nor runs.

A non-dry-run holds ``<main checkout>/logs/.pull-and-derive.lock.d``
(``src/run_lock.py``, shared by every worktree); progress files also live
in the main checkout's ``logs/``.

Usage:
    uv run python -m pipelines.refresh_by_day                       # catch up
    uv run python -m pipelines.refresh_by_day --agency wmata --date 2026-09-29
    uv run python -m pipelines.refresh_by_day --start 2026-09-29 --end 2026-11-23
    uv run python -m pipelines.refresh_by_day --dry-run             # plan only, writes nothing
    uv run python -m pipelines.refresh_by_day --agency wmata --abandon-date 2026-10-07
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from src import run_lock
from src.agency_config import load_agency_config, resolve_agency_db_url
from src.raw_archive import (
    ArchiveStream,
    fetch_day_files,
    last_unneeded_utc_date,
    prune_stream,
    streams_for_agency,
    summarize_prune,
    utc_dates_needed,
)
from src.timezones import (
    local_date_from_naive_utc,
    local_midnight_as_utc,
    local_today,
    utcnow_naive,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENCIES = ("wmata", "sfmta")
DEFAULT_LOOKBACK_DAYS = 14
# June 2026 dates need GTFS snapshot 12 and the recovery driver
# (scripts/local_recovery_2026_07.sh); never re-derive them from here.
DATA_FLOOR = date(2026, 6, 14)
DEFAULT_MIN_FREE_GB = 15.0
# Time after agency-local midnight following D before D's raw window is
# complete: last scheduled arrival (WMATA 26:58 ET, SFMTA 30:53 PT, current
# GTFS 2026-11-24) + ~2 h. Measured from S3 LastModified vs file-open epoch
# (2026-11-20): rows reach S3 at most ~16 min after their file opens
# (15-min rotation + upload), leaving ~1 h 45 min for late-running trips.
WINDOW_MARGIN = {"wmata": timedelta(hours=5), "sfmta": timedelta(hours=9)}
# bin/pull-and-derive.sh's historical local-dir overrides, per (agency, kind).
DIR_ENV = {
    ("wmata", "tu"): "LOCAL_ARCHIVE",
    ("sfmta", "tu"): "LOCAL_ARCHIVE_SFMTA",
    ("wmata", "vp"): "LOCAL_ARCHIVE_VP",
    ("sfmta", "vp"): "LOCAL_ARCHIVE_SFMTA_VP",
}

DERIVED = "derived"
DONE = "done"
HELD = "held"
FAILED = "failed"
# Operator gave up on the date (--abandon-date): never re-selected by default,
# excluded from aggregation like held/failed, always listed in the summary.
ABANDONED = "abandoned"
# Not yet complete when a default run planned it; default selection always
# returns to it, even after later dates were derived explicitly.
DEFERRED = "deferred"

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_HELD_ONLY = 3  # same meaning as run_daily_batch's EXIT_GUARD_SKIPS_ONLY
BATCH_GUARD_ERROR = 4  # run_daily_batch's EXIT_GUARD_ERROR


def log(msg: str) -> None:
    """Print one timestamped (UTC) progress line, flushed."""
    print(f"[{utcnow_naive():%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


class Progress:
    """Per-agency JSON record of each service date's refresh status.

    ``{"dates": {"2026-10-03": {"status": "done", "at": "...", "detail": ""}}}``.
    Written atomically (temp file + rename) after every status change so a
    crash never leaves a half-written file. It is a resume hint, never the
    source of truth — callers check ``runs`` too. An unreadable file is
    renamed to ``*.corrupt-<ts>`` with a warning and the run starts from
    the DB alone.
    """

    def __init__(self, path: Path, read_only: bool = False):
        """Load ``path`` if it exists and parses, else start empty.

        ``read_only`` (dry runs) leaves an unreadable file where it is.
        """
        self.path = path
        self.data: dict = {"dates": {}}
        if path.exists():
            try:
                data = json.loads(path.read_text())
                if not isinstance(data, dict) or not isinstance(data.get("dates", {}), dict):
                    raise ValueError("unexpected structure")
                data.setdefault("dates", {})
                self.data = data
            except (OSError, ValueError) as exc:
                aside = path.with_name(f"{path.name}.corrupt-{utcnow_naive():%Y%m%dT%H%M%S}")
                log(
                    f"WARNING: progress file {path} unreadable ({exc}); "
                    + ("ignoring it" if read_only else f"moved to {aside.name}")
                    + ", falling back to the DB (runs) alone"
                )
                if not read_only:
                    path.rename(aside)

    def status(self, d: date) -> str | None:
        """Return the recorded status for ``d`` (None if never attempted)."""
        entry = self.data["dates"].get(d.isoformat())
        return entry.get("status") if isinstance(entry, dict) else None

    def dates_with(self, *statuses: str) -> list[date]:
        """Dates whose recorded status is one of ``statuses``, ascending."""
        out = []
        for key, entry in self.data["dates"].items():
            if isinstance(entry, dict) and entry.get("status") in statuses:
                try:
                    out.append(date.fromisoformat(key))
                except ValueError:
                    continue
        return sorted(out)

    def record(self, d: date, status: str, detail: str = "") -> None:
        """Set ``d``'s status and persist immediately."""
        self.data["dates"][d.isoformat()] = {
            "status": status,
            "at": utcnow_naive().isoformat(timespec="seconds"),
            "detail": detail,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True))
        os.replace(tmp, self.path)


@dataclass
class AgencyRun:
    """One agency's state for this run: streams, progress, dates, runs presence, outcomes."""

    name: str
    tz_name: str
    tu: ArchiveStream
    vp: ArchiveStream
    progress: Progress
    dates: list[date]
    dates_with_runs: set[date] = field(default_factory=set)
    outcomes: dict[date, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    to_defer: list[date] = field(default_factory=list)
    stopped: bool = False

    @property
    def streams(self) -> tuple[ArchiveStream, ArchiveStream]:
        """Both raw streams (TU, VP)."""
        return (self.tu, self.vp)

    def first_needed_utc_date(self) -> date | None:
        """Earliest UTC file date this run's dates read (the per-run prune floor)."""
        if not self.dates:
            return None
        return min(min(utc_dates_needed(s, self.dates[0])) for s in self.streams)

    def is_done(self, d: date) -> bool:
        """Done = progress says done AND runs has rows for ``d`` (the DB wins)."""
        return self.progress.status(d) == DONE and d in self.dates_with_runs

    def excluded_for(self, d: date) -> list[date]:
        """Held/failed dates (other than ``d``) a neighbour's derive must not aggregate."""
        return [x for x in self.progress.dates_with(HELD, FAILED, ABANDONED) if x != d]


class RealSteps:
    """The production implementation of each per-date step.

    Pipelines run as subprocesses (``sys.executable -m ...``, the same
    pattern ``run_daily_batch`` uses) so each keeps its own CLI contract
    and exit code; fetch and prune run in-process against boto3 and the
    agency DB. Tests substitute a fake with the same methods.
    """

    def __init__(self, min_free_gb: float = DEFAULT_MIN_FREE_GB):
        """Build the boto3 S3 client; DB sessions are opened per prune call."""
        import boto3

        self._s3 = boto3.client("s3")
        self.min_free_gb = min_free_gb

    def _run(self, args: list[str]) -> int:
        """Run ``python -m <args>`` from the repo root, streaming output; return its rc."""
        cmd = [sys.executable, "-m", *args]
        log("$ " + " ".join(cmd))
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        return subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=False).returncode

    def fetch(self, run: AgencyRun, d: date) -> None:
        """Download the raw files ``d`` reads for both streams (raises on failure).

        Refuses to start when free disk is below ``min_free_gb``. That is
        a coarse floor checked once per date before anything is written —
        it keeps a backfill from starting a date with the disk nearly
        full, not from a single date's DB writes overrunning it.
        """
        for stream in run.streams:
            stream.local_dir.mkdir(parents=True, exist_ok=True)
            free_gb = shutil.disk_usage(stream.local_dir).free / 1e9
            if free_gb < self.min_free_gb:
                raise RuntimeError(
                    f"only {free_gb:.1f} GB free under {stream.local_dir} "
                    f"(< --min-free-gb {self.min_free_gb}); free space and rerun"
                )
        utc_today = utcnow_naive().date()
        for stream in run.streams:
            for utc_date in utc_dates_needed(stream, d):
                if utc_date > utc_today:
                    continue  # not written yet
                fetch_day_files(self._s3, stream, utc_date, log=log)

    def load_vp(self, run: AgencyRun, d: date) -> int:
        """Load (reconciling/reloading as needed) the VP UTC days ``d`` reads."""
        args = [
            "pipelines.load_vp_archive",
            "--agency",
            run.name,
            "--archive-root",
            str(run.vp.local_dir),
        ]
        for utc_date in utc_dates_needed(run.vp, d):
            args += ["--utc-date", utc_date.isoformat()]
        return self._run(args)

    def replay(self, run: AgencyRun, d: date, force: bool) -> int:
        """Replay ``d``'s trip updates into trip_update_state.

        SFMTA keeps ``--allow-empty`` (its archive starts 2026-07-22 and has
        known single-day holes); WMATA stays strict — a missing WMATA day is
        an anomaly that must fail loudly (NOTES-93).
        """
        args = [
            "pipelines.replay_archive_to_state",
            "--date",
            d.isoformat(),
            "--agency",
            run.name,
            "--archive-root",
            str(run.tu.local_dir),
        ]
        if run.name != "wmata":
            args.append("--allow-empty")
        if force:
            args.append("--force")
        return self._run(args)

    def derive(self, run: AgencyRun, d: date, exclude: list[date]) -> int:
        """Run the per-date chain for ``d`` only, never aggregating ``exclude`` dates."""
        args = [
            "pipelines.run_daily_batch",
            "--agency",
            run.name,
            "--date",
            d.isoformat(),
            "--skip-housekeeping",
        ]
        for x in exclude:
            args += ["--exclude-date", x.isoformat()]
        return self._run(args)

    def clean_state(self, run: AgencyRun, d: date) -> int:
        """Delete trip_update_state before ``min(d, today - 7)`` and VACUUM the table."""
        return self._run(
            [
                "pipelines.cleanup_trip_update_state",
                "--agency",
                run.name,
                "--before-date",
                d.isoformat(),
                "--vacuum",
            ]
        )

    def prune(self, run: AgencyRun, d: date) -> None:
        """Delete local raw files the dates after ``d`` don't need (verified + processed).

        Only files dated within this run's span are considered (older
        leftovers are ``scripts/prune_local_archive.py``'s job). Never
        raises: a prune problem (S3 listing error, DB hiccup) only means
        files stay on disk — the next date's prune in this run, or the
        standalone tool, picks them up.
        """
        from src.database import get_session

        cfg = load_agency_config(run.name)
        session = get_session(db_url=resolve_agency_db_url(cfg))
        try:
            for stream in run.streams:
                decisions, _count, _nbytes = prune_stream(
                    self._s3,
                    session,
                    stream,
                    max_utc_date=last_unneeded_utc_date(stream, d + timedelta(days=1)),
                    dry_run=False,
                    min_utc_date=run.first_needed_utc_date(),
                )
                for line in summarize_prune(f"prune {stream.label}", decisions, dry_run=False):
                    log(line)
        except Exception as exc:
            log(f"prune {run.name}: FAILED ({exc!r}) — files kept")
        finally:
            session.close()

    def housekeeping(self) -> int:
        """Run WMATA's HOUSEKEEPING_PIPELINES once (includes its trip_update_state cleanup)."""
        return self._run(["pipelines.run_daily_batch", "--housekeeping-only"])

    def cleanup(self, agency: str) -> int:
        """Run ``cleanup_trip_update_state`` (7-day window) for a non-default agency."""
        return self._run(["pipelines.cleanup_trip_update_state", "--agency", agency])


def process_date(run: AgencyRun, d: date, steps, force: bool) -> str:
    """Run every step for one (agency, date) and return its final status.

    Records progress after the derive (``derived``/``held``/``failed``)
    and again after the prune (``done``). A date already ``derived`` whose
    runs exist (the previous run died between derive and prune) only
    re-runs the prune; without runs it is processed from the top.
    """
    label = f"{run.name} {d}"
    if run.progress.status(d) == DERIVED and not force:
        if d in run.dates_with_runs:
            log(f"== {label}: derived earlier, finishing the prune")
            steps.prune(run, d)
            run.progress.record(d, DONE)
            return DONE
        log(f"== {label}: progress says derived but runs has no rows — processing in full")

    # A date that failed (or was held/abandoned) earlier may already have runs
    # (aggregate_runs depends only on the proximity derive) while its
    # trip_update_state was deleted by a later run's cleanup. Plain replay
    # would see manifest + runs and not re-fold, deriving from empty state —
    # so re-fold it from the raw files (fetched for this date anyway).
    refold = run.progress.status(d) in (FAILED, HELD, ABANDONED)
    if refold and not force:
        log(f"== {label}: previously {run.progress.status(d)} — replay will re-fold (--force)")

    log(f"== {label}: fetch")
    try:
        steps.fetch(run, d)
    except Exception as exc:
        run.progress.record(d, FAILED, f"fetch: {exc}")
        log(f"== {label}: FAILED at fetch ({exc})")
        return FAILED

    for step_name, call in (
        ("load_vp", lambda: steps.load_vp(run, d)),
        ("replay", lambda: steps.replay(run, d, force or refold)),
    ):
        log(f"== {label}: {step_name}")
        rc = call()
        if rc != 0:
            run.progress.record(d, FAILED, f"{step_name} rc={rc}")
            log(f"== {label}: FAILED at {step_name} (rc={rc})")
            return FAILED

    log(f"== {label}: derive")
    rc = steps.derive(run, d, run.excluded_for(d))
    detail = ""
    if rc in (0, BATCH_GUARD_ERROR):
        status = DERIVED
        run.dates_with_runs.add(d)
        if rc == BATCH_GUARD_ERROR:
            detail = "match-rate guard could not run; derived unchecked"
            run.warnings.append(f"{d}: {detail}")
            log(f"== {label}: WARNING {detail}")
    elif rc == EXIT_HELD_ONLY:
        status = HELD
        detail = "match-rate guard held the date"
    else:
        run.progress.record(d, FAILED, f"derive rc={rc}")
        log(f"== {label}: FAILED at derive (rc={rc})")
        return FAILED
    run.progress.record(d, status, detail)

    log(f"== {label}: trip_update_state cleanup (before {d})")
    rc = steps.clean_state(run, d)
    if rc != 0:
        run.warnings.append(f"{d}: per-date trip_update_state cleanup rc={rc}")
        log(f"== {label}: WARNING state cleanup rc={rc} (non-fatal; table grows until it works)")

    log(f"== {label}: prune")
    steps.prune(run, d)
    if status == DERIVED:
        run.progress.record(d, DONE, detail)
        status = DONE
    log(f"== {label}: {status}")
    return status


def run_refresh(runs: list[AgencyRun], steps, force: bool, housekeeping: bool) -> int:
    """Process every agency's dates interleaved oldest-first; return the exit code.

    Exit codes: 0 everything done; 3 the only problem was held dates;
    1 any hard failure, end-of-run housekeeping/cleanup failure, or warning
    (guard could not run, per-date state cleanup failed).
    """
    all_dates = sorted({d for r in runs for d in r.dates})
    for d in all_dates:
        for run in runs:
            if run.stopped or d not in run.dates:
                continue
            if run.is_done(d) and not force:
                log(f"== {run.name} {d}: already done (progress + runs), skipping")
                run.outcomes[d] = DONE
                continue
            status = process_date(run, d, steps, force)
            run.outcomes[d] = status
            if status == FAILED:
                run.stopped = True
                remaining = [x for x in run.dates if x > d]
                log(
                    f"== {run.name}: stopping at {d} — {len(remaining)} later date(s) not "
                    "attempted; fix the cause and rerun to resume here"
                )

    end_rc = 0
    for run in runs:
        worked = any(s in (DONE, HELD) for s in run.outcomes.values())
        failed = any(s == FAILED for s in run.outcomes.values())
        if not worked:
            continue
        if failed:
            log(
                f"== {run.name}: end-of-run "
                f"{'housekeeping' if run.name == 'wmata' else 'trip_update_state cleanup'} "
                "skipped — a date failed this run; its un-derived state is left for the retry"
            )
            continue
        if run.name == "wmata":
            if housekeeping:
                log("== wmata: housekeeping (once, after all dates)")
                if steps.housekeeping() != 0:
                    end_rc = 1
            else:
                log("== wmata: housekeeping skipped (--skip-housekeeping)")
        else:
            log(f"== {run.name}: trip_update_state cleanup (once, after all dates)")
            if steps.cleanup(run.name) != 0:
                end_rc = 1

    log("== summary")
    any_failed = any_held = any_warning = False
    for run in runs:
        counts: dict[str, list[str]] = {}
        for d, s in sorted(run.outcomes.items()):
            counts.setdefault(s, []).append(d.isoformat())
        not_attempted = [x.isoformat() for x in run.dates if x not in run.outcomes]
        parts = [f"{s}={len(v)}" for s, v in sorted(counts.items())]
        log(f"  {run.name}: {', '.join(parts) or 'nothing to do'}")
        for s in (HELD, FAILED):
            if s in counts:
                log(f"    {s}: {' '.join(counts[s])}")
        abandoned = run.progress.dates_with(ABANDONED)
        if abandoned:
            log(f"    abandoned (never derived, never retried): {' '.join(map(str, abandoned))}")
        for w in run.warnings:
            log(f"    WARNING {w}")
        if not_attempted:
            log(
                f"    not attempted: {not_attempted[0]} .. {not_attempted[-1]} ({len(not_attempted)})"
            )
        any_failed |= FAILED in counts
        any_held |= HELD in counts
        any_warning |= bool(run.warnings)
    if any_failed or end_rc or any_warning:
        return EXIT_FAILED
    if any_held:
        return EXIT_HELD_ONLY
    return EXIT_OK


def last_complete_date(tz_name: str, margin: timedelta, now_utc: datetime) -> date:
    """Latest service date D whose raw window is complete at ``now_utc``.

    Complete = ``now_utc >= local midnight after D (as UTC) + margin``.
    """
    d = local_date_from_naive_utc(now_utc, tz_name) - timedelta(days=1)
    while local_midnight_as_utc(d + timedelta(days=1), tz_name) + margin > now_utc:
        d -= timedelta(days=1)
    return d


@dataclass
class DefaultSelection:
    """Result of the default date selection for one agency."""

    dates: list[date]
    deferred: list[date]
    stale_held: list[date]


def default_dates(
    session,
    lookback_days: int,
    today: date,
    last_complete: date | None = None,
    progress: Progress | None = None,
) -> DefaultSelection:
    """Default per-agency dates: after latest runs, recent holes, and unfinished progress.

    ``last_complete`` (default: yesterday) caps every rule; dates after it
    up to yesterday are returned as ``deferred``. ``progress`` adds its
    ``failed``/``derived`` dates of any age and ``held`` dates within the
    lookback window; older held dates come back as ``stale_held``.
    Raises ``RuntimeError`` when ``runs`` is empty (no anchor — pass an
    explicit range instead).
    """
    from sqlalchemy import func

    from src.models import Run

    yesterday = today - timedelta(days=1)
    cap = min(last_complete or yesterday, yesterday)
    latest = session.query(func.max(Run.service_date)).scalar()
    if latest is None:
        raise RuntimeError("runs is empty — pass --start/--end or --date explicitly")
    latest_date = date.fromisoformat(latest)
    dates: set[date] = set()
    d = latest_date + timedelta(days=1)
    while d <= cap:
        dates.add(d)
        d += timedelta(days=1)
    hole_start = max(today - timedelta(days=lookback_days), DATA_FLOOR)
    have_runs = {
        row[0]
        for row in session.query(Run.service_date)
        .filter(Run.service_date >= hole_start.isoformat())
        .distinct()
    }
    d = hole_start
    abandoned = set(progress.dates_with(ABANDONED)) if progress is not None else set()
    while d <= min(cap, latest_date):
        if d.isoformat() not in have_runs and d not in abandoned:
            dates.add(d)
        d += timedelta(days=1)
    stale_held: list[date] = []
    if progress is not None:
        for x in progress.dates_with(FAILED, DERIVED, DEFERRED):
            if DATA_FLOOR <= x <= cap:
                dates.add(x)
        for x in progress.dates_with(HELD):
            if hole_start <= x <= cap:
                dates.add(x)
            elif x < hole_start:
                stale_held.append(x)
    deferred = []
    d = cap + timedelta(days=1)
    while d <= yesterday:
        deferred.append(d)
        d += timedelta(days=1)
    return DefaultSelection(sorted(dates), deferred, stale_held)


def runs_dates(session, dates: list[date]) -> set[date]:
    """Subset of ``dates`` that have at least one ``runs`` row."""
    from src.models import Run

    if not dates:
        return set()
    lo, hi = min(dates).isoformat(), max(dates).isoformat()
    found = {
        date.fromisoformat(row[0])
        for row in session.query(Run.service_date)
        .filter(Run.service_date >= lo, Run.service_date <= hi)
        .distinct()
    }
    return found & set(dates)


def explicit_dates(args) -> list[date] | None:
    """Return the explicit date list from ``--date`` / ``--start``+``--end``, or None."""
    if args.date:
        return sorted(set(args.date))
    if args.start or args.end:
        if not (args.start and args.end):
            raise ValueError("--start and --end must be given together")
        if args.end < args.start:
            raise ValueError("--end is before --start")
        n = (args.end - args.start).days
        return [args.start + timedelta(days=i) for i in range(n + 1)]
    return None


def build_parser() -> argparse.ArgumentParser:
    """CLI definition."""
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--agency", choices=(*AGENCIES, "both"), default="both")
    p.add_argument("--date", action="append", type=date.fromisoformat, help="repeatable")
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--force", action="store_true", help="re-process dates already done")
    p.add_argument(
        "--skip-housekeeping",
        action="store_true",
        help="skip WMATA's end-of-run housekeeping (~1.5 h); for one-day trials",
    )
    p.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB)
    p.add_argument(
        "--progress-dir", type=Path, default=run_lock.main_checkout_root(REPO_ROOT) / "logs"
    )
    p.add_argument("--dry-run", action="store_true", help="print the plan and exit; writes nothing")
    p.add_argument(
        "--abandon-date",
        action="append",
        type=date.fromisoformat,
        help=(
            "record this date (repeatable; needs --agency wmata|sfmta) as abandoned and exit: "
            "never re-selected by default, never aggregated as a side effect, always listed "
            "in the summary. `--date D` still processes it explicitly."
        ),
    )
    p.add_argument(
        "--check-args",
        action="store_true",
        help="validate the arguments and exit (bin/pull-and-derive.sh runs this first)",
    )
    return p


def validate_args(args) -> tuple[list[date] | None, str | None]:
    """Return ``(explicit_dates, error)``; ``error`` is a message for a usage problem."""
    try:
        explicit = explicit_dates(args)
    except ValueError as exc:
        return None, str(exc)
    if args.abandon_date:
        if args.agency == "both":
            return None, "--abandon-date needs --agency wmata or --agency sfmta"
        if explicit is not None or args.dry_run or args.force:
            return (
                None,
                "--abandon-date cannot be combined with --date/--start/--end/--dry-run/--force",
            )
    return explicit, None


def abandon(args, progress_dir: Path) -> int:
    """Record ``args.abandon_date`` dates as abandoned for ``args.agency`` (under the lock)."""
    progress = Progress(progress_dir / f"refresh_progress_{args.agency}.json")
    for d in sorted(set(args.abandon_date)):
        prior = progress.status(d)
        progress.record(d, ABANDONED, f"abandoned by operator (was {prior})")
        log(
            f"{args.agency} {d}: marked abandoned (was {prior}) — default runs skip it and "
            "never aggregate it; `--date D` processes it explicitly"
        )
    return EXIT_OK


def plan_runs(args, explicit: list[date] | None, now_utc: datetime) -> list[AgencyRun] | int:
    """Build each selected agency's AgencyRun (or return an exit code on a usage error)."""
    from src.database import get_session

    agencies = AGENCIES if args.agency == "both" else (args.agency,)
    runs: list[AgencyRun] = []
    for name in agencies:
        to_defer: list[date] = []
        cfg = load_agency_config(name)
        today = local_today(cfg.timezone)
        complete = last_complete_date(cfg.timezone, WINDOW_MARGIN[name], now_utc)
        progress = Progress(
            args.progress_dir / f"refresh_progress_{name}.json", read_only=args.dry_run
        )
        session = get_session(db_url=resolve_agency_db_url(cfg))
        try:
            if explicit is not None:
                dates = explicit
                for d in dates:
                    if progress.status(d) == ABANDONED:
                        log(f"plan {name}: {d} is abandoned; processing it because it was named")
                    if complete < d < today:
                        log(
                            f"WARNING ({name}): {d}'s raw window is not complete yet (needs "
                            f"local midnight after it + {WINDOW_MARGIN[name]}); it will be "
                            "derived from partial data and marked done — rerun it later with "
                            "--date --force"
                        )
            else:
                try:
                    sel = default_dates(session, args.lookback_days, today, complete, progress)
                except RuntimeError as exc:
                    print(f"ERROR ({name}): {exc}", file=sys.stderr)
                    return EXIT_USAGE
                dates = sel.dates
                to_defer = [d for d in sel.deferred if progress.status(d) is None]
                for d in sel.deferred:
                    log(
                        f"plan {name}: deferring {d} — raw window completes at local midnight "
                        f"after it + {WINDOW_MARGIN[name]} (last scheduled arrival + upload lag)"
                    )
                if sel.stale_held:
                    log(
                        f"plan {name}: held dates older than --lookback-days, NOT retried: "
                        f"{' '.join(x.isoformat() for x in sel.stale_held)} — reload GTFS, then "
                        f"`bin/pull-and-derive.sh --agency {name} --date D`"
                    )
            too_old = [d for d in dates if d < DATA_FLOOR]
            too_new = [d for d in dates if d >= today]
            if too_old or too_new:
                print(
                    f"ERROR ({name}): dates must be in [{DATA_FLOOR}, {today - timedelta(days=1)}] "
                    f"(agency-local yesterday); got {(too_old or too_new)[0]}",
                    file=sys.stderr,
                )
                return EXIT_USAGE
            with_runs = runs_dates(session, dates)
        finally:
            session.close()
        tu, vp = streams_for_agency(
            cfg,
            REPO_ROOT,
            tu_dir=os.environ.get(DIR_ENV[(name, "tu")]) or None,
            vp_dir=os.environ.get(DIR_ENV[(name, "vp")]) or None,
        )
        run = AgencyRun(name, cfg.timezone, tu, vp, progress, dates, with_runs)
        run.to_defer = to_defer
        runs.append(run)
    return runs


def record_deferrals(runs: list[AgencyRun]) -> None:
    """Persist deferred dates (under the lock) so default selection always returns to them.

    Without this, a deferred date would become a permanent hole if a later
    date were derived explicitly first and it then fell outside the
    zero-runs lookback.
    """
    for run in runs:
        for d in run.to_defer:
            run.progress.record(d, DEFERRED, "raw window not complete when planned")


def main(argv=None) -> int:
    """CLI entry point."""
    args = build_parser().parse_args(argv)
    explicit, error = validate_args(args)
    if error:
        print(f"ERROR: {error}", file=sys.stderr)
        return EXIT_USAGE
    if args.check_args:
        return EXIT_OK
    load_dotenv()

    if args.abandon_date:
        return with_lock(argv, lambda: abandon(args, args.progress_dir))

    planned = plan_runs(args, explicit, utcnow_naive())
    if isinstance(planned, int):
        return planned
    runs = planned
    for run in runs:
        span = f"{run.dates[0]} .. {run.dates[-1]}" if run.dates else "none"
        done = sum(1 for d in run.dates if run.is_done(d))
        log(f"plan {run.name}: {len(run.dates)} date(s) {span} ({done} already done)")
    if args.dry_run:
        for run in runs:
            for d in run.dates:
                needs = {
                    s.kind: [u.isoformat() for u in utc_dates_needed(s, d)] for s in run.streams
                }
                log(
                    f"  {run.name} {d}: progress={run.progress.status(d)} "
                    f"runs={'yes' if d in run.dates_with_runs else 'no'} needs {needs}"
                )
        return EXIT_OK

    def _go() -> int:
        """Record deferrals, then run the refresh (called with the lock held)."""
        record_deferrals(runs)
        steps = RealSteps(min_free_gb=args.min_free_gb)
        return run_refresh(runs, steps, force=args.force, housekeeping=not args.skip_housekeeping)

    return with_lock(argv, _go)


def with_lock(argv, fn) -> int:
    """Run ``fn`` holding the refresh lock (unless the parent shell already holds it)."""
    lock_dir = run_lock.LOCK_DIR  # read at call time so tests can point it elsewhere
    own_lock = not run_lock.held_by_parent(lock_dir)
    if own_lock:
        try:
            run_lock.acquire(
                os.getpid(), "pipelines.refresh_by_day " + " ".join(argv or sys.argv[1:]), lock_dir
            )
        except run_lock.LockHeldError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return run_lock.EXIT_LOCKED
    try:
        return fn()
    finally:
        if own_lock:
            run_lock.release(os.getpid(), lock_dir)


if __name__ == "__main__":
    sys.exit(main())
