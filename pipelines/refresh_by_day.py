"""Day-at-a-time refresh: fetch one service date's raw files, load, replay, derive, prune.

The laptop can't hold the raw archive (S3 has it all; ~1.7 GB/day for
both agencies), so freshness — whether yesterday or an eight-week
catch-up — processes ONE service date at a time, oldest first, per agency:

1. **fetch** exactly the raw files that date reads from S3
   (``src/raw_archive.py``: TU UTC D and D+1; VP UTC D..D+2),
2. **load VP** for those UTC days (``load_vp_archive.py --utc-date``,
   which reloads a day whose rows were pruned),
3. **replay** trip updates for D (``replay_archive_to_state.py``),
4. **derive** D (``run_daily_batch.py --date D --skip-housekeeping``),
5. **prune** local raw files no later date needs, once each is verified
   byte-identical in S3 and recorded in its manifest.

Peak extra disk is therefore about two UTC days of trip-update files plus
three of VP per agency (~3 GB WMATA + ~1 GB SFMTA), not the whole window.

Date selection:

- default: per agency, every date from the day after the latest
  ``runs.service_date`` through agency-local yesterday, plus any
  zero-``runs`` date in the last ``--lookback-days`` (default 14 — the
  old catch-up net for a held/failed date);
- ``--start``/``--end`` (inclusive) or ``--date`` (repeatable): exactly
  those dates.

Dates are interleaved (D for WMATA, then D for SFMTA, then D+1 ...).

Failure policy: a hard failure (fetch, VP load, replay, or derive exit 1)
STOPS that agency for the rest of the run — the other agency continues.
Later dates aren't attempted because (a) a failure is almost always
systemic (credentials, disk, DB, a broken feed day) so continuing would
fail and re-download N more times, and (b) stopping keeps "resume" exact:
the default range restarts at the failed date (it has no runs) and an
explicit range skips only the dates already ``done``. Its local raw files
are kept for the retry. A date the pre-derive match-rate guard holds back
(``run_daily_batch`` exit 3 — its trip_ids match no loaded schedule) is
reported as ``held`` and the run CONTINUES: retrying won't help until GTFS
is reloaded, and its raw files are in S3 for that later retry.

Progress lives in ``logs/refresh_progress_<agency>.json``
(``derived`` → ``done`` after the prune, or ``held`` / ``failed``), so an
interrupted run resumes at the first unfinished date and a date derived
but not yet pruned only re-runs the prune. ``--force`` re-processes
``done`` dates (and passes ``--force`` to replay, which otherwise treats
an already-folded date as finished even if its state was pruned).

trip_update_state retention: WMATA's ``cleanup_trip_update_state`` runs
in ``run_daily_batch`` housekeeping and SFMTA's is a separate call; both
delete state with ``service_date`` older than local today - 7, which
covers every backfilled date. Neither runs between dates: per-date
derives pass ``--skip-housekeeping``, and housekeeping (WMATA) / cleanup
(SFMTA, skipped if any SFMTA date hard-failed this run) run once, after
the last date — so a date's replayed state always survives until its own
derive has finished. A date whose state is pruned before it is derived
(e.g. after a failure) self-heals on the next run: replay re-folds a date
with neither state nor runs.

Usage:
    uv run python -m pipelines.refresh_by_day                       # catch up to yesterday
    uv run python -m pipelines.refresh_by_day --agency wmata --date 2026-09-29
    uv run python -m pipelines.refresh_by_day --start 2026-09-29 --end 2026-11-23
    uv run python -m pipelines.refresh_by_day --dry-run             # print the plan only
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

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
from src.timezones import local_today, utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENCIES = ("wmata", "sfmta")
DEFAULT_LOOKBACK_DAYS = 14
# June 2026 dates need GTFS snapshot 12 and the recovery driver
# (scripts/local_recovery_2026_07.sh); never re-derive them from here.
DATA_FLOOR = date(2026, 6, 14)
DEFAULT_MIN_FREE_GB = 15.0
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

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_HELD_ONLY = 3  # same meaning as run_daily_batch's EXIT_GUARD_SKIPS_ONLY


def log(msg: str) -> None:
    """Print one timestamped (UTC) progress line, flushed."""
    print(f"[{utcnow_naive():%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


class Progress:
    """Per-agency JSON record of each service date's refresh status.

    ``{"dates": {"2026-10-03": {"status": "done", "at": "...", "detail": ""}}}``.
    Written atomically (temp file + rename) after every status change so a
    crash never leaves a half-written file.
    """

    def __init__(self, path: Path):
        """Load ``path`` if it exists, else start empty."""
        self.path = path
        self.data: dict = {"dates": {}}
        if path.exists():
            self.data = json.loads(path.read_text())
            self.data.setdefault("dates", {})

    def status(self, d: date) -> str | None:
        """Return the recorded status for ``d`` (None if never attempted)."""
        entry = self.data["dates"].get(d.isoformat())
        return entry["status"] if entry else None

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
    """One agency's state for this run: streams, progress file, dates, outcomes."""

    name: str
    tz_name: str
    tu: ArchiveStream
    vp: ArchiveStream
    progress: Progress
    dates: list[date]
    outcomes: dict[date, str] = field(default_factory=dict)
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

        Refuses to start when free disk is below ``min_free_gb`` — a weeks-
        long backfill also grows the DB, and running the disk out mid-write
        to Postgres is the failure #261 exists to prevent.
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
                    continue  # not written yet (yesterday's D+2 VP window)
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

    def derive(self, run: AgencyRun, d: date) -> int:
        """Run the per-date pipeline chain for ``d`` only, without housekeeping."""
        return self._run(
            [
                "pipelines.run_daily_batch",
                "--agency",
                run.name,
                "--date",
                d.isoformat(),
                "--skip-housekeeping",
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
            log(f"prune {run.name}: FAILED ({exc!r}) — files kept, next run retries")
        finally:
            session.close()

    def housekeeping(self) -> int:
        """Run WMATA's HOUSEKEEPING_PIPELINES once (includes its trip_update_state cleanup)."""
        return self._run(["pipelines.run_daily_batch", "--housekeeping-only"])

    def cleanup(self, agency: str) -> int:
        """Run ``cleanup_trip_update_state`` for a non-default agency."""
        return self._run(["pipelines.cleanup_trip_update_state", "--agency", agency])


def process_date(run: AgencyRun, d: date, steps, force: bool) -> str:
    """Run every step for one (agency, date) and return its final status.

    Records progress after the derive (``derived``/``held``/``failed``)
    and again after the prune (``done``). A date already ``derived`` (the
    previous run died between derive and prune) only re-runs the prune.
    """
    label = f"{run.name} {d}"
    if run.progress.status(d) == DERIVED and not force:
        log(f"== {label}: derived earlier, finishing the prune")
        steps.prune(run, d)
        run.progress.record(d, DONE)
        return DONE

    log(f"== {label}: fetch")
    try:
        steps.fetch(run, d)
    except Exception as exc:
        run.progress.record(d, FAILED, f"fetch: {exc}")
        log(f"== {label}: FAILED at fetch ({exc})")
        return FAILED

    for step_name, call in (
        ("load_vp", lambda: steps.load_vp(run, d)),
        ("replay", lambda: steps.replay(run, d, force)),
    ):
        log(f"== {label}: {step_name}")
        rc = call()
        if rc != 0:
            run.progress.record(d, FAILED, f"{step_name} rc={rc}")
            log(f"== {label}: FAILED at {step_name} (rc={rc})")
            return FAILED

    log(f"== {label}: derive")
    rc = steps.derive(run, d)
    if rc == 0:
        status = DERIVED
    elif rc == EXIT_HELD_ONLY:
        status = HELD
    else:
        run.progress.record(d, FAILED, f"derive rc={rc}")
        log(f"== {label}: FAILED at derive (rc={rc})")
        return FAILED
    run.progress.record(d, status, "" if status == DERIVED else "match-rate guard held the date")

    log(f"== {label}: prune")
    steps.prune(run, d)
    if status == DERIVED:
        run.progress.record(d, DONE)
        status = DONE
    log(f"== {label}: {status}")
    return status


def run_refresh(runs: list[AgencyRun], steps, force: bool, housekeeping: bool) -> int:
    """Process every agency's dates interleaved oldest-first; return the exit code.

    Exit codes: 0 everything done; 3 the only problem was held dates;
    1 any hard failure (date, housekeeping, or cleanup).
    """
    all_dates = sorted({d for r in runs for d in r.dates})
    for d in all_dates:
        for run in runs:
            if run.stopped or d not in run.dates:
                continue
            if run.progress.status(d) == DONE and not force:
                log(f"== {run.name} {d}: already done (progress file), skipping")
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
        if run.name == "wmata":
            if worked and housekeeping:
                log("== wmata: housekeeping (once, after all dates)")
                if steps.housekeeping() != 0:
                    end_rc = 1
            elif worked:
                log("== wmata: housekeeping skipped (--skip-housekeeping)")
        elif worked and not failed:
            log(f"== {run.name}: trip_update_state cleanup (once, after all dates)")
            if steps.cleanup(run.name) != 0:
                end_rc = 1
        elif failed:
            log(
                f"== {run.name}: trip_update_state cleanup skipped — a date failed this run; "
                "its un-derived state is left for the retry"
            )

    log("== summary")
    any_failed = any_held = False
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
        if not_attempted:
            log(
                f"    not attempted: {not_attempted[0]} .. {not_attempted[-1]} ({len(not_attempted)})"
            )
        any_failed |= FAILED in counts
        any_held |= HELD in counts
    if any_failed or end_rc:
        return EXIT_FAILED
    if any_held:
        return EXIT_HELD_ONLY
    return EXIT_OK


def default_dates(session, lookback_days: int, today: date) -> list[date]:
    """Dates after the latest ``runs`` date through yesterday, plus recent zero-runs holes.

    Raises ``RuntimeError`` when ``runs`` is empty (no anchor — pass an
    explicit range instead).
    """
    from sqlalchemy import func

    from src.models import Run

    yesterday = today - timedelta(days=1)
    latest = session.query(func.max(Run.service_date)).scalar()
    if latest is None:
        raise RuntimeError("runs is empty — pass --start/--end or --date explicitly")
    latest_date = date.fromisoformat(latest)
    dates = set()
    d = latest_date + timedelta(days=1)
    while d <= yesterday:
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
    while d <= min(yesterday, latest_date):
        if d.isoformat() not in have_runs:
            dates.add(d)
        d += timedelta(days=1)
    return sorted(dates)


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
    p.add_argument("--progress-dir", type=Path, default=REPO_ROOT / "logs")
    p.add_argument("--dry-run", action="store_true", help="print the per-agency plan and exit")
    return p


def main(argv=None) -> int:
    """CLI entry point."""
    args = build_parser().parse_args(argv)
    load_dotenv()
    try:
        explicit = explicit_dates(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE

    agencies = AGENCIES if args.agency == "both" else (args.agency,)
    runs: list[AgencyRun] = []
    for name in agencies:
        cfg = load_agency_config(name)
        today = local_today(cfg.timezone)
        if explicit is not None:
            dates = explicit
        else:
            from src.database import get_session

            session = get_session(db_url=resolve_agency_db_url(cfg))
            try:
                dates = default_dates(session, args.lookback_days, today)
            except RuntimeError as exc:
                print(f"ERROR ({name}): {exc}", file=sys.stderr)
                return EXIT_USAGE
            finally:
                session.close()
        too_old = [d for d in dates if d < DATA_FLOOR]
        too_new = [d for d in dates if d >= today]
        if too_old or too_new:
            print(
                f"ERROR ({name}): dates must be in [{DATA_FLOOR}, {today - timedelta(days=1)}] "
                f"(agency-local yesterday); got {(too_old or too_new)[0]}",
                file=sys.stderr,
            )
            return EXIT_USAGE
        tu, vp = streams_for_agency(
            cfg,
            REPO_ROOT,
            tu_dir=os.environ.get(DIR_ENV[(name, "tu")]) or None,
            vp_dir=os.environ.get(DIR_ENV[(name, "vp")]) or None,
        )
        progress = Progress(args.progress_dir / f"refresh_progress_{name}.json")
        runs.append(AgencyRun(name, cfg.timezone, tu, vp, progress, dates))

    for run in runs:
        span = f"{run.dates[0]} .. {run.dates[-1]}" if run.dates else "none"
        done = sum(1 for d in run.dates if run.progress.status(d) == DONE)
        log(f"plan {run.name}: {len(run.dates)} date(s) {span} ({done} already done)")
    if args.dry_run:
        for run in runs:
            for d in run.dates:
                needs = {
                    s.kind: [u.isoformat() for u in utc_dates_needed(s, d)] for s in run.streams
                }
                log(f"  {run.name} {d}: status={run.progress.status(d)} needs {needs}")
        return EXIT_OK

    steps = RealSteps(min_free_gb=args.min_free_gb)
    return run_refresh(runs, steps, force=args.force, housekeeping=not args.skip_housekeeping)


if __name__ == "__main__":
    sys.exit(main())
