"""Post-replay canary: feed-trip_id vs current-GTFS-trip_id match rate.

PR #226: `bin/pull-and-derive.sh`'s GTFS reload gate (`run_gtfs_reload.py
--max-age-days N`) is a staleness *proxy* — it bounds how old the loaded
snapshot can get, but it can't see a service change landing while the
local copy is still "fresh" by the age gate. On 2026-08-30, Muni's fall
service change did exactly that: the loaded SFMTA GTFS was < 7 days old,
so the gate skipped a reload, but the feed's trip_ids had moved to a
brand-new space. Both derive paths gate on the intersection of observed
feed trip_ids and the currently-loaded (`is_current`) GTFS trip_ids
(`derive_stop_events_from_state.py`'s `active_trip_ids = vp_trip_ids &
set(trip_direction.keys())`; the proximity matcher computes the
equivalent). When that intersection collapses to ~0, both paths silently
write ~0 stop_events while exiting 0 — no error, just an all-but-empty
derive.

This script recomputes that same intersection, agency- and date-wide
(not per-route — a single cheap check is enough to catch the collapse
signature), and fails loudly when the match rate drops below a
threshold. It does NOT auto-reload or auto-re-derive (deliberately): a
human decides when the schedule data has genuinely changed enough to
warrant a reload, and re-derive requires first deleting the affected
date's `runs` rows — a near-zero derive still writes `runs` rows,
which blocks `run_daily_batch.py`'s auto-revisit, the shape that
required the manual SFMTA 8/9–8/10 top-up (PR #227). See
`docs/DEPLOYMENT.md` for the full recovery procedure and the
reasoning behind fail-loud over auto-recovery.

Per-date snapshot (issue #258): the comparison set is the trip_ids of the
GTFS snapshot `run_daily_batch.py` will actually derive the date against
(`src/gtfs_versioning.py:resolve_gtfs_snapshot_for_date`), not blindly
`is_current`. Otherwise a reload that lands a feed starting "today" makes
"yesterday" look collapsed (WMATA 2026-09-12: 0/9,917 vs snapshot 20) and
skips a derive that the per-date snapshot would have done correctly. A
genuine collapse — the date's own resolved schedule doesn't know the
feed's trip_ids, the 2026-08-30 Muni signature — still fails loudly.

Exit codes (distinguish "the data really collapsed" from "the check
itself couldn't run" — both are silent-collapse risks if conflated):
    0  ok — healthy match rate, or a skip (nothing observed yet)
    1  match-rate collapse — a real trip_id-space mismatch
    2  operational error — the canary itself couldn't run (unset
       config, unknown agency, a transient DB failure); one-line
       message, no traceback

Usage:
    uv run python scripts/gtfs_trip_match_canary.py --date 2026-08-30
    uv run python scripts/gtfs_trip_match_canary.py --date 2026-08-30 --agency sfmta
    uv run python scripts/gtfs_trip_match_canary.py --agency sfmta  # defaults to yesterday, agency-local
"""

import argparse
import sys
from datetime import date as date_type
from datetime import timedelta

from dotenv import load_dotenv

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.timezones import local_today
from src.trip_match_rate import (  # noqa: F401 -- re-exported for callers/tests
    DEFAULT_THRESHOLD,
    TripMatchResult,
    compute_trip_match_rate,
)

DEFAULT_AGENCY = "wmata"


class MatchRateCollapseError(RuntimeError):
    """Raised when a service date's feed/GTFS trip_id match rate collapses."""


def check_trip_match_rate(
    session,
    agency: str,
    service_date: date_type,
    tz_name: str,
    threshold: float = DEFAULT_THRESHOLD,
) -> TripMatchResult:
    """Compute the match rate and raise loudly if it has collapsed.

    No observed trip_ids at all is a skip (prints a note, returns the
    empty result) rather than a failure — a not-yet-replayed date isn't
    a service-change signature, it's just missing data.
    """
    result = compute_trip_match_rate(session, service_date, tz_name)
    if result.rate is None:
        print(
            f"gtfs_trip_match_canary: agency={agency} date={service_date.isoformat()} "
            "— no observed vehicle_positions for this date; skipping (nothing to check)."
        )
        return result

    pct = result.rate * 100
    if result.rate < threshold:
        raise MatchRateCollapseError(
            f"gtfs_trip_match_canary: COLLAPSED match rate for agency={agency} "
            f"date={service_date.isoformat()}: {result.matched_count}/{result.observed_count} "
            f"observed trip_ids matched a trip in GTFS snapshot {result.gtfs_snapshot_id} "
            f"(the snapshot resolved for this date) ({pct:.1f}%, threshold "
            f"{threshold * 100:.0f}%). This matches the 2026-08-30 Muni fall-service-change "
            "signature — a service change likely landed while the loaded GTFS snapshot was "
            "still 'fresh' by the age gate. See docs/DEPLOYMENT.md for the recovery "
            f"procedure: reload this agency's GTFS (`uv run python "
            f"scripts/reload_gtfs_complete.py --agency {agency}`), then re-derive "
            f"{service_date.isoformat()} for this agency."
        )

    print(
        f"gtfs_trip_match_canary: OK agency={agency} date={service_date.isoformat()} "
        f"— {result.matched_count}/{result.observed_count} matched ({pct:.1f}%) "
        f"against GTFS snapshot {result.gtfs_snapshot_id}."
    )
    return result


def main() -> int:
    """CLI entry point — run the canary for one agency/date.

    Exit codes (see the module docstring): 0 ok/skip, 1 match-rate
    collapse, 2 the canary itself couldn't run (operational error —
    unset config, unknown agency, a transient DB failure). Config/DB
    errors are deliberately kept distinguishable from a real collapse:
    a caller (`bin/pull-and-derive.sh`) treats them differently — a
    collapse means abort before derive, an operational error means the
    check just didn't run and should be surfaced without masquerading
    as "the data collapsed."
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--agency",
        default=DEFAULT_AGENCY,
        help=f"Agency name matching config/agencies/<agency>.yaml (default: {DEFAULT_AGENCY!r}).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Service date to check, YYYY-MM-DD (default: yesterday, agency-local).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Minimum acceptable match rate, 0-1 (default: {DEFAULT_THRESHOLD}).",
    )
    args = parser.parse_args()

    try:
        load_dotenv()
        cfg = load_agency_config(args.agency)
        service_date = (
            date_type.fromisoformat(args.date)
            if args.date
            else local_today(cfg.timezone) - timedelta(days=1)
        )
        session = get_session(db_url=resolve_agency_db_url(cfg))
    except Exception as exc:
        print(
            f"gtfs_trip_match_canary: could not run for agency={args.agency}: {exc}",
            file=sys.stderr,
        )
        return 2

    try:
        check_trip_match_rate(
            session, args.agency, service_date, cfg.timezone, threshold=args.threshold
        )
    except MatchRateCollapseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(
            f"gtfs_trip_match_canary: could not run for agency={args.agency}: {exc}",
            file=sys.stderr,
        )
        return 2
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
