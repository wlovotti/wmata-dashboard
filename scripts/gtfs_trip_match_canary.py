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
threshold. It does NOT auto-reload or auto-re-derive (deliberately —
see the module's PR description): a human decides when the schedule
data has genuinely changed enough to warrant a reload, and re-derive
requires first deleting the affected date's `runs` rows (NOTES-113's
failure shape — a near-zero derive still writes `runs` rows, which
blocks `run_daily_batch.py`'s auto-revisit). See PR #226's description
for the full recovery procedure and the reasoning behind fail-loud
over auto-recovery.

Usage:
    uv run python scripts/gtfs_trip_match_canary.py --date 2026-08-30
    uv run python scripts/gtfs_trip_match_canary.py --date 2026-08-30 --agency sfmta
    uv run python scripts/gtfs_trip_match_canary.py --agency sfmta  # defaults to yesterday, agency-local
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date as date_type
from datetime import timedelta

from dotenv import load_dotenv

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import Trip, VehiclePosition
from src.timezones import local_service_date_position_window_utc, local_today

# Below this fraction, treat the match as a collapse rather than normal
# day-to-day variation. The observed failure signature (PR #226, the
# 2026-08-30 Muni fall service change) was a drop from a healthy match
# rate to exactly 0% — a low, conservative bar catches that collapse
# without risking false positives on an ordinary reduced-service day.
DEFAULT_THRESHOLD = 0.05


@dataclass(frozen=True)
class TripMatchResult:
    """Outcome of comparing one service date's observed feed trip_ids
    against the currently-loaded GTFS trip_id space.

    ``rate`` is None when there were no observed trip_ids to compare at
    all (e.g. the date hasn't been replayed/loaded yet) — that's a
    "nothing to check" skip, not a collapse.
    """

    observed_count: int
    matched_count: int
    rate: float | None


class MatchRateCollapseError(RuntimeError):
    """Raised when a service date's feed/GTFS trip_id match rate collapses."""


def compute_trip_match_rate(session, service_date: date_type, tz_name: str) -> TripMatchResult:
    """Compute the feed-trip_id ∩ current-GTFS-trip_id match rate for one service date.

    Mirrors the gate both derive paths actually use
    (`derive_stop_events_from_state.py`'s `vp_trip_ids` query, scoped
    by the same agency-local service-date window) — feed-wide rather
    than per-route, since one collapsed date collapses every route at
    once and a single check is cheap.
    """
    window_start, window_end = local_service_date_position_window_utc(service_date, tz_name)
    observed = {
        row[0]
        for row in session.query(VehiclePosition.trip_id)
        .filter(
            VehiclePosition.timestamp >= window_start,
            VehiclePosition.timestamp < window_end,
        )
        .distinct()
        .all()
    }
    if not observed:
        return TripMatchResult(observed_count=0, matched_count=0, rate=None)

    current = {
        row[0]
        for row in session.query(Trip.trip_id).filter(Trip.is_current.is_(True)).distinct().all()
    }
    matched = observed & current
    return TripMatchResult(
        observed_count=len(observed),
        matched_count=len(matched),
        rate=len(matched) / len(observed),
    )


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
            f"observed trip_ids matched a current GTFS trip ({pct:.1f}%, threshold "
            f"{threshold * 100:.0f}%). This matches the 2026-08-30 Muni fall-service-change "
            "signature (PR #226) — a service change likely landed while the loaded GTFS "
            "snapshot was still 'fresh' by the age gate. "
            f"Reload this agency's GTFS (`uv run python scripts/reload_gtfs_complete.py "
            f"--agency {agency}`), delete {service_date.isoformat()}'s `runs` rows for "
            "this agency, then re-derive."
        )

    print(
        f"gtfs_trip_match_canary: OK agency={agency} date={service_date.isoformat()} "
        f"— {result.matched_count}/{result.observed_count} matched ({pct:.1f}%)."
    )
    return result


def main() -> int:
    """CLI entry point — run the canary for one agency/date and exit non-zero on collapse."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--agency",
        default="wmata",
        help="Agency name matching config/agencies/<agency>.yaml (default: 'wmata').",
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

    load_dotenv()
    cfg = load_agency_config(args.agency)
    service_date = (
        date_type.fromisoformat(args.date)
        if args.date
        else local_today(cfg.timezone) - timedelta(days=1)
    )

    session = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        check_trip_match_rate(
            session, args.agency, service_date, cfg.timezone, threshold=args.threshold
        )
    except MatchRateCollapseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
