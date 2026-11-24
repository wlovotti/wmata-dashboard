"""Observed-feed vs scheduled-GTFS trip_id match rate for one service date.

Shared by `scripts/gtfs_trip_match_canary.py` (the post-replay canary,
PR #226) and `pipelines/run_daily_batch.py`'s per-date pre-derive guard
(issue #258): a date whose observed GTFS-RT trip_ids don't exist in the
GTFS snapshot it would be derived against produces a near-empty derive
that still writes `runs` rows — which then display as a real 0% day and
block `determine_target_dates`' zero-runs auto-revisit.
"""

from dataclasses import dataclass
from datetime import date as date_type

from src.gtfs_versioning import (
    SnapshotChoice,
    gtfs_version_filter,
    resolve_gtfs_snapshot_for_date,
)
from src.models import Trip, VehiclePosition
from src.timezones import local_service_date_position_window_utc

# Below this fraction, treat the match as a collapse rather than normal
# day-to-day variation. The observed failure signature (the 2026-08-30
# Muni fall service change) was a drop from a healthy match rate to
# exactly 0% — a low, conservative bar catches that collapse without
# risking false positives on an ordinary reduced-service day.
DEFAULT_THRESHOLD = 0.05


@dataclass(frozen=True)
class TripMatchResult:
    """Outcome of comparing one service date's observed feed trip_ids
    against the trip_ids of the GTFS snapshot resolved for that date
    (``gtfs_snapshot_id``; None when no snapshot is loaded at all).

    ``rate`` is None when there were no observed trip_ids to compare at
    all (e.g. the date hasn't been replayed/loaded yet) — that's a
    "nothing to check" skip, not a collapse.
    """

    observed_count: int
    matched_count: int
    rate: float | None
    gtfs_snapshot_id: int | None = None


def compute_trip_match_rate(
    session,
    service_date: date_type,
    tz_name: str,
    snapshot_choice: SnapshotChoice | None = None,
) -> TripMatchResult:
    """Compute the feed-trip_id ∩ scheduled-GTFS-trip_id match rate for one service date.

    The scheduled side is the snapshot resolved for ``service_date``
    (issue #258) — pass ``snapshot_choice`` to reuse a resolution the
    caller already made, otherwise it is resolved here.

    Mirrors the gate both derive paths actually use
    (`derive_stop_events_from_state.py`'s `vp_trip_ids` query, and
    `derive_stop_events.py`'s equivalent): the same agency-local
    service-date window AND the same `trip_start_date` equality filter.
    The timestamp window alone is ~48h wide (past-midnight service),
    so without the `trip_start_date` filter a date's observed set
    would also pick up the *next* service date's positions that happen
    to fall inside this window — feed-wide rather than per-route,
    since one collapsed date collapses every route at once and a
    single check is cheap.
    """
    window_start, window_end = local_service_date_position_window_utc(service_date, tz_name)
    trip_start_date_str = service_date.strftime("%Y%m%d")
    observed = {
        row[0]
        for row in session.query(VehiclePosition.trip_id)
        .filter(
            VehiclePosition.trip_id.isnot(None),
            VehiclePosition.trip_start_date == trip_start_date_str,
            VehiclePosition.timestamp >= window_start,
            VehiclePosition.timestamp < window_end,
        )
        .distinct()
        .all()
    }
    if not observed:
        return TripMatchResult(observed_count=0, matched_count=0, rate=None)

    # Same snapshot choice run_daily_batch.py makes for this date (#258);
    # override_id is None when that's the current snapshot.
    # None means "not resolved yet" here; a caller whose pipelines will run
    # against is_current passes an explicit no-override SnapshotChoice
    # (run_daily_batch.make_match_guard does this after a resolver error).
    choice = (
        snapshot_choice
        if snapshot_choice is not None
        else resolve_gtfs_snapshot_for_date(session, service_date, tz_name)
    )
    scheduled = {
        row[0]
        for row in session.query(Trip.trip_id)
        .filter(gtfs_version_filter(Trip, choice.override_id))
        .distinct()
        .all()
    }
    matched = observed & scheduled
    return TripMatchResult(
        observed_count=len(observed),
        matched_count=len(matched),
        rate=len(matched) / len(observed),
        gtfs_snapshot_id=choice.snapshot_id,
    )
