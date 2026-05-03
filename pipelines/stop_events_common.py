"""
Shared helpers for stop_events derivation pipelines (NOTES.md NOTES-7).

Both `derive_stop_events.py` (proximity source) and
`derive_stop_events_trip_updates.py` (trip_update source) parse GTFS schedule
strings and resolve trip_id+stop_id to a scheduled (arrival, departure) pair.
This module owns those primitives so the two pipelines can't drift apart.
"""

from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.models import StopTime

EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def parse_gtfs_time_to_dt(time_str: str, anchor: date_type) -> datetime | None:
    """Parse a GTFS HH:MM:SS string into a naive UTC datetime anchored at the given service date.

    GTFS times can have HH ≥ 24 for trips that extend past midnight on the same
    service day; in that case the returned datetime is on the next calendar day.
    The anchor is interpreted as the Eastern service date and converted to UTC.
    Returns None on parse failure.
    """
    try:
        hours, minutes, seconds = (int(x) for x in time_str.split(":"))
    except (ValueError, AttributeError):
        return None

    days_offset, hour_within_day = divmod(hours, 24)
    eastern_midnight = datetime.combine(anchor, datetime.min.time())
    naive_eastern = eastern_midnight + timedelta(
        days=days_offset, hours=hour_within_day, minutes=minutes, seconds=seconds
    )
    aware = naive_eastern.replace(tzinfo=EASTERN)
    return aware.astimezone(UTC).replace(tzinfo=None)


def parse_trip_start_date(trip_start_date: str | None) -> date_type | None:
    """Parse a GTFS-RT trip_start_date (YYYYMMDD) into a date, or None if unparseable."""
    if not trip_start_date or len(trip_start_date) != 8:
        return None
    try:
        return datetime.strptime(trip_start_date, "%Y%m%d").date()
    except ValueError:
        return None


def build_stop_time_index(
    stop_times: list[StopTime],
) -> dict[tuple[str, str], list[dict]]:
    """Group stop_times by (trip_id, stop_id) → list of raw schedule entries.

    Raw GTFS time strings are kept here, not datetimes, because the same trip
    template runs on multiple service dates — the datetime anchor depends on
    the observation's `trip_start_date`, not on a pipeline-wide constant.

    A list rather than a single entry because GTFS allows one trip to visit the
    same stop_id at multiple stop_sequence values (loop / out-and-back patterns).
    Caller picks the closest sequence by observation time when more than one exists.
    """
    index: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for st in stop_times:
        index[(st.trip_id, st.stop_id)].append(
            {
                "stop_sequence": st.stop_sequence,
                "arrival_time": st.arrival_time,
                "departure_time": st.departure_time,
            }
        )
    return index


def build_stop_time_seq_index(
    stop_times: list[StopTime],
) -> dict[tuple[str, int], dict]:
    """Group stop_times by (trip_id, stop_sequence) → single schedule entry.

    The trip_update pipeline keys observations by stop_sequence directly (it
    comes from the GTFS-RT TripUpdate.StopTimeUpdate field), so a sequence-based
    index avoids the loop-disambiguation step that the proximity pipeline needs.
    """
    return {
        (st.trip_id, st.stop_sequence): {
            "stop_id": st.stop_id,
            "arrival_time": st.arrival_time,
            "departure_time": st.departure_time,
        }
        for st in stop_times
    }


def resolve_stop_time(
    candidates: list[dict],
    observed_ts: datetime,
    service_date: date_type,
) -> dict | None:
    """Pick the closest-in-time stop_sequence candidate and resolve its scheduled times.

    For the WMATA case there is always exactly one candidate per (trip_id, stop_id).
    The temporal-proximity tie-break is defensive against GTFS loop routes. Returns
    a dict with `stop_sequence`, `scheduled_arrival_ts`, `scheduled_departure_ts`
    parsed against the given (per-position) service_date, or None if no candidates.
    """
    if not candidates:
        return None
    chosen = candidates[0]
    if len(candidates) > 1:
        chosen = min(
            candidates,
            key=lambda c: abs(
                (
                    parse_gtfs_time_to_dt(c["arrival_time"], service_date) - observed_ts
                ).total_seconds()
                if c["arrival_time"]
                else float("inf")
            ),
        )
    return {
        "stop_sequence": chosen["stop_sequence"],
        "scheduled_arrival_ts": (
            parse_gtfs_time_to_dt(chosen["arrival_time"], service_date)
            if chosen["arrival_time"]
            else None
        ),
        "scheduled_departure_ts": (
            parse_gtfs_time_to_dt(chosen["departure_time"], service_date)
            if chosen["departure_time"]
            else None
        ),
    }
