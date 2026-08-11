"""
Shared helpers for the stop_events derivation pipelines.

Both `derive_stop_events.py` (proximity source) and
`derive_stop_events_from_state.py` (trip_update source) parse GTFS schedule
strings and resolve trip_id+stop_id to a scheduled (arrival, departure) pair.
This module owns those primitives so the two pipelines can't drift apart.
"""

from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.models import StopTime

UTC = ZoneInfo("UTC")


def parse_gtfs_time_to_dt(
    time_str: str, anchor: date_type, tz_name: str = "America/New_York"
) -> datetime | None:
    """Parse a GTFS HH:MM:SS string into a naive UTC datetime anchored at the given service date.

    GTFS times can have HH ≥ 24 for trips that extend past midnight on the same
    service day; in that case the returned datetime is on the next calendar day.
    The anchor is interpreted as a local calendar date in ``tz_name``
    (NOTES-100 multi-agency; default Eastern, matching every WMATA call
    site) and converted to UTC. Returns None on parse failure.
    """
    try:
        hours, minutes, seconds = (int(x) for x in time_str.split(":"))
    except (ValueError, AttributeError):
        return None

    days_offset, hour_within_day = divmod(hours, 24)
    local_midnight = datetime.combine(anchor, datetime.min.time())
    naive_local = local_midnight + timedelta(
        days=days_offset, hours=hour_within_day, minutes=minutes, seconds=seconds
    )
    aware = naive_local.replace(tzinfo=ZoneInfo(tz_name))
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


def resolve_scheduled_instants(
    arrival_time: str | None,
    departure_time: str | None,
    observed_ts: datetime,
    service_date: date_type,
    tz_name: str = "America/New_York",
) -> tuple[datetime | None, datetime | None]:
    """Resolve (scheduled_arrival_ts, scheduled_departure_ts), correcting for
    a possible one-day service-date misattribution (NOTES-105).

    GTFS-RT `trip_start_date` (or, for the trip_update source, its
    snapshot_ts-calendar-day fallback in
    ``src.wmata_collector._service_date_for_row``) can report the calendar
    day an update was *observed* rather than the GTFS service day the
    trip's schedule is anchored to. WMATA never exercises this at scale
    (service ends ~03:00), but SFMTA's 24-hour owl routes (14, 22, 24, 38,
    44, 48 confirmed; likely 5, 90, 91) schedule straight through midnight
    using GTFS times >= 24:00, and their RT feed's start_date flips to the
    next calendar day mid-trip once a poll lands after local midnight —
    even though the static schedule still anchors those >=24:00 times to
    the *previous* service day. Parsing against the misattributed date
    lands the scheduled instant almost exactly 24h away from the real
    observation (confirmed on route 14, 2026-07-23: a clean cluster at
    -89,077 to -80,032 sec).

    Rather than trying to fix the misattribution at its source (multiple
    candidate causes, some upstream of derivation — see NOTES-105), this
    recovers at resolution time: try anchoring the GTFS time string at both
    `service_date` and the day before, and keep whichever produces a
    scheduled arrival closest to `observed_ts`. For a schedule that never
    uses times >= 24:00 (WMATA), the day-before anchor is always ~24h
    further from any real observation, so it never wins — this is a no-op
    there, verified by regression test.

    Returns ``(None, None)`` if both inputs are ``None``.
    """
    if arrival_time is None and departure_time is None:
        return None, None

    best_anchor = service_date
    if arrival_time is not None:
        best_delta = None
        for anchor in (service_date, service_date - timedelta(days=1)):
            parsed = parse_gtfs_time_to_dt(arrival_time, anchor, tz_name)
            if parsed is None:
                continue
            delta = abs((parsed - observed_ts).total_seconds())
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_anchor = anchor

    return (
        parse_gtfs_time_to_dt(arrival_time, best_anchor, tz_name) if arrival_time else None,
        parse_gtfs_time_to_dt(departure_time, best_anchor, tz_name) if departure_time else None,
    )


def resolve_stop_time(
    candidates: list[dict],
    observed_ts: datetime,
    service_date: date_type,
    tz_name: str = "America/New_York",
) -> dict | None:
    """Pick the closest-in-time stop_sequence candidate and resolve its scheduled times.

    For the WMATA case there is always exactly one candidate per (trip_id, stop_id).
    The temporal-proximity tie-break is defensive against GTFS loop routes. Returns
    a dict with `stop_sequence`, `scheduled_arrival_ts`, `scheduled_departure_ts`
    parsed against the given (per-position) service_date, or None if no candidates.

    ``tz_name`` (NOTES-100 multi-agency; default Eastern) is forwarded to
    ``parse_gtfs_time_to_dt`` — see its docstring.

    Both the stop_sequence tie-break and the final scheduled-time parse
    consider `service_date` and the day before it (NOTES-105) — see
    ``resolve_scheduled_instants``.
    """
    if not candidates:
        return None

    def _min_delta(c: dict) -> float:
        if not c["arrival_time"]:
            return float("inf")
        deltas = []
        for anchor in (service_date, service_date - timedelta(days=1)):
            parsed = parse_gtfs_time_to_dt(c["arrival_time"], anchor, tz_name)
            if parsed is not None:
                deltas.append(abs((parsed - observed_ts).total_seconds()))
        return min(deltas) if deltas else float("inf")

    chosen = candidates[0]
    if len(candidates) > 1:
        chosen = min(candidates, key=_min_delta)

    scheduled_arrival_ts, scheduled_departure_ts = resolve_scheduled_instants(
        chosen["arrival_time"], chosen["departure_time"], observed_ts, service_date, tz_name
    )
    return {
        "stop_sequence": chosen["stop_sequence"],
        "scheduled_arrival_ts": scheduled_arrival_ts,
        "scheduled_departure_ts": scheduled_departure_ts,
    }
