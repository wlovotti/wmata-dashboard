"""
Shared helpers for the stop_events derivation pipelines.

Both `derive_stop_events.py` (proximity source) and
`derive_stop_events_from_state.py` (trip_update source) parse GTFS schedule
strings and resolve trip_id+stop_id to a scheduled (arrival, departure) pair.
This module owns those primitives so the two pipelines can't drift apart.
"""

from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import date as date_type
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.models import StopTime

UTC = ZoneInfo("UTC")

# PR #193 review round 2 (finding 1): the ``proxy_guard=True`` distance cap
# in `resolve_scheduled_instants` -- see that function's docstring for the
# full sizing rationale (production SFMTA genuine-shift distribution: median
# 149s, p99 2,844s, max 7,131s; 3h gives ~1.5x headroom over the p99/max).
_PROXY_GUARD_MAX_DISTANCE_SEC = 10_800

# Chunk size for tuple_(...).in_(<pairs>) queries (DELETEs/UPDATEs keyed on
# (trip_id, stop_sequence) pairs) in both derive_stop_events*.py pipelines.
# Postgres parses a tuple-IN filter as a deeply nested row-constructor OR
# expression; at tens of thousands of pairs the parser exhausts
# max_stack_depth (default 2MB) and the query fails with
# "StatementTooComplex: stack depth limit exceeded" (reviewer-measured
# failure at 20k pairs; worst real key set observed in production is 2,353).
# 1000 pairs per query stays comfortably under that limit on
# production-shaped batches.
_TUPLE_IN_CHUNK_SIZE = 1000


def _iter_chunks[T](items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield successive lists of length ``size`` from ``items`` (last may be short)."""
    if size <= 0:
        raise ValueError(f"chunk size must be positive, got {size}")
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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
    proxy_guard: bool = False,
) -> tuple[datetime | None, datetime | None, date_type]:
    """Resolve (scheduled_arrival_ts, scheduled_departure_ts, resolved_service_date),
    correcting for a possible one-day service-date misattribution (NOTES-105).

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
    scheduled instant closest to `observed_ts`. The true invariant this
    relies on has nothing to do with midnight-crossing per se: letting `x`
    be the (same-day-anchored scheduled instant) minus `observed_ts` in
    seconds, the day-before anchor (which shifts the parsed instant back
    exactly 86,400s) wins the abs-delta comparison iff `|x − 86400| < |x|`,
    i.e. iff `x > 43200` — the same-day scheduled instant lands more than
    12h *after* the observation (equivalently: the observation is more
    than 12h *earlier* than the same-day parse). WMATA's GTFS does have
    plenty of stop_times with hour >= 24 (~229,760 rows), so
    midnight-crossing alone is not the guard; what actually keeps this a
    no-op for WMATA in practice is that a real WMATA observation is never
    >12h earlier than its own scheduled time (WMATA's early-arrival
    clusters top out around 80 minutes, nowhere near half a day), so the
    same-day anchor always wins there — verified by regression test.

    ``resolved_service_date`` is whichever anchor (``service_date`` or the
    day before it) produced the returned instants — the anchor day the
    resolver determined is the trip's true GTFS service day. Callers that
    persist a per-row ``service_date`` (NOTES-110) must use this instead of
    the raw ``service_date`` argument, since the latter is exactly the
    misattributed value this function exists to correct for.

    ``proxy_guard`` (PR #193 review): set ``True`` when ``observed_ts`` is
    a wall-clock *proxy* rather than a genuine arrival observation — e.g.
    the SKIPPED-fallback branch's
    ``final_snapshot_ts`` (the last poll that saw a (trip, stop) before it
    dropped out of the feed). The "never >12h early" invariant above was
    verified only for real observations; a trip that goes dark early
    (vehicle offline, early cancellation, collector gap) can leave a proxy
    timestamp far from the true scheduled time for reasons that have
    nothing to do with an owl-route misattribution, on any agency
    including WMATA.

    The guard was originally the same ``prior_day_delta < same_day_delta
    AND same_day_lag > 43_200`` direction check the unguarded argmin
    already implies — a code-review pass (PR #193 review round 2, finding
    1) found the two clauses are mathematically equivalent by the
    ``|x − 86400| < |x| ⟺ x > 43200`` identity the module docstring
    itself derives, so the "guard" never actually rejected anything a
    plain closest-anchor search wouldn't already have picked. Repro: a
    23:00:00 schedule with service_date D and a same-day proxy at 08:00
    has same_day_lag = 54,000s (> 43,200, "clears" the old guard) even
    though the day-before anchor is 32,400s (9h) from the proxy — an
    ordinary early dropout, not an owl-route misattribution — and the old
    guard wrongly shifted the anchor a day.

    The guard is now a plain distance cap: the day-before anchor is
    accepted only when its scheduled instant lands within 3 hours
    (10,800s absolute difference) of the proxy observation; otherwise the
    plain same-day anchor is kept, matching pre-NOTES-110 SKIPPED-branch
    behavior exactly. 3h is sized from production data: across the
    289,962 SFMTA rows that genuinely shift, the winning day-before anchor
    lands median 149s, p99 2,844s, max 7,131s (~2.0h) from the
    observation — a 3h cap accepts all genuine corrections with ~1.5x
    headroom while rejecting gaps of several hours or more (like the 9h
    repro above) that are far more likely to be an unrelated early
    dropout than a genuine day-before correction.

    Returns ``(None, None, service_date)`` if both inputs are ``None``
    (nothing to anchor against, so the given ``service_date`` is kept
    as-is).
    """
    if arrival_time is None and departure_time is None:
        return None, None, service_date

    # Prefer arrival_time to pick the anchor day (it's what deviation_sec
    # is computed from), but fall back to departure_time when arrival_time
    # is absent — otherwise a departure-only stop_time on an owl trip would
    # silently keep the misattributed `service_date` anchor and end up ~24h
    # off exactly like the case this function exists to fix.
    anchor_source = arrival_time if arrival_time is not None else departure_time

    same_day_anchor = service_date
    prior_day_anchor = service_date - timedelta(days=1)
    best_anchor = same_day_anchor

    if anchor_source is not None:
        same_day_parsed = parse_gtfs_time_to_dt(anchor_source, same_day_anchor, tz_name)
        prior_day_parsed = parse_gtfs_time_to_dt(anchor_source, prior_day_anchor, tz_name)
        same_day_delta = (
            abs((same_day_parsed - observed_ts).total_seconds())
            if same_day_parsed is not None
            else None
        )
        prior_day_delta = (
            abs((prior_day_parsed - observed_ts).total_seconds())
            if prior_day_parsed is not None
            else None
        )

        if proxy_guard:
            if prior_day_delta is not None and prior_day_delta <= _PROXY_GUARD_MAX_DISTANCE_SEC:
                best_anchor = prior_day_anchor
            else:
                best_anchor = same_day_anchor
        else:
            best_delta = None
            for anchor, delta in (
                (same_day_anchor, same_day_delta),
                (prior_day_anchor, prior_day_delta),
            ):
                if delta is None:
                    continue
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best_anchor = anchor

    return (
        parse_gtfs_time_to_dt(arrival_time, best_anchor, tz_name) if arrival_time else None,
        parse_gtfs_time_to_dt(departure_time, best_anchor, tz_name) if departure_time else None,
        best_anchor,
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
    parsed against the given (per-position) service_date, plus
    `resolved_service_date` (NOTES-110 — the anchor day that actually
    produced those instants; see ``resolve_scheduled_instants``), or None if
    no candidates.

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

    scheduled_arrival_ts, scheduled_departure_ts, resolved_service_date = (
        resolve_scheduled_instants(
            chosen["arrival_time"], chosen["departure_time"], observed_ts, service_date, tz_name
        )
    )
    return {
        "stop_sequence": chosen["stop_sequence"],
        "scheduled_arrival_ts": scheduled_arrival_ts,
        "scheduled_departure_ts": scheduled_departure_ts,
        "resolved_service_date": resolved_service_date,
    }
