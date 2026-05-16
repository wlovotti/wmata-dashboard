"""
Per-route diagnostic primitives (NOTES-57) — slip, cumulative slip,
timepoint behavior classification, and direction asymmetry.

Codifies the analytical pattern from the D80 deep-dive (May 2026
session). Materialized nightly into the `route_diagnostic_*` tables
so dashboard panels (NOTES-58/59/60/61/62) are O(1) reads of
pre-aggregated rows rather than ad-hoc scans of `stop_events`.

Reference implementation: ``visualizations/slip_trajectory.py:fetch_slip``
already proved the segment-slip pattern on D80. This module
generalizes it across all routes / directions / periods, adds
timepoint behavior classification and direction asymmetry, and
returns plain dict rows suitable for upsert into the materialized
tables.

Source choice (slip is deviation-based)
---------------------------------------
We read ``stop_events.source = 'proximity'``. Slip is computed
between consecutive stop arrivals on a single trip — the
``trip_update`` source can't observe the origin departure (NOTES-31)
and would systematically blind us to the very segment we exclude on
purpose. ``proximity`` covers every stop the bus pings near and is
the right source for OTP / per-stop spatial analysis (CLAUDE.md).

Origin-departure segment is excluded
------------------------------------
The "slip" of the first observed segment is dominated by layover
artifact (the bus parking at the layover well before pull-out, not
real on-route slippage). Mirrors the drop applied in
``visualizations/slip_trajectory.py:fetch_slip``.

Period bucketing
----------------
Periods are determined by the *scheduled* time-of-day at the
"from" stop, evaluated in Eastern. We don't use the observed time
because a chronically-late bus would migrate into the wrong period
and skew the slip estimate for the period it ought to have been
operating in.

Stop_id direction-uniqueness
----------------------------
Termini and hub stops are shared across both directions under one
``stop_id``. Every grouping in this module includes
``direction_id`` to avoid the silent double-count documented in
CLAUDE.md.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import date as date_type
from datetime import timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.timezones import utcnow_naive

# ---------------------------------------------------------------------------
# Periods
# ---------------------------------------------------------------------------

# Period name -> (start_hour, end_hour). Half-open [start, end).
# `late` and `all` are sentinels handled below — `late` is the wrap-around
# 22:00-06:00 bucket; `all` means no time filter.
PERIOD_HOURS: dict[str, tuple[int, int] | None] = {
    "all": None,
    "am_peak": (6, 10),
    "midday": (10, 15),
    "pm_peak": (15, 19),
    "evening": (19, 22),
    "late": None,  # handled specially: hour >= 22 OR hour < 6
}

ALL_PERIODS: tuple[str, ...] = ("all", "am_peak", "midday", "pm_peak", "evening", "late")


# ---------------------------------------------------------------------------
# Classification thresholds
# ---------------------------------------------------------------------------

# Median deviation drop across a timepoint that qualifies as "recovery" — the
# bus arrives ~120s+ late on average and leaves much closer to schedule.
RECOVERY_MEDIAN_DROP_SEC = 120

# Downstream p10 drop that qualifies as "leaky" — the leading edge of the
# distribution moves notably earlier across the timepoint, i.e., a non-trivial
# share of buses depart early.
LEAKY_P10_DROP_SEC = 180

# Median deviation entering a timepoint that qualifies as "underpowered"
# territory — the bus is meaningfully late entering but the median doesn't
# compress.
UNDERPOWERED_ENTERING_MEDIAN_SEC = 120
UNDERPOWERED_MEDIAN_DROP_MAX_SEC = 60  # < this is "no material compression"

# Neutral band around zero median entering, used to distinguish "boring,
# well-behaved timepoint" from "underpowered".
NEUTRAL_MEDIAN_BAND_SEC = 60

# Minimum observations per (direction_id, stop_sequence) segment to be
# included. Below this the mean slip is too noisy to publish.
MIN_SEGMENT_OBSERVATIONS = 50

# Minimum observations per timepoint side (entering / leaving) for the
# classification to be emitted. Two-sided sample requirement; either side
# falling short suppresses the row.
MIN_TIMEPOINT_OBSERVATIONS = 30

# Asymmetry-signature thresholds. WMATA's −2 / +7 OTP window already buckets
# every deviation_sec into early / on_time / late; we just count the buckets
# per direction and compare percentages.
EARLY_THRESHOLD_SEC = -120  # -2 min, per OTP standard
LATE_THRESHOLD_SEC = 7 * 60  # +7 min, per OTP standard

# A direction is "early-dominant" if early% exceeds late% by this margin (and
# vice versa for late-dominant). Below the margin it's "balanced".
ASYMMETRY_MARGIN_PCT = 5.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def default_service_date_range(today: date_type, days: int = 30) -> tuple[date_type, date_type]:
    """Return (start, end) inclusive — last `days` Eastern service dates.

    Default window of 30 days matches the production data depth (NOTES-38
    deferral) and gives a stable per-segment / per-timepoint sample without
    over-pooling stale schedule revisions.
    """
    end = today - timedelta(days=1)  # yesterday is the latest complete day
    start = end - timedelta(days=days - 1)
    return start, end


# ---------------------------------------------------------------------------
# Per-segment slip + cumulative slip
# ---------------------------------------------------------------------------


def compute_canonical_stop_mapping(
    db: Session,
    route_id: str,
    service_date_range: tuple[date_type, date_type],
) -> dict[tuple[int, int], str]:
    """Most-served stop_id per (direction_id, stop_sequence) for one route.

    Determines the canonical stop pattern by trip frequency. Used by
    ``compute_segment_slip`` to filter out minority-variant trips so per-
    segment slip and cumulative slip are mathematically well-defined.

    Route variants (express/local splits, mid-route detours) legitimately
    produce multiple stop_ids at the same ``(direction_id, stop_sequence)``
    over a 30-day window. Without filtering, the slip aggregation emits
    one row per (from_stop_id, to_stop_id) pair at each
    ``(direction_id, from_seq, to_seq)``, which violates the
    ``route_diagnostic_segment`` unique constraint and silently inflates
    ``cum_slip_sec`` when the cumsum loop walks the duplicates.

    Selection rule: pick the stop_id with the most ``stop_events`` rows
    (proximity / SCHEDULED / observed-non-null) at each
    ``(direction_id, stop_sequence)``. Ties are broken by lexically
    smaller stop_id for determinism — without a deterministic tiebreak
    the canonical mapping could flip between runs and silently change
    which trips count.

    Uses the same source / schedule_relationship / observed-arrival
    filters as ``compute_segment_slip`` so the two are anchored to the
    same observation set.

    Returns ``{(direction_id, stop_sequence): stop_id}`` for every
    (direction_id, stop_sequence) the route has observed in the window.
    Returns an empty dict for a route with no eligible observations.
    """
    start, end = service_date_range
    sql = text(
        """
        WITH ranked AS (
          SELECT
            se.direction_id,
            se.stop_sequence,
            se.stop_id,
            COUNT(*) AS n,
            ROW_NUMBER() OVER (
              PARTITION BY se.direction_id, se.stop_sequence
              ORDER BY COUNT(*) DESC, se.stop_id ASC
            ) AS rk
          FROM stop_events se
          WHERE se.route_id = :route_id
            AND se.source = 'proximity'
            AND se.observed_arrival_ts IS NOT NULL
            AND se.scheduled_arrival_ts IS NOT NULL
            AND se.schedule_relationship = 'SCHEDULED'
            AND se.service_date BETWEEN :start AND :end
          GROUP BY se.direction_id, se.stop_sequence, se.stop_id
        )
        SELECT direction_id, stop_sequence, stop_id
        FROM ranked
        WHERE rk = 1;
        """
    )
    rows = db.execute(
        sql,
        {
            "route_id": route_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    ).fetchall()
    return {(int(r.direction_id), int(r.stop_sequence)): r.stop_id for r in rows}


def compute_segment_slip(
    db: Session,
    route_id: str,
    period: str,
    service_date_range: tuple[date_type, date_type],
) -> list[dict[str, Any]]:
    """Per-(direction, from_seq, to_seq) mean slip + count for one route/period.

    Slip = observed segment travel time − scheduled segment travel time,
    averaged across all observed trips on dates in
    ``service_date_range`` (inclusive). The origin-departure segment is
    excluded — its "slip" is dominated by layover artifact, not real
    on-route slippage. Mirrors
    ``visualizations/slip_trajectory.py:fetch_slip``.

    Each returned row carries ``cum_slip_sec`` — the running sum of mean
    slip from the first non-origin segment, in direction-order — so the
    materialization stores both the per-segment value and its cumulative
    counterpart in one pass.

    Route variants (express+local splits, mid-route detours) are collapsed
    to a single canonical pattern via :func:`compute_canonical_stop_mapping`
    — observations are kept only when both endpoint ``stop_id``s match
    the most-served stop at their ``(direction_id, stop_sequence)``
    position. Without this filter, multiple stop_id pairs land at the same
    ``(direction_id, from_seq, to_seq)`` and produce both a constraint
    violation on insert and an inflated cumsum on safe routes.
    """
    if period not in PERIOD_HOURS:
        raise ValueError(f"unknown period: {period}")
    start, end = service_date_range

    # Period filter on the *scheduled* hour of the from-stop in Eastern. We
    # don't filter on observed hour because chronic lateness would push
    # observations into the wrong period and confound the slip we're trying
    # to measure for the *scheduled* period.
    period_clause = ""
    if period == "late":
        period_clause = "AND (o.sched_et_hr >= 22 OR o.sched_et_hr < 6)"
    elif period != "all":
        lo, hi = PERIOD_HOURS[period]  # type: ignore[misc]
        period_clause = f"AND o.sched_et_hr >= {lo} AND o.sched_et_hr < {hi}"

    # The `canonical` CTE selects the most-served stop_id per
    # (direction_id, stop_sequence) — the dominant route pattern. The `seg`
    # CTE joins each event row + its LEAD-next row to canonical twice, once
    # on each endpoint, so any observation that diverges from the canonical
    # pattern at either end is dropped. Without this filter, route variants
    # produce multiple rows per (direction_id, from_seq, to_seq) which
    # violates the segment table's unique constraint and inflates the
    # downstream cumsum walk.
    sql = text(
        f"""
        WITH ordered AS (
          SELECT
            se.trip_id,
            se.service_date,
            se.direction_id,
            se.stop_sequence,
            se.stop_id,
            se.observed_arrival_ts,
            se.scheduled_arrival_ts,
            EXTRACT(HOUR FROM (se.scheduled_arrival_ts AT TIME ZONE 'UTC')
              AT TIME ZONE 'America/New_York')::INT AS sched_et_hr,
            LEAD(se.stop_sequence) OVER w AS next_seq,
            LEAD(se.stop_id) OVER w AS next_stop_id,
            LEAD(se.observed_arrival_ts) OVER w AS next_obs,
            LEAD(se.scheduled_arrival_ts) OVER w AS next_sched
          FROM stop_events se
          WHERE se.route_id = :route_id
            AND se.source = 'proximity'
            AND se.observed_arrival_ts IS NOT NULL
            AND se.scheduled_arrival_ts IS NOT NULL
            AND se.schedule_relationship = 'SCHEDULED'
            AND se.service_date BETWEEN :start AND :end
          WINDOW w AS (
            PARTITION BY se.service_date, se.trip_id
            ORDER BY se.stop_sequence
          )
        ),
        canonical_ranked AS (
          SELECT
            direction_id,
            stop_sequence,
            stop_id,
            ROW_NUMBER() OVER (
              PARTITION BY direction_id, stop_sequence
              ORDER BY COUNT(*) DESC, stop_id ASC
            ) AS rk
          FROM ordered
          GROUP BY direction_id, stop_sequence, stop_id
        ),
        canonical AS (
          SELECT direction_id, stop_sequence, stop_id
          FROM canonical_ranked
          WHERE rk = 1
        ),
        seg AS (
          SELECT
            o.direction_id,
            o.stop_sequence AS from_seq,
            o.stop_id AS from_stop_id,
            o.next_seq AS to_seq,
            o.next_stop_id AS to_stop_id,
            EXTRACT(EPOCH FROM (o.next_obs - o.observed_arrival_ts)) AS obs_gap,
            EXTRACT(EPOCH FROM (o.next_sched - o.scheduled_arrival_ts)) AS sched_gap
          FROM ordered o
          JOIN canonical c_from
            ON c_from.direction_id = o.direction_id
           AND c_from.stop_sequence = o.stop_sequence
           AND c_from.stop_id = o.stop_id
          JOIN canonical c_to
            ON c_to.direction_id = o.direction_id
           AND c_to.stop_sequence = o.next_seq
           AND c_to.stop_id = o.next_stop_id
          WHERE o.next_obs IS NOT NULL
            AND o.next_sched IS NOT NULL
            {period_clause}
            AND EXTRACT(EPOCH FROM (o.next_obs - o.observed_arrival_ts)) BETWEEN 0 AND 1800
        )
        SELECT
          direction_id,
          from_seq,
          from_stop_id,
          to_seq,
          to_stop_id,
          COUNT(*) AS n,
          AVG(obs_gap - sched_gap)::FLOAT AS mean_slip_sec
        FROM seg
        GROUP BY direction_id, from_seq, from_stop_id, to_seq, to_stop_id
        HAVING COUNT(*) >= :min_obs
        ORDER BY direction_id, from_seq;
        """
    )

    rows = db.execute(
        sql,
        {
            "route_id": route_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "min_obs": MIN_SEGMENT_OBSERVATIONS,
        },
    ).fetchall()

    raw_rows = [
        {
            "direction_id": int(r.direction_id),
            "from_seq": int(r.from_seq),
            "from_stop_id": r.from_stop_id,
            "to_seq": int(r.to_seq),
            "to_stop_id": r.to_stop_id,
            "n_observations": int(r.n),
            "mean_slip_sec": float(r.mean_slip_sec),
        }
        for r in rows
    ]
    return _assemble_segment_slip_output(raw_rows, period)


def _assemble_segment_slip_output(
    raw_rows: list[dict[str, Any]],
    period: str,
) -> list[dict[str, Any]]:
    """Drop the origin-departure segment per direction and attach cum_slip_sec.

    Splits from :func:`compute_segment_slip` so the cumsum walk is unit-
    testable without standing up the Postgres-only slip SQL. Assumes
    ``raw_rows`` already has exactly one row per
    ``(direction_id, from_seq, to_seq)`` — the canonical-pattern filter
    inside the SQL is what guarantees that invariant; if it ever fails
    upstream (e.g., the canonical CTE breaks), this helper will silently
    double-count, which is precisely the bug NOTES-57 fast-follow fixes.
    """
    by_dir: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in raw_rows:
        by_dir[r["direction_id"]].append(dict(r))

    out: list[dict[str, Any]] = []
    for direction_id, segs in by_dir.items():
        if not segs:
            continue
        segs.sort(key=lambda d: d["from_seq"])
        # Drop origin-departure (minimum from_seq for this direction).
        min_from = segs[0]["from_seq"]
        kept = [s for s in segs if s["from_seq"] != min_from]
        cum = 0.0
        for s in kept:
            cum += s["mean_slip_sec"]
            s["cum_slip_sec"] = cum
            s["period"] = period
            s["direction_id"] = direction_id
        out.extend(kept)
    return out


# ---------------------------------------------------------------------------
# Timepoint mapping + behavior classification
# ---------------------------------------------------------------------------


def fetch_route_timepoint_stops(db: Session, route_id: str) -> set[str]:
    """Return public-GTFS ``stop_id``s on `route_id` that match a WMATA timepoint.

    Joins ``timepoints`` (GTFS-Plus internal stop_ids) to public-GTFS
    ``stops`` via 50m haversine — direct stop_id joins return zero
    matches because the two id spaces don't share keys (CLAUDE.md).
    """
    sql = text(
        """
        WITH route_stops AS (
          SELECT DISTINCT se.stop_id, s.stop_lat, s.stop_lon
          FROM stop_events se
          JOIN stops s ON s.stop_id = se.stop_id AND s.is_current
          WHERE se.route_id = :route_id AND se.source = 'proximity'
        )
        SELECT DISTINCT rs.stop_id
        FROM route_stops rs
        JOIN timepoints tp ON
          6371000 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(rs.stop_lat - tp.stop_lat) / 2), 2) +
            COS(RADIANS(rs.stop_lat)) * COS(RADIANS(tp.stop_lat)) *
            POWER(SIN(RADIANS(rs.stop_lon - tp.stop_lon) / 2), 2)
          )) < 50;
        """
    )
    return {row[0] for row in db.execute(sql, {"route_id": route_id}).fetchall()}


def _percentile(values: list[float], p: float) -> float | None:
    """Linear-interpolation percentile (p in [0, 100]).

    Returns None for empty input. We implement this ourselves rather than
    leaning on ``numpy.percentile`` because the rest of this module stays
    in stdlib + sqlalchemy; the segment-slip code path uses SQL ``AVG``
    directly, so no per-row Python is required for slip. Only the
    timepoint classification needs medians / p10s, on small samples
    (one timepoint × one direction × one period), where the implementation
    is irrelevant for speed.
    """
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    k = (len(s) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return float(s[lo] + (s[hi] - s[lo]) * frac)


def classify_timepoint(
    entering_devs: list[float],
    leaving_devs: list[float],
) -> tuple[str, dict[str, float | None]]:
    """Classify a single timepoint from its entering / leaving deviation samples.

    The ``entering_devs`` sample is the deviation_sec at the stop one
    sequence position *before* the timepoint; ``leaving_devs`` is the
    deviation_sec at the timepoint itself. Distribution shift across the
    pair determines behavior:

      - **recovery** — median drops by ≥ ``RECOVERY_MEDIAN_DROP_SEC`` (the
        timepoint is doing its job: late buses arrive, on-time buses leave)
      - **leaky** — p10 drops by ≥ ``LEAKY_P10_DROP_SEC`` (early-departure
        bleed; operators hold a hair, then push off and run hot)
      - **underpowered** — median entering ≥ ``UNDERPOWERED_ENTERING_MEDIAN_SEC``
        and the drop is < ``UNDERPOWERED_MEDIAN_DROP_MAX_SEC`` (the
        timepoint is loaded with late arrivals but has no recovery padding
        — a candidate for schedule revision, not operator coaching)
      - **neutral** — well-behaved timepoint, median in
        ±``NEUTRAL_MEDIAN_BAND_SEC`` entering, no notable distribution shift

    Returns ``(classification, stats)`` where stats include the four
    distribution summaries the panels surface — ``median_dev_entering``,
    ``median_dev_leaving``, ``p10_dev_entering``, ``p10_dev_leaving``. If
    either sample is below ``MIN_TIMEPOINT_OBSERVATIONS`` the
    classification is ``"insufficient_data"`` and the stats may be None.
    """
    stats: dict[str, float | None] = {
        "median_dev_entering": _percentile(entering_devs, 50),
        "median_dev_leaving": _percentile(leaving_devs, 50),
        "p10_dev_entering": _percentile(entering_devs, 10),
        "p10_dev_leaving": _percentile(leaving_devs, 10),
        "n_entering": float(len(entering_devs)),
        "n_leaving": float(len(leaving_devs)),
    }
    if (
        len(entering_devs) < MIN_TIMEPOINT_OBSERVATIONS
        or len(leaving_devs) < MIN_TIMEPOINT_OBSERVATIONS
    ):
        return "insufficient_data", stats

    med_in = stats["median_dev_entering"]
    med_out = stats["median_dev_leaving"]
    p10_in = stats["p10_dev_entering"]
    p10_out = stats["p10_dev_leaving"]
    assert med_in is not None and med_out is not None
    assert p10_in is not None and p10_out is not None

    median_drop = med_in - med_out
    p10_drop = p10_in - p10_out

    # Recovery takes priority: a meaningful median compression is the most
    # informative signal even if the p10 also moved.
    if median_drop >= RECOVERY_MEDIAN_DROP_SEC:
        return "recovery", stats

    # Leaky: the early tail leaks earlier downstream of the timepoint.
    if p10_drop >= LEAKY_P10_DROP_SEC:
        return "leaky", stats

    # Underpowered: bus is late entering and the median doesn't compress.
    if (
        med_in >= UNDERPOWERED_ENTERING_MEDIAN_SEC
        and median_drop < UNDERPOWERED_MEDIAN_DROP_MAX_SEC
    ):
        return "underpowered", stats

    # Neutral: well-behaved timepoint, median near zero entering, no big shift.
    if abs(med_in) <= NEUTRAL_MEDIAN_BAND_SEC:
        return "neutral", stats

    # Catch-all when none of the diagnostic criteria fire — emit neutral as a
    # safe default. (Panels treat "neutral" as a quiet style.)
    return "neutral", stats


def compute_timepoint_behavior(
    db: Session,
    route_id: str,
    period: str,
    service_date_range: tuple[date_type, date_type],
) -> list[dict[str, Any]]:
    """Classify every timepoint on `route_id` for one period.

    For each (direction_id, timepoint_stop_id) we collect the deviation_sec
    samples at the timepoint (``leaving``) and at the immediately preceding
    sequence position (``entering``), then run :func:`classify_timepoint`.

    Returns one row per classified timepoint shaped for upsert.
    """
    if period not in PERIOD_HOURS:
        raise ValueError(f"unknown period: {period}")
    start, end = service_date_range

    timepoint_stop_ids = fetch_route_timepoint_stops(db, route_id)
    if not timepoint_stop_ids:
        return []

    # Pull per-(direction, stop_sequence, stop_id, trip) deviation samples
    # for this route. We can't pre-filter by period in SQL — the period is
    # determined by the *from* stop's scheduled hour, but the from-stop is
    # the stop one sequence position before each timepoint and is only
    # knowable trip-by-trip. So we read the unfiltered sequence and apply
    # the period filter in Python via ``_hour_in_period`` during pairing.
    sql = text(
        """
        SELECT
          se.service_date,
          se.trip_id,
          se.direction_id,
          se.stop_sequence,
          se.stop_id,
          se.deviation_sec,
          EXTRACT(HOUR FROM (se.scheduled_arrival_ts AT TIME ZONE 'UTC')
            AT TIME ZONE 'America/New_York')::INT AS sched_et_hr
        FROM stop_events se
        WHERE se.route_id = :route_id
          AND se.source = 'proximity'
          AND se.deviation_sec IS NOT NULL
          AND se.scheduled_arrival_ts IS NOT NULL
          AND se.schedule_relationship = 'SCHEDULED'
          AND se.service_date BETWEEN :start AND :end
        ORDER BY se.service_date, se.trip_id, se.direction_id, se.stop_sequence
        """
    )
    rows = db.execute(
        sql,
        {
            "route_id": route_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    ).fetchall()

    # Pair each timepoint observation with the immediately-preceding stop
    # observation on the same (service_date, trip_id). The deviation at the
    # preceding stop is the "entering" sample; the deviation at the
    # timepoint is the "leaving" sample.
    samples: dict[tuple[int, str], dict[str, list[float]]] = defaultdict(
        lambda: {"entering": [], "leaving": []}
    )
    # last_per_trip[(service_date, trip_id)] = (stop_sequence, deviation_sec,
    #     sched_et_hr). We don't require strict sequence adjacency — GTFS
    # uses non-contiguous stop_sequence values and one missed observation
    # shouldn't disqualify the pairing. The immediately-prior observation
    # on the trip is the closest entering sample we have either way.
    last_per_trip: dict[tuple[str, str], tuple[int, float, int | None]] = {}
    for r in rows:
        if r.deviation_sec is None:
            continue
        key = (r.service_date, r.trip_id)
        if r.stop_id in timepoint_stop_ids and key in last_per_trip:
            prev_seq, prev_dev, prev_hr = last_per_trip[key]
            # Period filter applies to the *from* stop's scheduled hour, the
            # same anchoring used for segment slip.
            in_period = _hour_in_period(prev_hr, period) if prev_hr is not None else False
            if in_period and prev_seq < r.stop_sequence:
                samples[(r.direction_id, r.stop_id)]["entering"].append(float(prev_dev))
                samples[(r.direction_id, r.stop_id)]["leaving"].append(float(r.deviation_sec))
        last_per_trip[key] = (
            r.stop_sequence,
            float(r.deviation_sec),
            r.sched_et_hr,
        )

    out: list[dict[str, Any]] = []
    for (direction_id, stop_id), sample in samples.items():
        cls, stats = classify_timepoint(sample["entering"], sample["leaving"])
        if cls == "insufficient_data":
            continue
        out.append(
            {
                "direction_id": direction_id,
                "timepoint_stop_id": stop_id,
                "period": period,
                "classification": cls,
                "median_dev_entering": stats["median_dev_entering"],
                "median_dev_leaving": stats["median_dev_leaving"],
                "p10_dev_entering": stats["p10_dev_entering"],
                "p10_dev_leaving": stats["p10_dev_leaving"],
                "n_observations": int(stats["n_leaving"] or 0),
            }
        )
    return out


def _hour_in_period(hour: int, period: str) -> bool:
    """Return True iff `hour` (Eastern, 0-23) falls inside `period`."""
    if period == "all":
        return True
    if period == "late":
        return hour >= 22 or hour < 6
    bounds = PERIOD_HOURS[period]
    if bounds is None:
        return False
    lo, hi = bounds
    return lo <= hour < hi


# ---------------------------------------------------------------------------
# Direction asymmetry signature
# ---------------------------------------------------------------------------


def compute_direction_asymmetry(
    db: Session,
    route_id: str,
    period: str,
    service_date_range: tuple[date_type, date_type],
) -> list[dict[str, Any]]:
    """Per-direction early% / late% / signature for one route / period.

    Reads ``stop_events`` rows with ``source='proximity'`` and a populated
    ``deviation_sec``; buckets each into early / on_time / late by the
    same −2/+7 thresholds the OTP module uses. Returns one row per
    direction with the resulting percentages and a categorical signature
    (early-dominant / late-dominant / balanced).

    The signature is intended for headline UI emphasis — D80 dir 0
    early-dominant at 26% early, dir 1 late-dominant at 26% late, etc.
    """
    if period not in PERIOD_HOURS:
        raise ValueError(f"unknown period: {period}")
    start, end = service_date_range

    period_clause = ""
    if period == "late":
        period_clause = "AND (sched_et_hr >= 22 OR sched_et_hr < 6)"
    elif period != "all":
        lo, hi = PERIOD_HOURS[period]  # type: ignore[misc]
        period_clause = f"AND sched_et_hr >= {lo} AND sched_et_hr < {hi}"

    sql = text(
        f"""
        WITH base AS (
          SELECT
            se.direction_id,
            se.deviation_sec,
            EXTRACT(HOUR FROM (se.scheduled_arrival_ts AT TIME ZONE 'UTC')
              AT TIME ZONE 'America/New_York')::INT AS sched_et_hr
          FROM stop_events se
          WHERE se.route_id = :route_id
            AND se.source = 'proximity'
            AND se.deviation_sec IS NOT NULL
            AND se.scheduled_arrival_ts IS NOT NULL
            AND se.schedule_relationship = 'SCHEDULED'
            AND se.service_date BETWEEN :start AND :end
        )
        SELECT
          direction_id,
          COUNT(*) AS n,
          SUM(CASE WHEN deviation_sec < :early_thresh THEN 1 ELSE 0 END) AS early_n,
          SUM(CASE WHEN deviation_sec > :late_thresh  THEN 1 ELSE 0 END) AS late_n
        FROM base
        WHERE 1=1
          {period_clause}
        GROUP BY direction_id
        ORDER BY direction_id;
        """
    )
    rows = db.execute(
        sql,
        {
            "route_id": route_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "early_thresh": EARLY_THRESHOLD_SEC,
            "late_thresh": LATE_THRESHOLD_SEC,
        },
    ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        n = int(r.n or 0)
        if n == 0:
            continue
        early_pct = 100.0 * (r.early_n or 0) / n
        late_pct = 100.0 * (r.late_n or 0) / n
        diff = early_pct - late_pct
        if diff > ASYMMETRY_MARGIN_PCT:
            signature = "early_dominant"
        elif diff < -ASYMMETRY_MARGIN_PCT:
            signature = "late_dominant"
        else:
            signature = "balanced"
        out.append(
            {
                "direction_id": int(r.direction_id),
                "period": period,
                "early_pct": float(early_pct),
                "late_pct": float(late_pct),
                "signature": signature,
                "n_observations": n,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------


def compute_route_diagnostics(
    db: Session,
    route_id: str,
    service_date_range: tuple[date_type, date_type],
    periods: Iterable[str] = ALL_PERIODS,
) -> dict[str, list[dict[str, Any]]]:
    """Compute all three diagnostic surfaces for one route across periods.

    Returns a dict shaped for the materialization layer::

        {
          "segments":   [ ...segment rows for every period... ],
          "timepoints": [ ...timepoint rows for every period... ],
          "directions": [ ...direction asymmetry rows for every period... ],
        }

    Each row carries a ``period`` field so the caller can upsert into the
    materialized tables with one (route_id, period) sweep per surface.
    """
    segments: list[dict[str, Any]] = []
    timepoints: list[dict[str, Any]] = []
    directions: list[dict[str, Any]] = []

    for period in periods:
        seg = compute_segment_slip(db, route_id, period, service_date_range)
        for s in seg:
            s["route_id"] = route_id
            s["computed_at"] = utcnow_naive()
        segments.extend(seg)

        tp = compute_timepoint_behavior(db, route_id, period, service_date_range)
        for t in tp:
            t["route_id"] = route_id
            t["computed_at"] = utcnow_naive()
        timepoints.extend(tp)

        ds = compute_direction_asymmetry(db, route_id, period, service_date_range)
        for d in ds:
            d["route_id"] = route_id
            d["computed_at"] = utcnow_naive()
        directions.extend(ds)

    return {
        "segments": segments,
        "timepoints": timepoints,
        "directions": directions,
    }
