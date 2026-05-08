"""Standard day-type / time-period buckets for the RouteDetail filter (NOTES-41).

Distinct from `src/ewt.py:EWT_TIME_PERIODS`, which slices the day into the
five buckets the per-period drilldown reports against (AM Peak 6-9 / Midday
9-15 / PM Peak 15-19 / Evening 19-24 / Night 0-6). Those bucket boundaries
match the EWT-headline aggregation contract — every observed cell-hour is
attributed to exactly one bucket and the labels are the canonical drilldown
column names.

The filter buckets here are coarser-grained "WMATA scorecard"-style windows
(AM peak 6-10, midday 10-15, PM peak 15-19, evening 19-22, late 22-6). They
are the *user-facing slice* of the day on RouteDetail, not the headline
aggregation contract — picking "AM Peak (6-10)" restricts which observed
hours feed the per-route KPIs and trend, full stop. The two systems are
deliberately decoupled: the drilldown's per-period rows still report
against EWT_TIME_PERIODS (so cell-hour math doesn't shift under the user)
while RouteDetail's filter restricts the input set to the headline.

`is_hour_in_period` handles the late-night wrap correctly — `Late` spans
22:00-06:00, so a 23 satisfies `>= 22` and a 3 satisfies `< 6`. Period
labels are the strings the API emits and the frontend renders; period keys
are the lowercase tokens the API accepts as the `period=` query parameter.
"""

from __future__ import annotations

from typing import NamedTuple

# Sentinel value for "no period filter" — accepted by the API and treated as
# "any hour qualifies." Kept as a constant rather than `None` so the filter
# parameter has a single ground-truth representation through the call stack.
ALL_HOURS = "all"

# Sentinel value for "no day-type filter" — accepts every service_date.
ALL_DAY_TYPES = "all"

# Recognized day_type tokens. Matches `_day_type_for(date)` in src/ewt.py
# (weekday / saturday / sunday) plus the "all" sentinel.
VALID_DAY_TYPES: tuple[str, ...] = (ALL_DAY_TYPES, "weekday", "saturday", "sunday")


class TimePeriod(NamedTuple):
    """One labeled time-of-day bucket for the RouteDetail filter.

    `start_hour` is inclusive, `end_hour` is exclusive. When `wraps_midnight`
    is True (only `late`) the bucket covers `[start_hour, 24) U [0, end_hour)`,
    so an hour qualifies iff `hour >= start_hour OR hour < end_hour`.

    `key` is the API token (lowercase, snake-case) and `label` is the UI
    label rendered in the filter dropdown.
    """

    key: str
    label: str
    start_hour: int
    end_hour: int
    wraps_midnight: bool


# Standard period buckets, ordered for the filter dropdown rendering. AM Peak
# / Midday / PM Peak / Evening / Late mirror typical WMATA scorecard slices;
# Late wraps midnight (22:00-06:00) which the wrap helper handles.
TIME_PERIODS: tuple[TimePeriod, ...] = (
    TimePeriod("am_peak", "AM Peak (6-10am)", 6, 10, False),
    TimePeriod("midday", "Midday (10am-3pm)", 10, 15, False),
    TimePeriod("pm_peak", "PM Peak (3-7pm)", 15, 19, False),
    TimePeriod("evening", "Evening (7-10pm)", 19, 22, False),
    TimePeriod("late", "Late (10pm-6am)", 22, 6, True),
)

# `key → TimePeriod` lookup so endpoint code doesn't have to scan the tuple.
_TIME_PERIODS_BY_KEY: dict[str, TimePeriod] = {p.key: p for p in TIME_PERIODS}

# Tokens accepted by the `period=` query parameter, including the `all`
# sentinel. Kept as a tuple so endpoints can validate without instantiating
# the dict every call.
VALID_PERIOD_KEYS: tuple[str, ...] = (ALL_HOURS, *(p.key for p in TIME_PERIODS))


def get_period(key: str) -> TimePeriod | None:
    """Return the `TimePeriod` for a key, or `None` for `ALL_HOURS`/unknown.

    `None` is returned for both "no filter" (`ALL_HOURS`) and "unrecognized
    key" — callers that need to distinguish should check `key == ALL_HOURS`
    explicitly. For endpoint validation use `VALID_PERIOD_KEYS`.
    """
    if key == ALL_HOURS:
        return None
    return _TIME_PERIODS_BY_KEY.get(key)


def is_hour_in_period(hour: int, period_key: str) -> bool:
    """Return True iff `hour` (Eastern, 0-23) falls within the period.

    `ALL_HOURS` returns True for every valid hour. `Late` (22-6) wraps
    midnight: 22, 23, 0, 1, 2, 3, 4, 5 all qualify; 6 does not (end-exclusive).
    Unknown period keys return False rather than raising — endpoints validate
    the key up front, so reaching this with a garbage key is a programmer
    error and silently filtering nothing prevents the wrong path from being
    accidentally exercised.
    """
    if period_key == ALL_HOURS:
        return True
    period = _TIME_PERIODS_BY_KEY.get(period_key)
    if period is None:
        return False
    if period.wraps_midnight:
        return hour >= period.start_hour or hour < period.end_hour
    return period.start_hour <= hour < period.end_hour


def hour_range_for_period(period_key: str) -> tuple[int, int, bool] | None:
    """Return `(start_hour, end_hour, wraps_midnight)` or None for `ALL_HOURS`.

    Lower-level filter helpers (the bunching / EWT cell-hour pools) consume
    a hour-range tuple directly so they don't have to import the named-tuple
    type. Returns None when no filter applies — caller short-circuits to the
    unfiltered path.
    """
    period = get_period(period_key)
    if period is None:
        return None
    return (period.start_hour, period.end_hour, period.wraps_midnight)
