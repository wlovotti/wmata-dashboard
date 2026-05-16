"""Marginal-bus EWT model — per-(route, period) "next bus" SWT-reduction ranking.

Answers the operator question "where would my next scheduled trip help most?"
by ranking (route, time_period) cells by the SWT reduction the closed-form
headway model predicts from adding one trip.

Closed-form approximation
-------------------------
For a period of length `T` minutes containing `N` evenly-spaced scheduled
trips, the scheduled headway is `T / N` and the rider-felt scheduled wait
time `SWT = h / 2 = T / (2 N)` (uniform-arrival assumption — riders show
up uniformly across the headway, each waiting on average half of it).

Adding one trip lifts the count to `N + 1`, giving `SWT' = T / (2 (N + 1))`.
The reduction is:

    SWT - SWT' = T / (2 N) - T / (2 (N + 1))
               = T * ((N + 1) - N) / (2 N (N + 1))
               = T / (2 N (N + 1))    # in MINUTES, since T is in minutes

This is the headline number this module surfaces, per `(route_id,
period_label)`. It scales as `1 / N^2` — diminishing returns are sharp,
so the periods that most benefit from one more bus are the sparse ones.

Why SWT and not AWT directly
----------------------------
AWT (the *observed* rider-felt wait) depends on how the new trip lands
relative to existing variance. A new trip scheduled to slot evenly between
two existing trips reduces AWT by approximately the same fraction as SWT.
A new trip scheduled into an already-tight cluster (the bunching case)
reduces AWT by less, and at the extreme can leave it nearly unchanged.

Modeling that requires a placement-strategy assumption that the marginal
analysis can't supply on its own — it's a dispatching/scheduling decision
the model doesn't see. So we surface SWT reduction (the rigorous
closed-form bound from the schedule) and note in the UI that the absolute
number is a best-case proxy; the *ranking* across (route, period) cells is
the part that's defensible.

Why this period bucketing
-------------------------
We use the same five Eastern-hour buckets as `src/ewt.py:EWT_TIME_PERIODS`
(AM Peak 6-9, Midday 9-15, PM Peak 15-19, Evening 19-24, Night 0-6) so the
marginal model's periods reconcile with the EWT drilldown a user is
already looking at on RouteDetail. Trip counts come from
`route_service_profile.scheduled_trips`, which is hourly and bucketed by
trip-origin hour at the route's trunk stop — close to "buses operating in
this period" without being the same as it (trips that originate at hour
H-1 but cross into hour H aren't counted in hour H). Acceptable for a
ranking-oriented model; absolute counts are interpretation-bounded
anyway.

Day-type semantics
------------------
The route_service_profile rows are keyed by `(route_id, day_type, hour)`,
so the marginal model is computed against the day_type that matches the
request (`weekday` / `saturday` / `sunday`, mapped from the request's
service_date via `_day_type_for`). Operationally, "where should I add a
trip on a typical Tuesday" is the most useful framing — that's what
day_type='weekday' answers.

Output contract
---------------
Per `(route_id, period_label)`:
  - `current_trip_count`: N from route_service_profile
  - `current_swt_minutes`: T / (2 N) (uniform-arrival SWT estimate)
  - `marginal_swt_reduction_minutes`: T / (2 N (N + 1)) — the headline
  - `marginal_swt_reduction_pct`: reduction / current_swt = 1 / (N + 1)
  - `period_minutes`: T (so the UI can show the divisor)

Periods with `N == 0` are skipped — adding a trip when no scheduled service
exists is a service-launch decision, not a marginal-bus decision, and the
formula degenerates (`SWT = ∞`). Periods with `N == 1` are also skipped:
they have no scheduled headway at all, so the random-arrival SWT premise
the formula rests on is mathematically undefined.

Frequent-service gate
---------------------
The closed-form `SWT = h / 2 = T / (2 N)` rests on the *random-arrival*
assumption — riders show up uniformly across the headway without
consulting a schedule. That assumption is only defensible when service is
*frequent*: by the project convention (`src/service_profile.py`
`FREQUENT_HEADWAY_MIN = 15.0` and `src/ewt.py`
`_is_cell_hour_frequent` / `FREQUENT_HEADWAY_MAX_SEC = 15 * 60`), that
means **mean scheduled headway ≤ 15 min**. On routes with a 30- or
60-min headway, riders consult the schedule and time their arrival; SWT
under uniform arrivals overstates rider-felt wait, and the marginal
reduction the model reports is not a meaningful operator signal.

Operationally, mean headway is `T / N` (period length / scheduled
trips), so the gate is `T / N ≤ 15` i.e. `N ≥ T / 15`. We apply this
per-cell — a route can be frequent in AM Peak and non-frequent in
Evening — matching the cell-level convention `_is_cell_hour_frequent`
already uses on the EWT side. (`route_service_profile.is_frequent` is a
route-level rollup, too coarse for this purpose.)
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from src.ewt import EWT_TIME_PERIODS, FREQUENT_HEADWAY_MAX_SEC, _day_type_for
from src.models import Route, RouteServiceProfile

# Same threshold as src/ewt.py `_is_cell_hour_frequent` and
# src/service_profile.py `FREQUENT_HEADWAY_MIN`, in minutes. 15 min mean
# headway is the project-wide boundary above which random-arrival
# assumptions stop applying.
FREQUENT_HEADWAY_MAX_MIN = FREQUENT_HEADWAY_MAX_SEC / 60.0


def _period_length_minutes(start_hour: int, end_hour: int) -> int:
    """Return period length in minutes, handling the Night 0-6 wrap.

    Most periods are start < end (e.g. AM Peak 6 → 9 = 180 min). Night
    is encoded as 0..6 so the wrap is invisible at this layer; we
    don't need a special case here because the EWT period table uses
    the 0..6 form (not 24..30) for Night.
    """
    return (end_hour - start_hour) * 60


def _hours_in_period(start_hour: int, end_hour: int) -> list[int]:
    """Enumerate the integer hours-of-day a period covers.

    Half-open `[start, end)` for every EWT period; Night 0..6 covers
    hours 0, 1, 2, 3, 4, 5. The hours returned match the
    `route_service_profile.hour` column so callers can sum
    `scheduled_trips` over them.
    """
    return list(range(start_hour, end_hour))


def compute_marginal_ewt_for_routes(
    db: Session,
    day_type: str,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Per-(route, period) marginal-bus SWT-reduction estimates for a day_type.

    Pulls `route_service_profile` rows for the day_type, groups hourly trip
    counts into the five EWT periods, and applies the closed-form
    `T / (2 N (N + 1))` formula. Returns a flat list of dicts sorted by
    `marginal_swt_reduction_minutes` descending — "where adding one trip
    helps most" — with route metadata joined in for rendering.

    Periods are dropped if any of the following hold:
      - `scheduled_trips == 0` (no service to model — a service-launch
        decision, not a marginal one)
      - `scheduled_trips == 1` (no scheduled headway — SWT under
        random-arrival assumptions is undefined)
      - mean scheduled headway > 15 min (i.e. `period_minutes / N > 15`):
        non-frequent service, where riders consult the schedule and the
        random-arrival SWT premise doesn't hold. Threshold matches the
        cell-level convention in `src/ewt.py` (`FREQUENT_HEADWAY_MAX_SEC`
        / `_is_cell_hour_frequent`) and the route-level convention in
        `src/service_profile.py` (`FREQUENT_HEADWAY_MIN`).

    Args:
        db: Database session.
        day_type: One of `weekday`, `saturday`, `sunday` — selects which
            route_service_profile rows feed the model.
        route_ids: Optional restriction. None = every route in the profile.

    Returns:
        List of dicts with keys `route_id`, `route_short_name`,
        `route_long_name`, `day_type`, `time_period`, `period_minutes`,
        `current_trip_count`, `current_swt_minutes`,
        `marginal_swt_reduction_minutes`, `marginal_swt_reduction_pct`.
        Sorted by `marginal_swt_reduction_minutes` desc (largest gain first).
    """
    if day_type not in ("weekday", "saturday", "sunday"):
        raise ValueError(f"Unsupported day_type: {day_type}")

    # Pull route metadata once for the join.
    routes_q = db.query(Route).filter(Route.is_current)
    if route_ids is not None:
        routes_q = routes_q.filter(Route.route_id.in_(route_ids))
    route_short_names = {r.route_id: r.route_short_name for r in routes_q.all()}
    route_long_names_q = db.query(Route).filter(Route.is_current)
    if route_ids is not None:
        route_long_names_q = route_long_names_q.filter(Route.route_id.in_(route_ids))
    route_long_names = {r.route_id: r.route_long_name for r in route_long_names_q.all()}

    # Pull all profile rows for the day_type in one query.
    profile_q = db.query(
        RouteServiceProfile.route_id,
        RouteServiceProfile.hour,
        RouteServiceProfile.scheduled_trips,
    ).filter(RouteServiceProfile.day_type == day_type)
    if route_ids is not None:
        profile_q = profile_q.filter(RouteServiceProfile.route_id.in_(route_ids))

    # `trips_by_route_hour[(route_id, hour)] = scheduled_trips`
    trips_by_route_hour: dict[tuple[str, int], int] = {}
    for route_id, hour, trips in profile_q.all():
        trips_by_route_hour[(route_id, hour)] = int(trips or 0)

    # The set of (route_id) actually present in the profile — only these
    # have any scheduled service to model.
    routes_seen = {r for (r, _h) in trips_by_route_hour.keys()}

    out: list[dict] = []
    for route_id in sorted(routes_seen):
        for label, start_h, end_h in EWT_TIME_PERIODS:
            period_minutes = _period_length_minutes(start_h, end_h)
            n_trips = sum(
                trips_by_route_hour.get((route_id, h), 0) for h in _hours_in_period(start_h, end_h)
            )
            if n_trips <= 1:
                # N == 0: no scheduled service to model (service-launch
                # decision, not a marginal one). N == 1: no scheduled
                # headway at all, so SWT under random-arrival
                # assumptions is mathematically undefined. Skip either.
                continue
            # Frequent-service gate: random-arrival SWT only applies when
            # mean scheduled headway is at most 15 min — i.e. service is
            # frequent enough that riders don't consult the schedule.
            # Equivalent to `N >= period_minutes / 15`. Matches the
            # cell-level rule in src/ewt.py `_is_cell_hour_frequent` and
            # the route-level threshold in src/service_profile.py
            # `FREQUENT_HEADWAY_MIN`.
            mean_headway_min = period_minutes / n_trips
            if mean_headway_min > FREQUENT_HEADWAY_MAX_MIN:
                continue
            current_swt_minutes = period_minutes / (2.0 * n_trips)
            marginal_reduction_minutes = period_minutes / (2.0 * n_trips * (n_trips + 1))
            # `1 / (N + 1)` algebraically; precomputed for clarity.
            marginal_reduction_pct = 1.0 / (n_trips + 1)
            out.append(
                {
                    "route_id": route_id,
                    "route_short_name": route_short_names.get(route_id),
                    "route_long_name": route_long_names.get(route_id),
                    "day_type": day_type,
                    "time_period": label,
                    "period_minutes": period_minutes,
                    "current_trip_count": n_trips,
                    "current_swt_minutes": round(current_swt_minutes, 2),
                    "marginal_swt_reduction_minutes": round(marginal_reduction_minutes, 3),
                    "marginal_swt_reduction_pct": round(marginal_reduction_pct, 4),
                }
            )

    # Largest absolute reduction first — the headline ranking.
    out.sort(key=lambda r: r["marginal_swt_reduction_minutes"], reverse=True)
    return out


def compute_marginal_ewt_for_today(
    db: Session,
    route_ids: list[str] | None = None,
) -> dict:
    """Wrapper that anchors the day_type on today's Eastern service date.

    Returns the same per-row contract as `compute_marginal_ewt_for_routes`
    inside an envelope dict with the resolved `day_type` so the API layer
    doesn't have to re-derive it for the response.
    """
    from src.timezones import eastern_today

    day_type = _day_type_for(eastern_today())
    rows = compute_marginal_ewt_for_routes(db, day_type, route_ids=route_ids)
    return {
        "day_type": day_type,
        "rankings": rows,
    }
