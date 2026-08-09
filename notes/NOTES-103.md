# NOTES-103. EWT/OTP/bunching hour-of-day bucketing hardcodes Eastern

**Severity: high** *(corrected from an earlier "medium" draft that
understated the blast radius — see below. Two of NOTES-99's four
headline KPIs — EWT and bunching rate — are actively wrong for SFMTA,
not just a peak/off-peak label cosmetic. OTP and service-delivered are
unaffected and safe to trust.)*
**Effort: medium** *(three separate private `_eastern_hour(ts)` helpers
— `src/ewt.py`, `src/otp_metrics.py`, `src/bunching.py` — each called
from several places inside their module; needs a `tz_name` parameter
threaded from the public `compute_ewt_*` / `compute_bunching_*`
functions down through every internal call site, with test coverage
for a Pacific-zone case per module).*

## Confirmed blast radius (not just time-period labels)

The SFMTA derivation rollout (NOTES-100 / PR #189) made `stop_events`/
`runs` service-date attribution, the schedule-anchor parser
(`stop_events.scheduled_arrival_ts`), and the ingest-completeness guard
correctly agency-local (Pacific for SFMTA). But three separate
metric-computation modules still bucket UTC timestamps into "hour of
day" via a module-level constant:

```python
EASTERN = ZoneInfo("America/New_York")

def _eastern_hour(ts):
    return ts.replace(tzinfo=UTC).astimezone(EASTERN).hour
```

**This is worse than a mislabeled time-period for EWT and bunching
rate specifically**, because the *scheduled* side of both computations
is bucketed by a *different, correct* clock:

- `src/ewt.py:_scheduled_headways_by_cell_hour` buckets scheduled
  headways by `(parsed_seconds // 3600) % 24` — GTFS clock-time
  seconds-since-midnight, i.e. the agency's **own local hour** (Pacific
  for SFMTA GTFS). This is already correct.
- `src/ewt.py:_observed_headways_by_cell_hour` and
  `src/bunching.py:_scheduled_observed_headways_by_cell_hour` bucket
  *observed* headways by `_eastern_hour(prev_ts)` — hardcoded Eastern,
  3h off for SFMTA.

`compute_ewt_for_route_date` joins these two pools **by cell-hour
key** (`(direction, stop, hour)`). For WMATA the keys agree (both
Eastern) so the join works. For SFMTA the keys are 3h apart, so:

- **EWT**: `compute_ewt_for_route_date` pairs a cell-hour's *scheduled*
  headways (correct Pacific hour, e.g. 17) with *observed* headways
  keyed at hour 17 in `obs_by_cell_hour` — but that dict was built with
  Eastern hours, so hour-17 observed headways are actually from ~14:00
  Pacific (2pm), not 5pm. EWT ends up comparing AM/midday observed
  service against PM-peak scheduled headways (or vice versa) for every
  cell — not a rounding error, a wrong pairing.
- **Bunching rate**: `compute_bunching_for_route_date` looks up
  `sched_by_cell_hour.get(cell_hour, [])` where `cell_hour` comes from
  the *Eastern*-keyed `obs_by_cell_hour`. Since the scheduled dict is
  Pacific-keyed, this lookup misses almost every cell (`threshold is
  None`), and the cell is `continue`d out of **both**
  `bunching_count` and `total_headways` — i.e. bunching rate is
  computed from whatever sliver of cells happen to collide by
  coincidence, not "no data" (which would show as `None`) but a
  silently-thin, silently-biased sample that still returns a number.

`src/otp_metrics.py`'s `_eastern_hour` only gates the **period-filtered**
branch of `compute_otp_split` (`period_key != "all"`); the default,
no-filter path — which is what `system_metrics_daily.otp_percentage`
and `service_delivered_ratio` actually use
(`api/aggregations.py:_system_otp_series`,
`_system_service_delivered_series`) — never calls it. **OTP and
service-delivered are safe for SFMTA today; EWT and bunching rate are
not**, until this item closes.

**Do not trust or publish SFMTA EWT / bunching-rate numbers — from
`system_metrics_daily`, `route_metrics_daily_overlay`, or
`route_headway_metrics` — until this closes.** Any SFMTA dates
backfilled before the fix lands must be re-derived afterward (re-run
`aggregate_runs` → `compute_bunching` → `upsert_system_metrics_daily`
→ `upsert_route_metrics_overlay` for those dates once the fix is in;
`derive_stop_events*` output itself is unaffected and doesn't need
re-running).

## To close

1. Add a `tz_name: str = "America/New_York"` parameter to each of the
   three `_eastern_hour` functions (or consolidate to one shared
   helper — they're identical) and thread it up through every public
   caller: `compute_ewt_for_route_date`, `compute_ewt_for_routes`,
   `compute_ewt_headline_for_route(s)`,
   `compute_ewt_headline_for_routes_multi_date`,
   `compute_bunching_for_route_date`, and whatever calls
   `otp_metrics.py`'s period-split functions.
2. Audit `src/time_periods.py` for the same hardcoded-Eastern pattern.
3. Thread `tz_name` the rest of the way from
   `src/system_metrics.py:upsert_system_metrics_for_date` and
   `src/route_metrics_overlay.py:upsert_route_metrics_for_date` (which
   already accept `tz_name` as of NOTES-100, but only pass it to the
   completeness guard — see the explicit caveat in both functions'
   docstrings) into `compute_system_metrics_for_date` /
   `compute_route_metrics_overlay_for_date`.
4. Add a Pacific-zone test per module proving a late-afternoon Pacific
   timestamp buckets into the correct local hour, not Eastern's —
   plus a regression test that an SFMTA-shaped fixture (schedule and
   observations both anchored Pacific) produces a non-degenerate EWT
   cell-hour join and a bunching `total_headways` that isn't
   suspiciously near zero.
5. **Re-derive** any SFMTA service dates that were backfilled before
   this fix lands (see blast-radius note above) — `aggregate_runs`,
   `compute_bunching`, `upsert_system_metrics_daily`, and
   `upsert_route_metrics_overlay` all need to re-run for those dates;
   `derive_stop_events`/`derive_stop_events_from_state` do not.

Blocks trustworthy SFMTA EWT and bunching-rate numbers specifically —
does not block NOTES-99's page from loading with OTP and
service-delivered, and does not block the raw backfill (item 4 of the
now-closed NOTES-100) from running, since `stop_events`/`runs` derivation
itself is correct today.
