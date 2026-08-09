# NOTES-103. EWT/OTP/bunching hour-of-day bucketing hardcodes Eastern

**Severity: medium** *(SFMTA metrics will compute without error, but
peak/off-peak time-period classification and the EWT frequent-route
cell-hour gate will be silently wrong by a fixed 3h offset — a
correctness bug, not a crash, so it won't surface until someone
eyeballs Muni rush-hour numbers against the clock).*
**Effort: medium** *(three separate private `_eastern_hour(ts)` helpers
— `src/ewt.py`, `src/otp_metrics.py`, `src/bunching.py` — each called
from several places inside their module; needs a `tz_name` parameter
threaded from the public `compute_ewt_*` / `compute_bunching_*`
functions down through every internal call site, with test coverage
for a Pacific-zone case per module).*

The SFMTA derivation rollout (NOTES-100) made `stop_events`/`runs`
service-date attribution and the ingest-completeness guard correctly
agency-local (Pacific for SFMTA), but three separate metric-computation
modules still bucket UTC timestamps into "hour of day" via a
module-level constant:

```python
EASTERN = ZoneInfo("America/New_York")

def _eastern_hour(ts):
    return ts.replace(tzinfo=UTC).astimezone(EASTERN).hour
```

This exists independently in `src/ewt.py` (`_eastern_hour`, used by
the per-cell-hour EWT pools and the `FREQUENT_HEADWAY_MAX_SEC`
data-driven frequent-route gate), `src/otp_metrics.py` (`_eastern_hour`,
used for period-of-day OTP splits), and `src/bunching.py`
(`_eastern_hour`, used for per-cell-hour bunching thresholds). None of
it is reached by `src/timezones.py`, so NOTES-100's timezone audit
(scoped to `src/timezones.py` call sites) didn't touch it.

Effect for SFMTA (Pacific, UTC-7/-8): every timestamp's "hour" comes
out 3h later than the true Pacific hour. A 5pm PT bus (peak) buckets as
8pm PT-equivalent-Eastern-clock (off-peak) internally. Time-of-day
splits (AM peak / midday / PM peak / evening / night in
`src/time_periods.py`, if it's driven by the same clock — audit that
too) and the EWT frequent-route cell-hour gate (`src/ewt.py:
FREQUENT_HEADWAY_MAX_SEC` applied per cell-hour) will silently
misclassify which hours are "frequent" and which period each
observation falls in.

To close:

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
   timestamp buckets into the correct local hour, not Eastern's.

Blocks trustworthy SFMTA EWT/peak-period numbers; does not block
NOTES-99's page from loading (OTP/service-delivered/bunching *rate*
computations elsewhere don't bucket by hour) but any hour-of-day-based
comparison built on top should wait for this.
