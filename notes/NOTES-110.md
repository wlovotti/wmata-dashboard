# NOTES-110. SFMTA owl-route `stop_events.service_date` still misattributed after PR #192's deviation-anchor fix

**Severity: medium** *(SFMTA-only; day_type-filtered metrics silently
bucket weekday owl service into the wrong day, and any
`service_date`→`calendar_dates` join resolves the wrong calendar day —
but the deviation/OTP numbers PR #192 fixed are correct)*.
**Effort: low-medium** *(the corrected anchor day is already computed
inside the resolver — this persists it, touching both derive
pipelines' row-construction code)*.

PR #192 fixed the ~24h deviation cluster on SFMTA's 24-hour owl routes
(14, 22, 24, 38, 44, 48) by trying both `service_date` and the day
before it when parsing a stop_time's schedule string, and keeping
whichever anchor lands the scheduled instant closest to the
observation (`pipelines/stop_events_common.py`'s
`resolve_scheduled_instants` / `resolve_stop_time`). That fixes
`scheduled_arrival_ts`, `scheduled_departure_ts`, and therefore
`deviation_sec` — but the corrected anchor day is never written back to
the output row's `stop_events.service_date` column, which both
pipelines still set from the outer loop's `service_date` argument (the
*misattributed* date, unchanged).

Verified in local data: trip `12097226_M11` (route 14, GTFS service_id
`78968`, active Mon–Fri per `calendar_dates` — confirmed no weekend
exceptions) has post-midnight stops (00:09–00:55 Pacific) whose true
service day is Friday, but 45 of its `stop_events` rows are filed under
`service_date = 2026-07-25` (a Saturday, true day Fri 7/24) and another
45 under `2026-08-08` (also a Saturday, true day Fri 8/7):

```
 service_date | source      | count
--------------+-------------+-------
 2026-07-25   | trip_update |    45   -- should be 2026-07-24 (Friday)
 2026-08-08   | trip_update |    45   -- should be 2026-08-07 (Friday)
```

(proximity-source counts are smaller per date since proximity only
captures rows within 50m of a stop, but the same misattributed dates
show up there too.)

Impact: every day_type-filtered SFMTA metric (weekday vs. weekend
splits, frequency classification, EWT's cell-hour bucketing once it's
SFMTA-aware) puts weekday owl service in the Saturday/Sunday bucket
instead of Friday's, and any `service_date` join against
`calendar_dates` (e.g. to resolve which `service_id` was active) picks
the wrong day's exceptions. This is upstream of NOTES-99's headline
KPIs for owl routes specifically.

**Related latent gap** (found during the same review, not yet
manifesting): in `pipelines/derive_stop_events_from_state.py`'s
`SKIPPED`-fallback branch (~lines 203-219), `scheduled_arrival_ts` /
`scheduled_departure_ts` are parsed directly against the plain
`service_date` anchor rather than through `resolve_scheduled_instants`,
because `SKIPPED` rows have no `observed_arrival_ts` to disambiguate
against (the resolver's anchor search needs *some* observation to
compare distances to). Those scheduled timestamps feed
`runs.sched_first_arrival_ts` / `sched_last_arrival_ts`, which in turn
feed `excess_trip_time`'s `scheduled_duration`. Currently there are
zero SFMTA `SKIPPED` rows in the backfilled data, so this hasn't
produced a wrong number yet — but `state.final_snapshot_ts` (the last
snapshot that observed the stop before it dropped out of the feed,
already captured on the `TripUpdateState` row) is available as a
proxy observation and could disambiguate the anchor "for free" when
this is picked up, the same way `observed_arrival_ts` does for
non-`SKIPPED` rows.

Acceptance: `stop_events.service_date` for owl-route trips matches the
trip's true GTFS service day (cross-checked against `calendar_dates`)
instead of the calendar day of a post-midnight observation; day_type
classification for SFMTA routes matches `calendar_dates` rather than
misattributed weekend buckets. The `SKIPPED`-fallback anchor gap is
fixed in the same pass if it's cheap once the resolver threads through
`final_snapshot_ts`, otherwise split out.

## Dependencies

None (unblocked). Gates day_type-correct SFMTA metrics (frequency
classification, weekday/weekend splits, any future SFMTA EWT
cell-hour bucketing) for owl routes specifically — not required for
NOTES-99's headline KPIs in aggregate, since those aren't day_type-split
today, but should land before any day_type-aware SFMTA metric ships.
