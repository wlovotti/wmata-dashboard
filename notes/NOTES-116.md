# NOTES-116. SFMTA route_service_profile has no weekday rows

**Severity: low-medium** *(anything consuming `route_service_profile`
for SFMTA silently sees weekend-only data — 2,020 rows, day_types
`saturday`/`sunday` only, as of 2026-08-11)*.
**Effort: low-medium** *(unknown — possibly just a profile re-run if
the PR #194 calendar_dates fix already covers classification; a
classifier change in `src/service_profile.py` if not)*.

Muni's current GTFS defines weekday service entirely through
`calendar_dates` added-service exceptions: the `calendar` table has
just 2 rows (both weekend patterns, all weekday columns 0), and each
weekday's service_id is added via `exception_type=1` rows (29 such
dates spanning 20260723–20260828, exactly one service_id per date).
The `route_service_profile` builder classifies day_type from the
calendar day-of-week booleans, so all SFMTA weekday service is
invisible to it — the profile holds only `saturday` and `sunday`
day_types while the raw GTFS clearly contains weekday trips (9,456
trips resolved for Thursday 2026-08-06 via calendar_dates).

Known unknown to resolve first: the calendar_dates resolution fix from
the NOTES-107 closure (PR #194) may already handle this at
classification time — check whether simply re-running the profile
builder against the current SFMTA snapshot produces weekday rows. If
it does, this is a re-run plus a regression test; if not, the builder
needs to derive day_type from the actual service dates a service_id is
active on (calendar ∪ calendar_dates), not from the calendar booleans
alone. Either way, add a test covering a calendar_dates-only weekday
feed, since WMATA's feed shape never exercises this path.

Acceptance: SFMTA `route_service_profile` contains weekday rows whose
per-route daytime `scheduled_trips` totals are consistent with raw
GTFS resolution for a sample weekday, and a regression test guards the
calendar_dates-only shape.
