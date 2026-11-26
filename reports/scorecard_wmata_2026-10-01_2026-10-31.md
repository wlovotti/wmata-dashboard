# WMATA Metrobus bus scorecard, 2026-10-01 to 2026-10-31

Period: 2026-10-01 to 2026-10-31 (31 service dates). Generated 2026-11-26T04:23:13 UTC by `scripts/generate_scorecard.py`. Every figure below is in the accompanying JSON file.

Provenance: generator commit `083829d75403` (the commit checked out at generation time; it may not be reachable after a squash merge), working tree clean apart from the output directory. Source content digest: sha256 `570c203586e0f148` over 29 files (every repository module loaded during generation plus the config files read; per-file hashes in the JSON). Library versions and the databases' contents are not covered by the digest.

## Summary

| Metric | WMATA Metrobus | SFMTA (Muni) |
|---|---|---|
| On-time performance | 67.8% (median day 67.8%; 31 days) | 65.8% (median day 66.7%; 31 days) |
| Service delivered | 95.9% (median day 96.1%; 31 days) | 93.3% (median day 94.3%; 31 days) |
| Excess wait time (frequent service) | 2.88 min (173 s) (median day 2.55 min (153 s); 31 days) [1] | 3.68 min (221 s) (median day 2.52 min (151 s); 31 days) [2] |
| Bunching rate | 3.8% (median day 4.0%; 31 days) | 5.9% (median day 6.1%; 31 days) |

Figures materially moved by a few days or routes:

1. WMATA Metrobus excess wait time (frequent service): without its unusual days (2026-10-02, 2026-10-19, 2026-10-21, 2026-10-22, 2026-10-23, 2026-10-25) the mean is 2.48 min (149 s); without routes whose figures are dominated by a few days (D10 (#269), D50 (#269), M6X (not yet tracked)) it is 2.41 min (145 s).
2. SFMTA (Muni) excess wait time (frequent service): without its unusual days (2026-10-17, 2026-10-21 (#270)) the mean is 2.61 min (157 s); without routes whose figures are dominated by a few days (15 routes; route detail not shown for peers) it is 3.37 min (202 s).

Differences that are not robust:

- Excess wait time (frequent service): the difference between WMATA Metrobus and SFMTA (Muni) changes direction depending on whether the mean, the median day, or the mean without unusual days or routes is used, so it is not a finding.

Period figures are means of daily system values over dates stamped complete; the median day is shown beside each. OTP uses a -2/+7 minute window. Days with unusual values are listed under Data quality and are included in these means.
Peer columns are shallow comparison points; read the comparability caveats before comparing across agencies.

## WMATA Metrobus

### System KPIs

| Metric | Period figure (mean of days) | Median day | Lowest to highest day | Days |
|---|---|---|---|---|
| On-time performance | **67.8%** | 67.8% | 64.9% (2026-10-17) to 69.7% (2026-10-04) | 31/31 |
| Service delivered | **95.9%** | 96.1% | 94.1% (2026-10-02) to 97.1% (2026-10-12) | 31/31 |
| Excess wait time (frequent service) | **2.88 min (173 s)** | 2.55 min (153 s) | 1.91 min (115 s) (2026-10-11) to 6.45 min (387 s) (2026-10-25) | 31/31 |
| Scheduled wait time (frequent service) | **7.33 min (440 s)** | 7.13 min (428 s) | 7.13 min (428 s) (2026-10-13) to 7.75 min (465 s) (2026-10-31) | 31/31 |
| Bunching rate | **3.8%** | 4.0% | 2.5% (2026-10-11) to 4.6% (2026-10-28) | 31/31 |

### By day type (mean of daily values)

| Metric | Weekday | Saturday | Sunday |
|---|---|---|---|
| On-time performance | 67.7% (n=22) | 67.7% (n=5) | 68.6% (n=4) |
| Service delivered | 95.9% (n=22) | 96.1% (n=5) | 95.3% (n=4) |
| Excess wait time (frequent service) | 2.87 min (172 s) (n=22) | 2.52 min (151 s) (n=5) | 3.44 min (207 s) (n=4) |
| Scheduled wait time (frequent service) | 7.16 min (430 s) (n=22) | 7.75 min (465 s) (n=5) | 7.75 min (465 s) (n=4) |
| Bunching rate | 4.1% (n=22) | 3.3% (n=5) | 2.8% (n=4) |

Note: the weekday column includes holiday-type service dates: 2026-10-12 (reduced service, 78% of the scheduled trips on comparable dates). Day type follows the calendar; without these dates the weekday means are: on-time performance 67.7%, service delivered 95.9%, excess wait time (frequent service) 2.89 min (173 s), bunching rate 4.1%.

## Best and worst routes

Routes with any complete overlay data in the period: 126. Frequent designation: config/frequent_routes.yaml (43 routes).

Route figures are pooled over the period. The median-day column is the median of the route's daily values; where the two differ widely, a few days dominate the period figure.

### Frequent routes, ranked by excess wait time (lower is better)

Floor: at least 1,000 observed headways and observed/scheduled headway coverage of at least 50%. Ranked: 41; below floor: 0; designated but no data: 0.

Lowest EWT:

| # | Route | Name | Excess wait time (frequent service) (pooled) | Median day | OTP | Service delivered | Evidence |
|---|---|---|---|---|---|---|---|
| 1 | A40 | Columbia Pike-National Landing | **1.25 min (75 s)** | 1.13 min (68 s) | 82.2% | 97.7% | 146,365 headways, coverage 94% |
| 2 | D6X | 16 St Limited | **1.36 min (82 s)** | 1.38 min (83 s) | 73.7% | 96.9% | 123,729 headways, coverage 92% |
| 3 | A58 | Wilson Blvd-Farragut Sq | **1.46 min (88 s)** | 1.36 min (81 s) | 73.1% | 98.9% | 112,966 headways, coverage 95% |
| 4 | M70 | East West Hwy | **1.53 min (92 s)** | 1.38 min (83 s) | 74.4% | 97.0% | 183,788 headways, coverage 93% |
| 5 | A1X | Pentagon City-Braddock Rd | **1.59 min (96 s)** | 1.27 min (76 s) | 73.0% | 95.1% | 47,487 headways, coverage 89% |

Highest EWT:

| # | Route | Name | Excess wait time (frequent service) (pooled) | Median day | OTP | Service delivered | Evidence |
|---|---|---|---|---|---|---|---|
| 1 | C53 | U St-Congress Hts | **4.54 min (272 s)** | 2.84 min (170 s) | 60.2% | 96.1% | 316,786 headways, coverage 92% |
| 2 | D70 | Connecticut Av | **4.22 min (253 s)** | 5.09 min (305 s) | 65.8% | 97.1% | 88,103 headways, coverage 93% |
| 3 | D36 | K St-Ivy City | **3.84 min (230 s)** | 3.33 min (200 s) | 56.3% | 93.5% | 66,080 headways, coverage 89% |
| 4 | D44 | 11 St | **3.47 min (208 s)** | 3.14 min (188 s) | 61.9% | 92.0% | 83,037 headways, coverage 86% |
| 5 | D60 | 16 St | **3.20 min (192 s)** | 3.39 min (203 s) | 71.2% | 95.7% | 148,194 headways, coverage 92% |

Not ranked: pooled figure dominated by a few days. D10 Pennsylvania Av: pooled 5.90 min (354 s), median day 2.60 min (156 s) (hand-entered annotation: #269); D50 14 St: pooled 31.15 min (1869 s), median day 2.83 min (170 s) (hand-entered annotation: #269).

### Other routes, ranked by on-time performance (higher is better)

Floor: at least 1,000 stop observations and 300 delivered trips in the period. Ranked: 81; below floor: 2 (F19, F83).

Highest OTP:

| # | Route | Name | On-time performance (pooled) | Median day | OTP | Service delivered | Evidence |
|---|---|---|---|---|---|---|---|
| 1 | F44 | Columbia Pike-Pentagon | **83.7%** | 83.8% | 83.7% | 96.8% | 37,906 stop observations, 1,996 trips |
| 2 | P33 | Queens Chapel Rd | **80.9%** | 81.2% | 80.9% | 94.0% | 28,550 stop observations, 2,408 trips |
| 3 | A28 | Pentagon-Holmes Run-Landmark | **80.5%** | 79.8% | 80.5% | 95.6% | 5,304 stop observations, 502 trips |
| 4 | A76 | Carlin Springs Rd | **80.2%** | 80.9% | 80.2% | 97.8% | 32,337 stop observations, 2,001 trips |
| 5 | A27 | Pentagon-Shirlington-Van Dorn St | **79.8%** | 80.4% | 79.8% | 98.5% | 41,047 stop observations, 3,448 trips |

Lowest OTP:

| # | Route | Name | On-time performance (pooled) | Median day | OTP | Service delivered | Evidence |
|---|---|---|---|---|---|---|---|
| 1 | A90 | Pentagon-Mark Center | **44.5%** | 44.4% | 44.5% | 98.0% | 3,201 stop observations, 2,614 trips |
| 2 | A29 | Metro Ctr-Alexandria W | **56.2%** | 55.5% | 56.2% | 96.6% | 4,586 stop observations, 345 trips |
| 3 | C71 | 18 St | **58.9%** | 58.3% | 58.9% | 96.5% | 43,672 stop observations, 2,497 trips |
| 4 | P15 | Riggs Rd-Adelphi | **59.1%** | 60.2% | 59.1% | 94.1% | 9,682 stop observations, 494 trips |
| 5 | C27 | Naylor Rd-Shepherd Pkwy | **59.4%** | 60.1% | 59.4% | 97.1% | 56,773 stop observations, 3,074 trips |


## SFMTA (Muni) (comparison, headline only)

| Metric | Period figure (mean of days) | Median day | Lowest to highest day | Days |
|---|---|---|---|---|
| On-time performance | **65.8%** | 66.7% | 57.9% (2026-10-31) to 68.8% (2026-10-12) | 31/31 |
| Service delivered | **93.3%** | 94.3% | 76.1% (2026-10-21) to 96.5% (2026-10-22) | 31/31 |
| Excess wait time (frequent service) | **3.68 min (221 s)** | 2.52 min (151 s) | 1.48 min (89 s) (2026-10-12) to 32.15 min (1929 s) (2026-10-21) | 31/31 |
| Scheduled wait time (frequent service) | **5.52 min (331 s)** | 5.49 min (329 s) | 5.49 min (329 s) (2026-10-01) to 5.57 min (334 s) (2026-10-31) | 31/31 |
| Bunching rate | **5.9%** | 6.1% | 3.5% (2026-10-25) to 7.7% (2026-10-30) | 31/31 |

### By day type (mean of daily values)

| Metric | Weekday | Saturday | Sunday |
|---|---|---|---|
| On-time performance | 67.2% (n=22) | 61.9% (n=5) | 62.8% (n=4) |
| Service delivered | 93.9% (n=22) | 91.9% (n=5) | 92.1% (n=4) |
| Excess wait time (frequent service) | 3.97 min (238 s) (n=22) | 3.26 min (195 s) (n=5) | 2.59 min (156 s) (n=4) |
| Scheduled wait time (frequent service) | 5.49 min (330 s) (n=22) | 5.57 min (334 s) (n=5) | 5.57 min (334 s) (n=4) |
| Bunching rate | 6.2% (n=22) | 5.9% (n=5) | 4.4% (n=4) |

Note: the weekday column includes holiday-type service dates: 2026-10-12 (ran the usual saturday service). Day type follows the calendar; without these dates the weekday means are: on-time performance 67.2%, service delivered 93.8%, excess wait time (frequent service) 4.09 min (245 s), bunching rate 6.3%.

### Comparability caveats

- Different networks. WMATA's GTFS feed is bus-only. SFMTA's feed also carries Muni Metro light rail and cable car. OTP and service delivered pool every route in each feed (so Muni's include rail and cable car); EWT, SWT and bunching are restricted to bus routes (GTFS route_type 3) for both agencies.
- One OTP window for both. Both agencies are measured on the same -2/+7 minute window at every observed stop. Neither agency publishes OTP this way (WMATA's scorecard uses timepoints; SFMTA publishes against its own standard), so these figures do not match either agency's official reporting.
- Frequent-service scope differs. WMATA's per-route EWT gate uses WMATA's published frequent-route list (15 min for high-frequency and undesignated routes, 20 min for medium-frequency). SFMTA has no equivalent list, so every Muni route uses the 15-minute per-cell-hour gate. System EWT and SWT pool only cell-hours that pass the gate.
- Feed semantics differ. Muni real-time data comes through the 511.org regional aggregator, which has a known duplicate stop_sequence artifact affecting per-stop matching; treat Muni figures as directionally reliable rather than exact. Polling cadence and completeness thresholds differ by agency (WMATA Metrobus: trip updates every 30 s, vehicle positions every 60 s, 'complete' at coverage >= 0.800; SFMTA (Muni): trip updates every 120 s, vehicle positions every 180 s, 'complete' at coverage >= 0.533).
- No ridership weighting. Neither dataset carries passenger counts; system figures weight observations (stops, trips, headways) within a day and days equally across the period, not riders.
- Service dates are agency-local (Eastern for WMATA, Pacific for SFMTA).

## Method

**Metrics.**

- On-time performance (OTP): share of observed stop arrivals between 2 minutes early and 7 minutes late versus the GTFS schedule, at every observed stop (source: `proximity` stop events, derived from vehicle positions). Higher is better.
- Service delivered: delivered scheduled trips divided by scheduled trips (source: `runs`, against the GTFS schedule for the date). Higher is better.
- Excess wait time (EWT): actual average wait minus scheduled average wait for a rider arriving at random, where average wait = sum(h^2) / (2 sum(h)) over headways h, pooled over (direction, stop, hour) cells whose scheduled headway is at most 15 minutes (20 minutes for WMATA medium-frequency routes). Source: `trip_update` stop events. Lower is better. Scheduled wait time (SWT) is the scheduled side of the same pool.
- Bunching rate: share of observed consecutive-arrival headways shorter than max(0.25 x scheduled headway, 2 minutes) (source: `trip_update` stop events, SCHEDULED trips only). Lower is better.

**Period aggregation.** A system figure for the period is the mean of the daily system values: each eligible service date counts once, regardless of how much service ran that day. This is the same rule the dashboard API uses for multi-day system figures from the same table, applied identically to every agency. The stored daily values are finalized ratios, so there is no per-day weight to pool with at system level. Route figures are pooled: early / on-time / late counts, scheduled and delivered trips, headway sums and bunching counts are summed over the period before the ratio is taken, which is exact for these statistics. The sensitivity table below shows the pooled alternative for system figures.

**Exclusions.** A date is excluded from a metric when it has no daily system row, when its row is not stamped `complete` (ingest coverage below the agency's threshold), or when the metric is null that day. Excluded dates are listed under Data quality and never averaged in. Route figures use only dates whose system row is complete and only per-route rows that are themselves complete. A day whose value is more than 3 robust standard deviations (1.4826 x median absolute deviation) from the median of same-day-type days in the period (scale estimated from all days' deviations) is flagged as unusual for disclosure but kept in the mean. SWT, being schedule-only, is not flagged.

**Route rankings.** Routes on WMATA's frequent-service list (`config/frequent_routes.yaml`) are ranked by EWT; all other routes by OTP. A route is ranked only if it meets the floor: for EWT, at least 1,000 observed headways and observed/scheduled headway coverage of at least 50%; for OTP, at least 1,000 stop observations and 300 delivered trips. A route that clears the floor but whose pooled figure is far from its median day is listed separately, not ranked: for EWT when the pooled and median-day values differ by more than the median day itself or 60 s, whichever is larger; for OTP when they differ by more than 10 percentage points. Route EWT, pooled and daily, is clamped at 0 like the system figure (the dashboard's route view shows it unclamped). The top and bottom 5 are shown. Route names come from the current GTFS feed.

**Day types and service changes.** Day type is the calendar weekday. A date's scheduled service is the set of GTFS services active that day (from `calendar` and `calendar_dates`). If the feed in use did not cover the date (snapshot resolution fell back to a feed whose calendar excludes it, or no service is scheduled although service was observed), its schedule pattern is reported as unknown. A date running the most common service set for its weekday within 4 weeks either side is regular. Otherwise a date is holiday-type when a weekday runs the same week's Saturday or Sunday service, has no scheduled service, or has more than 10% fewer scheduled trips than comparable dates (the other weekdays of its week; for weekend days, the same weekday in nearby weeks). Holiday-type dates stay in their calendar day type and are footnoted under the day-type table, with the means recomputed without them. Remaining differences are a schedule change within the feed when the new service set persists (elsewhere in the week for the same day type, or on the same weekday a week before or after), added service, or a timetable substitution not seen on nearby dates; and undetermined when no usual set can be established (no nearby date with service, or nearby dates split between patterns the date does not match). Unknown, undetermined, schedule-change and substitution dates are listed but do not alter the split.

**Robustness notes.** For each headline figure the report also computes the mean without that metric's unusual days, and the mean of daily system values re-derived from the per-route table without routes whose pooled EWT or OTP is dominated by a few days (the ranking guard and floors, applied to every route of every agency alike). A figure that either moves by more than 5% is marked in the Summary. A cross-agency difference whose direction is not the same for the mean, the median day and those variants is called out as not a finding. Known data issues are referenced by issue number from hand-entered annotations (`config/scorecard_annotations.yaml`), labelled as such; an annotation is not checked against the data and never changes a figure.

**Sources.** Only the kept-forever daily aggregate tables are read: `system_metrics_daily` (one row per service date) and `route_metrics_daily_overlay` (per-route, per-date sufficient statistics). Stop-level rows are pruned after a retention window, so reading only these tables keeps this report reproducible for past periods. Nothing is written to either database.

**GTFS feed versions in force.** For each date, the snapshot the derivation pipeline resolves today among the retained snapshots (the newest whose calendar schedules service that date). Duplicate loads of the same feed are de-duplicated, keeping the newest, after checking that no date's resolved schedule changes; so where a date's aggregates predate the retained snapshot's load, the load actually used is gone and the retained one is reported, with a count of such dates:

- WMATA Metrobus: feed_version S1000251 (snapshot 22, loaded 2026-11-24T18:17:19 UTC) for 2026-10-01 to 2026-10-31 (31 dates); resolution: covers
- SFMTA (Muni): feed_version 20260829 (snapshot 5, loaded 2026-11-24T19:05:26 UTC) for 2026-10-01 to 2026-10-31 (31 dates); resolution: covers

- WMATA Metrobus daily aggregates computed between 2026-11-24T20:33:04 and 2026-11-25T10:20:52 UTC.
- SFMTA (Muni) daily aggregates computed between 2026-11-24T21:10:48 and 2026-11-25T10:51:30 UTC.

### Sensitivity: mean of days vs pooled over the period

**WMATA Metrobus**

| Metric | Mean of days (headline) | Pooled over period |
|---|---|---|
| On-time performance | 67.8% | 67.8% |
| Service delivered | 95.9% | 95.9% |
| Excess wait time (frequent service) | 2.88 min (173 s) | 2.88 min (173 s) |
| Scheduled wait time (frequent service) | 7.33 min (440 s) | 7.31 min (438 s) |
| Bunching rate | 3.8% | 3.9% |

Overlay reproduces the stored daily system values on 31 of 31 complete dates.

**SFMTA (Muni)**

| Metric | Mean of days (headline) | Pooled over period |
|---|---|---|
| On-time performance | 65.8% | 66.0% |
| Service delivered | 93.3% | 93.4% |
| Excess wait time (frequent service) | 3.68 min (221 s) | 3.76 min (225 s) |
| Scheduled wait time (frequent service) | 5.52 min (331 s) | 5.51 min (331 s) |
| Bunching rate | 5.9% | 6.0% |

Overlay reproduces the stored daily system values on 31 of 31 complete dates.

## Data quality

### WMATA Metrobus

Dates in period: 31; stamped complete: 31 (coverage 100%). Completeness threshold for this agency: ingest coverage >= 0.800.

- No date was excluded from any metric.
- Unusual days for On-time performance (included in the mean): 2026-10-17 (64.9%)
- Unusual days for Service delivered (included in the mean): 2026-10-02 (94.1%), 2026-10-09 (94.6%), 2026-10-11 (96.7%)
- Unusual days for Excess wait time (frequent service) (included in the mean): 2026-10-02 (4.82 min (289 s)), 2026-10-19 (3.97 min (238 s)), 2026-10-21 (4.15 min (249 s)), 2026-10-22 (3.87 min (232 s)), 2026-10-23 (4.08 min (245 s)), 2026-10-25 (6.45 min (387 s))
- Unusual days for Bunching rate (included in the mean): 2026-10-17 (4.0%)
- Dates whose scheduled service is not the usual service for that weekday, or could not be determined (day type still follows the calendar): 2026-10-12 (weekday: reduced service, 78% of the scheduled trips on comparable dates); 2026-10-13 (weekday: timetable substitution not seen on nearby dates, comparable trip count (100% of the scheduled trips on comparable dates)); 2026-10-30 (weekday: timetable substitution not seen on nearby dates, comparable trip count (100% of the scheduled trips on comparable dates))

<details><summary>Daily values</summary>

| Date | Day type | Quality | Coverage | OTP | Service delivered | EWT | SWT | Bunching |
|---|---|---|---|---|---|---|---|---|
| 2026-10-01 | weekday | complete | 100.0% | 67.8% | 94.7% | 2.55 min (153 s) | 7.13 min (428 s) | 4.5% |
| 2026-10-02 | weekday | complete | 100.0% | 67.1% | 94.1% | 4.82 min (289 s) | 7.13 min (428 s) | 4.5% |
| 2026-10-03 | saturday | complete | 100.0% | 68.3% | 95.9% | 2.32 min (139 s) | 7.75 min (465 s) | 3.0% |
| 2026-10-04 | sunday | complete | 100.0% | 69.7% | 94.7% | 2.51 min (151 s) | 7.75 min (465 s) | 2.5% |
| 2026-10-05 | weekday | complete | 100.0% | 68.2% | 96.4% | 2.82 min (169 s) | 7.13 min (428 s) | 4.0% |
| 2026-10-06 | weekday | complete | 100.0% | 67.4% | 95.1% | 2.72 min (163 s) | 7.13 min (428 s) | 4.0% |
| 2026-10-07 | weekday | complete | 100.0% | 68.4% | 96.4% | 2.24 min (134 s) | 7.13 min (428 s) | 4.0% |
| 2026-10-08 | weekday | complete | 100.0% | 67.9% | 95.5% | 2.60 min (156 s) | 7.13 min (428 s) | 4.1% |
| 2026-10-09 | weekday | complete | 100.0% | 68.7% | 94.6% | 2.55 min (153 s) | 7.13 min (428 s) | 3.9% |
| 2026-10-10 | saturday | complete | 100.0% | 69.0% | 96.7% | 2.20 min (132 s) | 7.75 min (465 s) | 2.9% |
| 2026-10-11 | sunday | complete | 100.0% | 67.7% | 96.7% | 1.91 min (115 s) | 7.75 min (465 s) | 2.5% |
| 2026-10-12 | weekday | complete | 100.0% | 66.2% | 97.1% | 2.38 min (143 s) | 7.75 min (465 s) | 3.6% |
| 2026-10-13 | weekday | complete | 100.0% | 68.1% | 96.9% | 2.08 min (125 s) | 7.13 min (428 s) | 4.0% |
| 2026-10-14 | weekday | complete | 100.0% | 68.1% | 95.8% | 3.67 min (220 s) | 7.13 min (428 s) | 4.3% |
| 2026-10-15 | weekday | complete | 100.0% | 67.3% | 96.3% | 2.41 min (144 s) | 7.13 min (428 s) | 4.3% |
| 2026-10-16 | weekday | complete | 100.0% | 66.0% | 96.3% | 2.42 min (145 s) | 7.13 min (428 s) | 4.1% |
| 2026-10-17 | saturday | complete | 100.0% | 64.9% | 95.6% | 3.03 min (182 s) | 7.75 min (465 s) | 4.0% |
| 2026-10-18 | sunday | complete | 100.0% | 68.6% | 94.6% | 2.89 min (173 s) | 7.75 min (465 s) | 3.1% |
| 2026-10-19 | weekday | complete | 100.0% | 68.0% | 96.2% | 3.97 min (238 s) | 7.13 min (428 s) | 3.9% |
| 2026-10-20 | weekday | complete | 100.0% | 67.5% | 95.7% | 2.22 min (133 s) | 7.13 min (428 s) | 4.1% |
| 2026-10-21 | weekday | complete | 100.0% | 67.7% | 96.1% | 4.15 min (249 s) | 7.13 min (428 s) | 4.1% |
| 2026-10-22 | weekday | complete | 100.0% | 67.8% | 95.8% | 3.87 min (232 s) | 7.13 min (428 s) | 4.2% |
| 2026-10-23 | weekday | complete | 100.0% | 67.3% | 96.2% | 4.08 min (245 s) | 7.13 min (428 s) | 4.4% |
| 2026-10-24 | saturday | complete | 99.5% | 69.3% | 97.0% | 2.40 min (144 s) | 7.75 min (465 s) | 3.1% |
| 2026-10-25 | sunday | complete | 100.0% | 68.3% | 95.0% | 6.45 min (387 s) | 7.75 min (465 s) | 3.1% |
| 2026-10-26 | weekday | complete | 100.0% | 68.4% | 96.2% | 2.20 min (132 s) | 7.13 min (428 s) | 3.8% |
| 2026-10-27 | weekday | complete | 100.0% | 68.1% | 96.2% | 2.08 min (125 s) | 7.13 min (428 s) | 4.1% |
| 2026-10-28 | weekday | complete | 100.0% | 67.3% | 96.5% | 2.56 min (154 s) | 7.13 min (428 s) | 4.6% |
| 2026-10-29 | weekday | complete | 100.0% | 67.3% | 96.1% | 2.40 min (144 s) | 7.13 min (428 s) | 4.4% |
| 2026-10-30 | weekday | complete | 100.0% | 67.7% | 95.7% | 2.27 min (136 s) | 7.13 min (428 s) | 3.7% |
| 2026-10-31 | saturday | complete | 100.0% | 66.7% | 95.3% | 2.67 min (160 s) | 7.75 min (465 s) | 3.3% |

</details>

### SFMTA (Muni)

Dates in period: 31; stamped complete: 31 (coverage 100%). Completeness threshold for this agency: ingest coverage >= 0.533.

- No date was excluded from any metric.
- Unusual days for Service delivered (included in the mean): 2026-10-21 (76.1%; #270), 2026-10-30 (91.3%)
- Unusual days for Excess wait time (frequent service) (included in the mean): 2026-10-17 (6.18 min (371 s)), 2026-10-21 (32.15 min (1929 s); #270)
- Unusual days for Bunching rate (included in the mean): 2026-10-12 (4.2%)
- Dates whose scheduled service is not the usual service for that weekday, or could not be determined (day type still follows the calendar): 2026-10-12 (weekday: ran the usual saturday service)
- Known issue for 2026-10-21 (hand-entered annotation): stamped complete (coverage 0.564) despite outage-like metrics (#270).

<details><summary>Daily values</summary>

| Date | Day type | Quality | Coverage | OTP | Service delivered | EWT | SWT | Bunching |
|---|---|---|---|---|---|---|---|---|
| 2026-10-01 | weekday | complete | 66.0% | 67.7% | 95.3% | 2.61 min (156 s) | 5.49 min (329 s) | 6.7% |
| 2026-10-02 | weekday | complete | 65.6% | 66.3% | 95.0% | 2.52 min (151 s) | 5.49 min (329 s) | 7.3% |
| 2026-10-03 | saturday | complete | 66.4% | 63.9% | 93.3% | 2.52 min (151 s) | 5.57 min (334 s) | 5.9% |
| 2026-10-04 | sunday | complete | 66.5% | 64.5% | 91.7% | 3.70 min (222 s) | 5.57 min (334 s) | 4.2% |
| 2026-10-05 | weekday | complete | 65.6% | 67.6% | 95.2% | 3.27 min (196 s) | 5.49 min (329 s) | 5.7% |
| 2026-10-06 | weekday | complete | 65.8% | 66.1% | 94.8% | 2.89 min (173 s) | 5.49 min (329 s) | 6.7% |
| 2026-10-07 | weekday | complete | 63.8% | 66.7% | 95.5% | 2.19 min (132 s) | 5.49 min (329 s) | 6.4% |
| 2026-10-08 | weekday | complete | 62.8% | 66.5% | 95.8% | 3.57 min (214 s) | 5.49 min (329 s) | 6.5% |
| 2026-10-09 | weekday | complete | 66.5% | 65.3% | 94.9% | 3.62 min (217 s) | 5.49 min (329 s) | 7.0% |
| 2026-10-10 | saturday | complete | 66.6% | 60.1% | 92.1% | 2.27 min (136 s) | 5.57 min (334 s) | 6.6% |
| 2026-10-11 | sunday | complete | 66.6% | 59.3% | 89.5% | 2.39 min (143 s) | 5.57 min (334 s) | 5.7% |
| 2026-10-12 | weekday | complete | 66.6% | 68.8% | 95.6% | 1.48 min (89 s) | 5.57 min (334 s) | 4.2% |
| 2026-10-13 | weekday | complete | 66.4% | 66.6% | 94.2% | 2.72 min (163 s) | 5.49 min (329 s) | 6.9% |
| 2026-10-14 | weekday | complete | 66.7% | 68.7% | 95.2% | 2.74 min (164 s) | 5.49 min (329 s) | 6.2% |
| 2026-10-15 | weekday | complete | 66.2% | 67.9% | 94.3% | 2.96 min (178 s) | 5.49 min (329 s) | 6.7% |
| 2026-10-16 | weekday | complete | 66.2% | 67.0% | 93.7% | 2.53 min (152 s) | 5.49 min (329 s) | 6.7% |
| 2026-10-17 | saturday | complete | 66.0% | 62.0% | 91.7% | 6.18 min (371 s) | 5.57 min (334 s) | 6.5% |
| 2026-10-18 | sunday | complete | 66.4% | 65.3% | 94.1% | 2.44 min (146 s) | 5.57 min (334 s) | 4.2% |
| 2026-10-19 | weekday | complete | 66.2% | 66.7% | 95.2% | 2.36 min (142 s) | 5.49 min (329 s) | 5.3% |
| 2026-10-20 | weekday | complete | 65.3% | 68.4% | 93.6% | 2.04 min (123 s) | 5.49 min (329 s) | 5.5% |
| 2026-10-21 | weekday | complete | 56.4% | 67.3% | 76.1% | 32.15 min (1929 s) | 5.49 min (329 s) | 4.7% |
| 2026-10-22 | weekday | complete | 64.4% | 67.4% | 96.5% | 2.51 min (151 s) | 5.49 min (329 s) | 5.7% |
| 2026-10-23 | weekday | complete | 66.9% | 67.4% | 94.7% | 2.51 min (150 s) | 5.49 min (329 s) | 6.7% |
| 2026-10-24 | saturday | complete | 66.9% | 65.5% | 93.0% | 1.72 min (103 s) | 5.57 min (334 s) | 5.0% |
| 2026-10-25 | sunday | complete | 66.8% | 62.1% | 93.2% | 1.85 min (111 s) | 5.57 min (334 s) | 3.5% |
| 2026-10-26 | weekday | complete | 67.4% | 67.7% | 93.6% | 2.10 min (126 s) | 5.49 min (329 s) | 5.6% |
| 2026-10-27 | weekday | complete | 62.6% | 67.7% | 95.7% | 3.08 min (185 s) | 5.49 min (329 s) | 6.1% |
| 2026-10-28 | weekday | complete | 63.7% | 68.6% | 94.5% | 2.33 min (140 s) | 5.49 min (329 s) | 6.0% |
| 2026-10-29 | weekday | complete | 64.0% | 67.8% | 95.5% | 2.34 min (141 s) | 5.49 min (329 s) | 6.5% |
| 2026-10-30 | weekday | complete | 63.8% | 65.2% | 91.3% | 2.81 min (168 s) | 5.49 min (329 s) | 7.7% |
| 2026-10-31 | saturday | complete | 65.1% | 57.9% | 89.3% | 3.58 min (215 s) | 5.57 min (334 s) | 5.5% |

</details>
