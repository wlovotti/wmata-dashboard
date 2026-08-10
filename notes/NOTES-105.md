# NOTES-105. SFMTA owl routes: ~−24h deviation cluster poisons OTP / deviations

**Severity: high** *(SFMTA-only; ~14% of proximity rows on owl routes carry ≈−24h deviations — SFMTA OTP and any deviation-based number are untrustworthy until fixed)*.
**Effort: medium** *(root cause not yet localized; fix is likely small but needs the investigation first)*.

Found by the PR #189 runbook's parity spot-validation on the first full
SFMTA backfill (2026-08-09, service dates 7/22–8/8). Muni's 24-hour owl
routes (14, 22, 24, 38, 44, 48 — and likely 5, 90, 91) show mean
proximity-source `deviation_sec` of −1.5 to −3.4 **hours**, driven by a
clean trimodal histogram: e.g. route 14 over 7/23–8/8 has 5,337 of
36,885 rows clustered at **−89,077 to −80,032 sec (−22 to −24.7 h)**
while the rest are normal (11,915 rows 0 to −82 min early, 19,633 rows
0 to +2 h late). Non-owl routes (1, 49, 29, 38R, 19, 30…) are sane
(means −113 to −739 sec), and the system-wide early-arrival rate
(43.5%) matches WMATA's ~40% baseline — the pipeline is healthy except
for this one class.

Shape of the bug: an observed arrival is being paired with the same
trip's schedule copy from the **adjacent service date** — Muni's owl
network schedules through the night with GTFS times ≥ 24:00, a regime
WMATA (service ends ~03:00, no 24h routes) never exercises at scale.
Candidate suspects, in rough order: service-date attribution of the
VP/trip match in `pipelines/derive_stop_events.py` for trips whose
scheduled times exceed 24:00; `resolve_stop_time` /
`parse_gtfs_time_to_dt` pairing in `pipelines/stop_events_common.py`
picking the wrong day's schedule instant; trip_id-based matching
(`src/trip_matching.py`) when the same trip_id is active on two
consecutive service dates simultaneously (owl trips straddle the
boundary). Check whether the trip_update source has the analogous
defect before scoping the fix to proximity only.

Acceptance: owl-route deviation histograms are unimodal-ish with no
−20h..−26h cluster; owl-route mean deviations land in the same
few-minute band as non-owl routes. After the fix, re-derive all SFMTA
dates (`derive_stop_events*` onward — same chain as the PR #189
runbook step 3) before publishing any SFMTA OTP number on NOTES-99's
page. WMATA output should be byte-identical before/after (no 24h
routes) — verify on one date as a regression check.

## Dependencies

None (unblocked). Gates trustworthy SFMTA OTP for NOTES-99.
