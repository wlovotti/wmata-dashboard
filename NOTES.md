# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-06 (closed NOTES-24 — GTFS snapshot freshness surfaced in dashboard footer via `/api/gtfs/freshness`).

---

## Active priorities

The bulk of open work is a metrics redesign anchored on materialized
**stop events** as the foundational unit, replacing the daily-batch
recomputation from raw positions. The `stop_events` table is in place
(PRs #42, #43, #44), with two derivation paths (proximity + trip_update)
and a comparison harness confirming the two sources agree to within a
few seconds for 93% of events. The `runs` aggregation over `stop_events`
landed in PR #45, and the OTP origin/destination split (`src/otp_metrics.py`)
landed in PR #46. Downstream metrics build on that foundation —
sequencing still matters.

### P4 — Surface to API + UI

- **NOTES-18 Update grading rubric.** Currently OTP-only; should
  incorporate service-delivered and EWT now that both have shipped.

### P5 — Cleanup

- **NOTES-19 Drop `route_metrics_daily` and `route_metrics_summary`.**
  Once the new metrics fully replace them. Coexist for now to avoid UI
  breakage during the transition.
- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.

### Independent of the redesign

- **NOTES-29 Replace `datetime.utcnow()` with timezone-aware UTC.**
  Deprecated in Python 3.12; emits a DeprecationWarning on every call
  (visible in the GTFS reload log). ~50 call sites across `src/models.py`
  (Column defaults), pipelines, scripts, API, and tests. The naive-UTC
  storage convention complicates the migration — needs a small helper
  rather than a blind sed.
- **NOTES-34 service_delivered ceiling on 2-stop routes (TU structural
  exclusion).** Side effect of the NOTES-30 closing PR (proportional
  threshold). The new threshold is `max(2, stops_observable // 3)`; on a
  2-stop route, TU rows have `stops_observable = 1` and can never reach
  2, so TU never counts toward delivered. Proximity rows
  (`stops_observable = 2`) cover the gap when they observe both stops,
  but A90 weekday on 2026-05-05 came out at 61/127 delivered (48%)
  despite 88% OTP — the residual gap is partly proximity rows that only
  saw one stop and partly TU runs for trips with no proximity coverage.
  Acceptable trade-off for closing the 0%-everywhere bug, but documents
  a known ceiling on short-route delivered ratios. A more permissive
  short-route rule — e.g. accept `stops_observed >= 1` when
  `stops_observable <= 2`, or treat any observation at all as
  delivered once `stops_observable` is small enough — would lift the
  ceiling at the cost of admitting more single-ping ghost runs. Not
  urgent; revisit if a second short express route appears and the
  ~50% ceiling becomes a problem.
---

## NOTES-18. Grading rubric refresh

**Severity: low.**

Current grade (A–F) is OTP-only, computed in `api/aggregations.py`.
With service-delivered (PR #47) and EWT (PR #52) both shipped and now
surfaced through the UI, the rubric should incorporate both —
service-delivered especially, since that's the most rider-felt failure
mode. Worth a separate decision conversation about weighting before
implementing.

---

## NOTES-19. Drop `route_metrics_daily` / `route_metrics_summary`

**Severity: low (cleanup, after the new metrics fully replace them).**

Both tables and the daily batch pipeline that populates them
(`pipelines/compute_daily_metrics.py`) become dead code once the new
stop_events-based pipeline covers all current API consumers. Coexist
for now to avoid UI breakage during the transition. With NOTES-17
closed, the only remaining `route_metrics_summary` consumers are the
legacy scorecard fields (avg_headway_minutes, avg_speed_mph,
total_observations) and the OTP-only grade — track as one final cleanup
PR once those move to the new path.

---

## NOTES-20. Tighter rider-experience OTP

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. Future option: expose a stricter
"rider-experience OTP" alongside the official one (e.g., −60s / +180s)
for non-frequent routes (frequent routes get EWT instead — see `src/ewt.py`).
The constants live in `src/otp_constants.py`, so this is a one-line
change — could even be a query-parameter toggle on the API.

---

## NOTES-29. Replace `datetime.utcnow()` with timezone-aware UTC

**Severity: low — tooling hygiene. Deprecated since Python 3.12; not yet
scheduled for removal but emits a DeprecationWarning on every call.**

### Where it lives

Surfaced visibly in the GTFS reload log (`scripts/reload_gtfs_complete.py:115`),
but the issue is broader. Repo-wide grep on 2026-05-05:

| File group | Approx. count |
|---|---|
| `src/models.py` (Column defaults) | ~25 |
| `pipelines/*.py` (`derived_at`, `computed_at`, retention cutoffs) | ~7 |
| `tests/*.py` (fixtures + test data) | ~12 |
| `scripts/*.py` | ~3 |
| `api/*.py` | ~3 |
| `src/wmata_collector.py` | ~2 |

~50 call sites total.

### Why it's not a blind sed

The repo convention (CLAUDE.md): "Datetime storage is naive UTC. Every
`DateTime` column in the DB holds UTC." `datetime.now(UTC)` returns a
**timezone-aware** datetime. Substituting it directly into
`Column(default=datetime.now)` would change the storage shape — SQLAlchemy
would either reject it (if the column doesn't accept tz-aware) or accept
it as tz-aware, breaking the convention everywhere downstream code expects
naive UTC.

### Implementation

1. Add a small helper in `src/timezones.py` (already the canonical
   datetime module):
   ```python
   def utcnow_naive() -> datetime:
       """Return current UTC time as a naive datetime (matches DB storage convention)."""
       return datetime.now(UTC).replace(tzinfo=None)
   ```
2. Replace every `datetime.utcnow()` and `datetime.utcnow` (the bare
   reference used in `Column(default=...)`) with `utcnow_naive` /
   `utcnow_naive()`. Most are mechanical; the SQLAlchemy `default=`
   calls take a callable, so pass `utcnow_naive` not `utcnow_naive()`.
3. Run `uv run pytest` and the lint suite — the migration shouldn't
   change behavior, only suppress the deprecation warning.

### Dependencies

- Independent of every other open NOTES item.

