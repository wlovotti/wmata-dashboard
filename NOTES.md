# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-06 (PRs adding NOTES-30 and NOTES-31 — service_delivered short-route bug + trip_update origin-miss bias).

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

- **NOTES-24 Surface GTFS snapshot freshness in the dashboard.**
  Show the newest `gtfs_snapshots.snapshot_date` somewhere visible
  (footer on RouteList?) so a stale schedule is observable instead
  of silent.
- **NOTES-25 Add `tests/` to the lint scope.** CI lints
  `src/ scripts/ api/ pipelines/` only — `tests/` is omitted from
  the path list (not from `[tool.ruff]` config), so test code drifts.
  Small one-off: `ruff check tests/ --fix` clears the existing
  violations, then add `tests/` to both lint args in
  `.github/workflows/test.yml` and the CLAUDE.md command.
- **NOTES-29 Replace `datetime.utcnow()` with timezone-aware UTC.**
  Deprecated in Python 3.12; emits a DeprecationWarning on every call
  (visible in the GTFS reload log). ~50 call sites across `src/models.py`
  (Column defaults), pipelines, scripts, API, and tests. The naive-UTC
  storage convention complicates the migration — needs a small helper
  rather than a blind sed.
- **NOTES-30 service_delivered always 0% on short routes.** A90 reports
  0/127 delivered on every service date despite 239 runs and 88% OTP —
  the `stops_observed >= 3` filter in `compute_service_delivered` is
  structurally unreachable on a route whose GTFS trips have ≤3 stops.
  Currently only A90 is affected, but any future short express hits the
  same bug.
- **NOTES-31 trip_update source structurally never observes origin.**
  GTFS-RT TripUpdates only publish refining predictions for *upcoming*
  stops; by the time a trip first appears in the stream, the origin has
  already been passed and never gets a finalized arrival_ts. On
  2026-05-05, 12,435/12,435 trip_update runs missed the origin (gap=0
  literally never occurs); the median gap is exactly 1. Persist a
  per-source `stops_observable` so completeness filters, including the
  `service_delivered` numerator, compare against an honest denominator.

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

## NOTES-24. Surface GTFS snapshot freshness in the dashboard

**Severity: low — observability.**

Display the most recent `gtfs_snapshots.snapshot_date` somewhere
visible in the UI (footer on RouteList?) so a stale schedule is
observable instead of silent. Useful even after NOTES-23 schedules
the reload — gives a "last refreshed" sanity check to anyone
viewing the dashboard, and is the first place to look when metrics
start looking off. Pure read; thin API addition.

### Dependencies

- Independent of NOTES-14 through NOTES-21 and NOTES-23.

---

## NOTES-25. Add `tests/` to the lint scope

**Severity: low — tooling hygiene.**

`.github/workflows/test.yml` and the CLAUDE.md commands lint
`src/ scripts/ api/ pipelines/` only. The `tests/` directory is
omitted from the path list — not from `[tool.ruff]` in
`pyproject.toml`, which has no per-directory exclusion — so test
code drifts. Probed 2026-05-04: 7 pre-existing violations
(unused imports, deprecated `typing.Generator`, unsorted
imports), all auto-fixable.

### Implementation

1. `uv run ruff check tests/ --fix && uv run ruff format tests/`
   to clear existing violations.
2. Add `tests/` to both lint args in `.github/workflows/test.yml`
   (the `ruff check` step and the `ruff format --check` step).
3. Update the CLAUDE.md `ruff check` command to include `tests/`.

### Dependencies

- Independent of every other open NOTES item.

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
- Pairs naturally with NOTES-25 (lint scope) — fixing tests/ first
  surfaces any test usages that the broader CI doesn't currently catch.

---

## NOTES-30. service_delivered always 0% on short routes

**Severity: medium — silently wrong public-facing metric.**

A90 (Pentagon–Mark Center, express) shows `service_delivered_ratio = 0.0`
(0 of 127 trips delivered) on every service date, even though the
collector observes the route normally — 239 runs, 122 distinct trip_ids,
4,520 positions, 88.1% OTP, EWT 86.59s on 2026-05-05 alone. The buses
clearly ran. `compute_service_delivered` says they didn't.

### Root cause

`src/service_delivered.py` filters the numerator by
`Run.stops_observed >= 3`, inheriting the "RUN_EXISTED" threshold from
the Run model docstring. A90 GTFS trips have 2 or 3 scheduled stops —
endpoints plus at most one intermediate timepoint. Observed stop counts
on 2026-05-05 distribute as 146 runs at 1, 93 runs at 2, and 0 at ≥3.
The threshold is structurally unreachable.

For routes whose longest trip has fewer than 3 stops, the filter is
mathematically guaranteed false. For routes with 3-stop trips, observation
rarely hits all three since stop-event derivation depends on collector
cadence vs. inter-stop spacing. The constant was tuned for typical
30-50-stop urban routes.

A90 is the only WMATA route currently affected (verified on 2026-05-05
across all routes with runs that day). The bug is structural, though:
any future short express (A91/A92/A93/A94 if reactivated, shuttle
patterns) will exhibit the same.

### Possible fixes (decide before implementing)

1. **Threshold proportional to trip length**, e.g.
   `stops_observed >= max(2, ceil(stops_scheduled / 3))`. Preserves
   spurious-run rejection on long routes while admitting valid short
   ones. `Run.stops_scheduled` is already populated per-row.
2. **Fixed lower bound at 2.** Simpler; risks more false positives on
   routes where 1-stop "ghost" runs are common.
3. **Replace the existence rule** — e.g. require any matching
   `vehicle_positions` row paired with at least one `stop_event`. More
   robust but a larger refactor.

Probably (1): local to `service_delivered.py`, doesn't change RUN_EXISTED
semantics elsewhere, and `ceil(n/3)` is a defensible heuristic.

### Dependencies

- Independent of every other open NOTES item.
- Blast radius is limited to `service_delivered`. OTP / EWT / bunching
  do not use `stops_observed >= 3`.
- Pairs naturally with NOTES-31: if `stops_observable` lands first, the
  threshold here can be expressed against it (e.g.
  `stops_observed >= max(2, stops_observable // 3)`) instead of a flat
  constant.

---

## NOTES-31. trip_update source structurally never observes origin

**Severity: medium — 1-stop bias on every trip_update run, propagates
into completeness filters and any UI surfacing observed/scheduled.**

`runs.stops_observed` runs ~1 short of `stops_scheduled` on every single
trip_update row. The cause is feed semantics, not a derivation bug.

### Evidence (2026-05-05, all routes)

| Source | Runs | Coverage | gap=0 | gap=1 | gap≥2 |
|---|---|---|---|---|---|
| trip_update | 12,435 | 94.9% | **0** | 11,266 | 1,169 |
| proximity   | 12,346 | 46.5% | 4 | 96 | 12,246 |

Of the 12,435 trip_update runs, **12,271 (98.7%)** have the exact pattern
`first_obs_seq > sched_first_seq AND last_obs_seq = sched_last_seq` —
destination observed, origin missed, no inner gaps. **Zero** runs have
both endpoints observed. The bias is invariant to route length: gap-1
share is 100% in the <5-stop bucket, 98.5% in 30-49, 90.0% in 50+.

### Why TripUpdate cannot see the origin

`pipelines/derive_stop_events_trip_updates.py` records, for each
(trip, stop_sequence) pair, the LAST `predicted_arrival_ts` published by
the feed before the row drops out — that's the system's final estimate
of actual arrival. The origin's row never appears in any snapshot we see:
TripUpdates publishes only *upcoming* stops, and a trip doesn't enter the
TU stream until the bus is en route. The origin has already been passed
by the time we receive snapshot #1 for the trip.

This is GTFS-RT spec behavior, not WMATA-specific, and not recoverable
from the feed alone.

### What we *could* do for origin times (outside this NOTES item)

| Approach | What it gives us | Cost |
|---|---|---|
| VehiclePosition match near origin coordinates | Approximate origin departure — proximity already does it when it sees the trip near the stop, 100% origin coverage among observed | proximity overall coverage is only 46.5%; usually we miss the bus during the 60s polling window |
| First-appearance-in-TU timestamp as proxy | A bound: "origin departed before HH:MM:SS" | Not an arrival time; semantically different signal |
| Cross-reference VP first observation for the trip | Earliest known position; ≥ origin-departure | Approximation, not finalized arrival |

None of these is the same data point as the recorded `observed_arrival_ts`
elsewhere in `stop_events`. Reasonable to defer all of them and treat the
1-stop loss as structural.

### Proposed fix: per-source `stops_observable`

Add `stops_observable` to `Run` (or compute in `aggregate_runs.py` as a
derived field, then persist):

```
stops_observable = stops_scheduled - 1          # source = 'trip_update'
stops_observable = stops_scheduled              # source = 'proximity'
```

Then every consumer of `stops_observed / stops_scheduled` switches to
`stops_observed / stops_observable`, and "is this run complete?" becomes
`stops_observed >= stops_observable` (or a fraction of it). This:

- Makes a missing-origin TU run register as complete instead of perpetually
  one short.
- Makes inner-gap miss observations actually visible (right now they hide
  behind the constant origin miss).
- Keeps proximity semantics unchanged — proximity *does* see the origin
  when it sees the trip at all, so its denominator stays at
  `stops_scheduled`.

### Implementation

1. Add `stops_observable` column to `runs` (Alembic-style or rebuild script;
   default expression at write time in `aggregate_run_rows`).
2. Backfill via re-aggregation or `UPDATE runs SET stops_observable =
   stops_scheduled - CASE WHEN source='trip_update' THEN 1 ELSE 0 END`.
3. Adjust `compute_service_delivered` (NOTES-30) and any other
   completeness filter to use `stops_observable`.
4. Surface `stops_observable` in the API where `stops_scheduled` currently
   appears for run-level UI fields.

### Dependencies

- Pairs with NOTES-30. Doing NOTES-31 first lets NOTES-30 reuse
  `stops_observable` instead of inventing a separate fix.
- Independent of the metrics-redesign Phase E sequence (UI-side).

