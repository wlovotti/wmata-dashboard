# WMATA vs SFMTA (Muni) Bus Comparison — Design Spec

**Date:** 2026-07-21
**Status:** Approved design, pre-implementation
**Decision context:** The WMATA-only service-health post reads as descriptive
rather than argumentative. The project's next arc is a peer comparison —
"riding Muni feels better than Metrobus; is that real?" — collected and
computed with our own instrumentation so both systems are measured by
identical definitions. Secondary goal: every WMATA-hardcoded assumption this
surfaces becomes a requirement for the NOTES-95 stateless-collector rewrite
(the multi-agency engine).

## Approach (chosen: A — sidecar instance)

Two engines, one codebase. A second Postgres database (`sfmta_dashboard`)
with the identical schema, fed by a small 511.org poller on the Lightsail VM,
derived by the existing pipeline chain parameterized with an `--agency` flag.
No schema migration, no contact with production WMATA tables. Rejected for
now: (B) first-classing `agency_id` through the schema — right end state,
but weeks of migration before any Muni data lands; it becomes the NOTES-95
rewrite informed by this project's friction log. (C) using SFMTA's published
metrics — incomparable methodology; retained only as an external sanity
check.

## 1. Data acquisition — Muni sidecar collector

- **Source:** 511.org open data API (MTC's regional clearinghouse; the only
  first-party GTFS-RT source for Muni — SFMTA itself publishes only static
  GTFS; NextBus/UmoIQ is retired). Whole-agency GTFS-RT protobuf feeds:
  `tripupdates?agency=SF`, `vehiclepositions?agency=SF`. Free API key.
- **Rate cap:** 511 default is 60 requests/rolling hour per token (429
  beyond). Cadence budget:

  | Feed | Cadence | Req/hr |
  |---|---|---|
  | TripUpdates | 120 s | 30 |
  | VehiclePositions | 180 s | 20 |
  | **Total** | | **50 (10/hr slack for retries, GTFS download, debugging)** |

  TU gets the tighter cadence because all headline metrics (EWT, headways)
  derive from it; VP is supporting data. Cadence is config, not code — email
  511 for a rate increase on day one; if granted, tighten in config.
- **Process:** new `scripts/sfmta_collector.py` on the VM as
  `sfmta-collector.service`, mirroring the WMATA combined collector: archive
  raw TU snapshots to per-date `jsonl.zst`, write VP rows + heartbeats to the
  VM's `sfmta_dashboard`. Ops hygiene from prior incidents: explicit
  SIGINT/SIGTERM handlers (PR #129 lesson), `PYTHONUNBUFFERED=1` in the unit
  file, its own healthchecks.io dead-man check. Deploy PR cites
  DEPLOY.md §2 for unit installation.
- **Day-1 validation spike:** before trusting anything, confirm Muni RT
  `trip_id`s match 511 static GTFS `trip_id`s (the trip-matching fast path
  assumption, ~90% of WMATA matches). If they don't, scope fallback matching
  before proceeding.
- **Storage:** ~160 MB/day (~130 raw TU + ~30 VP) ≈ 2.5 GB for the window.
  No VM disk risk.

## 2. GTFS static + agency config

- **Static GTFS:** one-time download from 511 (operator `SF`), loaded into
  `sfmta_dashboard` via the existing `reload_gtfs_complete.py` versioned
  `is_current` flow. GTFS-Plus extras (`timepoints`/`timepoint_times`) are
  WMATA-specific and stay empty. Load all Muni modes; filter to bus at the
  analysis layer (`route_type` 3 incl. trolleybus; exclude cable car and
  Muni Metro LRV) so the mode decision is visible in comparison queries.
- **Agency config:** `config/agencies/sfmta.yaml` + `wmata.yaml` (capturing
  today's implicit defaults): 511/WMATA operator ID, API base + key env-var
  name, feed cadences, database env-var name, timezone. Collector and
  pipelines take `--agency <name>`. The yaml is the seed of the multi-agency
  engine; anything inexpressible in it is a friction-log entry.
- **Timezone (the one real code touch):** `src/timezones.py` hardcodes
  Eastern. Helpers gain a timezone parameter defaulting to Eastern (existing
  WMATA call sites untouched); sfmta config supplies `America/Los_Angeles`.
  Muni service dates computed in Pacific or day-level metrics skew.
  Friction-log entry #1.
- **Symmetric "frequent" definition:** ignore both agencies'
  self-designations (Better Bus / Muni Rapid marketing lists). Both cities
  use the same data-driven gate: scheduled headway ≤ 15 min during measured
  cell-hours (the gate `src/ewt.py` already applies internally).

## 3. Pipeline reuse & cadence-matched decimation

- **Derivation chain unchanged:** `replay_archive_to_state.py` →
  `derive_stop_events*` → `aggregate_runs` → `compute_bunching` →
  `run_daily_batch.py`, gaining `--agency` (resolves DB URL + config).
  `upsert_rows`, heartbeat coverage gating, and `is_current` filtering work
  as-is because the sidecar writes the same tables.
- **Ops topology mirrors Path 2a:** VM = collection + staging (raw archive
  on disk, VP + heartbeats in VM `sfmta_dashboard`); laptop = system of
  record + derivation. Agency-aware sibling/parameter for
  `bin/pull-and-derive.sh` rsyncs the Muni archive and pulls the VP delta
  over the existing tunnel into a local `sfmta_dashboard`. Comparison
  notebook queries two local DBs via two connections.
- **Source discipline:** Muni EWT/headways from `trip_update`-source stop
  events (same as WMATA). Muni proximity/OTP is second-class at VP 180 s —
  include only if match rate holds, caveated. TU origin-blindness (NOTES-31)
  is symmetric across both systems.
- **Decimation experiment:** disposable laptop-only `wmata_decimated` DB;
  replay WMATA TU archive keeping every 4th snapshot (30 s → 120 s
  effective, matching Muni), run the identical chain. Two runs: (a)
  *sensitivity check, runnable now* on any existing 1–2 week archive window
  — 30 s vs 120 s metric deltas (also the empirical answer to "could WMATA
  poll less and cut the ~920 MB/day archive?"); (b) *comparison input,
  later* — the matched calendar window replayed at 120 s to produce the
  WMATA numbers the Muni comparison actually uses.
  Production WMATA data and cadence are never touched; WMATA VP stays at
  60 s regardless (proximity/OTP genuinely degrades with coarser VP).
- **Cadence constants audit:** any replay/derivation thresholds tuned to
  30 s snapshots (gap/staleness cutoffs) get promoted to agency config.
  Friction-log entry #2.
- **Backup posture:** VM disk + laptop rsync copy (two copies) suffices for
  a 2-week experiment; no S3 retention wiring unless collection becomes
  permanent.

## 4. Analysis & deliverable

- **Matched calendar window:** two clean weeks, identical dates in both
  cities, starting after Muni collector teething. Week-1 peek for an early
  read; week 2 is the replicate + slack. Matched dates control seasonality
  and holidays better than longer unmatched windows.
- **Comparisons (identical code + definitions both sides):**
  1. *Scheduled service* (static GTFS, day one): headway distributions,
     span, share of stops with ≤15-min scheduled service.
  2. *EWT — headline* (TU-derived; WMATA side decimated): per-route
     distributions on frequent corridors, overlaid dot-histograms reusing
     the validated figure system. Distributions, not just system means.
  3. *Delivered vs scheduled* service, headway regularity; *bunching* only
     at matched cadence, caveated.
  4. *Weighting caveat stated plainly:* no symmetric ridership data, so
     route-level distributions and observation-weighted means only.
- **External check:** SFMTA published performance stats as a sanity bound.
- **Deliverable:** `analysis/sfmta_comparison/` mirroring
  `service_health_post/` (frozen window, `queries.py` bound to both local
  DBs, validated figures) + the cadence-sensitivity sidebar. Whether this
  becomes act two of the existing post or its own post is decided when the
  data is in.

## Success criteria

- Muni collector survives ≥ 2 weeks with < 5% missed polls and no 429 holes.
- Day-1 trip_id spike passes, or fallback matching is scoped before build-out.
- Cadence-sensitivity delta quantified (validates methodology; prices a
  future WMATA TU cadence reduction).
- Comparison figures render from frozen queries against the matched window.

## Risks

| Risk | Mitigation |
|---|---|
| Muni TU trip_ids don't match static GTFS | Day-1 spike before any pipeline work |
| 429 rate-limit holes in time series | 10 req/hr slack + bounded retry budget; rate-increase email day one |
| Wrong service dates from hardcoded Eastern | Timezone parameterization in §2; test with late-night Pacific trips |
| One weird week | Matched dates + week-over-week replicate |
| Cadence-sensitive derivation constants | §3 audit; promote to config |

## Sequencing

1. Collector + configs + day-1 spike (data starts accruing immediately).
2. Rate-increase email to 511.
3. Static GTFS load + timezone parameterization + pipeline `--agency` flag.
4. Decimation experiment on existing WMATA archive (no waiting on Muni).
5. Scheduled-service comparison (static GTFS only — publishable early).
6. After 2 clean matched weeks: EWT comparison + figures + post.
