# Trip update collection refactor — `trip_update_state` model + parquet archive

**Status:** Draft (brainstormed 2026-05-17)
**Author:** wlovotti + Claude
**Type:** Architecture refactor

## Background

The `trip_update_snapshots` table is an append-only mirror of WMATA's GTFS-RT
TripUpdate feed: every collection cycle inserts one row per (trip, stop)
pair currently being predicted by WMATA. With ~6,000 active trips/day and
~30 stops per trip, the table accumulates **~21M rows/day** and currently
holds **521M rows / 129 GB** spanning 14 days (2026-05-03 → 2026-05-17).

The downstream consumer — `pipelines/derive_stop_events_trip_updates.py` —
reduces all those snapshots to a final-state record per (trip_id,
stop_sequence) before materializing `stop_events`. The intermediate
storage of the prediction-evolution trajectory is never read by any
downstream code; verified by `grep` across `src/`, `api/`, `pipelines/`.

The data is irreplaceable: WMATA's feed has no replay window.

## Goals

1. **Reduce live PG storage by ~30×** (raw snapshots → final-state only).
2. **Preserve full raw evidence** in cheap object storage so historical
   re-derivation remains possible.
3. **Decouple collector from derivation semantics** — collector writes
   state, derivation reads state, neither leaks into the other.
4. **Keep all downstream metrics and APIs identical** — `stop_events`,
   `runs`, `system_metrics_daily`, and every API endpoint behave the
   same after cutover.

## Non-goals

1. **Cloud migration** — deferred. This refactor runs locally on the same
   Postgres instance. A separate spec will plan the cloud move once this
   refactor has operated reliably for some period.
2. **API or frontend changes** — no API contract changes.
3. **Re-architecting `vehicle_positions`** — that table is small (5.4 GB,
   slow growth) and out of scope.
4. **Changing derivation logic** — the rules for inferring
   `observed_arrival_ts` from final-state predictions are preserved
   byte-for-byte.

## Architecture

### Data flow (new)

```
WMATA GTFS-RT TripUpdate feed
              │
              ▼
   continuous_combined_collector.py
              │
      ┌───────┴────────────────────────┐
      ▼                                ▼
 trip_update_state (Postgres)     raw_snapshots/<date>.jsonl.zst
   UPSERT one row per                (collector appends)
   (trip_id, stop_sequence)                ▼
   ~180K rows/day                    Nightly rotate to parquet,
   ~1-3 GB steady-state              upload to Backblaze B2
              │
              ▼
   derive_stop_events_from_state.py
   (reads trip_update_state directly)
              │
              ▼
   stop_events (unchanged) → runs → metrics → API
```

### Component changes

| Component | Change | Reversible? |
|---|---|---|
| `src/models.py` | Add `TripUpdateState` model | Yes |
| `scripts/continuous_combined_collector.py` | Add UPSERT + JSONL append | Yes |
| `pipelines/derive_stop_events_from_state.py` | New file, reads state table | Yes |
| `pipelines/rotate_archive.py` | New file, rotates JSONL → parquet → B2 | Yes |
| `pipelines/cleanup_trip_update_state.py` | New file, deletes derived rows after 2d | Yes |
| `pipelines/derive_stop_events_trip_updates.py` | **Deleted after cutover** | One-way |
| `trip_update_snapshots` (DB table) | **Dropped after cutover** | One-way |

## Schema

```sql
CREATE TABLE trip_update_state (
    -- Identity (primary key)
    trip_id            VARCHAR NOT NULL,
    stop_sequence      INTEGER NOT NULL,

    -- Stable per (trip, stop)
    stop_id            VARCHAR NOT NULL,
    vehicle_id         VARCHAR,  -- latest non-null

    -- Final-state fields (always overwritten)
    final_snapshot_ts          TIMESTAMP NOT NULL,
    final_schedule_relationship VARCHAR,

    -- Last-non-null-prediction fields (overwritten only when new is non-null)
    last_pred_snapshot_ts      TIMESTAMP,
    last_predicted_arrival_ts  TIMESTAMP,

    -- Lifecycle marker
    derived_at                 TIMESTAMP,

    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE INDEX idx_tus_final_snapshot_ts ON trip_update_state (final_snapshot_ts);
CREATE INDEX idx_tus_trip_id ON trip_update_state (trip_id);
```

### Columns dropped vs the old `trip_update_snapshots`

- `id` — composite PK on `(trip_id, stop_sequence)` is the natural key.
- `route_id` — denormalized, not used by any query (derivation filters
  by `trip_id` via `vehicle_positions`).
- `predicted_departure_ts` — confirmed unused at
  `derive_stop_events_trip_updates.py:265` (only arrival is read).
- `collected_at` — same information as `final_snapshot_ts`.

### UPSERT semantics

On each WMATA snapshot containing predictions for (trip, stop):

```sql
INSERT INTO trip_update_state (
    trip_id, stop_sequence, stop_id, vehicle_id,
    final_snapshot_ts, final_schedule_relationship,
    last_pred_snapshot_ts, last_predicted_arrival_ts
) VALUES (...)
ON CONFLICT (trip_id, stop_sequence) DO UPDATE SET
    stop_id = EXCLUDED.stop_id,
    vehicle_id = COALESCE(EXCLUDED.vehicle_id, trip_update_state.vehicle_id),
    final_snapshot_ts = EXCLUDED.final_snapshot_ts,
    final_schedule_relationship = EXCLUDED.final_schedule_relationship,
    last_pred_snapshot_ts = CASE
        WHEN EXCLUDED.last_predicted_arrival_ts IS NOT NULL
        THEN EXCLUDED.last_pred_snapshot_ts
        ELSE trip_update_state.last_pred_snapshot_ts
    END,
    last_predicted_arrival_ts = COALESCE(
        EXCLUDED.last_predicted_arrival_ts,
        trip_update_state.last_predicted_arrival_ts
    );
```

This preserves the existing algorithm's behavior:

- `final_*` fields always reflect the most recent snapshot (used to
  detect SKIPPED stops at the final state).
- `last_pred_*` fields reflect the most recent snapshot with a non-null
  prediction (WMATA sometimes nullifies predictions right at arrival;
  we want the last meaningful estimate).

### Lifecycle of a row

1. Trip starts → rows inserted (one per upcoming stop).
2. Bus moves through trip → rows update as predictions refine.
3. Bus passes each stop → row no longer in feed; final state captured.
4. End of service day → `derive_stop_events_from_state.py` materializes
   the corresponding `stop_event` row and sets `derived_at` on the
   source row.
5. Cleanup cron (daily) runs two passes:
   ```sql
   -- Normal cleanup: rows whose stop_events were materialized.
   DELETE FROM trip_update_state
   WHERE derived_at IS NOT NULL
     AND derived_at < NOW() - INTERVAL '2 days';

   -- Safety net: rows that were never derived (trip had no
   -- vehicle_position to anchor service_date — see
   -- derive_stop_events_trip_updates.py:189). Without this,
   -- un-derivable trips accumulate forever.
   DELETE FROM trip_update_state
   WHERE final_snapshot_ts < NOW() - INTERVAL '7 days';
   ```

The 2-day window provides a re-derivation buffer without requiring the
parquet archive. Beyond 2 days, re-derivation falls back to parquet.
The 7-day safety net catches un-derivable trips so the table can't
grow unbounded from "trip never matched."

## Parquet archive

### Hot path (during the day)

Collector appends one JSONL line per (trip, stop) per snapshot to a
ZSTD-streaming file:

```
archive/raw_snapshots/2026-05-17.jsonl.zst
```

JSONL is chosen over direct parquet for the live append path because
parquet's columnar layout makes appends expensive (file rewrites),
whereas JSONL is line-atomic and ZSTD streaming compression handles the
size.

Estimated size: ~150 MB/day compressed.

### Nightly rotation

At 03:00 UTC (3 hours after collector's UTC-midnight file rotation),
`pipelines/rotate_archive.py`:

1. Reads yesterday's JSONL file (collector opened today's file at 00:00
   UTC and is no longer writing to yesterday's).
2. Reads JSONL → writes parquet (zstd) with full schema.
3. Uploads `s3://wmata-archive/raw_snapshots/<YYYY-MM-DD>.parquet` to
   Backblaze B2 (S3-compatible API).
4. Verifies upload by checking B2 object size matches local.
5. Deletes local JSONL + local parquet after verification.

Estimated final size: ~30-60 MB/day parquet (~10-20 GB/year).

### Cost

Backblaze B2 storage: $0.005/GB/month. **~$0.10/month** for the
complete archive.

## Cutover phases

**Principle:** Never disable the working system until the new system
produces identical output for ≥ 7 days. Cutover is reversible up to
the moment we DROP the old table.

### Phase A — Preserve existing raw data (~hours)

Run `pipelines/archive_trip_update_snapshots.py --retention-days 0` to
push all 14 days of accumulated `trip_update_snapshots` to parquet
archives. No new code; uses the existing tool.

**Validation:** Parquet files for every date 2026-05-03 through
2026-05-16 exist in `archive/trip_update_snapshots/`, row counts match
table COUNT(*).

**Reversible:** Yes (parquet only; nothing deleted yet).

### Phase B — Dual-write in the collector (~half day)

Modify `scripts/continuous_combined_collector.py` to additionally:

- UPSERT each (trip, stop) into the new `trip_update_state` table.
- Append a JSONL line to today's archive file.

Old INSERT path to `trip_update_snapshots` is **untouched**.

**Validation:** After 24h, both `trip_update_snapshots` and
`trip_update_state` are growing as expected. JSONL file is rotating
correctly at UTC midnight.

**Reversible:** Yes (revert collector changes; drop new table).

### Phase C — New derivation pipeline (~1 day)

New file `pipelines/derive_stop_events_from_state.py`:

- Reads from `trip_update_state` (no snapshot scan needed).
- Writes to a **side table** `stop_events_v2` with identical schema to
  `stop_events`.
- Updates `derived_at` on each consumed `trip_update_state` row.

Old `derive_stop_events_trip_updates.py` continues running, writing to
the real `stop_events` table.

**Validation:** First night's run completes successfully. Row count in
`stop_events_v2` is similar to `stop_events` for the same date.

**Reversible:** Yes (drop side table).

### Phase D — Validation period (≥ 7 days)

Nightly comparison via `pipelines/compare_old_vs_new_derivation.py`
(pattern from `pipelines/compare_stop_event_sources.py`, PR #44):

For each (route_id, service_date):

- Compare row counts (`stop_events` vs `stop_events_v2`).
- For matching (trip_id, stop_id) pairs, compare:
  - `observed_arrival_ts`
  - `schedule_relationship`
  - `deviation_sec`
- Report % agreement; print any per-route deltas > 1%.

**Pass criteria:**

- ≥ 99.5% row-level agreement for at least 7 consecutive days.
- Includes at least one full weekend (different service patterns).
- Any divergences below 99.5% must be investigated and explained
  (e.g., known semantic difference) before cutover.

**Reversible:** Trivially — phases A-D are all parallel-only.

### Phase E — Cutover (~1 hour)

In a single short maintenance window:

1. Stop the collector.
2. Apply the final collector change: stop writing to
   `trip_update_snapshots` (keep the UPSERT and JSONL paths).
3. Stop running `pipelines/derive_stop_events_trip_updates.py` (remove
   from `pipelines/run_daily_batch.py`).
4. Point `derive_stop_events_from_state.py` at the real `stop_events`
   table instead of `stop_events_v2` (one-line config change). The
   pipeline is already idempotent via `upsert_rows`, so re-deriving
   into the existing `stop_events` table produces the same rows.
5. Drop the side table: `DROP TABLE stop_events_v2`.
6. Restart the collector.
7. Run the new derivation manually for today's date as a smoke test.

**Validation:** Next daily batch run produces metrics identical to the
prior day's. API endpoints return identical data. The `stop_events`
table receives upserts from the new pipeline.

**Reversible:** Up to step 4, fully reversible (revert collector to
dual-write; resume old derivation). After step 5 (DROP side table),
reverting requires re-establishing dual-derivation. After Phase F,
fully one-way.

### Phase F — Retire (after 14 days clean operation)

Once Phase E has run cleanly for 14 days:

- `DROP TABLE trip_update_snapshots` — frees the 129 GB.
- Delete `pipelines/derive_stop_events_trip_updates.py`.
- Delete `pipelines/archive_trip_update_snapshots.py` (replaced by
  `rotate_archive.py`).
- Update `CLAUDE.md`: the section on `stop_events.source` and the
  derivation paths.
- Update `NOTES.md`: close NOTES-21 reference (archive job) and link
  this refactor.

**Irreversible at this point.** That's why phases C-E exist.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Derivation discrepancy not caught in Phase D | Low | High (silent metric drift post-cutover) | 99.5% agreement threshold; explicit per-route diff; manual review of any > 1% divergence |
| Collector dual-write slows ingestion | Low | Medium (data gap) | Benchmark before Phase B; UPSERT path is ~120× fewer rows but each row needs a lookup |
| JSONL file corruption on crash | Low | Low (one snapshot lost) | ZSTD-streaming flushes on every line; collector writes atomically |
| B2 upload fails | Medium | Low | `rotate_archive.py` retains local copy until upload verified |
| Algorithm subtlety in `last_pred_*` UPSERT logic | Medium | Medium | Unit-test the UPSERT against curated snapshot sequences before Phase B |
| Existing `trip_update_snapshots` not fully archived before cutover | Low | High (permanent data loss) | Phase A is explicit, with row-count verification |

## Open questions

1. **B2 vs R2 vs S3.** B2 is cheapest; R2 has zero egress (matters if
   we ever re-derive in bulk); S3 is most expensive but most universally
   supported. Defer to cloud-migration spec; for local-only operation,
   B2 is fine.
2. **Should we keep `predicted_departure_ts` "just in case"?**
   Currently dropping. Cheap to add back later if a future metric wants
   it. Decided: drop.
3. **What if a `trip_update_state` row never gets cleaned up?**
   Possible if a trip is never derived (e.g., the trip has no
   `vehicle_position` to anchor service_date — see
   `derive_stop_events_trip_updates.py:189`). The cleanup cron should
   also age-out rows where `final_snapshot_ts < NOW() - INTERVAL '7
   days'` regardless of `derived_at`, to prevent unbounded growth from
   un-derivable trips.

## Acceptance criteria

The refactor is considered complete when:

1. `stop_events` is being populated from `trip_update_state` (not from
   `trip_update_snapshots`).
2. The `trip_update_snapshots` table has been dropped.
3. The parquet archive has accumulated at least 14 days of daily files
   in B2.
4. `pipelines/run_daily_batch.py` runs cleanly with the new pipeline
   in the lineup.
5. All existing tests pass.
6. The API smoke test (`uv run pytest -m smoke`) passes.
7. Frontend Playwright visual regression passes.
8. `CLAUDE.md` and `NOTES.md` reflect the new architecture.

## Out of scope (deferred to future specs)

- **Cloud migration.** Once this refactor is stable, the resulting <5
  GB DB makes cloud-target selection trivial. A separate spec will
  plan it.
- **Vehicle position retention.** 5.4 GB, slow growth, not urgent.
- **Streaming derivation.** Investigated and rejected — see
  brainstorm transcript for the architectural reasoning.

## Brainstorm transcript reference

This design emerged from a brainstorming session on 2026-05-17. The
session began as a "cloud migration" brainstorm but pivoted when
investigation revealed:

- The `trip_update_snapshots` table contained 73 GB of indexes (more
  than the 56 GB of data).
- Two indexes had zero scans (10 GB combined).
- The existing 14-day retention job had never run (empty `archive/`
  directory).
- The derivation algorithm only needs final-state per (trip, stop),
  not the full prediction trajectory.

These findings shifted the project from "move ~150 GB to cloud" to
"refactor to ~5 GB locally, then revisit cloud."
