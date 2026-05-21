# `trip_update_state` schema addendum: service_date in PK

**Status:** Active (supersedes the schema section of
`2026-05-17-trip-update-state-refactor-design.md`)
**Author:** wlovotti + Claude
**Type:** Architecture amendment

## Why

The original 2026-05-17 spec keyed `trip_update_state` on `(trip_id,
stop_sequence)` and relied on a tight derive-then-cleanup race against
the next day's snapshots overwriting state. In practice:

1. WMATA's GTFS-RT `trip_id`s repeat day-over-day (94% reuse on
   consecutive weekdays — empirically measured 2026-05-18 → 19). The
   UPSERT on `(trip_id, stop_sequence)` overwrites yesterday's state
   with today's run.
2. The "derive before tomorrow's trips start" race assumes the nightly
   batch never fails. The Phase D v2 derivation failed 4 nights in a
   row (2026-05-16 → 19) before anyone noticed.
3. The original cleanup rules (`derived_at < NOW() - INTERVAL '2 days'`
   plus a 7-day safety net) compensated for an irrecoverable design
   rather than just retaining history.

The combination meant no past-day stop_events could be re-derived from
state alone — the only recovery path was to wait 7+ days forward, or
build a new tool to replay from the JSONL archive.

## What changes

**PK**: `(trip_id, stop_sequence)` → `(trip_id, stop_sequence, service_date)`.

`service_date` is computed at UPSERT time:

- Preferred: `tripDescriptor.start_date` from the GTFS-RT feed (parsed
  YYYYMMDD). WMATA populates this on ~76% of vehicle-position rows;
  trip-update population is similar in spot checks.
- Fallback: Eastern calendar day of the snapshot's `snapshot_ts`,
  via `src/timezones.eastern_date_from_naive_utc()`. Correct for
  99%+ of WMATA bus trips since service-day-crossing overnight bus
  operations are rare.

**Cleanup** collapses to a single rule:

```
DELETE FROM trip_update_state WHERE service_date < eastern_today() - retention_days
```

Default `retention_days = 7`. The `derived_at` column is preserved as a
per-row diagnostic but no longer load-bearing for cleanup.

**Recovery** is now possible for any service_date in the JSONL archive
window, via two idempotent commands:

```bash
uv run python pipelines/replay_archive_to_state.py --date YYYY-MM-DD
uv run python pipelines/derive_stop_events_from_state.py \
    --all-routes --date YYYY-MM-DD --target-table stop_events_v2
```

Both steps are idempotent. Re-running them produces the same outputs.

## Companion bug fix

The Phase D side-by-side validation (`stop_events_v2`) hadn't actually
been working: `_resolve_side_table` returned a wrapper class with
`__table__` attribute, which `pg_insert` rejects with
`ArgumentError: subject table for an INSERT, UPDATE or DELETE
expected`. The same PR fixes that — the resolver returns the underlying
`Table` directly, and `upsert_rows` accepts both ORM mapped classes
(via `getattr(model, '__table__', model)`) and bare Table objects.

A row-count guard in `pipelines/run_daily_batch.py` ensures this
specific silent-failure mode (process exits 0, writes 0 rows) flips
the wrapper to non-zero exit so launchd surfaces it.

## Storage impact

Per the original spec's estimate: ~180K rows/day. With a 7-day
retention window, ~1.3M rows live, ≈20–30 MB. Negligible vs the 129 GB
the legacy snapshot table held. The 30× compression vs the original
snapshot design is preserved — it came from collapsing the
prediction-trajectory dimension, not the calendar dimension.

## Migration

Idempotent single transaction:

```sql
ALTER TABLE trip_update_state ADD COLUMN IF NOT EXISTS service_date DATE;

UPDATE trip_update_state SET service_date =
    (final_snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
WHERE service_date IS NULL;

ALTER TABLE trip_update_state ALTER COLUMN service_date SET NOT NULL;

ALTER TABLE trip_update_state DROP CONSTRAINT IF EXISTS trip_update_state_pkey;
ALTER TABLE trip_update_state
    ADD CONSTRAINT trip_update_state_pkey
    PRIMARY KEY (trip_id, stop_sequence, service_date);

CREATE INDEX IF NOT EXISTS idx_tus_service_date
    ON trip_update_state (service_date);
```

(`scripts/migrate_add_service_date_to_state.py` runs exactly this.)

Pre-requisite: stop the running collector before migrating. The
collector code preceding this PR has the old model and will fail
inserts after `NOT NULL` is set. Restart with the new code after
migration completes.

## Phase D restart

The 2026-05-17 spec's Phase D bar (≥7 days at 100% agreement including
≥1 weekend day) is unchanged. Two paths to satisfy it:

| Path | Action | Earliest cutover |
|---|---|---|
| **Forward-only** | Apply migration → restart collector → 7 days of v2 nightly comparison going forward | **2026-05-27** (covers weekend of 5/23–24) |
| **Replay + forward** | Apply migration → restart collector → replay archive for 2026-05-18 and 2026-05-19 → derive v2 for both dates → 7 days going forward including 5/18 | **2026-05-25** (original target) |

The replay tool makes both paths cheap to execute and idempotent to
re-run.

## Lessons captured

1. **The natural key of a state table must include every dimension the
   data varies in, even when one of those dimensions is "today."** The
   original PK conflated "the scheduled trip" with "this instance of
   the scheduled trip running on this calendar day." WMATA recycles
   scheduled trip_ids day-over-day; without `service_date`, the table
   could not represent two instances.

2. **Silent zero-output failures need an explicit guard.** "Exit code
   0" was load-bearing for the nightly batch's failure signal, but the
   v2 derivation managed to exit 0 while writing zero rows. Any
   pipeline whose success criterion is "produces non-empty output for
   the target date" should assert that explicitly.

3. **`_resolve_side_table` should have been the original implementation.**
   The wrapper-class trick was a workaround for a misread of
   `upsert_rows`'s contract. The Table-or-mapped-class duck-typing in
   `upsert_rows` is the right abstraction; once both sides agree on
   it, the side-table pattern becomes trivial.
