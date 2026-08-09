# NOTES-96. Replay archive support for multi-agency pipelines

**Severity: medium (blocking Plan-2 SFMTA integration until replay
archives work correctly).**
**Effort: medium (two changes to `pipelines/replay_archive_to_state.py`).**

The SFMTA comparison design (spec `docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md` §3)
requires replaying raw SFMTA JSONL archives through `pipelines/replay_archive_to_state.py` to build
state for stop-events derivation. The replay script currently has two blockers:

1. **Agency-aware service-date timezone.** The script calls `_service_date_for_row` without
   an agency context, applying the hardcoded Eastern default. SFMTA service dates run on Pacific
   time. Add an agency parameter to the replay flow (pass via CLI or config) and thread it
   through `_service_date_for_row` so each agency's local tz is respected.

2. **Duplicate stop_sequence dedup.** SFMTA 511.org feed repeats `stop_sequence` values within
   a trip in ~0.24% of rows (likely due to multiple vehicles on the same block/trip, or
   feed artifacts); the collector's `_save_trip_updates` gained a dedup filter (PR #180) to
   avoid CardinalityViolation crashes. Replay archives carry raw rows by design, so
   `pipelines/replay_archive_to_state.py` must apply the same dedup before upsert.

Work:
1. Add `--agency SFMTA` (or load from a config) to the replay CLI and thread the agency
   through to `_service_date_for_row`.
2. Extract the stop_sequence dedup logic from `src/wmata_collector.py:_save_trip_updates`
   into a shared helper and call it in the replay path before upsert.

## Dependencies

Independent. First step of the comparison sprint — blocks NOTES-100,
which blocks NOTES-99.
