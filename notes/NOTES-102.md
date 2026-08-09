# NOTES-102. Backfill June recovery-window trip_update truncation

**Severity: low** *(historical data quality — predates the SFMTA
comparison window, so it doesn't block the sprint)*.
**Effort: low** *(runbook-style re-run of existing pipelines; no code
changes)*.

Before PR #188 fixed the UTC-keyed archive glob in
`pipelines/replay_archive_to_state.py`, every service day replayed from
archive lost its late-evening tail: the day's post-UTC-midnight rows
live in the *next* UTC day's archive file, which the glob never opened.
The 2026-08-09 backfill re-ran replay + derivation for the steady-state
window (2026-07-18 → 08-08), but the **June recovery window
(2026-06-14 → 07-03)** — rebuilt via the same replay path during the
July incident recovery — is still truncated: `stop_events` trip_update
rows for those dates stop at ~01:00 UTC while proximity rows run to
~06:50 UTC. Dates 2026-07-04 → 07-17 came straight from the VM DB and
are intact; they need no work.

To close, per date in 2026-06-14 → 2026-07-03:

1. Archives: 2026-06-17 onward are on the laptop
   (`archive/raw_snapshots/`); **2026-06-14 → 06-16 exist only in S3**
   (`raw-jsonl-archive/` prefix) and must be pulled first. Each date D
   also needs the D+1 file as its supplement.
2. `uv run python pipelines/replay_archive_to_state.py --date <D>`
   (idempotent upsert into `trip_update_state`).
3. Re-derive with the **snapshot-12 GTFS pin** — these dates predate
   snapshot 15 (loaded 2026-07-12), and deriving them against the
   current snapshot mismatches trips (same reason
   `bin/pull-and-derive.sh` warns June dates must not go through its
   default path): `derive_stop_events_from_state --all-routes --date <D>
   --gtfs-snapshot-id 12`, then `aggregate_runs`, `compute_bunching`,
   and `upsert_system_metrics_daily` for the same date with the same
   pin.

Known wrinkle: 2026-06-19 (Juneteenth) has the documented SD=0.38
artifact (GTFS lacks holiday calendar_dates exceptions) — the backfill
won't fix that and shouldn't try.
