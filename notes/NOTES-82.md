# NOTES-82. Redundant indexes on vehicle_positions

**Severity: low (write amplification + maintenance cost).**

`vehicle_positions` carries **9 indexes**; the model defines all of
them (`src/models.py`), so this is design debt, not drift. Three
single-column indexes (`ix_vehicle_positions_vehicle_id`, `_route_id`,
`_trip_id`, from `index=True`) are shadowed by the composites
(`idx_vehicle_timestamp`, `idx_route_timestamp`, `idx_trip_timestamp`)
whose leading column serves the same lookups. `_collected_at` usage is
unknown.

**Re-homed 2026-08-12:** originally scoped against the VM's Postgres
(live-collector insert overhead at ~1M rows/day; the post-retention
VACUUM index sweep observed 2026-06-10). The laptop is now the system
of record and the NOTES-95 rewrite removes Postgres from the VM
entirely — don't spend effort measuring or dropping indexes there.
The cost now lands on the **laptop DB**: bulk inserts during
`bin/pull-and-derive.sh` / archive replay maintain all 9, and the
redundant ones hold disk hostage on the box that also stores the
30-day JSONL working set.

Work:
1. Measure locally: `SELECT indexrelname, idx_scan FROM
   pg_stat_user_indexes WHERE relname = 'vehicle_positions';` (stats
   accumulate since the last stats reset — confirm the window before
   trusting zeros).
2. Drop confirmed-unused indexes: remove `index=True` in
   `src/models.py` and `DROP INDEX` on the laptop DB in the same
   change — the migration ritual in `docs/MIGRATIONS.md` still
   applies (backup first, transaction-wrapped).
3. Expected win: faster replay/pull bulk inserts and a few GB of
   disk back.

2026-07 incident note: the derive pipelines bound their scans to use
`idx_route_timestamp`; do NOT add a `(route_id, trip_start_date)`
index — it would deepen the write amplification this item exists to
reduce.

## Dependencies

Independent. The VM side is mooted by NOTES-95 (no Postgres on the VM
in the target architecture); if any index drop happens before NOTES-95
lands, skip the VM copy anyway — it's disposable.
