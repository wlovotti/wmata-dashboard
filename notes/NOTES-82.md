# NOTES-82. Redundant indexes on vehicle_positions

**Severity: low (write amplification + maintenance cost).**

Production `vehicle_positions` carries **9 indexes**; the model defines
all of them, so this is design debt, not drift. Three single-column
indexes (`ix_vehicle_positions_vehicle_id`, `_route_id`, `_trip_id`,
from `index=True`) are shadowed by the composites
(`idx_vehicle_timestamp`, `idx_route_timestamp`, `idx_trip_timestamp`)
whose leading column serves the same lookups. `_collected_at` usage is
unknown. Costs observed 2026-06-10: every collector insert (~1M
rows/day) maintains all 9; the post-retention VACUUM index sweep — the
dominant cost of the nightly job's first run — scanned all 9.

Work:
1. Measure on the VM after ≥1 week of normal traffic:
   `SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE
   relname = 'vehicle_positions';` (stats accumulate since the last
   reset — confirm the window before trusting zeros).
2. Drop confirmed-unused indexes via the migration ritual
   (`docs/MIGRATIONS.md`): remove `index=True` in `src/models.py` and
   `DROP INDEX CONCURRENTLY` on the VM in the same change.
3. Expected win: lower insert overhead and faster nightly VACUUMs;
   a few GB of disk back.

2026-07 incident note: the derive pipelines now bound their scans to
use `idx_route_timestamp`; do NOT add a `(route_id, trip_start_date)`
index — it would deepen the write amplification this item exists to
reduce.

## Dependencies

Independent.
