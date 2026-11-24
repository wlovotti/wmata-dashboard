"""
Data pipelines for WMATA dashboard.

The nightly batch is driven by `run_daily_batch.py`, which dispatches the
per-date derivation pipelines (`derive_stop_events`,
`derive_stop_events_from_state`, `aggregate_runs`, `compute_bunching`,
`upsert_system_metrics_daily`, `upsert_route_metrics_overlay`) and the housekeeping pipelines
(`cleanup_trip_update_state`, `refresh_route_diagnostic_profile`,
`refresh_cross_route_segments`, `refresh_corridor_slip`).

Local-DB retention (stop_events/runs, vehicle_positions, trip_update_state)
is `apply_retention.py` (dry-run by default; tiers in the agency YAML,
guards in `src/retention.py`), optionally run at the end of
`refresh_by_day.py` (`--apply-retention`, default off).
"""
