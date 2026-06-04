"""
Data pipelines for WMATA dashboard.

The nightly batch is driven by `run_daily_batch.py`, which dispatches the
per-date derivation pipelines (`derive_stop_events`,
`derive_stop_events_from_state`, `aggregate_runs`, `compute_bunching`,
`upsert_system_metrics_daily`) and the housekeeping pipelines
(`cleanup_trip_update_state`, `refresh_route_diagnostic_profile`,
`refresh_cross_route_segments`, `refresh_corridor_slip`).

Standalone retention for `trip_update_state` is handled by
`retain_trip_update_state.py`, invoked by a separate launchd timer.
"""
