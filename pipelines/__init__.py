"""
Data pipelines for WMATA dashboard.

The nightly batch is driven by `run_daily_batch.py`, which dispatches the
per-date derivation pipelines (`derive_stop_events`,
`derive_stop_events_trip_updates`, `aggregate_runs`, `compute_bunching`,
`upsert_system_metrics_daily`) and the housekeeping pipelines
(`archive_trip_update_snapshots`).
"""
