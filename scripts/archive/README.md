# Archived Migration Scripts

These scripts were used during the development phase to migrate from SQLite to PostgreSQL and add various schema improvements. They are no longer needed for ongoing operations but are kept for historical reference.

## Migration Scripts

### SQLite to PostgreSQL Migration
- **migrate_sqlite_to_postgres.py** - Main migration script to copy all data from SQLite to PostgreSQL

### Schema Evolution Scripts
- **migrate_complete_gtfs_schema.py** - Added complete GTFS schema with all standard tables
- **migrate_add_shapes.py** - Added shapes table for route path visualization
- **migrate_add_gtfs_versioning.py** - Added versioning support for GTFS data
- **migrate_add_headway_metrics.py** - Added pre-computed headway metrics tables
- **migrate_vehicle_positions.py** - Initial vehicle positions schema setup
- **add_position_stats_columns.py** - Added position statistics columns to route_metrics_summary

## Current Database Setup

The project now uses **PostgreSQL exclusively**. To initialize a fresh database, use:

```bash
uv run python scripts/init_database.py
```

This script:
- Creates all necessary tables
- Downloads and loads GTFS static data
- Is idempotent (safe to run multiple times)

## Migration History

1. **Oct 2025** - Project started with SQLite for rapid development
2. **Oct 2025** - Migrated to PostgreSQL for production readiness
3. **Nov 2025** - SQLite support removed, PostgreSQL is now required

The data flow is now:
1. `init_database.py` - Initialize DB and load GTFS
2. `collect_sample_data.py` / `continuous_collector.py` - Collect vehicle positions
3. `pipelines/compute_daily_metrics.py` - Compute performance metrics
4. API serves pre-computed metrics from PostgreSQL
