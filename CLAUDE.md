# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WMATA Performance Dashboard - A transit metrics dashboard for Washington DC Metro bus and rail lines, inspired by the TransitMatters Dashboard. The project collects real-time vehicle position data from WMATA's GTFS feeds and stores it for analysis of headways and on-time performance.

## Technology Stack

- **Python 3.9+** with `uv` for package management
- **SQLAlchemy** for ORM and database operations
- **SQLite** for local development (PostgreSQL-ready for production)
- **GTFS & GTFS-RT** for transit data (static schedules + real-time positions)
- **protobuf** for parsing GTFS-RT vehicle position feeds

## Development Commands

### Initial Setup
```bash
# Install uv package manager
brew install uv

# Install dependencies
uv sync

# Initialize database and load GTFS static data (run once, takes 5-10 minutes)
python init_database.py

# For PostgreSQL in production, add to .env:
# DATABASE_URL=postgresql://user:pass@host/dbname
```

### Running Data Collection
```bash
# Quick test - collect C51 route vehicle positions once
python wmata_collector.py

# Continuous collection - runs every 60 seconds (for production)
python continuous_collector.py
```

### Database Access
```bash
# Query SQLite database directly
sqlite3 wmata_dashboard.db

# Example queries:
# SELECT COUNT(*) FROM vehicle_positions;
# SELECT * FROM routes WHERE route_short_name = 'C51';
```

## Architecture

### Data Flow
1. **GTFS Static Data** → Downloaded once from WMATA API (routes, stops, trips, stop_times)
2. **GTFS-RT Data** → Polled every 60s from WMATA API (vehicle positions)
3. **Database** → Stores both static schedule data and real-time position snapshots
4. **Future**: Analytics layer to calculate headways and on-time performance

### Database Models (`models.py`)

- **Route**: Static GTFS routes (125 routes in WMATA system)
- **Stop**: Static GTFS stops (7,505 stops)
- **Trip**: Static GTFS trips (~130k trips representing scheduled service)
- **StopTime**: Static GTFS stop_times (scheduled arrival/departure at each stop, ~479k records)
- **VehiclePosition**: Real-time vehicle snapshots collected every 60s with lat/lon, timestamp, route_id, trip_id

Key relationships:
- Routes → Trips → StopTimes → Stops (static schedule data)
- VehiclePosition → Route/Trip (real-time observations)

### Key Files

- **models.py**: SQLAlchemy ORM models (see above)
- **database.py**: Database connection factory, supports both SQLite and PostgreSQL via `DATABASE_URL` env var
- **wmata_collector.py**: Main WMATADataCollector class with methods for fetching GTFS static/RT data
- **init_database.py**: One-time setup script that downloads ~40MB GTFS zip and loads into database
- **continuous_collector.py**: Production collector that runs indefinitely, polling every 60s
- **.env**: Contains `WMATA_API_KEY` (required, get from https://developer.wmata.com)

### WMATADataCollector Class

The collector (`wmata_collector.py`) provides:
- `download_gtfs_static()`: Downloads and parses GTFS static zip file, saves to database
- `get_realtime_vehicle_positions()`: Fetches GTFS-RT protobuf feed, returns list of vehicle dicts
- `get_route_vehicles()`: Filters vehicles by route_id, saves to database
- `_save_vehicle_positions()`: Bulk inserts vehicle positions to database

## Environment Variables

Required in `.env` file:
- `WMATA_API_KEY`: API key from https://developer.wmata.com (rate limit: 10 calls/sec, 50k/day)
- `DATABASE_URL`: Optional, defaults to SQLite `./wmata_dashboard.db`, use PostgreSQL URI for production

## Database Initialization Details

When running `init_database.py`:
1. Creates all tables via SQLAlchemy
2. Downloads GTFS static data (~40MB zip from WMATA)
3. Parses routes, stops, trips, stop_times CSV files
4. Bulk inserts data with progress indicators (stop_times takes 3-5 min for ~479k records)
5. Uses upsert logic to avoid duplicates on re-runs

## Production Deployment Notes

For continuous collection in production:
1. Deploy to cloud server (DigitalOcean, AWS EC2, etc.)
2. Set up PostgreSQL and configure `DATABASE_URL` in `.env`
3. Run `python init_database.py` to load initial GTFS data
4. Run `python continuous_collector.py` as a systemd service or similar
5. Consider setting up cron job to refresh GTFS static data weekly (WMATA updates schedules periodically)

## Current Status & Roadmap

**Completed:**
- GTFS static data loading and database storage
- Real-time vehicle position collection and storage
- SQLite local development setup
- PostgreSQL production-ready architecture

**Next Steps:**
1. Calculate headways from collected vehicle positions (time between consecutive buses)
2. Calculate on-time performance (compare vehicle positions to scheduled stop_times)
3. Build FastAPI backend with REST endpoints
4. Create React dashboard frontend with charts/maps
- Never infer the planned schedule from the actual vehicle position data.