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

## Repository Structure

```
wmata-dashboard/
├── src/                    # Core application modules
│   ├── __init__.py
│   ├── database.py        # Database connection and session management
│   ├── models.py          # SQLAlchemy ORM models
│   ├── wmata_collector.py # GTFS/GTFS-RT data collection
│   ├── analytics.py       # Headway and OTP calculations
│   └── trip_matching.py   # Match real-time vehicles to scheduled trips
│
├── scripts/               # Runnable scripts
│   ├── init_database.py   # Initialize database and load GTFS data
│   ├── collect_sample_data.py  # Collect data for testing
│   └── continuous_collector.py # Production data collector
│
├── tests/                 # Test files
│   ├── test_analytics.py
│   └── test_otp_with_matching.py
│
├── debug/                 # Debug and exploration scripts
│   ├── debug_otp.py
│   ├── debug_directions.py
│   ├── check_valid_trips.py
│   └── test_headway_detailed.py
│
├── .github/workflows/     # CI/CD workflows
│   └── test.yml          # Basic test workflow
│
├── .env                   # Environment variables (not in git)
├── pyproject.toml        # Python dependencies
└── wmata_dashboard.db    # SQLite database (not in git)
```

## Development Commands

### Initial Setup
```bash
# Install uv package manager
brew install uv

# Install dependencies
uv sync

# Initialize database and load GTFS static data (run once, takes 5-10 minutes)
uv run python scripts/init_database.py

# For PostgreSQL in production, add to .env:
# DATABASE_URL=postgresql://user:pass@host/dbname
```

### Running Data Collection
```bash
# Collect sample data for a specific route
uv run python scripts/collect_sample_data.py C51 20    # 20 cycles of C51 data
uv run python scripts/collect_sample_data.py C53 30    # 30 cycles of C53 data

# Continuous collection - runs every 60 seconds (for production)
uv run python scripts/continuous_collector.py
```

### Running Analytics
```bash
# Test analytics with collected data
uv run python tests/test_analytics.py
uv run python tests/test_otp_with_matching.py

# Debug specific issues
uv run python debug/debug_otp.py
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

### Database Models (`src/models.py`)

- **Route**: Static GTFS routes (125 routes in WMATA system)
- **Stop**: Static GTFS stops (7,505 stops)
- **Trip**: Static GTFS trips (~130k trips representing scheduled service)
- **StopTime**: Static GTFS stop_times (scheduled arrival/departure at each stop, ~5.5M records)
- **VehiclePosition**: Real-time vehicle snapshots collected every 60s with lat/lon, timestamp, route_id, trip_id

Key relationships:
- Routes → Trips → StopTimes → Stops (static schedule data)
- VehiclePosition → Route/Trip (real-time observations)

### Core Modules

**src/database.py** - Database connection factory
- `get_session()`: Returns new database session
- `init_db()`: Creates all tables
- Supports both SQLite (dev) and PostgreSQL (prod) via `DATABASE_URL` env var

**src/wmata_collector.py** - GTFS/GTFS-RT data collection
- `download_gtfs_static()`: Downloads and parses GTFS static zip file
- `get_realtime_vehicle_positions()`: Fetches GTFS-RT protobuf feed
- `get_route_vehicles()`: Filters vehicles by route_id
- `_save_vehicle_positions()`: Bulk inserts vehicle positions

**src/analytics.py** - Transit performance metrics
- `calculate_headways()`: Measures time between consecutive buses at reference stops
- `calculate_on_time_performance()`: Compares actual vs scheduled arrivals (LA Metro standard: -1min to +5min)
- `get_route_summary()`: Returns data availability summary for a route

**src/trip_matching.py** - Trip matching with RT trip_id prioritization
- `find_matching_trip()`: Matches real-time vehicles to scheduled trips
- Prioritizes using GTFS-RT trip_id directly when available (fast path, ~90% of cases)
- Falls back to position/time-based matching when RT trip_id is missing or invalid
- Returns confidence score (0-1) based on time/distance accuracy and realism

## Environment Variables

Required in `.env` file:
- `WMATA_API_KEY`: API key from https://developer.wmata.com (rate limit: 10 calls/sec, 50k/day)
- `DATABASE_URL`: Optional, defaults to SQLite `./wmata_dashboard.db`, use PostgreSQL URI for production

## Database Initialization Details

When running `scripts/init_database.py`:
1. Creates all tables via SQLAlchemy
2. Downloads GTFS static data (~40MB zip from WMATA)
3. Parses routes, stops, trips, stop_times CSV files
4. Bulk inserts data with progress indicators (stop_times takes 3-5 min for ~5.5M records)
5. Uses upsert logic to avoid duplicates on re-runs
6. Use `--no-confirm` flag for non-interactive mode (CI/CD, Docker, automation)

## Production Deployment Notes

For continuous collection in production:
1. Deploy to cloud server (DigitalOcean, AWS EC2, etc.)
2. Set up PostgreSQL and configure `DATABASE_URL` in `.env`
3. Run `uv run python scripts/init_database.py` to load initial GTFS data
4. Run `uv run python scripts/continuous_collector.py` as a systemd service or similar
5. Consider setting up cron job to refresh GTFS static data weekly (WMATA updates schedules periodically)

## Current Status & Roadmap

**Completed:**
- ✅ GTFS static data loading and database storage
- ✅ Real-time vehicle position collection and storage
- ✅ SQLite local development setup
- ✅ PostgreSQL production-ready architecture
- ✅ Analytics layer with headway calculation
- ✅ On-time performance calculation with trip matching
- ✅ Repository restructuring (src/, scripts/, tests/, debug/)
- ✅ Basic CI/CD with GitHub Actions

**Next Steps:**
1. Build FastAPI backend with REST API endpoints
2. Create React dashboard frontend with charts/maps
3. Deploy to production environment
4. Add more analytics metrics (bunching detection, service gaps, etc.)

**Important Notes:**
- WMATA's GTFS-RT trip_ids DO match GTFS static trip_ids (100% match rate verified)
- All RT trip_ids have complete stop_times data in GTFS static (56-57 stops per trip typical for C51)
- Trip matching prioritizes RT trip_id for accuracy and performance (~90% fast path usage)
- Position/time-based matching serves as fallback for edge cases where RT trip_id is invalid
- Never infer the planned schedule from actual vehicle position data - always use GTFS static data