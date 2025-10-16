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
│   ├── models.py          # SQLAlchemy ORM models (VehiclePosition, BusPosition, etc.)
│   ├── wmata_collector.py # GTFS/GTFS-RT data collection
│   ├── analytics.py       # Multi-level OTP and headway calculations
│   └── trip_matching.py   # Match real-time vehicles to scheduled trips
│
├── scripts/               # Runnable scripts
│   ├── init_database.py   # Initialize database and load GTFS data
│   ├── collect_sample_data.py  # Collect data (supports "all" for system-wide)
│   └── continuous_collector.py # Production data collector
│
├── tests/                 # Test files
│   ├── test_analytics.py
│   ├── test_otp_with_matching.py
│   └── test_multi_level_otp.py  # Test stop/time-period/line level OTP
│
├── debug/                 # Debug and exploration scripts
│   ├── check_early_arrivals.py      # Analyze early arrival distribution
│   ├── compare_route_otp.py         # Compare OTP across routes
│   ├── test_bus_positions.py        # Test WMATA BusPositions API
│   ├── test_collect_bus_positions.py # Test BusPos collection
│   ├── test_otp_bus_positions.py    # Test BusPos OTP calculation
│   ├── validate_deviation.py        # Validate WMATA deviation (CRITICAL)
│   ├── debug_otp.py
│   ├── debug_directions.py
│   ├── check_valid_trips.py
│   └── test_headway_detailed.py
│
├── docs/                  # Documentation
│   ├── OTP_METHODOLOGY.md # Detailed OTP calculation methodology
│   └── SESSION_SUMMARY.md # Session notes and findings
│
├── .github/workflows/     # CI/CD workflows
│   └── test.yml          # Basic test workflow
│
├── .env                   # Environment variables (not in git)
├── CLAUDE.md             # This file - project context for Claude Code
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

# Collect all vehicles system-wide (useful for system-wide analysis)
uv run python scripts/collect_sample_data.py all 120   # 120 cycles (2 hours) of all vehicles

# Continuous collection - runs every 60 seconds (for production)
uv run python scripts/continuous_collector.py
```

### Running Analytics
```bash
# Test multi-level OTP calculations
uv run python tests/test_multi_level_otp.py

# Test analytics with collected data
uv run python tests/test_analytics.py
uv run python tests/test_otp_with_matching.py

# Validate WMATA's deviation data (shows unreliability)
uv run python debug/validate_deviation.py

# Compare OTP across multiple routes
uv run python debug/compare_route_otp.py

# Debug specific issues
uv run python debug/debug_otp.py
uv run python debug/check_early_arrivals.py
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
- **Shape**: GTFS shapes data defining vehicle paths (503k points for accurate distance/speed calculations)
- **VehiclePosition**: Real-time vehicle snapshots from GTFS-RT with ALL 17 fields including lat/lon, speed, bearing, occupancy, trip details (PRIMARY DATA SOURCE)
- **BusPosition**: WMATA BusPositions API data with deviation field (SUPPLEMENTARY - validation shows unreliable deviation data)
- **Agency, Calendar, CalendarDate, FeedInfo, Timepoint, TimepointTime**: Additional GTFS static data tables

Key relationships:
- Routes → Trips → StopTimes → Stops (static schedule data)
- VehiclePosition → Route/Trip (real-time observations - primary)
- BusPosition → Route/Trip (WMATA proprietary API - supplementary only)

### Core Modules

**src/database.py** - Database connection factory
- `get_session()`: Returns new database session
- `init_db()`: Creates all tables
- Supports both SQLite (dev) and PostgreSQL (prod) via `DATABASE_URL` env var

**src/wmata_collector.py** - GTFS/GTFS-RT data collection
- `download_gtfs_static()`: Downloads and parses GTFS static zip file
- `get_realtime_vehicle_positions()`: Fetches GTFS-RT protobuf feed (PRIMARY)
- `get_bus_positions()`: Fetches WMATA BusPositions API (supplementary)
- `get_route_vehicles()`: Filters vehicles by route_id
- `_save_vehicle_positions()`: Bulk inserts vehicle positions
- `_save_bus_positions()`: Bulk inserts bus positions from WMATA API

**src/analytics.py** - Transit performance metrics
- `calculate_headways()`: Measures time between consecutive buses at reference stops
- `calculate_stop_level_otp()`: OTP at specific stop on route
- `calculate_time_period_otp()`: OTP by time of day (AM Peak, Midday, PM Peak, Evening, Night)
- `calculate_line_level_otp()`: Overall route OTP (GTFS-based, PRIMARY METHOD)
- `calculate_otp_from_bus_positions()`: OTP using WMATA deviation field (SUPPLEMENTARY ONLY - unreliable)
- `get_route_summary()`: Returns data availability summary for a route
- `find_nearest_stop()`: Find closest stop to position (with caching for performance)

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

### Data Collection Strategy

**Collection Volume (60-second intervals, system-wide):**
- ~500 active vehicles per collection
- ~720,000 records/day = 9.4 GB/day
- ~281 GB/month
- ~3.4 TB/year

**Recommended Approach:**
- Collect at 60-second intervals for maximum granularity
- Implement automatic data aggregation/retention strategy:
  - Keep raw 60-second data for 2-4 weeks (recent analysis)
  - Aggregate to 5-10 minute averages for older data
  - Results in ~60-120 GB steady-state storage
- Strategy TBD based on analytics requirements (develop analytics first)

**Cost Estimates (for production deployment):**
- DIY PostgreSQL on VPS: $18-24/month (80-160 GB storage)
- Managed PostgreSQL: $60-120/month (with auto-scaling)
- Note: DigitalOcean offers $200 credit for new users (~9-10 months free)

### Deployment Steps (when ready)

For continuous collection in production:
1. Deploy to cloud server (DigitalOcean, AWS EC2, etc.)
2. Set up PostgreSQL and configure `DATABASE_URL` in `.env`
3. Run `uv run python scripts/init_database.py` to load initial GTFS data
4. Run `uv run python scripts/continuous_collector.py` as a systemd service or similar
5. Set up cron job to refresh GTFS static data weekly (WMATA updates schedules periodically)
6. Implement data retention/aggregation automation (scripts TBD)

## Current Status & Roadmap

**Completed:**
- ✅ GTFS static data loading and database storage
- ✅ Complete GTFS schema with all fields and tables (agencies, calendar, feed_info, timepoints, etc.)
- ✅ Real-time vehicle position collection (GTFS-RT VehiclePositions) - ALL 17 fields captured
- ✅ WMATA BusPositions API integration (supplementary)
- ✅ SQLite local development setup
- ✅ PostgreSQL production-ready architecture
- ✅ Multi-level OTP calculations (stop/time-period/line level)
- ✅ Analytics layer with headway calculation
- ✅ Trip matching with high accuracy
- ✅ Performance optimizations (caching)
- ✅ Validation of WMATA deviation data (found unreliable)
- ✅ Repository restructuring (src/, scripts/, tests/, debug/)
- ✅ Comprehensive documentation (OTP_METHODOLOGY.md, SESSION_SUMMARY.md)
- ✅ Basic CI/CD with GitHub Actions
- ✅ GTFS shapes support for accurate distance/speed calculations
- ✅ Database migration scripts for schema updates

**In Progress:**
- 🔄 Analytics function development and refinement
- 🔄 Web dashboard UI/visualization layer

**Next Steps (Priority Order):**
1. **Analytics & Visualization** (Current Focus)
   - Refine OTP calculation methods
   - Develop headway analysis algorithms
   - Determine aggregation strategies for long-term data retention
   - Build initial web dashboard UI
   - Create data visualization components

2. **Production Data Collection** (After Analytics)
   - Design data retention/aggregation strategy based on analytics needs
   - Create automated aggregation scripts (compress old detailed data)
   - Deploy continuous collector to cloud (DigitalOcean VPS)
   - Set up PostgreSQL database (managed or DIY based on budget)
   - Implement 60-second collection with automatic data lifecycle management

3. **Production Infrastructure**
   - API/backend for serving metrics (FastAPI)
   - Monitoring and alerting
   - Backup strategy
   - Weekly GTFS static data refresh automation

4. **Advanced Analytics**
   - Bunching detection algorithms
   - Service gap identification
   - Route reliability scoring
   - Comparative route analysis

**Important Notes:**
- **GTFS-based OTP is PRIMARY** - Don't use WMATA deviation as sole source (validation showed up to 7.7 min discrepancies)
- WMATA's GTFS-RT trip_ids DO match GTFS static trip_ids (100% match rate verified)
- All RT trip_ids have complete stop_times data in GTFS static (56-57 stops per trip typical for C51)
- Trip matching prioritizes RT trip_id for accuracy and performance (~90% fast path usage)
- Position/time-based matching serves as fallback for edge cases where RT trip_id is invalid
- Never infer the planned schedule from actual vehicle position data - always use GTFS static data
- BusPositions API useful for cross-validation and schedule discrepancy detection

**Key Findings:**
- ~40% of bus arrivals are early (real operational pattern, not data error)
- LA Metro OTP standard (-1 to +5 min) is stricter than WMATA's published standard
- Polling-based collection gives ±30-60 second accuracy (acceptable for trend analysis)
- Route stops caching provides 100x+ performance improvement for multi-route analysis