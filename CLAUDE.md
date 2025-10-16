# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WMATA Performance Dashboard - A transit metrics dashboard for Washington DC Metro bus and rail lines, inspired by the TransitMatters Dashboard. The project collects real-time vehicle position data from WMATA's GTFS feeds and computes performance metrics (OTP, headways, speeds) via a FastAPI backend.

## Technology Stack

- **Python 3.9+** with `uv` for package management
- **SQLAlchemy** for ORM and database operations
- **SQLite** for local development (PostgreSQL-ready for production)
- **FastAPI** for REST API backend
- **GTFS & GTFS-RT** for transit data (static schedules + real-time positions)
- **protobuf** for parsing GTFS-RT vehicle position feeds
- **ruff** for code linting and formatting

## Repository Structure

```
wmata-dashboard/
├── src/                    # Core application modules
│   ├── __init__.py
│   ├── database.py        # Database connection and session management
│   ├── models.py          # SQLAlchemy ORM models (all GTFS tables + aggregations)
│   ├── wmata_collector.py # GTFS/GTFS-RT data collection
│   ├── analytics.py       # Multi-level OTP, headway, and speed calculations
│   └── trip_matching.py   # Match real-time vehicles to scheduled trips
│
├── api/                   # FastAPI backend
│   ├── main.py           # API routes and FastAPI app
│   └── aggregations.py   # Aggregation functions for API endpoints
│
├── pipelines/            # Data processing jobs (nightly batch jobs)
│   ├── __init__.py
│   └── compute_daily_metrics.py  # Compute and store daily route metrics
│
├── scripts/              # Runnable scripts
│   ├── init_database.py           # Initialize database and load GTFS data
│   ├── collect_sample_data.py     # Collect sample data (supports "all" for system-wide)
│   ├── continuous_collector.py    # Production data collector (60s polling)
│   ├── reload_gtfs_complete.py    # Refresh GTFS static data
│   └── migrate_*.py              # Database migration scripts
│
├── tests/                # Test files
│   ├── test_analytics.py
│   ├── test_otp_with_matching.py
│   └── test_multi_level_otp.py
│
├── debug/                # Debug and exploration scripts
│   ├── check_early_arrivals.py
│   ├── compare_route_otp.py
│   ├── validate_deviation.py      # Validates WMATA deviation (found unreliable)
│   └── ...
│
├── docs/                 # Documentation
│   ├── OTP_METHODOLOGY.md        # Detailed OTP calculation methodology
│   └── SESSION_SUMMARY.md        # Session notes and findings
│
├── .github/workflows/    # CI/CD workflows
│   └── test.yml         # PR checks: ruff linting + import tests
│
├── .env                  # Environment variables (not in git)
├── CLAUDE.md            # This file - project context for Claude Code
├── README.md            # User-facing documentation
├── pyproject.toml       # Python dependencies and ruff config
└── wmata_dashboard.db   # SQLite database (not in git)
```

## Development Commands

### Initial Setup
```bash
# Install uv package manager
brew install uv

# Install dependencies (including dev dependencies)
uv sync --extra dev

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

### Computing Metrics
```bash
# Compute daily metrics for all routes with sufficient data
uv run python pipelines/compute_daily_metrics.py --days 7

# Compute metrics for a specific route only
uv run python pipelines/compute_daily_metrics.py --route C51 --days 7
```

### Running the API
```bash
# Start FastAPI server (development mode with auto-reload)
uv run uvicorn api.main:app --reload

# API available at http://localhost:8000
# Endpoints:
#   GET /api/routes - All routes scorecard
#   GET /api/routes/{route_id} - Detailed route metrics
#   GET /api/routes/{route_id}/trends - Time-series trend data
#   GET /api/routes/{route_id}/time-periods - Performance by time of day

# Test API
curl http://localhost:8000/api/routes
curl http://localhost:8000/api/routes/C51
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
```

### Code Quality
```bash
# Lint code (checks for issues)
uv run ruff check src/ scripts/ api/ pipelines/

# Format code
uv run ruff format src/ scripts/ api/ pipelines/

# Auto-fix issues
uv run ruff check --fix src/ scripts/ api/ pipelines/
```

### Database Access
```bash
# Query SQLite database directly
sqlite3 wmata_dashboard.db

# Example queries:
# SELECT COUNT(*) FROM vehicle_positions;
# SELECT * FROM routes WHERE route_short_name = 'C51';
# SELECT * FROM route_metrics_summary;
```

## Architecture

### Data Flow
1. **GTFS Static Data** → Downloaded from WMATA API (routes, stops, trips, stop_times, shapes)
2. **GTFS-RT Data** → Polled every 60s from WMATA API (vehicle positions)
3. **Database** → Stores both static schedule data and real-time position snapshots
4. **Nightly Batch Job** → `pipelines/compute_daily_metrics.py` computes daily metrics
5. **Aggregation Tables** → Pre-computed metrics stored in `route_metrics_daily` and `route_metrics_summary`
6. **API** → FastAPI serves pre-computed metrics with <100ms response times

### Database Models (`src/models.py`)

**Static Data (GTFS):**
- **Route**: Static GTFS routes (125 routes in WMATA system)
- **Stop**: Static GTFS stops (7,505 stops)
- **Trip**: Static GTFS trips (~130k trips representing scheduled service)
- **StopTime**: Static GTFS stop_times (scheduled arrival/departure at each stop, ~5.5M records)
- **Shape**: GTFS shapes data defining vehicle paths (503k points for accurate distance/speed calculations)
- **Agency, Calendar, CalendarDate, FeedInfo, Timepoint, TimepointTime**: Additional GTFS static data tables

**Real-time Data:**
- **VehiclePosition**: Real-time vehicle snapshots from GTFS-RT with ALL 17 fields including lat/lon, speed, bearing, occupancy, trip details (PRIMARY DATA SOURCE)
- **BusPosition**: WMATA BusPositions API data with deviation field (SUPPLEMENTARY - validation shows unreliable deviation data)

**Aggregation Tables (Pre-computed Metrics):**
- **RouteMetricsDaily**: Daily performance metrics for each route (OTP, headway, speed) - populated by nightly batch job
- **RouteMetricsSummary**: Rolling 7-day summary for each route - used by API scorecard endpoint for fast responses

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
- `calculate_average_speed()`: Average speed calculation using shapes data
- `calculate_otp_from_bus_positions()`: OTP using WMATA deviation field (SUPPLEMENTARY ONLY - unreliable)
- `get_route_summary()`: Returns data availability summary for a route
- `find_nearest_stop()`: Find closest stop to position (with caching for performance)

**src/trip_matching.py** - Trip matching with RT trip_id prioritization
- `find_matching_trip()`: Matches real-time vehicles to scheduled trips
- Prioritizes using GTFS-RT trip_id directly when available (fast path, ~90% of cases)
- Falls back to position/time-based matching when RT trip_id is missing or invalid
- Returns confidence score (0-1) based on time/distance accuracy and realism

**api/main.py** - FastAPI application
- Defines REST API endpoints for route metrics
- CORS middleware for frontend integration
- Routes:
  - `GET /api/routes` - Scorecard for all routes
  - `GET /api/routes/{route_id}` - Detailed metrics for specific route
  - `GET /api/routes/{route_id}/trends` - Time-series data (TODO: implement)
  - `GET /api/routes/{route_id}/time-periods` - Performance by time of day

**api/aggregations.py** - API aggregation functions
- `get_all_routes_scorecard()`: Returns scorecard from pre-computed summary table (37ms response time)
- `get_route_detail_metrics()`: Returns detailed metrics for a route
- `get_route_trend_data()`: Time-series trend data (TODO: implement)
- `get_route_speed_segments()`: Speed by route segment for map viz (TODO: implement)
- `get_route_time_period_summary()`: Performance metrics by time of day
- `calculate_performance_grade()`: Converts OTP percentage to letter grade (A-F)

**pipelines/compute_daily_metrics.py** - Nightly batch job
- `compute_metrics_for_route_day()`: Computes all metrics for single route/day
- `compute_daily_metrics()`: Main function - computes for all routes over N days
- `compute_summary_metrics()`: Aggregates daily metrics into rolling summaries
- Stores results in `route_metrics_daily` and `route_metrics_summary` tables
- Run via cron: `0 2 * * * cd /path/to/wmata-dashboard && uv run python pipelines/compute_daily_metrics.py --days 7`

## Environment Variables

Required in `.env` file:
- `WMATA_API_KEY`: API key from https://developer.wmata.com (rate limit: 10 calls/sec, 50k/day)
- `DATABASE_URL`: Optional, defaults to SQLite `./wmata_dashboard.db`, use PostgreSQL URI for production

## Performance Optimizations

### API Performance
- **Pre-computed Aggregations**: Nightly batch job computes metrics once, API reads from tables
- **Response Time**: 37ms for scorecard endpoint (vs 30+ seconds for live calculation)
- **Speedup**: ~1000x improvement by separating computation from serving

### Analytics Performance
- **Route Stops Caching**: 100x+ performance improvement for multi-route analysis
- **Trip Matching Fast Path**: ~90% of matches use RT trip_id directly (no position/time matching needed)
- **Batch Loading**: Load all stops/trips upfront rather than individual queries
- **NumPy Vectorization**: Use numpy arrays for distance/speed calculations

## Current Status & Next Steps

**Completed:**
- ✅ GTFS static data loading and database storage
- ✅ Complete GTFS schema with all fields and tables
- ✅ Real-time vehicle position collection (GTFS-RT VehiclePositions) - ALL 17 fields captured
- ✅ WMATA BusPositions API integration (supplementary)
- ✅ SQLite local development setup
- ✅ PostgreSQL production-ready architecture
- ✅ Multi-level OTP calculations (stop/time-period/line level)
- ✅ Analytics layer with headway and speed calculations
- ✅ Trip matching with high accuracy
- ✅ Performance optimizations (caching, batch loading)
- ✅ Validation of WMATA deviation data (found unreliable)
- ✅ Repository restructuring (src/, scripts/, tests/, debug/, api/, pipelines/)
- ✅ Comprehensive documentation (OTP_METHODOLOGY.md, SESSION_SUMMARY.md)
- ✅ CI/CD with GitHub Actions (ruff linting on PRs)
- ✅ GTFS shapes support for accurate distance/speed calculations
- ✅ Database migration scripts for schema updates
- ✅ **FastAPI REST API backend**
- ✅ **Pre-computed aggregation system** (route_metrics_daily, route_metrics_summary)
- ✅ **Nightly batch job pipeline** (compute_daily_metrics.py)
- ✅ **Ruff linting integration** (PR checks, code quality enforcement)

**In Progress:**
- 🔄 Web dashboard UI/visualization layer (React frontend)
- 🔄 Additional API endpoints (trends, speed segments)

**Next Steps (Priority Order):**

1. **Frontend Dashboard** (Next Priority)
   - Set up React/Vite project in `frontend/` directory
   - Create route scorecard view (table of all routes with OTP/headway/speed)
   - Create route detail page (charts, time-series, map visualization)
   - Integrate with FastAPI backend
   - Add filtering/sorting capabilities

2. **API Enhancements**
   - Implement `get_route_trend_data()` - time-series data for charts
   - Implement `get_route_speed_segments()` - segment-level speeds for map viz
   - Add pagination for large result sets
   - Add filtering parameters (date ranges, time periods)
   - Add caching layer (Redis) for frequently accessed data

3. **Production Deployment** (After Frontend MVP)
   - Deploy to cloud platform (DigitalOcean, AWS, etc.)
   - Set up PostgreSQL database
   - Configure continuous data collection (systemd service)
   - Set up cron jobs for nightly metrics computation
   - Set up weekly GTFS refresh
   - Configure Nginx reverse proxy
   - Set up SSL certificates (Let's Encrypt)
   - Implement monitoring and alerting

4. **Advanced Analytics**
   - Bunching detection algorithms
   - Service gap identification
   - Route reliability scoring
   - Comparative route analysis
   - Historical trend analysis

**Important Notes:**
- **GTFS-based OTP is PRIMARY** - Don't use WMATA deviation as sole source (validation showed up to 7.7 min discrepancies)
- WMATA's GTFS-RT trip_ids DO match GTFS static trip_ids (100% match rate verified)
- All RT trip_ids have complete stop_times data in GTFS static (56-57 stops per trip typical for C51)
- Trip matching prioritizes RT trip_id for accuracy and performance (~90% fast path usage)
- Position/time-based matching serves as fallback for edge cases where RT trip_id is invalid
- Never infer the planned schedule from actual vehicle position data - always use GTFS static data
- BusPositions API useful for cross-validation and schedule discrepancy detection
- Pre-computed aggregations are critical for API performance - never calculate metrics live in API endpoints
- Run nightly batch job to keep metrics up to date

**Key Findings:**
- ~40% of bus arrivals are early (real operational pattern, not data error)
- LA Metro OTP standard (-1 to +5 min) is stricter than WMATA's published standard
- Polling-based collection gives ±30-60 second accuracy (acceptable for trend analysis)
- Route stops caching provides 100x+ performance improvement for multi-route analysis
- API response time: 37ms with pre-computed aggregations (vs 30+ seconds with live calculation)

## Git Workflow

- **Never commit directly to main** - Always create feature branches
- Branch naming: `feature/description`, `fix/description`
- Create PR and merge to main after review
- All PRs run through GitHub Actions checks (ruff linting, import tests)

## Code Style

- Use **ruff** for linting and formatting (configured in pyproject.toml)
- Python 3.9+ type hints: Use `list[dict]` instead of `List[Dict]`
- Line length: 100 characters
- Import sorting: Automatic via ruff
- All code must pass `ruff check` and `ruff format --check` before merging

## Session Notes

**Last Session (2025-10-15):**
- Added ruff linting to PR checks in GitHub Actions workflow
- Fixed all 91 code quality issues (type hints, imports, formatting)
- Updated both README.md and CLAUDE.md to reflect current state
- FastAPI backend fully functional with 37ms response times
- Pre-computed aggregation system working well
- Ready to start frontend development

**FastAPI Server Status:**
- Server running on localhost:8000
- `/api/routes` endpoint returning data for 3 routes (C53, D80, F20) with computed metrics
- 122 routes showing null values (need data collection + metrics computation)
- Response time: 37ms (1000x faster than live calculation)

**Data Status:**
- Test data collected from October 12, 2025
- 3 routes have computed metrics (C53, D80, F20)
- Need to collect more recent data for full route coverage
- Pipeline ready to compute metrics when more data available
