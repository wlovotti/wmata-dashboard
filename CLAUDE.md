# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WMATA Performance Dashboard - A transit metrics dashboard for Washington DC Metro bus and rail lines, inspired by the TransitMatters Dashboard. The project collects real-time vehicle position data from WMATA's GTFS feeds and computes performance metrics (OTP, headways, speeds) via a FastAPI backend.

## Technology Stack

**Backend:**
- **Python 3.9+** with `uv` for package management
- **SQLAlchemy** for ORM and database operations
- **PostgreSQL** (required - no SQLite support)
- **FastAPI** for REST API backend
- **GTFS & GTFS-RT** for transit data (static schedules + real-time positions)
- **protobuf** for parsing GTFS-RT vehicle position feeds
- **NumPy** for vectorized array operations (performance-critical calculations)
- **pytest** for testing (smoke tests + integration tests)
- **ruff** for code linting and formatting

**Frontend:**
- **React 18** with Vite build tool
- **React Router** for client-side navigation
- **Recharts** for data visualization (trend charts)
- **React Leaflet** for interactive maps
- **WMATA Brand Guidelines** for colors and typography

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
│   └── archive/                   # Archived migration scripts (no longer needed)
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
├── frontend/              # React frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── RouteList.jsx    # Route scorecard table
│   │   │   ├── RouteDetail.jsx  # Individual route detail page
│   │   │   └── RouteMap.jsx     # Leaflet map with route visualization
│   │   ├── App.jsx              # Main application with routing
│   │   ├── App.css              # WMATA brand styling
│   │   └── main.jsx             # React entry point
│   ├── index.html               # HTML template with Leaflet CSS
│   ├── package.json             # Node.js dependencies
│   └── vite.config.js           # Vite build configuration
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
└── pyproject.toml       # Python dependencies, ruff config, pytest config
```

## Development Commands

### Initial Setup
```bash
# Install uv package manager
brew install uv

# Install dependencies (including dev dependencies)
uv sync --extra dev

# Set up PostgreSQL (REQUIRED)
createdb wmata_dashboard
echo "DATABASE_URL=postgresql://localhost/wmata_dashboard" >> .env
echo "WMATA_API_KEY=your_key_here" >> .env

# Initialize database and load GTFS static data (run once, takes 5-10 minutes)
uv run python scripts/init_database.py
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
#   GET /api/routes/{route_id}/trend?days=30&metric=otp - Time-series trend data
#   GET /api/routes/{route_id}/time-periods - Performance by time of day
#   GET /api/routes/{route_id}/shapes - GTFS shapes for map visualization
#   GET /api/routes/{route_id}/segments - Speed segments (disabled by default)

# Test API
curl http://localhost:8000/api/routes
curl http://localhost:8000/api/routes/C51
curl 'http://localhost:8000/api/routes/C51/trend?days=30&metric=otp'
```

### Running the Frontend
```bash
# Navigate to frontend directory
cd frontend

# Install dependencies (first time only)
npm install

# Start development server
npm run dev

# Frontend available at http://localhost:5173
# The dashboard connects to the backend API at http://localhost:8000
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
# Query PostgreSQL database directly
psql wmata_dashboard

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
7. **Frontend** → React dashboard with route scorecard, detail pages, charts, and maps

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
- `get_engine()`: Creates database engine with appropriate settings
- Supports both PostgreSQL (production) and SQLite (development) via `DATABASE_URL` env var
- Automatically configures connection pooling for PostgreSQL

**src/wmata_collector.py** - GTFS/GTFS-RT data collection
- `download_gtfs_static()`: Downloads and parses GTFS static zip file
- `get_realtime_vehicle_positions()`: Fetches GTFS-RT protobuf feed (PRIMARY)
- `get_bus_positions()`: Fetches WMATA BusPositions API (supplementary)
- `get_route_vehicles()`: Filters vehicles by route_id
- `_save_vehicle_positions()`: Bulk inserts vehicle positions
- `_save_bus_positions()`: Bulk inserts bus positions from WMATA API

**src/analytics.py** - Transit performance metrics
- `get_date_format_expr()`: Database-agnostic date formatting (PostgreSQL/SQLite compatibility)
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
  - `GET /api/routes/{route_id}/trend?days=30&metric=otp` - Time-series trend data
  - `GET /api/routes/{route_id}/time-periods` - Performance by time of day
  - `GET /api/routes/{route_id}/shapes` - GTFS shapes for map visualization
  - `GET /api/routes/{route_id}/segments` - Speed segments for map (vectorized with NumPy)

**api/aggregations.py** - API aggregation functions
- `get_all_routes_scorecard()`: Returns scorecard from pre-computed summary table (37ms response time)
- `get_route_detail_metrics()`: Returns detailed metrics for a route (uses RouteMetricsSummary)
- `get_route_trend_data()`: Time-series trend data (supports otp, headway, speed metrics)
- `get_route_speed_segments()`: Vectorized NumPy calculation for speed by route segment
- `get_route_time_period_summary()`: Performance metrics by time of day
- `calculate_performance_grade()`: Converts OTP percentage to letter grade (A-F)

**frontend/src/App.jsx** - Main React application
- React Router setup with routes for RouteList and RouteDetail
- WMATA branding (colors, typography)
- Responsive layout

**frontend/src/components/RouteList.jsx** - Route scorecard table
- Sortable, filterable table showing all routes
- Performance grades (A-F) with color coding
- Click navigation to route detail pages
- Sticky table headers

**frontend/src/components/RouteDetail.jsx** - Individual route page
- Route header with key metrics (OTP, headway, speed)
- Performance by time of day (bar chart)
- 30-day trend charts (OTP, headway, speed)
- Interactive route map with WMATA branding

**frontend/src/components/RouteMap.jsx** - Leaflet map component
- Displays route geometry from GTFS shapes
- Optional speed segments with color coding (disabled by default for performance)
- Auto-fits bounds to route extent
- WMATA red (#C8102E) for route lines

**pipelines/compute_daily_metrics.py** - Nightly batch job
- `convert_numpy_types()`: Converts NumPy types to Python native types (PostgreSQL compatibility)
- `compute_metrics_for_route_day()`: Computes all metrics for single route/day
- `compute_daily_metrics()`: Main function - computes for all routes over N days
- `compute_summary_metrics()`: Aggregates daily metrics into rolling summaries
- Stores results in `route_metrics_daily` and `route_metrics_summary` tables
- Database-agnostic date formatting for PostgreSQL/SQLite compatibility
- Run via cron: `0 2 * * * cd /path/to/wmata-dashboard && uv run python pipelines/compute_daily_metrics.py --days 7`

**scripts/migrate_sqlite_to_postgres.py** - Database migration script
- `migrate_data()`: Migrates vehicle positions, daily metrics, and summary metrics from SQLite to PostgreSQL
- Uses bulk insert operations for performance
- Batch processing for large datasets (1000 records per batch)
- Gracefully handles schema differences using `getattr()` with defaults

## Environment Variables

Required in `.env` file:
- `WMATA_API_KEY`: API key from https://developer.wmata.com (rate limit: 10 calls/sec, 50k/day)
- `DATABASE_URL`: PostgreSQL URI (recommended: `postgresql://localhost/wmata_dashboard`) or SQLite path for development (`sqlite:///./wmata_dashboard.db`)

## Performance Optimizations

### API Performance
- **Pre-computed Aggregations**: Nightly batch job computes metrics once, API reads from tables
- **Response Time**: 37ms for scorecard endpoint (vs 30+ seconds for live calculation)
- **Speedup**: ~1000x improvement by separating computation from serving

### Analytics Performance
- **Route Stops Caching**: 100x+ performance improvement for multi-route analysis
- **Trip Matching Fast Path**: ~90% of matches use RT trip_id directly (no position/time matching needed)
- **Batch Loading**: Load all stops/trips upfront rather than individual queries
- **NumPy Vectorization**: Use numpy arrays for distance/speed calculations (speed segments: ~10x faster)

### Frontend Performance
- **Speed Segments Disabled by Default**: Heavy calculation disabled by default, only computed on-demand
- **Pre-computation Philosophy**: Heavy calculations done in offline pipelines, not live in API endpoints

### Database Performance
- **PostgreSQL Benefits**: No write locks, concurrent read/write operations, better query optimization
- **SQLite Limitations**: Write locks block reads (pause collection during development), not suitable for production
- **Database-Agnostic Code**: All queries work on both PostgreSQL and SQLite with automatic detection
- **Type Conversion**: NumPy types automatically converted to Python native types for PostgreSQL compatibility

## Database Migration Notes

### PostgreSQL Migration (Completed October 2025)

The project successfully migrated from SQLite to PostgreSQL for production use. Key changes:

**Compatibility Fixes:**
1. **Date Formatting**: Added `get_date_format_expr()` helper function in both `analytics.py` and `compute_daily_metrics.py` to handle database-specific date formatting:
   - PostgreSQL: `to_char(timestamp, 'YYYYMMDD')`
   - SQLite: `strftime('%Y%m%d', timestamp)`

2. **NumPy Type Conversion**: Created `convert_numpy_types()` helper in `compute_daily_metrics.py` to convert NumPy types (np.float64, np.integer) to Python native types before database insertion. PostgreSQL interprets NumPy types as schema qualifiers, causing errors.

3. **Schema Fields**: Added `unique_vehicles` field to daily metrics calculation to track distinct vehicles per route/day.

**Migration Process:**
1. Created PostgreSQL database: `createdb wmata_dashboard`
2. Updated `.env` with `DATABASE_URL=postgresql://localhost/wmata_dashboard`
3. Initialized PostgreSQL schema: `scripts/init_database.py --no-confirm`
4. Migrated data: `scripts/migrate_sqlite_to_postgres.py` (vehicle positions, daily metrics, summary metrics)
5. Recomputed metrics: `pipelines/compute_daily_metrics.py --days 7 --recalculate`

**Benefits:**
- Concurrent read/write operations (no more write locks)
- Better query optimization and performance
- Production-ready reliability
- Proper type checking and data validation

**Backward Compatibility:**
- All code remains SQLite-compatible via environment detection
- Developers can use either database by setting `DATABASE_URL`
- Migration script available for easy transition

## Current Status & Next Steps

**Completed:**
- ✅ GTFS static data loading and database storage
- ✅ Complete GTFS schema with all fields and tables
- ✅ Real-time vehicle position collection (GTFS-RT VehiclePositions) - ALL 17 fields captured
- ✅ WMATA BusPositions API integration (supplementary)
- ✅ PostgreSQL production database (required for concurrent access and performance)
- ✅ Multi-level OTP calculations (stop/time-period/line level)
- ✅ Analytics layer with headway and speed calculations
- ✅ **Headway regularity metrics** (standard deviation, coefficient of variation for bunching detection)
- ✅ Trip matching with high accuracy
- ✅ Performance optimizations (caching, batch loading, NumPy vectorization)
- ✅ Validation of WMATA deviation data (found unreliable)
- ✅ Repository restructuring (src/, scripts/, tests/, debug/, api/, pipelines/)
- ✅ Comprehensive documentation (OTP_METHODOLOGY.md, SESSION_SUMMARY.md, README.md)
- ✅ CI/CD with GitHub Actions (ruff linting on PRs)
- ✅ GTFS shapes support for accurate distance/speed calculations
- ✅ Database migration scripts for schema updates
- ✅ **FastAPI REST API backend** with all major endpoints
- ✅ **Pre-computed aggregation system** (route_metrics_daily, route_metrics_summary)
- ✅ **Nightly batch job pipeline** (compute_daily_metrics.py)
- ✅ **Ruff linting integration** (PR checks, code quality enforcement)
- ✅ **React frontend dashboard** (Vite, React Router, Recharts, Leaflet)
- ✅ **Route scorecard table** with sorting, filtering, and headway regularity column
- ✅ **Route detail pages** with comprehensive metrics and charts
- ✅ **OTP breakdown visualization** (early/on-time/late percentages)
- ✅ **Trend charts** for all metrics: OTP, early%, late%, headway, headway regularity, speed
- ✅ **Route map visualization** with GTFS shapes and optional speed segments
- ✅ **Data collection frequency analysis** (60-second intervals validated as optimal)
- ✅ **PostgreSQL migration** with database-agnostic code and migration script
- ✅ **Database compatibility layer** (automatic detection and adaptation for PostgreSQL/SQLite)
- ✅ **"Last 7 days" date calculation fix** - Metrics use last data collection date as reference (robust to gaps)
- ✅ **API performance optimization** - Position statistics pre-computation (700ms → 18ms)
- ✅ **PostgreSQL connection pooling** - Production-ready database configuration
- ✅ **Date range fields in API** - Frontend displays actual metric time periods

**Next Steps (Priority Order):**

1. **Pre-Deployment Maintenance** (Current Priority)
   - Fix frontend map functionality (investigate API response/frontend integration)
   - Create comprehensive test suite (API smoke tests, integration tests)
   - Update CI/CD to run automated tests on PRs
   - Validate PostgreSQL migration complete and remove SQLite artifacts
   - Update documentation to reflect current state

2. **Production Deployment** (After Maintenance)
   - Deploy to cloud platform (DigitalOcean, AWS, Heroku, etc.)
   - Set up PostgreSQL database
   - Configure continuous data collection (systemd service or cloud scheduler)
   - Set up cron jobs for nightly metrics computation
   - Set up weekly GTFS refresh
   - Configure reverse proxy (Nginx or cloud provider)
   - Set up SSL certificates (Let's Encrypt or cloud provider)
   - Build and serve frontend static files
   - Implement monitoring and alerting

3. **Data Retention & Archival**
   - Implement data archival strategy (keep raw 60s data for 2-4 weeks)
   - Aggregate older data to 5-10 minute averages
   - Set up automated cleanup jobs
   - Target steady-state storage: ~1-2 GB

4. **Frontend Enhancements**
   - Add date range selectors for trend charts
   - Improve responsive design for mobile devices
   - Add loading states and error handling improvements
   - Export functionality (CSV, JSON downloads)

5. **API Enhancements**
   - Add pagination for large result sets
   - Add date range filtering parameters
   - Add caching layer (Redis) for frequently accessed data
   - Add API rate limiting
   - Add API authentication/authorization (if needed)

6. **Advanced Analytics & Features**
   - Automated bus bunching alerts
   - Service gap identification and alerting
   - Route reliability scoring (beyond just OTP)
   - Comparative route analysis dashboard
   - Historical performance comparisons (month-over-month, year-over-year)
   - Email/SMS alerts for service disruptions

**Important Notes:**
- **GTFS-based OTP is PRIMARY** - Don't use WMATA deviation as sole source (validation showed up to 7.7 min discrepancies)
- WMATA's GTFS-RT trip_ids DO match GTFS static trip_ids (100% match rate verified)
- All RT trip_ids have complete stop_times data in GTFS static (56-57 stops per trip typical for C51)
- Trip matching prioritizes RT trip_id for accuracy and performance (~90% fast path usage)
- Position/time-based matching serves as fallback for edge cases where RT trip_id is invalid
- Never infer the planned schedule from actual vehicle position data - always use GTFS static data
- BusPositions API useful for cross-validation and schedule discrepancy detection
- **Pre-computed aggregations are critical for API performance** - never calculate metrics live in API endpoints
- **When adding new functionality**, consider whether calculation is expensive; if so, use offline pre-computation
- Run nightly batch job to keep metrics up to date
- **PostgreSQL is now the primary database** - SQLite only for local development
- **Database compatibility**: Code automatically detects and adapts to PostgreSQL vs SQLite
- **SQLite write locks**: For SQLite development, pause data collection when using API/dashboard (write locks block reads)
- **NumPy type conversion**: All metrics automatically converted from NumPy to Python native types for PostgreSQL
- **60-second collection frequency is optimal** - provides good data quality with minimal API usage (1,440/50,000 daily limit)
- **22.75% match rate is expected and healthy** - buses spend 75-80% of time between stops, not at them

**Key Findings:**
- ~40% of bus arrivals are early (real operational pattern, not data error)
- LA Metro OTP standard (-1 to +5 min) is stricter than WMATA's published standard
- Polling-based collection gives ±30-60 second accuracy (acceptable for trend analysis)
- Route stops caching provides 100x+ performance improvement for multi-route analysis
- API response time: 37ms with pre-computed aggregations (vs 30+ seconds with live calculation)
- NumPy vectorization: ~10x speedup for speed segment calculations (3.4M → 350K operations)
- Speed segments disabled by default in frontend to avoid performance issues on route detail pages
- Match rate analysis shows 60s collection frequency is optimal:
  - Overall: 22.75% match rate (expected - buses between stops most of the time)
  - Top routes: 45-50% match rate (excellent for high-frequency service)
  - 256 positions per vehicle per day provides excellent temporal resolution
  - Less frequent collection (2-3 min) would lose 30-50% of matched arrivals

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

**Current Session (2025-10-27):**
- Preparing for production deployment with pre-deployment maintenance
- Fixed "last 7 days" date calculation to use last data collection date (robust to gaps)
- Optimized API performance with position statistics pre-computation (700ms → 18ms)
- Added date range fields to API responses (date_range_start, date_range_end)
- Fixed off-by-one error in date range calculation (was 8 days, now correctly 7 days)
- Created comprehensive test suite plan (API smoke tests, integration tests)
- Updated documentation to reflect current state and priorities

**Previous Session (2025-10-25):**
- Completed PostgreSQL migration from SQLite
- Fixed database compatibility issues (strftime vs to_char, NumPy types)
- Added database-agnostic helper functions for date formatting
- Created migration script for data transfer (vehicle positions, metrics)
- Recomputed metrics pipeline to populate new PostgreSQL tables
- Updated documentation (README.md, CLAUDE.md) with PostgreSQL setup and migration guide
- System now running on PostgreSQL in production with no write lock issues
- All API endpoints tested and working with PostgreSQL backend

**Previous Session (2025-10-19):**
- Added headway regularity metrics (standard deviation, coefficient of variation)
- Implemented bus bunching detection in analytics pipeline
- Created database migration script for new headway metrics columns
- Updated pipeline to compute and store headway std dev and CV
- Added OTP breakdown visualization (early/on-time/late stacked bar chart)
- Added 30-day trend charts for early%, late%, and headway regularity
- Added headway regularity stat card to route detail page
- Added headway regularity column to route scorecard table
- Updated API to support new trend metrics (early, late, headway_std_dev)
- Fixed API validation to accept new metric types
- Analyzed vehicle position match rate (22.75% - optimal for current use case)
- Created match rate analysis tools (`debug/analyze_match_rate.py`, `debug/match_rate_detailed_analysis.md`)
- Validated 60-second collection frequency as optimal balance
- Updated README.md and CLAUDE.md with latest features and findings

**Dashboard Status:**
- **Frontend**: Fully functional with all comprehensive features
  - Route scorecard table with sorting/filtering and headway regularity
  - Route detail pages with all performance metrics
  - OTP breakdown chart, 6 trend charts (OTP, early, late, headway, headway regularity, speed)
  - Interactive route maps with Leaflet
  - Time-period performance analysis
- **API**: All endpoints complete and performant (37ms response time)
- **Pipeline**: Processing 103 routes, computing all metrics including bunching detection
- **Data**: System-wide collection at 60s intervals (~550-630 vehicles/minute)

**Data Collection Analysis:**
- **Match Rate**: 22.75% overall (expected - buses spend most time between stops)
- **Top Routes**: 45-50% match rates (F20, D80, C53)
- **Collection Volume**: 243,016 positions → 55,275 arrivals per week
- **Temporal Resolution**: 256 positions per vehicle per day
- **API Usage**: 1,440 calls/day (well within 50,000/day limit)
- **Conclusion**: 60-second intervals optimal - less frequent would lose 30-50% of data

**Ready for Production:**
- All core features implemented and tested
- Frontend and backend fully integrated
- API endpoints complete and performant
- Data collection strategy validated and optimized
- Documentation updated and comprehensive
- Match rate analysis confirms current approach is optimal
- NumPy vectorization provides ~10x speedup for distance/proximity calculations
- SQLite write locks block reads - PostgreSQL required for production with concurrent access
- Speed segments expensive to compute on-demand - should be pre-computed or disabled by default
- Before committing new code to git, check if there are any linting or formatting errors with ruff.