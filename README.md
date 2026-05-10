# WMATA Performance Dashboard

A real-time transit performance dashboard for Washington DC Metro bus and rail lines, inspired by the TransitMatters Dashboard. Tracks on-time performance (OTP), headways, and average speeds using WMATA's GTFS and real-time vehicle position feeds.

## Features

- **Web Dashboard**: React-based frontend with route scorecards, detail pages, performance charts, and interactive maps
- **Real-time Data Collection**: Polls WMATA's GTFS-RT feed every 60 seconds to track vehicle positions
- **Performance Metrics**:
  - On-time performance (OTP) with early/on-time/late breakdown
  - Headway analysis with bunching detection (standard deviation, coefficient of variation)
  - Average speed calculations
  - Service regularity metrics
- **Multi-level Analysis**:
  - Stop-level OTP (performance at specific stops)
  - Time-period OTP (AM Peak, Midday, PM Peak, Evening, Night)
  - Line-level OTP (overall route performance)
  - 30-day trend analysis for all metrics
- **Fast API**: Pre-computed aggregation tables for sub-100ms API responses
- **REST API**: FastAPI backend serving route scorecards and detailed metrics
- **Interactive Maps**: Leaflet-based route visualization with WMATA branding
- **Database**: PostgreSQL for production, SQLite also supported for development

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+ (for frontend)
- [uv](https://github.com/astral-sh/uv) package manager
- PostgreSQL 12+ (recommended) or SQLite for development
- WMATA API key from https://developer.wmata.com

### Installation

```bash
# Install uv package manager
brew install uv

# Clone the repository
git clone https://github.com/yourusername/wmata-dashboard.git
cd wmata-dashboard

# Install Python dependencies
uv sync

# Install frontend dependencies
cd frontend
npm install
cd ..

# Create .env file with your WMATA API key and database URL
cat > .env << EOF
WMATA_API_KEY=your_key_here
DATABASE_URL=postgresql://localhost/wmata_dashboard
EOF

# Create PostgreSQL database (skip if using SQLite)
createdb wmata_dashboard

# Initialize database and load GTFS static data (takes 5-10 minutes)
uv run python scripts/init_database.py --no-confirm
```

### Running the Application

#### 1. Collect Sample Data

```bash
# Collect data for a specific route (20 cycles = ~20 minutes)
uv run python scripts/collect_sample_data.py C51 20

# Collect system-wide data (all vehicles, recommended)
uv run python scripts/collect_sample_data.py all 120

# Note: For development with SQLite, you may need to stop data collection
# temporarily to avoid database locks when using the API/dashboard
```

#### 2. Run the Daily Batch

```bash
# Derive stop_events / runs / bunching for yesterday and any catch-up
# dates, then upsert system_metrics_daily for each.
uv run python pipelines/run_daily_batch.py
```

#### 3. Start the Backend API

```bash
uv run uvicorn api.main:app --reload
```

The API will be available at `http://localhost:8000`

#### 4. Start the Frontend

```bash
cd frontend
npm run dev
```

The dashboard will be available at `http://localhost:5173`

### API Endpoints

- `GET /api/routes` - Get scorecard for all routes (live OTP split, service-delivered, EWT, bunching, composite grade)
- `GET /api/routes/{route_id}` - Get detailed metrics for a specific route
- `GET /api/routes/{route_id}/trend?days=30&metric=<metric>` - Get time-series trend data
  - Supported metrics: `otp`, `service_delivered`, `excess_trip_time`
- `GET /api/routes/{route_id}/time-periods` - Get performance by time of day
- `GET /api/routes/{route_id}/shapes` - Get GTFS shapes for map visualization

Example:
```bash
curl http://localhost:8000/api/routes
curl http://localhost:8000/api/routes/C51
curl 'http://localhost:8000/api/routes/C51/trend?days=30&metric=otp'
```

## Architecture

### Data Pipeline

1. **GTFS Static Data** → Downloaded from WMATA (routes, stops, trips, schedules, shapes)
2. **GTFS-RT Data** → Polled every 60s (vehicle positions + trip updates)
3. **Database** → Stores schedule data, raw position/trip-update snapshots
4. **Daily Batch** → Derives `stop_events` (proximity + trip_update sources),
   aggregates them into `runs`, then computes per-route bunching and the
   system-wide rollup (`system_metrics_daily`). Orchestrated by
   `pipelines/run_daily_batch.py`.
5. **API** → FastAPI serves a mix of materialized aggregates
   (`system_metrics_daily`) and live overlay metrics (per-route OTP,
   service-delivered, EWT, bunching) computed from `stop_events` / `runs`
6. **Frontend** → React dashboard with charts and maps

### Database Schema

**Static Data (GTFS):**
- Routes, Stops, Trips, StopTimes, Shapes
- Agencies, Calendar, CalendarDates, FeedInfo

**Real-time Data:**
- VehiclePosition - GTFS-RT vehicle positions
- TripUpdateSnapshot - GTFS-RT trip updates

**Foundation tables:**
- StopEvent - per (trip, stop) observed arrival/skip from proximity + trip_update sources
- Run - per (trip, source) trip-level aggregate over StopEvent
- SystemMetricsDaily - one row per service_date with OTP / service_delivered / EWT / bunching for the system

### Key Modules

**Backend:**
- `src/database.py` - Database connection and session management
- `src/models.py` - SQLAlchemy ORM models
- `src/wmata_collector.py` - GTFS/GTFS-RT data collection
- `src/analytics.py` - OTP, headway, and speed calculations
- `src/trip_matching.py` - Match real-time vehicles to scheduled trips
- `src/system_metrics.py` - Per-date system rollup compute + upsert
- `api/main.py` - FastAPI application
- `api/aggregations.py` - API aggregation functions
- `pipelines/run_daily_batch.py` - Nightly batch orchestrator

**Frontend:**
- `frontend/src/App.jsx` - Main application with routing
- `frontend/src/components/RouteList.jsx` - Route scorecard table
- `frontend/src/components/RouteDetail.jsx` - Individual route detail page
- `frontend/src/components/RouteMap.jsx` - Leaflet map with route visualization

## Development

### Project Structure

```
wmata-dashboard/
├── src/                    # Core Python modules
│   ├── analytics.py        # Performance calculations
│   ├── database.py         # DB connection
│   ├── models.py          # ORM models
│   ├── trip_matching.py   # Trip matching logic
│   └── wmata_collector.py # Data collection
├── api/                    # FastAPI backend
│   ├── main.py            # API routes
│   └── aggregations.py    # Aggregation functions
├── pipelines/             # Data processing jobs
│   └── run_daily_batch.py
├── scripts/               # Utility scripts
│   ├── init_database.py
│   ├── collect_sample_data.py
│   └── continuous_collector.py
├── frontend/              # React frontend
│   ├── src/
│   │   ├── components/
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── package.json
│   └── vite.config.js
├── tests/                 # Test files
└── docs/                  # Documentation
```

### Running Tests

```bash
# Run analytics tests
uv run python tests/test_analytics.py
uv run python tests/test_multi_level_otp.py
uv run python tests/test_otp_with_matching.py
```

### Code Quality

```bash
# Lint code
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
wmata_dashboard=# SELECT COUNT(*) FROM vehicle_positions;
wmata_dashboard=# SELECT * FROM routes WHERE route_short_name = 'C51';
wmata_dashboard=# SELECT * FROM system_metrics_daily ORDER BY service_date DESC LIMIT 7;

# For SQLite (if using SQLite instead of PostgreSQL)
sqlite3 wmata_dashboard.db
```

## Database Migration

### Migrating from SQLite to PostgreSQL

If you previously used SQLite and want to migrate to PostgreSQL:

```bash
# 1. Set up PostgreSQL database
createdb wmata_dashboard

# 2. Update .env to point to PostgreSQL
echo "DATABASE_URL=postgresql://localhost/wmata_dashboard" >> .env

# 3. Initialize PostgreSQL database schema
uv run python scripts/init_database.py --no-confirm

# 4. Migrate collected data from SQLite (optional - or start fresh)
uv run python scripts/migrate_sqlite_to_postgres.py

# 5. Run the daily batch to populate stop_events / runs / system_metrics_daily
uv run python pipelines/run_daily_batch.py
```

**Note:** The migration script copies vehicle position data and trip-update
snapshots. The aggregation tables (`stop_events`, `runs`,
`system_metrics_daily`) are re-derived by the daily batch.

GTFS static data (routes, stops, trips, etc.) is reloaded from WMATA API via `init_database.py` to ensure consistency.

### PostgreSQL Benefits

- **Concurrent Access**: No write locks - data collection and API can run simultaneously
- **Performance**: Better query optimization for large datasets
- **Production Ready**: Designed for 24/7 operation with high availability
- **Type Safety**: Stricter type checking prevents data quality issues

### Database Compatibility

The codebase is database-agnostic and works with both SQLite and PostgreSQL:
- Date formatting functions detect database type automatically
- NumPy type conversion ensures PostgreSQL compatibility
- All queries tested on both databases

## Production Deployment

### Data Collection Strategy

**Collection Volume (60-second intervals, system-wide):**
- ~500 active vehicles per collection
- ~720,000 records/day (~13 MB/day)
- ~400 MB/month

**Recommended Approach:**
- **Use PostgreSQL** (not SQLite) to avoid database locking issues
- Collect at 60-second intervals continuously for real-time monitoring
- Keep raw 60-second data for 2-4 weeks
- Aggregate older data to 5-10 minute averages
- Results in ~1-2 GB steady-state storage

### Deployment Steps

1. **Set up PostgreSQL database**
   ```bash
   # Create database
   createdb wmata_dashboard

   # Add to .env:
   DATABASE_URL=postgresql://localhost/wmata_dashboard
   ```

2. **Initialize database**
   ```bash
   uv run python scripts/init_database.py --no-confirm
   ```

3. **Set up continuous data collection** (systemd, supervisor, etc.)
   ```bash
   uv run python scripts/continuous_collector.py
   ```

4. **Set up nightly metrics computation** (cron / launchd / systemd timer)
   ```bash
   0 2 * * * cd /path/to/wmata-dashboard && uv run python pipelines/run_daily_batch.py
   ```

5. **Run API server** (gunicorn, uvicorn, etc.)
   ```bash
   uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
   ```

6. **Build and serve frontend**
   ```bash
   cd frontend
   npm run build
   # Serve dist/ directory with nginx or other web server
   ```

7. **Weekly GTFS refresh** (cron job)
   ```bash
   0 3 * * 0 cd /path/to/wmata-dashboard && uv run python scripts/reload_gtfs_complete.py
   ```

## Documentation

- **CLAUDE.md** - Project context and development guide for Claude Code
- **docs/OTP_METHODOLOGY.md** - Detailed OTP calculation methodology
- **docs/SESSION_SUMMARY.md** - Development session notes and findings

## Performance Notes

- **API Response Time**: 37ms for scorecard endpoint (1000x faster than live calculation)
- **Frontend Performance**: Trend charts and maps optimized for fast loading
- **OTP Calculation**: Uses GTFS-based matching (WMATA deviation data found unreliable)
- **Trip Matching**: ~90% use fast path (RT trip_id), fallback to position/time matching
- **Database**: SQLite for dev (may require pausing collection), PostgreSQL for production
- **Pre-computation**: All heavy calculations done in nightly pipeline, not on-demand
- **Match Rate**: 22.75% overall (vehicle positions to stop arrivals) - expected and healthy
  - Top routes achieve 45-50% match rates
  - Low overall rate is normal (buses spend 75-80% of time between stops)

## Key Findings

- ~40% of bus arrivals are early (real operational pattern, not data error)
- LA Metro OTP standard (-1 to +5 min) is stricter than WMATA's published standard
- Polling-based collection gives ±30-60 second accuracy (acceptable for trend analysis)
- WMATA's GTFS-RT trip_ids match GTFS static trip_ids with 100% accuracy
- 60-second collection frequency is optimal for headway/bunching analysis
  - Provides 256 positions per vehicle per day
  - Well within WMATA API limits (1,440 calls/day vs 50,000 limit)
  - Less frequent collection (2-3 min) would lose 30-50% of data

## Environment Variables

Create a `.env` file with:

```bash
WMATA_API_KEY=your_api_key_here
DATABASE_URL=postgresql://localhost/wmata_dashboard  # PostgreSQL (recommended)
# DATABASE_URL=sqlite:///./wmata_dashboard.db  # SQLite (development only)
```

## License

MIT
