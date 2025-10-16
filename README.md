# WMATA Performance Dashboard

A real-time transit performance dashboard for Washington DC Metro bus and rail lines, inspired by the TransitMatters Dashboard. Tracks on-time performance (OTP), headways, and average speeds using WMATA's GTFS and real-time vehicle position feeds.

## Features

- **Real-time Data Collection**: Polls WMATA's GTFS-RT feed every 60 seconds to track vehicle positions
- **Performance Metrics**: Calculates on-time performance, headways, and average speeds for all routes
- **Multi-level Analysis**:
  - Stop-level OTP (performance at specific stops)
  - Time-period OTP (AM Peak, Midday, PM Peak, Evening, Night)
  - Line-level OTP (overall route performance)
- **Fast API**: Pre-computed aggregation tables for sub-100ms API responses
- **REST API**: FastAPI backend serving route scorecards and detailed metrics
- **Database**: SQLite for development, PostgreSQL-ready for production

## Quick Start

### Prerequisites

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) package manager
- WMATA API key from https://developer.wmata.com

### Installation

```bash
# Install uv package manager
brew install uv

# Clone the repository
git clone https://github.com/yourusername/wmata-dashboard.git
cd wmata-dashboard

# Install dependencies
uv sync

# Create .env file with your WMATA API key
echo "WMATA_API_KEY=your_key_here" > .env

# Initialize database and load GTFS static data (takes 5-10 minutes)
uv run python scripts/init_database.py
```

### Running the Application

#### 1. Collect Sample Data

```bash
# Collect data for a specific route (20 cycles = ~20 minutes)
uv run python scripts/collect_sample_data.py C51 20

# Collect system-wide data (all vehicles)
uv run python scripts/collect_sample_data.py all 120
```

#### 2. Compute Metrics

```bash
# Compute daily metrics for all routes with data
uv run python pipelines/compute_daily_metrics.py --days 7

# Compute metrics for a specific route
uv run python pipelines/compute_daily_metrics.py --route C51 --days 7
```

#### 3. Start the API Server

```bash
uv run uvicorn api.main:app --reload
```

The API will be available at `http://localhost:8000`

### API Endpoints

- `GET /api/routes` - Get scorecard for all routes
- `GET /api/routes/{route_id}` - Get detailed metrics for a specific route
- `GET /api/routes/{route_id}/trends` - Get time-series trend data
- `GET /api/routes/{route_id}/time-periods` - Get performance by time of day

Example:
```bash
curl http://localhost:8000/api/routes
curl http://localhost:8000/api/routes/C51
```

## Architecture

### Data Pipeline

1. **GTFS Static Data** → Downloaded from WMATA (routes, stops, trips, schedules)
2. **GTFS-RT Data** → Polled every 60s (real-time vehicle positions)
3. **Database** → Stores schedule data and position snapshots
4. **Nightly Batch Job** → Computes daily metrics (OTP, headway, speed)
5. **API** → Serves pre-computed metrics with <100ms response times

### Database Schema

**Static Data (GTFS):**
- Routes, Stops, Trips, StopTimes, Shapes
- Agencies, Calendar, CalendarDates, FeedInfo

**Real-time Data:**
- VehiclePosition - GTFS-RT vehicle positions (primary)
- BusPosition - WMATA BusPositions API (supplementary)

**Aggregations:**
- RouteMetricsDaily - Daily performance metrics per route
- RouteMetricsSummary - Rolling 7-day summaries for API

### Key Modules

- `src/database.py` - Database connection and session management
- `src/models.py` - SQLAlchemy ORM models
- `src/wmata_collector.py` - GTFS/GTFS-RT data collection
- `src/analytics.py` - OTP, headway, and speed calculations
- `src/trip_matching.py` - Match real-time vehicles to scheduled trips
- `api/main.py` - FastAPI application
- `api/aggregations.py` - API aggregation functions
- `pipelines/compute_daily_metrics.py` - Nightly metrics computation

## Development

### Project Structure

```
wmata-dashboard/
├── src/                    # Core modules
│   ├── analytics.py        # Performance calculations
│   ├── database.py         # DB connection
│   ├── models.py          # ORM models
│   ├── trip_matching.py   # Trip matching logic
│   └── wmata_collector.py # Data collection
├── api/                    # FastAPI backend
│   ├── main.py            # API routes
│   └── aggregations.py    # Aggregation functions
├── pipelines/             # Data processing jobs
│   └── compute_daily_metrics.py
├── scripts/               # Utility scripts
│   ├── init_database.py
│   ├── collect_sample_data.py
│   └── continuous_collector.py
├── tests/                 # Test files
├── debug/                 # Debug scripts
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
```

### Database Access

```bash
# Query SQLite database directly
sqlite3 wmata_dashboard.db

# Example queries:
sqlite> SELECT COUNT(*) FROM vehicle_positions;
sqlite> SELECT * FROM routes WHERE route_short_name = 'C51';
sqlite> SELECT * FROM route_metrics_summary;
```

## Production Deployment

### Data Collection Strategy

**Collection Volume (60-second intervals, system-wide):**
- ~500 active vehicles per collection
- ~720,000 records/day (~13 MB/day)
- ~400 MB/month

**Recommended Approach:**
- Collect at 60-second intervals for maximum granularity
- Keep raw 60-second data for 2-4 weeks
- Aggregate older data to 5-10 minute averages
- Results in ~1-2 GB steady-state storage

### Deployment Steps

1. **Set up PostgreSQL database**
   ```bash
   # Add to .env:
   DATABASE_URL=postgresql://user:pass@host/dbname
   ```

2. **Initialize database**
   ```bash
   uv run python scripts/init_database.py
   ```

3. **Set up continuous data collection** (systemd, supervisor, etc.)
   ```bash
   uv run python scripts/continuous_collector.py
   ```

4. **Set up nightly metrics computation** (cron job)
   ```bash
   0 2 * * * cd /path/to/wmata-dashboard && uv run python pipelines/compute_daily_metrics.py --days 7
   ```

5. **Run API server** (gunicorn, uvicorn, etc.)
   ```bash
   uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
   ```

6. **Weekly GTFS refresh** (cron job)
   ```bash
   0 3 * * 0 cd /path/to/wmata-dashboard && uv run python scripts/reload_gtfs_complete.py
   ```

## Documentation

- **CLAUDE.md** - Project context and development guide for Claude Code
- **docs/OTP_METHODOLOGY.md** - Detailed OTP calculation methodology
- **docs/SESSION_SUMMARY.md** - Development session notes and findings

## Performance Notes

- **API Response Time**: 37ms for scorecard endpoint (1000x faster than live calculation)
- **OTP Calculation**: Uses GTFS-based matching (WMATA deviation data found unreliable)
- **Trip Matching**: ~90% use fast path (RT trip_id), fallback to position/time matching
- **Caching**: Route stops caching provides 100x+ performance improvement

## Key Findings

- ~40% of bus arrivals are early (real operational pattern, not data error)
- LA Metro OTP standard (-1 to +5 min) is stricter than WMATA's published standard
- Polling-based collection gives ±30-60 second accuracy (acceptable for trend analysis)
- WMATA's GTFS-RT trip_ids match GTFS static trip_ids with 100% accuracy

## Environment Variables

Create a `.env` file with:

```bash
WMATA_API_KEY=your_api_key_here
DATABASE_URL=sqlite:///./wmata_dashboard.db  # or PostgreSQL URI for production
```

## License

MIT
