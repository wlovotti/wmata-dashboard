# Project Context

## Goal
Building a WMATA performance dashboard inspired by TransitMatters Dashboard (https://dashboard.transitmatters.org)

## Current Status ✓
- ✓ Python with uv for package management
- ✓ SQLAlchemy database integration (SQLite for local, PostgreSQL-ready for production)
- ✓ GTFS static data loaded into database (125 routes, 7,505 stops, 130k+ trips)
- ✓ Real-time vehicle position collection working
- ✓ C51 bus data collection and storage working
- ✓ Database models: Route, Stop, Trip, StopTime, VehiclePosition

## Database
- **Local**: SQLite (`wmata_dashboard.db`)
- **Production**: PostgreSQL (configure via `DATABASE_URL` env var)
- **Current data**: 125 routes, 7,505 stops, 130,665 trips, 479,000 stop times
- **Query**: `sqlite3 wmata_dashboard.db` for SQL access

## Next Steps
1. ~~Add database storage~~ ✓ DONE
2. Set up continuous data collection (deploy to server)
   - Deploy to cloud server (DigitalOcean, AWS EC2, etc.)
   - Set up PostgreSQL database
   - Run `continuous_collector.py` as a service
3. Calculate headways from collected vehicle positions
4. Calculate on-time performance (compare actual vs scheduled)
5. Build FastAPI backend with REST API endpoints
6. Create React dashboard frontend

## Key Files
- **models.py** - SQLAlchemy database models
- **database.py** - Database connection and initialization
- **wmata_collector.py** - Test collector for C51 route (quick test)
- **init_database.py** - One-time database setup (loads GTFS data)
- **continuous_collector.py** - Continuous collection every 60s (for production)
- **.env** - Contains WMATA_API_KEY and DATABASE_URL (not in git)

## Running the Project

### Initial Setup (run once)
```bash
uv sync
python init_database.py  # Downloads GTFS data, takes 5-10 minutes
```

### Test Data Collection
```bash
python wmata_collector.py  # Quick test - collects C51 vehicle positions
```

### Continuous Collection (for production)
```bash
python continuous_collector.py  # Runs every 60 seconds
```

## API Details
- Rate limit: 10 calls/sec, 50,000 calls/day
- Using GTFS and GTFS-RT feeds
- Focus on C51 bus route initially
- Real-time API: ~200 active buses across all routes
