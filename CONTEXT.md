# Project Context

## Goal
Building a WMATA performance dashboard inspired by TransitMatters Dashboard (https://dashboard.transitmatters.org)

## Current Status
- Using Python with uv for package management
- Successfully collecting C51 bus data via WMATA API
- Real-time vehicle positions working
- GTFS static data parsing working

## Next Steps
1. Add database storage (PostgreSQL or SQLite)
2. Store vehicle positions every 30-60 seconds
3. Calculate headways and on-time performance
4. Build FastAPI backend
5. Create React dashboard

## Key Files
- wmata_collector.py - Main data collector
- .env - Contains WMATA_API_KEY (not in git)

## API Details
- Rate limit: 10 calls/sec, 50,000 calls/day
- Using GTFS and GTFS-RT feeds
- Focus on C51 bus route initially
