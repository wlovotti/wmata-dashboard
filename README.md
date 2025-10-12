# WMATA Performance Dashboard

Performance metrics dashboard for Washington DC Metro bus and rail lines.

## Setup

1. Install uv: `brew install uv`
2. Clone this repository
3. Get a WMATA API key from https://developer.wmata.com
4. Create a `.env` file in the project root with: `WMATA_API_KEY=your_key_here`
5. Install dependencies: `uv sync`
6. Run: `uv run python wmata_collector.py`

## Current Status

### Data Collection
- [x] GTFS Static data (routes, stops, schedules)
- [x] GTFS-RT VehiclePositions (real-time locations) - PRIMARY
- [x] WMATA BusPositions API (proprietary, includes deviation) - SUPPLEMENTARY
- [x] SQLite database with 60-second polling

### Analytics Implemented
- [x] Multi-level OTP calculations (stop, time-period, line)
- [x] Trip matching (GTFS-RT to GTFS static)
- [x] Headway calculations using closest approach method
- [x] Validation of WMATA deviation data (found unreliable)
- [x] Performance optimizations (route stops caching)

### Documentation
- [x] OTP methodology documented (`docs/OTP_METHODOLOGY.md`)
- [x] Project context (`CLAUDE.md`)
- [x] Session summary (`docs/SESSION_SUMMARY.md`)

### Next Steps
- [ ] Visualizations and dashboard UI
- [ ] API/backend for serving metrics
- [ ] Real-time data collection service

## License

MIT
