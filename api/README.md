# WMATA Performance Dashboard API

FastAPI backend serving transit performance metrics for the web dashboard.

## Quick Start

```bash
# Start the development server
uv run uvicorn api.main:app --reload

# Server will be available at:
# - API: http://localhost:8000
# - Interactive docs: http://localhost:8000/docs
# - Alternative docs: http://localhost:8000/redoc
```

## API Endpoints

### `GET /api/routes`
Get performance scorecard for all routes.

**Query Parameters:**
- `days` (int, default=7): Number of days to analyze

**Response:**
```json
[
  {
    "route_id": "C51",
    "route_name": "C51",
    "route_long_name": "Langley - Pentagon City",
    "otp_percentage": 67.5,
    "avg_headway_minutes": 18.2,
    "avg_speed_mph": 11.3,
    "grade": "B",
    "total_observations": 1234,
    "data_updated_at": "2025-10-15T21:00:00"
  }
]
```

### `GET /api/routes/{route_id}`
Get detailed metrics for a specific route.

**Path Parameters:**
- `route_id` (string): Route identifier (e.g., "C51")

**Query Parameters:**
- `days` (int, default=7): Number of days to analyze

**Response:**
```json
{
  "route_id": "C51",
  "route_name": "C51",
  "otp_percentage": 67.5,
  "avg_headway_minutes": 18.2,
  "avg_speed_mph": 11.3,
  "total_arrivals_analyzed": 1234,
  "grade": "B"
}
```

### `GET /api/routes/{route_id}/trend`
Get time-series trend data for a metric.

**Path Parameters:**
- `route_id` (string): Route identifier

**Query Parameters:**
- `metric` (string): One of "otp", "headway", "speed"
- `days` (int, default=30): Number of days to analyze

**Response:**
```json
{
  "route_id": "C51",
  "metric": "otp",
  "time_series": [
    {"date": "2025-10-01", "value": 65.2},
    {"date": "2025-10-02", "value": 68.1"}
  ],
  "avg": 67.5,
  "trend": "improving"
}
```

### `GET /api/routes/{route_id}/time-periods` (deprecated — returns 501)
Legacy VehiclePosition-based time-of-day OTP breakdown. Predates the
stop_events/runs architecture that is now the source of truth for
per-route metrics (see CLAUDE.md); the underlying query is an uncapped
7-day `get_vehicle_positions` pull with per-row Python loops. Guarded to
return `501 Not Implemented` rather than run (NOTES-114). Use
`GET /api/routes/{route_id}/period-drilldown` for the stop_events-based
per-time-period EWT/bunching breakdown instead.

## Project Structure

```
api/
├── __init__.py
├── main.py            # FastAPI app and route definitions
├── aggregations.py    # Analytics aggregation functions
└── README.md          # This file
```

## Development

### Running Tests
```bash
# Run API with hot reload
uv run uvicorn api.main:app --reload --port 8000

# Test endpoint with curl
curl http://localhost:8000/api/routes

# Or use the interactive docs
open http://localhost:8000/docs
```

### Performance Notes

- Route scorecard uses `sample_rate=3` for faster computation
- Individual route queries use `sample_rate=1` for accuracy
- Future: Pre-computed aggregation tables for instant responses

## Status

**Implemented:**
- ✅ GET /api/routes (all routes scorecard)
- ✅ GET /api/routes/{route_id} (route detail)
- 🚫 GET /api/routes/{route_id}/time-periods (deprecated, returns 501 — see above)

**TODO:**
- ⏳ GET /api/routes/{route_id}/trend (daily time-series)
