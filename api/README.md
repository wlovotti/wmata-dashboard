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

### `GET /api/routes/{route_id}/segments`
Get average speed by route segment for map visualization.

**Path Parameters:**
- `route_id` (string): Route identifier

**Query Parameters:**
- `days` (int, default=7): Number of days to analyze

**Response:**
```json
{
  "route_id": "C51",
  "segments": [
    {
      "from_stop": "STOP_A",
      "to_stop": "STOP_B",
      "from_coords": [38.9, -77.0],
      "to_coords": [38.91, -77.01],
      "avg_speed_mph": 15.2,
      "speed_category": "normal",
      "shape_points": [[38.9,-77.0], [38.901,-77.005]]
    }
  ]
}
```

### `GET /api/routes/{route_id}/time-periods`
Get performance metrics by time of day.

**Path Parameters:**
- `route_id` (string): Route identifier

**Query Parameters:**
- `days` (int, default=7): Number of days to analyze

**Response:**
```json
{
  "route_id": "C51",
  "periods": {
    "AM Peak (6-9)": {
      "otp_percentage": 65.0,
      "avg_headway_minutes": 15.0
    },
    "Midday (9-15)": {
      "otp_percentage": 72.0,
      "avg_headway_minutes": 20.0
    }
  }
}
```

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
- ✅ GET /api/routes/{route_id}/time-periods (time-of-day breakdown)

**TODO:**
- ⏳ GET /api/routes/{route_id}/trend (daily time-series)
- ⏳ GET /api/routes/{route_id}/segments (speed by segment)
