# On-Time Performance (OTP) Methodology

## Overview

This dashboard uses **GTFS-based calculations as the primary OTP metric**. WMATA's BusPositions API was evaluated as a supplementary source but its `Deviation` field was found unreliable (up to 7.7 min discrepancies vs GTFS) and is no longer used.

## Primary Method: GTFS-Based OTP

### Data Sources
- **GTFS Static**: Schedule data (routes, trips, stops, stop_times)
- **GTFS-RT VehiclePositions**: Real-time bus locations every 60 seconds

### Calculation Method

1. **Trip Matching** (`src/trip_matching.py`)
   - Match real-time vehicle positions to scheduled GTFS trips
   - Uses trip_id (fast path ~90% success) or position/time matching (fallback)
   - Minimum confidence threshold: 0.3

2. **Stop Proximity** (`find_nearest_stop()`)
   - Find nearest scheduled stop to vehicle position
   - Threshold: 50 meters (strict)
   - Cached per-route for performance

3. **Schedule Comparison**
   - Look up scheduled arrival time from GTFS stop_times
   - Calculate difference: actual_time - scheduled_time
   - Classify as early/on-time/late

### OTP Thresholds (LA Metro Standard)

- **Early**: More than 1 minute early (< -60 seconds)
- **On-time**: Between 1 min early and 5 min late (-60 to +300 seconds)
- **Late**: More than 5 minutes late (> +300 seconds)

Note: This is stricter than WMATA's published standard (-2 to +7 minutes)

### Three Levels of Analysis

#### 1. Stop-Level OTP
`calculate_stop_level_otp(db, route_id, stop_id)`

Analyzes reliability at a specific stop on a route. Useful for identifying problem areas.

#### 2. Time-Period OTP
`calculate_time_period_otp(db, route_id)`

Breaks down OTP by time of day:
- AM Peak (6:00-9:00)
- Midday (9:00-15:00)
- PM Peak (15:00-19:00)
- Evening (19:00-24:00)
- Night (0:00-6:00)

#### 3. Line-Level OTP
`calculate_line_level_otp(db, route_id)`

Overall route performance. Uses simple average (all arrivals weighted equally).

## Why BusPositions Was Rejected

WMATA's `BusPositions` JSON API exposes a `Deviation` field (schedule adherence in minutes) that looked promising as a supplementary OTP source. Validation showed it diverges from GTFS-based calculations by up to 7.7 minutes on ~25% of observations (avg difference 2.48 min), most likely because WMATA uses internal schedules that differ from the published GTFS feed. The integration was removed; GTFS-based OTP is the only source.

## Accuracy Limitations

### Polling-Based Data Collection
- Vehicle positions collected every ~60 seconds
- **Cannot capture exact arrival times** at stops
- Uses "closest approach" method for headway analysis

### Accuracy Estimates
- **OTP**: ±30-60 seconds depending on polling frequency
- **Headway**: ±30-60 seconds depending on bus speed and polling

### Good For:
- Identifying trends and patterns
- Comparing routes
- Detecting systematic issues (bunching, gaps, chronic lateness)

### Not Suitable For:
- Contractual enforcement
- Fine-grained performance metrics
- Legal/regulatory reporting

## Implementation Files

- `src/analytics.py`: All OTP calculation functions
- `src/trip_matching.py`: GTFS-RT to GTFS static matching
- `src/models.py`: Database models (VehiclePosition)
- `src/wmata_collector.py`: Data collection from APIs

## Validation Scripts

- `debug/check_early_arrivals.py`: Analyze distribution of lateness
- `tests/test_multi_level_otp.py`: Test all three OTP levels

## References

- **LA Metro OTP Standard**: -1 to +5 minutes
- **WMATA Published Standard**: -2 to +7 minutes (not used)
- **GTFS Specification**: https://gtfs.org/
- **GTFS Realtime**: https://gtfs.org/realtime/
