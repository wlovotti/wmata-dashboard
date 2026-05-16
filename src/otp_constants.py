"""
On-time performance window constants.

Centralizes the early/late thresholds used to classify schedule deviations
across all OTP calculations. Defined here so future tightening (e.g.
NOTES.md NOTES-20 rider-experience window) is a one-line change.

Aligned with WMATA's published scorecard standard for schedule-based
timepoints: -2 minutes early to +7 minutes late.

Frequent-service routes — two levels of designation
---------------------------------------------------
There are two related-but-distinct notions of "frequent" in the codebase;
do not conflate them.

1. WMATA's route-level designation (config/frequent_routes.yaml).
   The authoritative list pulled from WMATA's High-Frequency Metrobus
   Service Maps (Better Bus, June 2025). Loaded via
   `src/frequent_routes.py:load_frequent_route_ids()`. Drives headline-KPI
   choice in the UI — EWT is the headline for these routes, OTP for the
   rest. WMATA's published criteria: ≤12 min headways (high-frequency)
   or ≤20 min headways (medium-frequency), held across the 7am-9pm
   all-day-all-week window.

2. Data-driven per-cell-hour gate (`src/ewt.py:FREQUENT_HEADWAY_MAX_SEC`,
   15 min). Operates on `(direction, stop, hour)` cells: a cell is
   frequent iff its own mean scheduled headway is ≤ 15 min. This is
   what feeds AWT/SWT — branches that aren't frequent at a given hour
   drop out automatically, so pooling stays rider-faithful even on
   routes with mixed-frequency branches.

The historical illustrative list of headway-based routes — (70, 79, X2,
90, 92, 16Y, Metroway) — is preserved below for context. That was a
pre-Better-Bus example of where WMATA's `scheduled_headway + 3 min`
rule would have applied; it is NOT the current authoritative list. Use
`config/frequent_routes.yaml` for any route-level frequent-service
decision today, and `FREQUENT_HEADWAY_MAX_SEC` for any cell-hour-level
computation.

Out of scope here, tracked separately:
- `scheduled_headway + 3 min` OTP rule for headway-based routes. WMATA
  does not publish a frequencies.txt in their bus GTFS feed, so any
  implementation would have to derive scheduled headways per cell-hour
  from stop_times. Not in current code; the EWT metric covers
  rider-experience for frequent routes adequately on its own.
- Timepoint-only filtering. WMATA's scorecard measures only at timepoints
  (~10-15% of stops, available via the GTFS-Plus timepoints.txt extension).
  Current code measures at every stop, which biases our numbers vs WMATA's.
"""

OTP_EARLY_SEC = -120  # WMATA: more than 2 min early
OTP_LATE_SEC = 420  # WMATA: more than 7 min late
