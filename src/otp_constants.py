"""
On-time performance window constants.

Centralizes the early/late thresholds used to classify schedule deviations
across all OTP calculations. Defined here so future tightening (e.g.
NOTES.md #20 rider-experience window) is a one-line change.

Aligned with WMATA's published scorecard standard for schedule-based
timepoints: -2 minutes early to +7 minutes late.

Out of scope here, tracked separately:
- Headway-based routes (70, 79, X2, 90, 92, 16Y, Metroway) use a different
  rule: scheduled_headway + 3 min. WMATA does not publish a frequencies.txt
  in their bus GTFS feed, so the route list must be hardcoded if/when the
  rule is implemented.
- Timepoint-only filtering. WMATA's scorecard measures only at timepoints
  (~10-15% of stops, available via the GTFS-Plus timepoints.txt extension).
  Current code measures at every stop, which biases our numbers vs WMATA's.
"""

OTP_EARLY_SEC = -120  # WMATA: more than 2 min early
OTP_LATE_SEC = 420  # WMATA: more than 7 min late
