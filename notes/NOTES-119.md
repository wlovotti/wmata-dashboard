# NOTES-119: Bus-only filtering across the comparison KPIs

**Severity:** low-medium
**Effort:** low-medium

## Problem

The agency-comparison page's service-level, EWT, and SWT pools are all
drawn from *every route in each agency's GTFS feed*, and the two
agencies' feeds are not mode-symmetric: WMATA's feed is bus-only
(route_type 3, 128 routes), while SFMTA's includes 7 Muni Metro
light-rail routes (route_type 0) and 3 cable-car routes (route_type 5)
alongside 58 bus routes. Rail and cable-car headways skew SFMTA's
pooled figures relative to a true bus-vs-bus comparison.

Measured impact on the daytime service-level tile (NOTES-115): SFMTA
all-modes is 10.0 min median headway / 78.9% of scheduled service at
<=15 min, vs 11.0 min / 74.3% bus-only. The same asymmetry applies to
SFMTA's EWT and SWT pools, since they draw from the same
frequent-gated cell-hour resolution over all routes in the feed.

## Interim

A caveat disclosing this was added to `AGENCY_COMPARISON_CAVEATS` in
`api/aggregations.py` (PR #200 final-review fix wave) so the comparison
page is honest about the mode mix in the meantime.

## Fix

Filter the schedule/cell-hour pools that feed service-level, EWT, and
SWT to `route_type = 3` (bus) for both agencies before computing
comparison KPIs, so the comparison is bus-to-bus. Likely touches
`src.ewt.fetch_scheduled_cell_hours_for_routes` (or its callers) and
`src.service_level.service_level_for_agency`. Confirm WMATA's feed
really is 100% route_type 3 before assuming the filter is a no-op there
(it should be, per NOTES-115's design spec, but verify against current
GTFS rather than trusting the prior measurement).
