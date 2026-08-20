// Per-metric magnitude floor for "did this change" indicators, shared by
// every surface that renders a server-side 7-day-vs-prior-7-day delta
// (MoversPanel, RouteList, RouteDetail). A route's delta must clear its
// metric's floor (strictly, matching DeltaIndicator's own `>` boundary) to
// render as a colored up/down arrow rather than DeltaIndicator's default
// flat gray → — otherwise a route could rank as "worse"/"better" (or show a
// non-trivial percentage) while its own arrow renders flat, which reads as
// self-contradictory (NOTES-121, NOTES-127).
//
// Originally introduced for MoversPanel's ranking filter (NOTES-121);
// hoisted here (NOTES-127) so RouteList's and RouteDetail's
// `renderServerDelta` helpers use the same floor instead of silently
// falling back to DeltaIndicator's unit-blind 0.5 default. That default is
// fine for `otp` (deltas arrive already in percentage points) but two
// orders of magnitude too tight for `service_delivered` and `bunching`,
// whose deltas arrive as 0..1 fractions — a real 2 pp swing arrives as
// `0.02`, which never clears 0.5 and renders flat regardless of actual
// magnitude.
//
// Research (2026-08-19): a short pass over TRB/TCRP literature (TCRP
// Report 95 - Transit Reliability, TCRP Report 100 - Transit Capacity and
// Quality of Service Manual, FHWA travel-time-reliability guidance) plus
// public agency dashboards (VIA Metro, MnDOT) surfaced OTP *target*
// conventions (e.g. 90% of trips within a lateness window) but no
// agency-published or TRB-published week-over-week "noise floor" figure
// for any of these four metrics specifically — the literature is thin on
// this exact question. Per project convention (anchor to the existing
// flat-rendering band when the literature doesn't supply a number):
//   - otp, service_delivered, and bunching are all percentage-point-like
//     metrics already anchored elsewhere in this codebase to a 0.5 pp
//     flat band (DeltaIndicator's own default flatThreshold, and
//     OverviewHero's HERO_FLAT_PP built on the same rationale) — reuse
//     that 0.5 pp floor here, expressed in each metric's wire units: otp
//     deltas arrive already in pp, service_delivered/bunching deltas
//     arrive as 0..1 fractions (0.5 pp == 0.005).
//   - ewt has no existing pp-based precedent (its unit is seconds, not a
//     percentage), so reusing 0.5 unit-blind (what DeltaIndicator's
//     un-overridden default silently does elsewhere in the app today,
//     e.g. SystemTrend/RouteTrend's EWT cards) would mean a 0.5-SECOND
//     floor — far too tight, effectively never flat. Chosen
//     independently instead: 10s, roughly 5-6% of the system EWT
//     baseline/target (~150-200s observed, 180s target per
//     config/route_targets.yaml) — comfortably above Math.round()
//     display noise (~1s) and below the tens-of-seconds gaps that
//     separate routes actually worth flagging.
export const MOVERS_FLAT_FLOOR = {
  otp: 0.5, // percentage points
  service_delivered: 0.005, // fraction (== 0.5 pp)
  ewt: 10, // seconds
  bunching: 0.005, // fraction (== 0.5 pp)
}

/**
 * Single source of truth for a metric's magnitude floor, shared by every
 * caller that renders a server-side delta with `DeltaIndicator` — the
 * ranking filter in MoversPanel and the `flatThreshold` prop passed to
 * each surface's `DeltaIndicator` (MoversPanel, RouteList, RouteDetail). A
 * metric absent from `MOVERS_FLAT_FLOOR` (e.g. a future metric not yet
 * given an explicit floor) falls back to 0.5 — `DeltaIndicator`'s own
 * default — identically at every call site, so the
 * ranked/rendered-implies-non-flat invariant holds structurally rather
 * than by keeping several independently-written `?? ...` fallbacks in
 * sync by hand.
 *
 * @param {string} metric - one of 'otp', 'service_delivered', 'ewt', 'bunching'.
 * @returns {number} the magnitude floor in that metric's native delta units.
 */
export function getMoversFloor(metric) {
  return MOVERS_FLAT_FLOOR[metric] ?? 0.5
}
