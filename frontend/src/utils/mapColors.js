import { computeSpectrumBar, COLOR_NEUTRAL } from './spectrumBar'

/**
 * Line color for a route polyline on the Overview system map (NOTES-84).
 *
 * Colors by OTP vs the route's target using the same banding as the
 * scorecard spectrum bars (computeSpectrumBar), so "yellow on the map"
 * and "yellow in the table" always agree. The `targets` block on a
 * /api/routes row already collapses per-route override vs system default.
 *
 * @param {object|null} scorecardRow - a route row from /api/routes, or
 *   null/undefined when the route has no scorecard entry.
 * @returns {string} hex color; COLOR_NEUTRAL for unmeasured routes.
 */
export function routeLineColor(scorecardRow) {
  if (!scorecardRow) return COLOR_NEUTRAL
  const bar = computeSpectrumBar({
    current: scorecardRow.otp_all_pct,
    target: scorecardRow.targets?.otp ?? null,
    higherIsBetter: true,
  })
  return bar ? bar.color : COLOR_NEUTRAL
}
