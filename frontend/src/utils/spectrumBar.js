// Spectrum-bar value-to-color mapping (NOTES-55).
//
// Renders a thin horizontal performance bar inside each scorecard cell so
// the eye can pre-classify "good / watch / needs attention" before parsing
// digits. Color tracks the route's target with a single ±10% yellow band
// applied uniformly across metrics — hand-tuning per metric was explicitly
// rejected in NOTES-55.
//
// Inputs `current` and `target` must be in the SAME units. The cell's
// formatter normalizes service-delivered and bunching to percent before
// calling in, so all four metrics arrive on comparable scales.

// Single yellow band reused for every metric. Within `±YELLOW_BAND` of the
// target on the unfavorable side is yellow; outside it is red. Past target
// on the favorable side is green.
const YELLOW_BAND = 0.1

const COLOR_GREEN = '#0E8A6F'
const COLOR_YELLOW = '#D97706'
const COLOR_RED = '#C8102E'
const COLOR_NEUTRAL = '#94a3b8'

/**
 * Classify `current` against `target` for a higher- or lower-is-better
 * metric and return the bar color plus the proportional fill width.
 *
 * @param {object} args
 * @param {number|null|undefined} args.current - Observed metric value.
 * @param {number|null|undefined} args.target - Route's target value (per
 *   route if set, system default otherwise; both already collapsed into
 *   the `targets` block on `/api/routes`).
 * @param {boolean} args.higherIsBetter - True for OTP / service-delivered,
 *   false for EWT / bunching.
 * @returns {{color: string, fillPct: number}|null} Color hex + width
 *   percentage in [0, 100], or `null` when no bar should render
 *   (missing target *or* missing value — the cell falls back to a bare
 *   number / em-dash).
 */
export function computeSpectrumBar({ current, target, higherIsBetter }) {
  if (current == null || target == null || target <= 0) {
    return null
  }

  // "Performance ratio" normalizes so >= 1.0 always means "at/past target."
  // For lower-is-better metrics that means target/value (a smaller observed
  // EWT is better, so target/current > 1 when we're beating it).
  const ratio = higherIsBetter ? current / target : target / current

  let color
  if (ratio >= 1.0) {
    color = COLOR_GREEN
  } else if (ratio >= 1.0 - YELLOW_BAND) {
    color = COLOR_YELLOW
  } else {
    color = COLOR_RED
  }

  // Fill width visualizes the ratio: a full bar means at/above target, a
  // half bar means halfway there. Clamp to [0, 100] so massively-missed
  // targets still render a (thin) red bar instead of disappearing.
  const fillPct = Math.max(0, Math.min(1, ratio)) * 100

  return { color, fillPct }
}

export { COLOR_NEUTRAL }
