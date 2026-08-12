// Formatting helpers for the agency comparison page (NOTES-99). Split out
// of AgencyComparison.jsx so the pure display logic is unit-testable the
// same way utils/formatters.js is.

// Canonical metric order + labels for the four headline KPIs. Mirrors the
// OTP / service_delivered / EWT / bunching unit conventions used elsewhere
// (Targets.jsx's formatTarget, formatters.js's formatContribMetricValue):
// OTP is 0-100 percent, service_delivered and bunching are 0-1 fractions,
// EWT is seconds.
export const METRIC_ORDER = ['otp', 'service_delivered', 'ewt', 'bunching']

export const METRIC_LABELS = {
  otp: 'On-time performance',
  service_delivered: 'Service delivered',
  ewt: 'Excess wait time',
  bunching: 'Bunching rate',
}

// Direction each metric needs to move to count as "improving" — used to
// tint the week-over-week delta pill green/red.
export const HIGHER_IS_BETTER = {
  otp: true,
  service_delivered: true,
  ewt: false,
  bunching: false,
}

/**
 * Format one metric's window-mean value for display, given its canonical
 * unit. Returns '—' for null (no data in the window for that metric).
 *
 * @param {'otp'|'service_delivered'|'ewt'|'bunching'} metric
 * @param {number|null|undefined} value
 * @returns {string}
 */
export function formatMetricValue(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${value.toFixed(1)}%`
  if (metric === 'service_delivered') return `${(value * 100).toFixed(1)}%`
  if (metric === 'ewt') return `${(value / 60).toFixed(1)} min`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

/**
 * Format one metric's week-over-week delta as a signed magnitude plus a
 * green/red/neutral tint, or null when the API reported no delta yet (the
 * matched window doesn't hold a full 14 days for that agency).
 *
 * @param {'otp'|'service_delivered'|'ewt'|'bunching'} metric
 * @param {number|null|undefined} delta
 * @returns {{text: string, tint: 'green'|'red'|'neutral'} | null}
 */
export function formatDelta(metric, delta) {
  if (delta == null) return null
  const higherIsBetter = HIGHER_IS_BETTER[metric]
  const improving = higherIsBetter ? delta > 0 : delta < 0
  const neutral = delta === 0

  let magnitude
  if (metric === 'otp') magnitude = `${Math.abs(delta).toFixed(1)} pts`
  else if (metric === 'service_delivered') magnitude = `${Math.abs(delta * 100).toFixed(1)} pts`
  else if (metric === 'ewt') magnitude = `${Math.abs(delta / 60).toFixed(1)} min`
  else magnitude = `${Math.abs(delta * 100).toFixed(1)} pts`

  const sign = delta > 0 ? '+' : delta < 0 ? '−' : '±'
  return {
    text: `${sign}${magnitude} vs prior week`,
    tint: neutral ? 'neutral' : improving ? 'green' : 'red',
  }
}
