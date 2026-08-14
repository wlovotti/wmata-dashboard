// Hero verdict math (NOTES-84). Extracted verbatim from the retired
// HealthPulse banner in Overview.jsx so the OverviewHero component and its
// tests can share it. Unit conventions follow NOTES-47: OTP percent,
// service_delivered / bunching fractions scaled x100 by CALLERS before
// comparison where noted.

/**
 * Most recent non-null value of `key` in a trend_data series, or null.
 * Falls back through earlier days so early-morning hits (before the daily
 * pipeline runs) read yesterday instead of nothing.
 */
export function latestNonNull(series, key) {
  if (!Array.isArray(series)) return null
  for (let i = series.length - 1; i >= 0; i--) {
    const value = series[i]?.[key]
    if (value != null) return value
  }
  return null
}

/**
 * Normalized gap to target: positive = wrong side of target, negative =
 * beating it, null when either side is missing or target is 0. Magnitude is
 * gap/|target| so a 10% miss reads identically across metrics.
 */
export function gapFraction({ current, target, higherIsBetter }) {
  if (current == null || target == null || target === 0) return null
  const rawGap = higherIsBetter ? target - current : current - target
  return rawGap / Math.abs(target)
}

/**
 * The metric with the largest normalized gap — the hero's "sore spot".
 *
 * @param {Array<{key, label, current, target, higherIsBetter}>} systemMetrics
 * @returns {{key, label, current, target, gap}|null} null when no metric has
 *   both a current value and a target.
 */
export function worstMetric(systemMetrics) {
  let worst = null
  for (const m of systemMetrics || []) {
    const gap = gapFraction({
      current: m.current,
      target: m.target,
      higherIsBetter: m.higherIsBetter,
    })
    if (gap == null) continue
    if (worst == null || gap > worst.gap) {
      worst = { key: m.key, label: m.label, current: m.current, target: m.target, gap }
    }
  }
  return worst
}

/**
 * Count scorecard routes on the wrong side of any of their four targets.
 * A route is "evaluated" only if at least one metric has both a current
 * value and a target — unmeasured routes are not "below target", they're
 * unmeasured. Mirrors the retired HealthPulse loop exactly (including the
 * x100 scaling of the fraction-unit metrics before comparison).
 *
 * @returns {{below: number, evaluated: number}}
 */
export function countRoutesBelowTarget(routes) {
  let below = 0
  let evaluated = 0
  for (const r of routes || []) {
    const targets = r.targets || {}
    const checks = [
      { current: r.otp_all_pct, target: targets.otp, higherIsBetter: true },
      {
        current: r.service_delivered_ratio != null ? r.service_delivered_ratio * 100 : null,
        target: targets.service_delivered != null ? targets.service_delivered * 100 : null,
        higherIsBetter: true,
      },
      { current: r.ewt_seconds, target: targets.ewt, higherIsBetter: false },
      {
        current: r.bunching_rate != null ? r.bunching_rate * 100 : null,
        target: targets.bunching != null ? targets.bunching * 100 : null,
        higherIsBetter: false,
      },
    ]
    let hasAnyMeasurement = false
    let isBelow = false
    for (const c of checks) {
      if (c.current == null || c.target == null) continue
      hasAnyMeasurement = true
      const gap = c.higherIsBetter ? c.target - c.current : c.current - c.target
      if (gap > 0) {
        isBelow = true
        break
      }
    }
    if (hasAnyMeasurement) {
      evaluated += 1
      if (isBelow) below += 1
    }
  }
  return { below, evaluated }
}
