/**
 * heroSummary (NOTES-84): the worst-of-four + routes-below-target math
 * extracted from the retired HealthPulse banner, now driving the
 * OverviewHero verdict. Semantics are pinned to the HealthPulse originals.
 */
import {
  latestNonNull,
  gapFraction,
  worstMetric,
  countRoutesBelowTarget,
} from '../../src/utils/heroSummary'

describe('latestNonNull', () => {
  test('walks backward to the most recent non-null value', () => {
    const series = [
      { date: 'a', otp_percentage: 70 },
      { date: 'b', otp_percentage: 75 },
      { date: 'c', otp_percentage: null },
    ]
    expect(latestNonNull(series, 'otp_percentage')).toBe(75)
  })

  test('returns null for empty or non-array input', () => {
    expect(latestNonNull([], 'x')).toBeNull()
    expect(latestNonNull(null, 'x')).toBeNull()
  })
})

describe('gapFraction', () => {
  test('positive when worse than target, normalized by target', () => {
    expect(gapFraction({ current: 45, target: 50, higherIsBetter: true })).toBeCloseTo(0.1)
    expect(gapFraction({ current: 110, target: 100, higherIsBetter: false })).toBeCloseTo(0.1)
  })

  test('negative when beating target; null on missing sides', () => {
    expect(gapFraction({ current: 55, target: 50, higherIsBetter: true })).toBeCloseTo(-0.1)
    expect(gapFraction({ current: null, target: 50, higherIsBetter: true })).toBeNull()
    expect(gapFraction({ current: 55, target: 0, higherIsBetter: true })).toBeNull()
  })
})

describe('worstMetric', () => {
  const metrics = [
    { key: 'otp', label: 'OTP', higherIsBetter: true, current: 74, target: 75 },
    { key: 'bunching', label: 'Bunching', higherIsBetter: false, current: 14, target: 10 },
  ]

  test('picks the largest normalized gap and reports it', () => {
    const worst = worstMetric(metrics)
    expect(worst.key).toBe('bunching') // 0.4 gap beats OTP's ~0.013
    expect(worst.gap).toBeCloseTo(0.4)
  })

  test('null when no metric has both sides', () => {
    expect(worstMetric([{ key: 'otp', current: null, target: 75, higherIsBetter: true }])).toBeNull()
  })
})

describe('countRoutesBelowTarget', () => {
  test('counts routes missing any of their four targets; unmeasured routes excluded', () => {
    const routes = [
      // Below on OTP.
      { otp_all_pct: 60, targets: { otp: 75 } },
      // Meets OTP, no other targets.
      { otp_all_pct: 80, targets: { otp: 75 } },
      // No live data at all → excluded from `evaluated`.
      { otp_all_pct: null, targets: { otp: 75 } },
      // Below on bunching (fractions on both sides, NOTES-47 units).
      { bunching_rate: 0.2, targets: { bunching: 0.1 } },
    ]
    expect(countRoutesBelowTarget(routes)).toEqual({ below: 2, evaluated: 3 })
  })

  test('empty/missing input counts nothing', () => {
    expect(countRoutesBelowTarget(null)).toEqual({ below: 0, evaluated: 0 })
  })
})
