/**
 * routeLineColor (NOTES-84 system map): OTP-vs-target banding for route
 * polylines, delegating to computeSpectrumBar so map colors match the
 * scorecard's spectrum bars exactly. Missing row/value/target → neutral.
 */
import { routeLineColor } from '../../src/utils/mapColors'
import { COLOR_NEUTRAL } from '../../src/utils/spectrumBar'

describe('routeLineColor', () => {
  test('at/above target is green', () => {
    expect(routeLineColor({ otp_all_pct: 80, targets: { otp: 75 } })).toBe('#0E8A6F')
  })

  test('within the 10% band below target is yellow', () => {
    expect(routeLineColor({ otp_all_pct: 70, targets: { otp: 75 } })).toBe('#D97706')
  })

  test('past the band is red', () => {
    expect(routeLineColor({ otp_all_pct: 50, targets: { otp: 75 } })).toBe('#C8102E')
  })

  test('missing row, value, or target is neutral', () => {
    expect(routeLineColor(null)).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: null, targets: { otp: 75 } })).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: 70, targets: {} })).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: 70 })).toBe(COLOR_NEUTRAL)
  })
})
