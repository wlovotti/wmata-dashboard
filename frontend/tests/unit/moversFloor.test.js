/**
 * moversFloor (NOTES-127): the per-metric magnitude floor hoisted out of
 * MoversPanel.jsx into a shared module so RouteList and RouteDetail's
 * `renderServerDelta` helpers can pass the same `flatThreshold` MoversPanel
 * uses, instead of silently falling back to DeltaIndicator's unit-blind 0.5
 * default (two orders of magnitude too tight for the 0..1-fraction
 * service_delivered/bunching deltas).
 */
import { MOVERS_FLAT_FLOOR, getMoversFloor } from '../../src/moversFloor'

describe('moversFloor', () => {
  test('known metrics return their explicit per-metric floor', () => {
    expect(getMoversFloor('otp')).toBe(0.5)
    expect(getMoversFloor('service_delivered')).toBe(0.005)
    expect(getMoversFloor('ewt')).toBe(10)
    expect(getMoversFloor('bunching')).toBe(0.005)
  })

  test('a metric absent from MOVERS_FLAT_FLOOR falls back to 0.5', () => {
    expect(getMoversFloor('some_future_metric')).toBe(0.5)
    expect(getMoversFloor('excess_trip_time_pct')).toBe(0.5)
  })

  test('getMoversFloor is backed by MOVERS_FLAT_FLOOR, not an independent copy', () => {
    for (const [metric, floor] of Object.entries(MOVERS_FLAT_FLOOR)) {
      expect(getMoversFloor(metric)).toBe(floor)
    }
  })
})
