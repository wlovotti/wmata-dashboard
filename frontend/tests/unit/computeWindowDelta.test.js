/**
 * Characterization tests for computeWindowDelta.
 *
 * Lock in current behavior: 7-vs-prior-7 window, minimum DELTA_MIN_DAYS=3
 * per window, null suppression, date-based sorting.
 */
import { computeWindowDelta } from '../../src/utils/computeWindowDelta'

/** Build n days of {date, value} starting from day 1. */
function days(n, valueFn = (i) => i + 1) {
  return Array.from({ length: n }, (_, i) => ({
    date: `2026-01-${String(i + 1).padStart(2, '0')}`,
    value: valueFn(i),
  }))
}

describe('computeWindowDelta', () => {
  test('null input returns null', () => {
    expect(computeWindowDelta(null)).toBeNull()
  })

  test('empty array returns null', () => {
    expect(computeWindowDelta([])).toBeNull()
  })

  // DELTA_MIN_DAYS=3 → need at least DELTA_MIN_DAYS * 2 = 6 valid points to
  // pass the first guard. But the algorithm takes slice(-7) as recent and
  // slice(-14, -7) as prior. With only 6 points, prior slice is empty → null.
  // In practice you need at least 10 valid points (7 recent + 3 prior minimum).
  test('fewer than 6 valid points returns null (first guard)', () => {
    expect(computeWindowDelta(days(5))).toBeNull()
  })

  test('exactly 6 valid points still returns null (prior window empty)', () => {
    // Characterization: slice(-7) absorbs all 6 → slice(-14,-7) is empty →
    // prior.length=0 < DELTA_MIN_DAYS=3 → null. Surprising but correct.
    const result = computeWindowDelta(days(6, (i) => (i < 3 ? 10 : 20)))
    expect(result).toBeNull()
  })

  test('10 valid points: 7 recent + 3 prior → returns a result', () => {
    // slice(-7) = last 7, slice(-14,-7) = first 3 of these 10
    const result = computeWindowDelta(days(10, (i) => (i < 3 ? 10 : 20)))
    expect(result).not.toBeNull()
    expect(result.recentN).toBe(7)
    expect(result.priorN).toBe(3)
  })

  test('14 days: returns delta = recentMean - priorMean', () => {
    // First 7 days value=10, last 7 days value=20 → delta=10
    const series = [
      ...days(7, () => 10).map((d, i) => ({
        date: `2026-01-${String(i + 1).padStart(2, '0')}`,
        value: 10,
      })),
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 8).padStart(2, '0')}`,
        value: 20,
      })),
    ]
    const result = computeWindowDelta(series)
    expect(result).not.toBeNull()
    expect(result.delta).toBeCloseTo(10, 5)
    expect(result.recentMean).toBeCloseTo(20, 5)
    expect(result.priorMean).toBeCloseTo(10, 5)
  })

  test('recentN and priorN reflect actual window sizes', () => {
    const series = days(14, () => 1)
    const result = computeWindowDelta(series)
    expect(result.recentN).toBe(7)
    expect(result.priorN).toBe(7)
  })

  test('null values in the series are dropped before windowing', () => {
    // Mix 10 non-null with 5 null — should still compute from the 10 valid.
    const series = [
      ...days(10, () => 50),
      ...Array.from({ length: 5 }, (_, i) => ({
        date: `2026-02-${String(i + 1).padStart(2, '0')}`,
        value: null,
      })),
    ]
    const result = computeWindowDelta(series)
    expect(result).not.toBeNull()
    // All valid values are 50, so both windows average 50, delta = 0.
    expect(result.delta).toBeCloseTo(0, 5)
  })

  test('dates are sorted ascending before slicing (out-of-order input)', () => {
    // Build forward and shuffle to simulate out-of-order input. The function
    // must sort ascending before slicing so the most-recent dates end up in
    // the "recent" window.
    const forward = days(14, (i) => (i < 7 ? 10 : 20))
    const shuffled = [...forward].sort(() => Math.random() - 0.5)
    const result = computeWindowDelta(shuffled)
    expect(result).not.toBeNull()
    // last 7 (days 8-14) have value 20; prior 7 (days 1-7) have value 10
    expect(result.delta).toBeCloseTo(10, 5)
  })

  test('negative delta when recent window is lower than prior', () => {
    const series = [
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 1).padStart(2, '0')}`,
        value: 80,
      })),
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 8).padStart(2, '0')}`,
        value: 70,
      })),
    ]
    const result = computeWindowDelta(series)
    expect(result.delta).toBeCloseTo(-10, 5)
  })

  test('window larger than 14: only last 14 valid rows are used', () => {
    // 21 valid days — the window only looks at the last 14.
    // Days 1-7 = 5, days 8-14 = 10, days 15-21 = 20
    const series = [
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 1).padStart(2, '0')}`,
        value: 5,
      })),
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 8).padStart(2, '0')}`,
        value: 10,
      })),
      ...Array.from({ length: 7 }, (_, i) => ({
        date: `2026-01-${String(i + 15).padStart(2, '0')}`,
        value: 20,
      })),
    ]
    const result = computeWindowDelta(series)
    // recent = days 15-21 = mean 20; prior = days 8-14 = mean 10
    expect(result.recentMean).toBeCloseTo(20, 5)
    expect(result.priorMean).toBeCloseTo(10, 5)
    expect(result.delta).toBeCloseTo(10, 5)
  })

  test('prior window with fewer than DELTA_MIN_DAYS (3) valid → null', () => {
    // 9 valid total: last 7 = recent (fine), prior = slice(-14,-7) gives
    // indices 0..1 from a 9-element array = 2 entries, below DELTA_MIN_DAYS=3.
    const series = days(9, (i) => i + 1)
    expect(computeWindowDelta(series)).toBeNull()
  })
})
