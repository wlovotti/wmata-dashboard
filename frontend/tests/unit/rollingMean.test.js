/**
 * rollingMean (NOTES-84 trend smoothing): 7-day trailing mean over a daily
 * series, excluding partial-quality days and nulls from window means. The
 * output keeps every input date so the smoothed line and the raw ghost dots
 * share an x-axis.
 */
import { rollingMean, ROLLING_WINDOW_DAYS } from '../../src/utils/rollingMean'

const day = (i) => `2026-08-${String(i).padStart(2, '0')}`

describe('rollingMean', () => {
  test('window constant is 7', () => {
    expect(ROLLING_WINDOW_DAYS).toBe(7)
  })

  test('flat series smooths to itself', () => {
    const series = [1, 2, 3, 4, 5, 6, 7, 8].map((i) => ({ date: day(i), value: 50 }))
    const out = rollingMean(series)
    expect(out).toHaveLength(8)
    out.forEach((row) => expect(row.value).toBe(50))
  })

  test('trailing window: early points average over fewer days', () => {
    const series = [
      { date: day(1), value: 10 },
      { date: day(2), value: 20 },
      { date: day(3), value: 30 },
    ]
    const out = rollingMean(series)
    expect(out[0].value).toBe(10)      // window = [10]
    expect(out[1].value).toBe(15)      // window = [10, 20]
    expect(out[2].value).toBe(20)      // window = [10, 20, 30]
  })

  test('partial days and nulls are excluded from the mean but keep their date', () => {
    const series = [
      { date: day(1), value: 10 },
      { date: day(2), value: 99, data_quality: 'partial' },
      { date: day(3), value: null },
      { date: day(4), value: 20 },
    ]
    const out = rollingMean(series)
    expect(out.map((r) => r.date)).toEqual([day(1), day(2), day(3), day(4)])
    expect(out[3].value).toBe(15) // mean of clean 10 and 20 only
  })

  test('all-partial window yields null, and input order does not matter', () => {
    const series = [
      { date: day(2), value: 99, data_quality: 'partial' },
      { date: day(1), value: 88, data_quality: 'partial' },
    ]
    const out = rollingMean(series)
    expect(out[0].date).toBe(day(1)) // sorted ascending
    expect(out[0].value).toBeNull()
    expect(out[1].value).toBeNull()
  })

  test('non-array input returns empty array', () => {
    expect(rollingMean(null)).toEqual([])
  })
})
