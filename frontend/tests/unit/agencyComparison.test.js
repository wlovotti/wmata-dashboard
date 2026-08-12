/**
 * Characterization tests for utils/agencyComparison.js — the NOTES-99
 * agency comparison page's pure formatting logic (window-mean display +
 * week-over-week delta tinting).
 */
import { formatMetricValue, formatDelta } from '../../src/utils/agencyComparison'

// ── formatMetricValue ────────────────────────────────────────────────────────

describe('formatMetricValue', () => {
  test('null value for any metric → "—"', () => {
    expect(formatMetricValue('otp', null)).toBe('—')
    expect(formatMetricValue('service_delivered', null)).toBe('—')
    expect(formatMetricValue('ewt', null)).toBe('—')
    expect(formatMetricValue('bunching', null)).toBe('—')
  })

  test('undefined value → "—"', () => {
    expect(formatMetricValue('otp', undefined)).toBe('—')
  })

  test('otp: one decimal place, appends %', () => {
    expect(formatMetricValue('otp', 68.394)).toBe('68.4%')
    expect(formatMetricValue('otp', 0)).toBe('0.0%')
  })

  test('service_delivered: fraction to percent, one decimal', () => {
    expect(formatMetricValue('service_delivered', 0.8579)).toBe('85.8%')
    expect(formatMetricValue('service_delivered', 1.0)).toBe('100.0%')
  })

  test('ewt: seconds to minutes, one decimal, "min" suffix', () => {
    expect(formatMetricValue('ewt', 162.5)).toBe('2.7 min')
    expect(formatMetricValue('ewt', 0)).toBe('0.0 min')
  })

  test('bunching: fraction to percent, one decimal', () => {
    expect(formatMetricValue('bunching', 0.0375)).toBe('3.8%')
    expect(formatMetricValue('bunching', 0)).toBe('0.0%')
  })

  test('unknown metric → String(value)', () => {
    expect(formatMetricValue('headway', 42)).toBe('42')
  })
})

// ── formatDelta ───────────────────────────────────────────────────────────────

describe('formatDelta', () => {
  test('null delta → null (no full 14-day window yet)', () => {
    expect(formatDelta('otp', null)).toBeNull()
    expect(formatDelta('otp', undefined)).toBeNull()
  })

  test('otp: positive delta is improving (higher-is-better) → green', () => {
    const result = formatDelta('otp', 2.3)
    expect(result.tint).toBe('green')
    expect(result.text).toBe('+2.3 pts vs prior week')
  })

  test('otp: negative delta is regressing → red', () => {
    const result = formatDelta('otp', -1.5)
    expect(result.tint).toBe('red')
    expect(result.text).toBe('−1.5 pts vs prior week')
  })

  test('otp: zero delta → neutral, "±" sign', () => {
    const result = formatDelta('otp', 0)
    expect(result.tint).toBe('neutral')
    expect(result.text).toBe('±0.0 pts vs prior week')
  })

  test('service_delivered: fraction delta scaled to percentage points', () => {
    const result = formatDelta('service_delivered', 0.021)
    expect(result.tint).toBe('green')
    expect(result.text).toBe('+2.1 pts vs prior week')
  })

  test('ewt: lower-is-better, so a positive delta (worse EWT) is red', () => {
    const result = formatDelta('ewt', 30)
    expect(result.tint).toBe('red')
    expect(result.text).toBe('+0.5 min vs prior week')
  })

  test('ewt: a negative delta (EWT dropped) is green', () => {
    const result = formatDelta('ewt', -60)
    expect(result.tint).toBe('green')
    expect(result.text).toBe('−1.0 min vs prior week')
  })

  test('bunching: lower-is-better, positive delta (more bunching) is red', () => {
    const result = formatDelta('bunching', 0.01)
    expect(result.tint).toBe('red')
    expect(result.text).toBe('+1.0 pts vs prior week')
  })

  test('bunching: negative delta (less bunching) is green', () => {
    const result = formatDelta('bunching', -0.005)
    expect(result.tint).toBe('green')
    expect(result.text).toBe('−0.5 pts vs prior week')
  })
})
