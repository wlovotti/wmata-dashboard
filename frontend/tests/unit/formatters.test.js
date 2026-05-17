/**
 * Characterization tests for utils/formatters.js.
 *
 * All four exported functions: formatDeviationMmSs, formatDeviationSignedSec,
 * formatContribMetricValue, todayEasternIso.
 */
import {
  formatDeviationMmSs,
  formatDeviationSignedSec,
  formatContribMetricValue,
  todayEasternIso,
} from '../../src/utils/formatters'

// ── formatDeviationMmSs ──────────────────────────────────────────────────────

describe('formatDeviationMmSs', () => {
  test('null → "—"', () => {
    expect(formatDeviationMmSs(null)).toBe('—')
  })

  test('undefined → "—"', () => {
    expect(formatDeviationMmSs(undefined)).toBe('—')
  })

  test('0 → "0:00"', () => {
    expect(formatDeviationMmSs(0)).toBe('0:00')
  })

  test('positive 90s → "1:30"', () => {
    expect(formatDeviationMmSs(90)).toBe('1:30')
  })

  test('negative -90s → "1:30" (abs value; no sign in output)', () => {
    // The docstring says magnitude only — the column header carries meaning.
    expect(formatDeviationMmSs(-90)).toBe('1:30')
  })

  test('59s → "0:59"', () => {
    expect(formatDeviationMmSs(59)).toBe('0:59')
  })

  test('60s → "1:00"', () => {
    expect(formatDeviationMmSs(60)).toBe('1:00')
  })

  test('large value 3661s → "61:01"', () => {
    expect(formatDeviationMmSs(3661)).toBe('61:01')
  })

  test('9s → "0:09" (seconds are zero-padded to 2 digits)', () => {
    expect(formatDeviationMmSs(9)).toBe('0:09')
  })
})

// ── formatDeviationSignedSec ─────────────────────────────────────────────────

describe('formatDeviationSignedSec', () => {
  test('null → "—"', () => {
    expect(formatDeviationSignedSec(null)).toBe('—')
  })

  test('undefined → "—"', () => {
    expect(formatDeviationSignedSec(undefined)).toBe('—')
  })

  test('0 → "0s"', () => {
    expect(formatDeviationSignedSec(0)).toBe('0s')
  })

  test('positive value → "+Ns"', () => {
    expect(formatDeviationSignedSec(45)).toBe('+45s')
  })

  test('negative value → "-Ns" (no + prefix)', () => {
    expect(formatDeviationSignedSec(-30)).toBe('-30s')
  })

  test('large positive → "+420s"', () => {
    expect(formatDeviationSignedSec(420)).toBe('+420s')
  })

  test('large negative → "-300s"', () => {
    expect(formatDeviationSignedSec(-300)).toBe('-300s')
  })
})

// ── formatContribMetricValue ─────────────────────────────────────────────────

describe('formatContribMetricValue', () => {
  test('null value for any metric → "—"', () => {
    expect(formatContribMetricValue('otp', null)).toBe('—')
    expect(formatContribMetricValue('ewt', null)).toBe('—')
    expect(formatContribMetricValue('service_delivered', null)).toBe('—')
    expect(formatContribMetricValue('bunching', null)).toBe('—')
  })

  test('undefined value → "—"', () => {
    expect(formatContribMetricValue('otp', undefined)).toBe('—')
  })

  test('otp: rounds to nearest integer and appends %', () => {
    expect(formatContribMetricValue('otp', 72.4)).toBe('72%')
    expect(formatContribMetricValue('otp', 72.5)).toBe('73%') // Math.round
    expect(formatContribMetricValue('otp', 100)).toBe('100%')
    expect(formatContribMetricValue('otp', 0)).toBe('0%')
  })

  test('service_delivered: multiplies by 100 then rounds, appends %', () => {
    expect(formatContribMetricValue('service_delivered', 0.875)).toBe('88%')
    expect(formatContribMetricValue('service_delivered', 1.0)).toBe('100%')
    expect(formatContribMetricValue('service_delivered', 0)).toBe('0%')
  })

  test('ewt: rounds to nearest integer, appends s', () => {
    expect(formatContribMetricValue('ewt', 87.3)).toBe('87s')
    expect(formatContribMetricValue('ewt', 87.5)).toBe('88s')
    expect(formatContribMetricValue('ewt', 0)).toBe('0s')
  })

  test('bunching: multiplies by 100, fixed to 1 decimal, appends %', () => {
    expect(formatContribMetricValue('bunching', 0.123)).toBe('12.3%')
    expect(formatContribMetricValue('bunching', 0.0)).toBe('0.0%')
    expect(formatContribMetricValue('bunching', 1.0)).toBe('100.0%')
  })

  test('unknown metric → String(value)', () => {
    expect(formatContribMetricValue('headway', 42)).toBe('42')
    expect(formatContribMetricValue('', 99)).toBe('99')
  })
})

// ── todayEasternIso ──────────────────────────────────────────────────────────

describe('todayEasternIso', () => {
  test('returns a YYYY-MM-DD formatted string', () => {
    const result = todayEasternIso()
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/)
  })

  test('returns a valid date', () => {
    const result = todayEasternIso()
    const parsed = new Date(result + 'T00:00:00')
    expect(parsed).not.toBeNaN()
    expect(parsed.getFullYear()).toBeGreaterThanOrEqual(2024)
  })
})
