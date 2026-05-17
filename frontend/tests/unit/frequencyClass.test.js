/**
 * Characterization tests for frequencyClass.js.
 *
 * Exports: FREQUENCY_CLASS_COLORS, FREQUENCY_CLASS_LABELS, badgeColor.
 */
import {
  FREQUENCY_CLASS_COLORS,
  FREQUENCY_CLASS_LABELS,
  badgeColor,
} from '../../src/frequencyClass'

// Internal defaults mirrored for assertions.
const DEFAULT_BADGE_COLOR = '#002F6C' // navy — used when frequencyClass not recognized but hasMetrics
const NO_DATA_BADGE_COLOR = '#919D9D' // gray — no metrics at all

describe('FREQUENCY_CLASS_COLORS', () => {
  test('has the expected four primary classes plus limited_stop', () => {
    expect(FREQUENCY_CLASS_COLORS).toHaveProperty('high')
    expect(FREQUENCY_CLASS_COLORS).toHaveProperty('medium')
    expect(FREQUENCY_CLASS_COLORS).toHaveProperty('low')
    expect(FREQUENCY_CLASS_COLORS).toHaveProperty('limited')
    expect(FREQUENCY_CLASS_COLORS).toHaveProperty('limited_stop')
  })

  test('high frequency is WMATA red', () => {
    expect(FREQUENCY_CLASS_COLORS.high).toBe('#C8102E')
  })

  test('medium frequency is WMATA navy', () => {
    expect(FREQUENCY_CLASS_COLORS.medium).toBe('#002F6C')
  })

  test('all values are valid 7-char hex strings', () => {
    for (const color of Object.values(FREQUENCY_CLASS_COLORS)) {
      expect(color).toMatch(/^#[0-9A-Fa-f]{6}$/)
    }
  })
})

describe('FREQUENCY_CLASS_LABELS', () => {
  test('has labels for all five classes', () => {
    const keys = Object.keys(FREQUENCY_CLASS_LABELS)
    expect(keys).toContain('high')
    expect(keys).toContain('medium')
    expect(keys).toContain('low')
    expect(keys).toContain('limited')
    expect(keys).toContain('limited_stop')
  })

  test('high label includes "12 min"', () => {
    expect(FREQUENCY_CLASS_LABELS.high).toMatch(/12 min/)
  })

  test('limited label implies 30+ min intervals', () => {
    expect(FREQUENCY_CLASS_LABELS.limited).toMatch(/30\+/)
  })
})

describe('badgeColor', () => {
  test('known frequencyClass → returns FREQUENCY_CLASS_COLORS entry', () => {
    expect(badgeColor('high', true)).toBe(FREQUENCY_CLASS_COLORS.high)
    expect(badgeColor('medium', true)).toBe(FREQUENCY_CLASS_COLORS.medium)
    expect(badgeColor('low', true)).toBe(FREQUENCY_CLASS_COLORS.low)
    expect(badgeColor('limited', true)).toBe(FREQUENCY_CLASS_COLORS.limited)
    expect(badgeColor('limited_stop', true)).toBe(FREQUENCY_CLASS_COLORS.limited_stop)
  })

  test('known frequencyClass ignores hasMetrics flag', () => {
    expect(badgeColor('high', false)).toBe(FREQUENCY_CLASS_COLORS.high)
    expect(badgeColor('high', true)).toBe(FREQUENCY_CLASS_COLORS.high)
  })

  test('null frequencyClass + hasMetrics=true → DEFAULT_BADGE_COLOR (navy)', () => {
    expect(badgeColor(null, true)).toBe(DEFAULT_BADGE_COLOR)
  })

  test('null frequencyClass + hasMetrics=false → NO_DATA_BADGE_COLOR (gray)', () => {
    expect(badgeColor(null, false)).toBe(NO_DATA_BADGE_COLOR)
  })

  test('undefined frequencyClass + hasMetrics=true → DEFAULT_BADGE_COLOR', () => {
    expect(badgeColor(undefined, true)).toBe(DEFAULT_BADGE_COLOR)
  })

  test('undefined frequencyClass + hasMetrics=false → NO_DATA_BADGE_COLOR', () => {
    expect(badgeColor(undefined, false)).toBe(NO_DATA_BADGE_COLOR)
  })

  test('unrecognized class string + hasMetrics=true → DEFAULT_BADGE_COLOR', () => {
    expect(badgeColor('express', true)).toBe(DEFAULT_BADGE_COLOR)
  })

  test('unrecognized class string + hasMetrics=false → NO_DATA_BADGE_COLOR', () => {
    expect(badgeColor('express', false)).toBe(NO_DATA_BADGE_COLOR)
  })

  test('empty string frequencyClass + hasMetrics=true → DEFAULT_BADGE_COLOR', () => {
    // Empty string is falsy in JS → falls through to hasMetrics check.
    expect(badgeColor('', true)).toBe(DEFAULT_BADGE_COLOR)
  })
})
