/**
 * Characterization tests for utils/spectrumBar.js.
 *
 * computeSpectrumBar: higher/lower-is-better, boundary behavior, null guards.
 * COLOR_NEUTRAL: just a re-export.
 */
import { computeSpectrumBar, COLOR_NEUTRAL } from '../../src/utils/spectrumBar'

// Internal constants mirrored here for readable assertions.
const GREEN = '#0E8A6F'
const YELLOW = '#D97706'
const RED = '#C8102E'
const YELLOW_BAND = 0.1 // ±10% of target

describe('computeSpectrumBar', () => {
  // ── Null / missing guards ─────────────────────────────────────────────────

  test('null current → null', () => {
    expect(computeSpectrumBar({ current: null, target: 80, higherIsBetter: true })).toBeNull()
  })

  test('null target → null', () => {
    expect(computeSpectrumBar({ current: 75, target: null, higherIsBetter: true })).toBeNull()
  })

  test('target <= 0 → null', () => {
    expect(computeSpectrumBar({ current: 75, target: 0, higherIsBetter: true })).toBeNull()
    expect(computeSpectrumBar({ current: 75, target: -5, higherIsBetter: true })).toBeNull()
  })

  // ── higherIsBetter=true (OTP / service-delivered) ─────────────────────────

  test('ratio >= 1.0 → green', () => {
    // current >= target
    const r = computeSpectrumBar({ current: 85, target: 80, higherIsBetter: true })
    expect(r.color).toBe(GREEN)
  })

  test('ratio exactly 1.0 → green', () => {
    const r = computeSpectrumBar({ current: 80, target: 80, higherIsBetter: true })
    expect(r.color).toBe(GREEN)
    expect(r.fillPct).toBeCloseTo(100, 1)
  })

  test('ratio in [0.9, 1.0) → yellow (within YELLOW_BAND below target)', () => {
    // ratio = 73 / 80 = 0.9125 → 1.0 - 0.1 = 0.9, so 0.9125 >= 0.9 → yellow
    const r = computeSpectrumBar({ current: 73, target: 80, higherIsBetter: true })
    expect(r.color).toBe(YELLOW)
  })

  test('ratio exactly at YELLOW_BAND boundary (1-YELLOW_BAND) → yellow', () => {
    // ratio = 72 / 80 = 0.9 exactly → >= (1.0 - 0.1) → yellow
    const r = computeSpectrumBar({ current: 72, target: 80, higherIsBetter: true })
    expect(r.color).toBe(YELLOW)
  })

  test('ratio below YELLOW_BAND → red', () => {
    // ratio = 70 / 80 = 0.875 < 0.9 → red
    const r = computeSpectrumBar({ current: 70, target: 80, higherIsBetter: true })
    expect(r.color).toBe(RED)
  })

  test('fillPct = ratio * 100 clamped to [0, 100]', () => {
    // current=160, target=80 → ratio=2.0 → clamped to 1.0 → fillPct=100
    const r = computeSpectrumBar({ current: 160, target: 80, higherIsBetter: true })
    expect(r.fillPct).toBeCloseTo(100, 5)
    expect(r.color).toBe(GREEN)
  })

  test('fillPct is proportional for ratio < 1', () => {
    // current=40, target=80 → ratio=0.5 → fillPct=50
    const r = computeSpectrumBar({ current: 40, target: 80, higherIsBetter: true })
    expect(r.fillPct).toBeCloseTo(50, 5)
  })

  test('current=0, target=80 → red, fillPct=0 (clamped)', () => {
    const r = computeSpectrumBar({ current: 0, target: 80, higherIsBetter: true })
    expect(r.color).toBe(RED)
    expect(r.fillPct).toBeCloseTo(0, 5)
  })

  // ── higherIsBetter=false (EWT / bunching) ────────────────────────────────

  test('current < target → ratio > 1 → green (beating lower target)', () => {
    // EWT: target=120s, current=90s → ratio = 120/90 ≈ 1.33 → green
    const r = computeSpectrumBar({ current: 90, target: 120, higherIsBetter: false })
    expect(r.color).toBe(GREEN)
  })

  test('current === target → ratio=1 → green', () => {
    const r = computeSpectrumBar({ current: 120, target: 120, higherIsBetter: false })
    expect(r.color).toBe(GREEN)
    expect(r.fillPct).toBeCloseTo(100, 1)
  })

  test('current slightly above target → yellow', () => {
    // ratio = 120/132 ≈ 0.909 → >= 0.9 → yellow
    const r = computeSpectrumBar({ current: 132, target: 120, higherIsBetter: false })
    expect(r.color).toBe(YELLOW)
  })

  test('current well above target → red', () => {
    // ratio = 120/200 = 0.6 < 0.9 → red
    const r = computeSpectrumBar({ current: 200, target: 120, higherIsBetter: false })
    expect(r.color).toBe(RED)
  })
})

describe('COLOR_NEUTRAL', () => {
  test('is a hex color string', () => {
    expect(typeof COLOR_NEUTRAL).toBe('string')
    expect(COLOR_NEUTRAL).toMatch(/^#[0-9A-Fa-f]{6}$/)
  })
})
