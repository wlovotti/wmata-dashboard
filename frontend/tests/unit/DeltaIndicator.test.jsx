/**
 * Characterization tests for DeltaIndicator (RouteTrend.jsx).
 *
 * These lock in current behavior — they describe what the code DOES today,
 * not what it should ideally do. If a case looks surprising, a comment
 * explains why.
 */
import { render } from '@testing-library/react'
import { DeltaIndicator } from '../../src/components/RouteTrend'

/** Helper: render DeltaIndicator and return the <span> element. */
function renderDelta(props) {
  const { container } = render(<DeltaIndicator {...props} />)
  return container.firstChild
}

const fmt = (d) => `${d.toFixed(1)} pp`

describe('DeltaIndicator', () => {
  test('returns null (renders nothing) when delta is null', () => {
    const el = renderDelta({ delta: null, format: fmt })
    expect(el).toBeNull()
  })

  test('returns null when delta is undefined', () => {
    const el = renderDelta({ delta: undefined, format: fmt })
    expect(el).toBeNull()
  })

  // ── Positive delta above threshold ──────────────────────────────────────

  test('positive above threshold, lowerIsBetter=false → ▲ green', () => {
    const el = renderDelta({ delta: 2, format: fmt, lowerIsBetter: false })
    expect(el).toHaveTextContent('▲')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  test('positive above threshold, lowerIsBetter=true → ▲ red (worsening)', () => {
    const el = renderDelta({ delta: 2, format: fmt, lowerIsBetter: true })
    expect(el).toHaveTextContent('▲')
    expect(el).toHaveStyle({ color: '#C8102E' })
  })

  // ── Negative delta below threshold ───────────────────────────────────────

  test('negative below threshold, lowerIsBetter=false → ▼ red (worsening)', () => {
    const el = renderDelta({ delta: -2, format: fmt, lowerIsBetter: false })
    expect(el).toHaveTextContent('▼')
    expect(el).toHaveStyle({ color: '#C8102E' })
  })

  test('negative below threshold, lowerIsBetter=true → ▼ green (improving)', () => {
    const el = renderDelta({ delta: -2, format: fmt, lowerIsBetter: true })
    expect(el).toHaveTextContent('▼')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  // ── Near-zero (within default ±0.5 flatThreshold) ────────────────────────

  test('delta=0.3 (below default threshold 0.5) → → gray flat arrow', () => {
    const el = renderDelta({ delta: 0.3, format: fmt })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  test('delta=-0.3 (above default threshold -0.5) → → gray flat arrow', () => {
    const el = renderDelta({ delta: -0.3, format: fmt })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  // ── Exactly at threshold — boundary behavior ──────────────────────────────
  // delta === flatThreshold is NOT > flatThreshold so it renders as flat (→).
  // This is a characterization: the code uses strict >, so 0.5 at the default
  // threshold is treated as flat, not as up.

  test('delta exactly at default threshold (0.5) → → gray flat (strict > only)', () => {
    const el = renderDelta({ delta: 0.5, format: fmt })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  test('delta exactly at negative default threshold (-0.5) → → gray flat', () => {
    const el = renderDelta({ delta: -0.5, format: fmt })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  // ── Just past threshold ───────────────────────────────────────────────────

  test('delta 0.51 (just past default threshold) → ▲ with directional color', () => {
    const el = renderDelta({ delta: 0.51, format: fmt, lowerIsBetter: false })
    expect(el).toHaveTextContent('▲')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  // ── Sign prefix ───────────────────────────────────────────────────────────
  // Positive delta gets '+' prefix; negative does not.

  test('positive delta shows + sign prefix', () => {
    const el = renderDelta({ delta: 3.0, format: fmt })
    expect(el).toHaveTextContent('+3.0 pp')
  })

  test('negative delta has no + sign prefix', () => {
    const el = renderDelta({ delta: -3.0, format: fmt })
    // Should show "-3.0 pp" — the minus comes from toFixed on the negative number
    expect(el).not.toHaveTextContent('+')
    expect(el).toHaveTextContent('-3.0 pp')
  })

  // ── Custom flatThreshold ──────────────────────────────────────────────────

  test('custom flatThreshold=2: delta=1.5 → → flat', () => {
    const el = renderDelta({ delta: 1.5, format: fmt, flatThreshold: 2 })
    expect(el).toHaveTextContent('→')
  })

  test('custom flatThreshold=2: delta=2.1 → ▲ colored', () => {
    const el = renderDelta({ delta: 2.1, format: fmt, flatThreshold: 2, lowerIsBetter: false })
    expect(el).toHaveTextContent('▲')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  // ── Default title prop ────────────────────────────────────────────────────

  test('default title is "7-day mean vs prior 7-day mean"', () => {
    const el = renderDelta({ delta: 1, format: fmt })
    expect(el).toHaveAttribute('title', '7-day mean vs prior 7-day mean')
  })

  test('custom title prop is used', () => {
    const el = renderDelta({ delta: 1, format: fmt, title: 'Custom tooltip' })
    expect(el).toHaveAttribute('title', 'Custom tooltip')
  })
})
