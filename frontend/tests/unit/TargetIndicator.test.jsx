/**
 * Characterization tests for TargetIndicator (RouteTrend.jsx).
 *
 * TargetIndicator compares value vs target and renders ✓/✗/→ with green/red/gray.
 * higherIsBetter flips which side of target is "good."
 */
import { render } from '@testing-library/react'
import { TargetIndicator } from '../../src/components/RouteTrend'

const fmt = (t) => `${t.toFixed(0)}%`

function renderTarget(props) {
  const { container } = render(<TargetIndicator {...props} />)
  return container.firstChild
}

describe('TargetIndicator', () => {
  test('returns null when value is null', () => {
    expect(renderTarget({ value: null, target: 80, format: fmt })).toBeNull()
  })

  test('returns null when target is null', () => {
    expect(renderTarget({ value: 75, target: null, format: fmt })).toBeNull()
  })

  test('returns null when both are null', () => {
    expect(renderTarget({ value: null, target: null, format: fmt })).toBeNull()
  })

  // ── higherIsBetter=true (default) — OTP-style ────────────────────────────

  test('value > target, higherIsBetter=true → ✓ green (gap > flatThreshold=0)', () => {
    const el = renderTarget({ value: 85, target: 80, format: fmt })
    expect(el).toHaveTextContent('✓')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  test('value < target, higherIsBetter=true → ✗ red (gap < 0)', () => {
    const el = renderTarget({ value: 70, target: 80, format: fmt })
    expect(el).toHaveTextContent('✗')
    expect(el).toHaveStyle({ color: '#C8102E' })
  })

  // value === target → gap=0, not > 0 and not < 0, so → gray.
  test('value === target, higherIsBetter=true → → gray (at exactly target)', () => {
    const el = renderTarget({ value: 80, target: 80, format: fmt })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  // ── higherIsBetter=false — EWT/bunching-style ─────────────────────────────

  test('value < target, higherIsBetter=false → ✓ green (beating lower target)', () => {
    // EWT: target=120s, current=90s → gap = target-value = 30 > 0 → green
    const el = renderTarget({ value: 90, target: 120, format: fmt, higherIsBetter: false })
    expect(el).toHaveTextContent('✓')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  test('value > target, higherIsBetter=false → ✗ red (exceeding lower target)', () => {
    // EWT: target=120s, current=150s → gap = 120-150 = -30 < 0 → red
    const el = renderTarget({ value: 150, target: 120, format: fmt, higherIsBetter: false })
    expect(el).toHaveTextContent('✗')
    expect(el).toHaveStyle({ color: '#C8102E' })
  })

  // ── Custom flatThreshold ──────────────────────────────────────────────────
  // Default flatThreshold=0: any gap above 0 is green.
  // Custom flatThreshold=5: a gap of 3 (inside the band) should be neutral gray.

  test('gap within flatThreshold → → gray', () => {
    // gap = 83 - 80 = 3, flatThreshold = 5 → 3 is not > 5, not < -5 → gray
    const el = renderTarget({ value: 83, target: 80, format: fmt, flatThreshold: 5 })
    expect(el).toHaveTextContent('→')
    expect(el).toHaveStyle({ color: '#64748b' })
  })

  test('gap beyond flatThreshold → ✓ green', () => {
    // gap = 90 - 80 = 10 > 5 → green
    const el = renderTarget({ value: 90, target: 80, format: fmt, flatThreshold: 5 })
    expect(el).toHaveTextContent('✓')
    expect(el).toHaveStyle({ color: '#0E8A6F' })
  })

  // ── Rendered content ──────────────────────────────────────────────────────

  test('renders label and formatted target value', () => {
    const el = renderTarget({ value: 85, target: 80, format: fmt, label: 'Target' })
    expect(el).toHaveTextContent('Target')
    expect(el).toHaveTextContent('80%')
  })

  test('custom label is shown', () => {
    const el = renderTarget({ value: 85, target: 80, format: fmt, label: 'Goal' })
    expect(el).toHaveTextContent('Goal')
  })

  test('title attribute includes the lowercased label', () => {
    const el = renderTarget({ value: 85, target: 80, format: fmt, label: 'Target' })
    expect(el).toHaveAttribute('title', 'Current vs target')
  })
})
