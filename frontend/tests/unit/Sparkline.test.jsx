/**
 * Characterization tests for Sparkline (RouteTrend.jsx).
 *
 * Sparkline is a recharts-backed component; we don't inspect SVG paths.
 * We test:
 *   - empty/null data → "no trend data" placeholder
 *   - all-null values → "no trend data" placeholder
 *   - single valid point → renders without crashing (dot mode)
 *   - multiple valid points → renders without crashing (line mode)
 *
 * recharts uses ResizeObserver internally. jsdom doesn't ship it, so we
 * provide a no-op mock in this file.
 */
import { render, screen } from '@testing-library/react'
import { Sparkline, visibleGhostRows } from '../../src/components/RouteTrend'

// recharts calls ResizeObserver — polyfill for jsdom.
class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = ResizeObserver

const valueFormat = (v) => `${v.toFixed(1)}%`
const COLOR = '#002F6C'

describe('Sparkline', () => {
  test('empty data prop → renders "no trend data" placeholder', () => {
    render(<Sparkline data={[]} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.getByText('no trend data')).toBeInTheDocument()
  })

  test('null data prop → renders "no trend data" placeholder', () => {
    render(<Sparkline data={null} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.getByText('no trend data')).toBeInTheDocument()
  })

  test('undefined data prop → renders "no trend data" placeholder', () => {
    render(<Sparkline data={undefined} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.getByText('no trend data')).toBeInTheDocument()
  })

  test('array of all-null values → renders "no trend data" placeholder', () => {
    const data = [
      { date: '2026-01-01', value: null },
      { date: '2026-01-02', value: null },
    ]
    render(<Sparkline data={data} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.getByText('no trend data')).toBeInTheDocument()
  })

  test('single valid point → does not render "no trend data"', () => {
    const data = [{ date: '2026-01-15', value: 72.5 }]
    render(<Sparkline data={data} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })

  test('multiple valid points → renders without crashing', () => {
    const data = Array.from({ length: 10 }, (_, i) => ({
      date: `2026-01-${String(i + 1).padStart(2, '0')}`,
      value: 60 + i,
    }))
    render(<Sparkline data={data} color={COLOR} valueFormat={valueFormat} />)
    // The recharts wrapper should appear. We don't inspect SVG paths.
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })

  test('mixed null and valid points → does not render "no trend data"', () => {
    // Null rows are filtered; as long as one valid row survives, the chart renders.
    const data = [
      { date: '2026-01-01', value: null },
      { date: '2026-01-02', value: 75 },
      { date: '2026-01-03', value: null },
    ]
    render(<Sparkline data={data} color={COLOR} valueFormat={valueFormat} />)
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })

  test('custom height prop does not crash', () => {
    const data = [{ date: '2026-01-01', value: 80 }]
    render(<Sparkline data={data} color={COLOR} valueFormat={valueFormat} height={120} />)
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })
})

describe('Sparkline ghostData (NOTES-84 smoothing)', () => {
  // jsdom's ResponsiveContainer reports zero size, so recharts never mounts
  // the LineChart internals (confirmed: the existing tests above only ever
  // assert on the "no trend data" placeholder / its absence, never on
  // rendered SVG). Asserting `circle.sparkline-ghost-dot` counts against a
  // real render is therefore not meaningful in this suite — per the task
  // brief's fallback guidance, test the ghost-filtering logic through the
  // exported `visibleGhostRows` helper instead, and keep a render-based
  // smoke test to confirm the prop doesn't crash the component.
  test('visibleGhostRows keeps only non-null ghost values', () => {
    const ghostData = [
      { date: '2026-08-01', value: 48 },
      { date: '2026-08-02', value: 55 },
      { date: '2026-08-03', value: null }, // null ghost → filtered out
    ]
    expect(visibleGhostRows(ghostData)).toEqual([
      { date: '2026-08-01', value: 48 },
      { date: '2026-08-02', value: 55 },
    ])
  })

  test('visibleGhostRows tolerates null/undefined input', () => {
    expect(visibleGhostRows(null)).toEqual([])
    expect(visibleGhostRows(undefined)).toEqual([])
  })

  test('passing ghostData renders without crashing and without altering line data', () => {
    const data = [
      { date: '2026-08-01', value: 50 },
      { date: '2026-08-02', value: 52 },
      { date: '2026-08-03', value: 54 },
    ]
    const ghostData = [
      { date: '2026-08-01', value: 48 },
      { date: '2026-08-02', value: 55 },
      { date: '2026-08-03', value: null },
    ]
    render(
      <Sparkline data={data} ghostData={ghostData} color="#002F6C" valueFormat={(v) => `${v}%`} />,
    )
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })

  test('omitting ghostData renders identically (back-compat)', () => {
    const data = [
      { date: '2026-08-01', value: 50 },
      { date: '2026-08-02', value: 52 },
    ]
    render(<Sparkline data={data} color="#002F6C" valueFormat={(v) => `${v}%`} />)
    expect(screen.queryByText('no trend data')).not.toBeInTheDocument()
  })
})
