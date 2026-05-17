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
import { Sparkline } from '../../src/components/RouteTrend'

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
