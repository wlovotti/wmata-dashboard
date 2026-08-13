/**
 * Regression test for BunchingCauseBar (PeriodDrilldown.jsx) — NOTES-83.
 *
 * The real /api/routes/{id}/bunching-causes endpoint always returns
 * `n_bunched_pairs` and `breakdown` together (see
 * src/bunching.py:compute_bunching_cause_breakdown), but a malformed or
 * short-circuit-fulfilled payload (e.g. `{}`) has neither. Before the
 * NOTES-83 fix, `data.n_bunched_pairs === 0` was false for `undefined`,
 * so the component fell through to `data.breakdown[key]` and threw —
 * with no error boundary above it, that unmounted the whole RouteDetail
 * page (the blank checked-in Playwright baselines this item closes).
 */
import { render } from '@testing-library/react'
import { BunchingCauseBar } from '../../src/components/PeriodDrilldown'

describe('BunchingCauseBar', () => {
  test('malformed data (no breakdown, non-zero n_bunched_pairs) renders empty state instead of throwing', () => {
    expect(() => render(<BunchingCauseBar data={{ n_bunched_pairs: undefined }} />)).not.toThrow()
  })

  test('empty object payload renders empty state instead of throwing', () => {
    expect(() => render(<BunchingCauseBar data={{}} />)).not.toThrow()
  })

  test('null data renders the "no bunched pairs" empty state', () => {
    const { getByText } = render(<BunchingCauseBar data={null} />)
    expect(getByText('No bunched pairs in the selected window.')).toBeInTheDocument()
  })

  test('n_bunched_pairs === 0 renders the empty state', () => {
    const { getByText } = render(
      <BunchingCauseBar data={{ n_bunched_pairs: 0, breakdown: {} }} />,
    )
    expect(getByText('No bunched pairs in the selected window.')).toBeInTheDocument()
  })

  test('well-formed data renders the cause breakdown', () => {
    const { getByText } = render(
      <BunchingCauseBar
        data={{
          n_bunched_pairs: 42,
          days: 30,
          breakdown: {
            leader_late_only: { count: 18, pct: 0.4286 },
            trailer_early_only: { count: 11, pct: 0.2619 },
            both_off: { count: 6, pct: 0.1429 },
            neither_off: { count: 5, pct: 0.119 },
            unknown: { count: 2, pct: 0.0476 },
          },
        }}
      />,
    )
    expect(getByText('42 bunched pairs over the past 30 days.')).toBeInTheDocument()
    expect(getByText('18 (42.9%)')).toBeInTheDocument()
  })
})
