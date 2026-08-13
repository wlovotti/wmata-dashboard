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
 *
 * Missing/malformed data is a distinct claim from "confirmed zero bunched
 * pairs": the former renders "Cause breakdown unavailable.", the latter
 * renders "No bunched pairs in the selected window." — asserting on the
 * exact text keeps this from silently reverting to a false zero-claim.
 */
import { render } from '@testing-library/react'
import { BunchingCauseBar } from '../../src/components/PeriodDrilldown'

describe('BunchingCauseBar', () => {
  test('non-zero n_bunched_pairs with no breakdown renders "unavailable", not a zero-claim, without throwing', () => {
    const { getByText } = render(<BunchingCauseBar data={{ n_bunched_pairs: 42 }} />)
    expect(getByText('Cause breakdown unavailable.')).toBeInTheDocument()
  })

  test('empty object payload renders "unavailable" without throwing', () => {
    const { getByText } = render(<BunchingCauseBar data={{}} />)
    expect(getByText('Cause breakdown unavailable.')).toBeInTheDocument()
  })

  test('null data renders "unavailable" (not a zero-claim — no fetch response yet)', () => {
    const { getByText } = render(<BunchingCauseBar data={null} />)
    expect(getByText('Cause breakdown unavailable.')).toBeInTheDocument()
  })

  test('n_bunched_pairs === 0 with a present breakdown renders the true empty state', () => {
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
