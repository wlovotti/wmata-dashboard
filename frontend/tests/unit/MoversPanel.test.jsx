/**
 * MoversPanel (NOTES-84): WhatChangedPanel's degradations list promoted to
 * the top fold. Pins the honesty rules: only valid deltas rank, and fewer
 * than MIN_VALID_MOVERS rows renders a message, not a pseudo-ranking
 * (the NOTES-44 information-content lesson).
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import MoversPanel, { getMoversFloor } from '../../src/components/MoversPanel'

const route = (id, otpDelta, valid = true) => ({
  route_id: id,
  route_name: id,
  route_long_name: `Route ${id}`,
  otp_all_pct: 70,
  deltas: { otp: { value: otpDelta, valid, current_n: 7, prior_n: 7 } },
})

/** Build a scorecard row with a delta on an arbitrary metric (NOTES-121 magnitude-floor tests). */
const routeMetric = (id, metric, deltaValue, valid = true) => ({
  route_id: id,
  route_name: id,
  route_long_name: `Route ${id}`,
  otp_all_pct: 70,
  service_delivered_ratio: 0.9,
  ewt_seconds: 180,
  bunching_rate: 0.1,
  deltas: { [metric]: { value: deltaValue, valid, current_n: 7, prior_n: 7 } },
})

function renderPanel(routes) {
  return render(
    <MemoryRouter>
      <MoversPanel routes={routes} />
    </MemoryRouter>,
  )
}

describe('MoversPanel', () => {
  test('ranks worsening routes by |delta| descending, worst first', () => {
    renderPanel([route('A', -1.2), route('B', -6.1), route('C', -3.9), route('D', 2.0)])
    const rows = screen.getAllByRole('row').slice(1) // drop header row
    expect(rows[0]).toHaveTextContent('B')
    expect(rows[1]).toHaveTextContent('C')
    expect(rows[2]).toHaveTextContent('A')
    // D improved — not in the worse list.
    expect(screen.queryByText('Route D')).not.toBeInTheDocument()
  })

  test('invalid deltas never rank', () => {
    renderPanel([route('A', -9.9, false), route('B', -1.0), route('C', -2.0), route('D', -3.0)])
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
  })

  test('fewer than 3 valid movers renders the not-enough-history message', () => {
    renderPanel([route('A', -1.0), route('B', -2.0)])
    expect(screen.getByText(/not enough history this week/i)).toBeVisible()
    expect(
      screen.getByText(/fewer than 3 routes moved meaningfully on on-time % week-over-week/i),
    ).toBeVisible()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  // A quiet week where many routes have *valid* but sub-floor deltas must
  // still explain itself as "didn't move enough," not "didn't have valid
  // deltas" — the two are different failure modes since NOTES-121 (empty
  // state copy must track the actual filter, not just MIN_VALID_MOVERS).
  test('all-valid but all-sub-floor deltas still render the meaningful-movement message', () => {
    renderPanel([
      route('A', -0.1),
      route('B', -0.2),
      route('C', -0.3),
      route('D', -0.4),
      route('E', 0.1),
    ])
    expect(
      screen.getByText(/fewer than 3 routes moved meaningfully on on-time % week-over-week/i),
    ).toBeVisible()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  test('toggle switches to improving routes', async () => {
    const user = userEvent.setup()
    renderPanel([
      route('A', 1.0),
      route('B', 2.0),
      route('C', 3.0),
      route('D', -1.0),
      route('E', -2.0),
      route('F', -3.0),
    ])
    await user.click(screen.getByRole('button', { name: /getting better/i }))
    const rows = screen.getAllByRole('row').slice(1)
    expect(rows[0]).toHaveTextContent('C')
    expect(screen.queryByText('Route F')).not.toBeInTheDocument()
  })

  // ── NOTES-121: per-metric magnitude floor ─────────────────────────────
  // A row must not rank in either movers direction when its own delta
  // would render flat (gray →) via DeltaIndicator. Floors are per-metric
  // native units — see MOVERS_FLAT_FLOOR in MoversPanel.jsx.

  test('otp: delta at or below the 0.5pp floor is excluded from ranking', () => {
    renderPanel([
      route('A', -0.3), // below floor
      route('B', -0.5), // exactly at floor — DeltaIndicator's own boundary is flat too
      route('C', -1.0),
      route('D', -2.0),
      route('E', -3.0),
    ])
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
    expect(screen.queryByText('Route B')).not.toBeInTheDocument()
    expect(screen.getByText('Route C')).toBeInTheDocument()
  })

  test('ewt: delta at or below the 10s floor is excluded from ranking', async () => {
    const user = userEvent.setup()
    renderPanel([
      routeMetric('A', 'ewt', 5),
      routeMetric('B', 'ewt', 10),
      routeMetric('C', 'ewt', 15),
      routeMetric('D', 'ewt', 20),
      routeMetric('E', 'ewt', 25),
    ])
    await user.selectOptions(screen.getByLabelText(/metric/i), 'ewt')
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
    expect(screen.queryByText('Route B')).not.toBeInTheDocument()
    expect(screen.getByText('Route C')).toBeInTheDocument()
  })

  test('service_delivered: delta below the 0.005-fraction floor is excluded from ranking', async () => {
    const user = userEvent.setup()
    renderPanel([
      routeMetric('A', 'service_delivered', -0.001),
      routeMetric('B', 'service_delivered', -0.01),
      routeMetric('C', 'service_delivered', -0.02),
      routeMetric('D', 'service_delivered', -0.03),
    ])
    await user.selectOptions(screen.getByLabelText(/metric/i), 'service_delivered')
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
    expect(screen.getByText('Route B')).toBeInTheDocument()
  })

  test('bunching: delta below the 0.005-fraction floor is excluded from ranking', async () => {
    const user = userEvent.setup()
    renderPanel([
      routeMetric('A', 'bunching', 0.001),
      routeMetric('B', 'bunching', 0.01),
      routeMetric('C', 'bunching', 0.02),
      routeMetric('D', 'bunching', 0.03),
    ])
    await user.selectOptions(screen.getByLabelText(/metric/i), 'bunching')
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
    expect(screen.getByText('Route B')).toBeInTheDocument()
  })

  test('rows that clear the floor never render a flat DeltaIndicator arrow (acceptance criterion)', async () => {
    // service_delivered deltas here (±0.01-0.03 fraction) all clear the
    // 0.005 floor but sit well under DeltaIndicator's own 0.5 default
    // flatThreshold — this only passes if MoversPanel passes a matching
    // per-metric flatThreshold prop into DeltaIndicator, not just filters
    // the ranking list.
    const user = userEvent.setup()
    renderPanel([
      routeMetric('A', 'service_delivered', -0.01),
      routeMetric('B', 'service_delivered', -0.02),
      routeMetric('C', 'service_delivered', -0.03),
    ])
    await user.selectOptions(screen.getByLabelText(/metric/i), 'service_delivered')
    const rows = screen.getAllByRole('row').slice(1)
    expect(rows).toHaveLength(3)
    for (const row of rows) {
      expect(row).not.toHaveTextContent('→')
    }
  })

  // Review finding (PR #215): the ranking filter and the DeltaIndicator
  // flatThreshold prop must derive from the same fallback, not two
  // independently-written `?? ...` defaults — otherwise a metric key absent
  // from MOVERS_FLAT_FLOOR could rank a row whose arrow renders flat again
  // (the exact bug NOTES-121 fixed, reintroduced for any future metric).
  // getMoversFloor is the single source of truth both call sites read from
  // (MoversPanel.jsx); testing it directly pins the fallback itself rather
  // than depending on MOVER_METRICS happening to expose an unlisted key
  // through the UI today.
  test('getMoversFloor falls back to 0.5 for a metric absent from MOVERS_FLAT_FLOOR', () => {
    expect(getMoversFloor('some_future_metric')).toBe(0.5)
    // And known metrics still get their explicit per-metric value, not the
    // fallback.
    expect(getMoversFloor('ewt')).toBe(10)
  })
})
