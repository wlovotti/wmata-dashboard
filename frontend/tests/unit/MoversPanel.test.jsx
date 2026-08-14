/**
 * MoversPanel (NOTES-84): WhatChangedPanel's degradations list promoted to
 * the top fold. Pins the honesty rules: only valid deltas rank, and fewer
 * than MIN_VALID_MOVERS rows renders a message, not a pseudo-ranking
 * (the NOTES-44 information-content lesson).
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import MoversPanel from '../../src/components/MoversPanel'

const route = (id, otpDelta, valid = true) => ({
  route_id: id,
  route_name: id,
  route_long_name: `Route ${id}`,
  otp_all_pct: 70,
  deltas: { otp: { value: otpDelta, valid, current_n: 7, prior_n: 7 } },
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
})
