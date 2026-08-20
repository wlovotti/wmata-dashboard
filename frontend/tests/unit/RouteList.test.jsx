/**
 * RouteList server-delta flatThreshold pass-through (PR #216).
 *
 * `renderServerDelta` wraps `DeltaIndicator` but historically never passed
 * a `flatThreshold`, so it fell back to `DeltaIndicator`'s own 0.5 default.
 * For `service_delivered` and `bunching` the server delta is a 0..1
 * fraction, so a real 2 pp swing (`0.02`) sat two orders of magnitude below
 * that default and rendered as a flat gray arrow regardless of actual
 * magnitude. This pins that a per-metric floor (from `../../src/moversFloor`,
 * the module MoversPanel's NOTES-121 fix now shares) is passed through, so
 * the same delta reads the same way on both surfaces.
 */
import { render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import RouteList from '../../src/components/RouteList'

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

const routes = [
  {
    route_id: 'D72',
    route_name: 'D72',
    route_long_name: 'Friendly Shores - Wheaton',
    otp_all_pct: 70,
    service_delivered_ratio: 0.9,
    ewt_seconds: 180,
    bunching_rate: 0.1,
    deltas: {
      otp: { value: 1.0, valid: true, current_n: 7, prior_n: 7 },
      // 0.02 fraction == 2 pp: clears the 0.005-fraction floor but sits well
      // under DeltaIndicator's own 0.5 default — only a per-metric
      // flatThreshold pass-through renders this as a colored, non-flat arrow.
      service_delivered: { value: 0.02, valid: true, current_n: 7, prior_n: 7 },
      ewt: { value: 15, valid: true, current_n: 7, prior_n: 7 },
      bunching: { value: 0.02, valid: true, current_n: 7, prior_n: 7 },
    },
  },
]

function mockFetch() {
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      const u = String(url)
      if (u.startsWith('/api/routes/contributors')) {
        return jsonResponse({ contributors: [], baseline_value: null })
      }
      if (u.startsWith('/api/routes')) {
        return jsonResponse({ window: null, routes })
      }
      return jsonResponse({})
    }),
  )
}

function renderRouteList() {
  return render(
    <MemoryRouter>
      <RouteList />
    </MemoryRouter>,
  )
}

describe('RouteList server delta flatThreshold', () => {
  beforeEach(() => {
    mockFetch()
  })

  test('service_delivered and bunching deltas of 0.02 (2pp) render colored, non-flat arrows', async () => {
    renderRouteList()
    // Switch to the "All routes" table, where renderServerDelta is used.
    await waitFor(() => screen.getByText('Friendly Shores - Wheaton'))
    const row = screen.getByText('Friendly Shores - Wheaton').closest('tr')
    const cells = within(row).getAllByRole('cell')
    // Columns: Route, Name, On-Time %, Service Delivered, EWT, Bunching.
    const serviceDeliveredCell = cells[3]
    const bunchingCell = cells[5]
    expect(serviceDeliveredCell).toHaveTextContent('▲')
    expect(serviceDeliveredCell).not.toHaveTextContent('→')
    expect(bunchingCell).toHaveTextContent('▲')
    expect(bunchingCell).not.toHaveTextContent('→')
  })
})
