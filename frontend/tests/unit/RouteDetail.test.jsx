/**
 * RouteDetail server-delta flatThreshold pass-through (NOTES-127).
 *
 * Same fix as RouteList.test.jsx: RouteDetail's local `renderServerDelta`
 * historically never passed a `flatThreshold`, so service_delivered/bunching
 * deltas (0..1 fractions) fell back to DeltaIndicator's unit-blind 0.5
 * default and rendered flat regardless of actual magnitude. Heavy
 * subcomponents (map, diagnostics, runs/blocks tabs) are stubbed out —
 * unrelated to this fix and each would need their own fetch mocking.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { vi } from 'vitest'
import RouteDetail from '../../src/components/RouteDetail'

vi.mock('../../src/components/RouteMap', () => ({
  default: () => <div data-testid="route-map-stub" />,
}))
vi.mock('../../src/components/StopDiagnostic', () => ({
  default: () => <div data-testid="stop-diagnostic-stub" />,
}))
vi.mock('../../src/components/PeriodDrilldown', () => ({
  default: () => <div data-testid="period-drilldown-stub" />,
}))
vi.mock('../../src/components/RouteDiagnosisPanel', () => ({
  default: () => <div data-testid="route-diagnosis-stub" />,
}))
vi.mock('../../src/components/RecentRuns', () => ({
  default: () => <div data-testid="recent-runs-stub" />,
}))
vi.mock('../../src/components/BlockList', () => ({
  default: () => <div data-testid="block-list-stub" />,
}))

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

const routeData = {
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
    // under DeltaIndicator's own 0.5 default.
    service_delivered: { value: 0.02, valid: true, current_n: 7, prior_n: 7 },
    ewt: { value: 15, valid: true, current_n: 7, prior_n: 7 },
    bunching: { value: 0.02, valid: true, current_n: 7, prior_n: 7 },
  },
}

function mockFetch() {
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      const u = String(url)
      if (u.startsWith('/api/routes/D72/trend')) return jsonResponse({ trend_data: [] })
      if (u.startsWith('/api/routes/D72')) return jsonResponse(routeData)
      return jsonResponse({})
    }),
  )
}

function renderDetail() {
  return render(
    <MemoryRouter initialEntries={['/route/D72']}>
      <Routes>
        <Route path="/route/:routeId" element={<RouteDetail />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('RouteDetail server delta flatThreshold', () => {
  beforeEach(() => {
    mockFetch()
  })

  test('service_delivered delta of 0.02 (2pp) renders a colored, non-flat arrow', async () => {
    const { container } = renderDetail()
    await waitFor(() => screen.getAllByText('Service Delivered'))
    // "Service Delivered" also labels a RouteTrend sparkline card — scope to
    // the KPI stat-card label specifically (`.stat-label`, not
    // `.route-trend-label`) to avoid an ambiguous match.
    const card = [...container.querySelectorAll('.stat-label')].find((el) =>
      el.textContent.includes('Service Delivered'),
    )
    expect(card).toHaveTextContent('▲')
    expect(card).not.toHaveTextContent('→')
  })

  test('bunching delta of 0.02 (2pp) renders a colored, non-flat arrow', async () => {
    const { container } = renderDetail()
    await waitFor(() => screen.getByText('Bunching Rate'))
    const card = [...container.querySelectorAll('.stat-label')].find((el) =>
      el.textContent.includes('Bunching Rate'),
    )
    expect(card).toHaveTextContent('▲')
    expect(card).not.toHaveTextContent('→')
  })
})
