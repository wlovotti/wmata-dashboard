/**
 * RouteDetail rider-experience OTP toggle (NOTES-143, backend in PR #241).
 *
 * Covers: the headline label swap, the OTP delta arrow being hidden when
 * `deltas_otp_window` doesn't match the requested `otp_window` (rather
 * than a hardcoded "hide when rider" check), and that flipping the toggle
 * sends `otp_window=rider` on the route-detail and OTP-trend fetches.
 * Heavy subcomponents (map, diagnostics, runs/blocks tabs) are stubbed
 * out, matching RouteDetail.test.jsx's existing convention.
 *
 * "On-Time Performance" is rendered twice when metrics are present — once
 * as the KPI card's `.stat-label`, once as the trend sparkline's
 * `.route-trend-label` — so assertions scope to `.stat-label` the same way
 * RouteDetail.test.jsx already does for "Service Delivered".
 */
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
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

const baseRouteData = {
  route_id: 'D72',
  route_name: 'D72',
  route_long_name: 'Friendly Shores - Wheaton',
  otp_all_pct: 70,
  service_delivered_ratio: 0.9,
  ewt_seconds: 180,
  bunching_rate: 0.1,
  // The real API always echoes both fields (PR #242 review finding 8):
  // `otp_window` is the window it actually computed the live OTP fields
  // with, `deltas_otp_window` is the window the (separately-sourced)
  // delta block used. The mismatch check compares these two server-echoed
  // fields to each other, not to this component's own local URL state.
  otp_window: 'official',
  deltas_otp_window: 'official',
  deltas: {
    otp: { value: 1.0, valid: true, current_n: 7, prior_n: 7 },
  },
  is_frequent: false,
  targets: { otp: 90.0, service_delivered: 0.95, ewt: 180.0, bunching: 0.04 },
}

function findStatLabel(container, text) {
  return [...container.querySelectorAll('.stat-label')].find((el) =>
    el.textContent.includes(text),
  )
}

function renderDetail(initialPath, routeDataOverrides = {}) {
  const calls = []
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      calls.push(String(url))
      const u = String(url)
      if (u.startsWith('/api/routes/D72/trend')) return jsonResponse({ trend_data: [] })
      if (u.startsWith('/api/routes/D72')) {
        return jsonResponse({ ...baseRouteData, ...routeDataOverrides })
      }
      return jsonResponse({})
    }),
  )
  const utils = render(
    <MemoryRouter initialEntries={[initialPath]}>
      <Routes>
        <Route path="/route/:routeId" element={<RouteDetail />} />
      </Routes>
    </MemoryRouter>,
  )
  return { ...utils, calls }
}

describe('RouteDetail rider-experience OTP toggle', () => {
  afterEach(() => vi.unstubAllGlobals())

  test('official window (default): headline reads "On-Time Performance" and the delta arrow shows', async () => {
    const { container } = renderDetail('/route/D72')
    await waitFor(() => screen.getAllByText('On-Time Performance'))
    // The toggle's own label always reads "Rider-experience OTP" — only the
    // headline variant (with the window annotation) signals rider mode.
    expect(screen.queryByText(/Rider-experience OTP \(−1\/\+3 min\)/)).not.toBeInTheDocument()
    const card = findStatLabel(container, 'On-Time Performance')
    expect(card).toHaveTextContent('▲')
    expect(screen.queryByText(/scorecard and system-wide pages/i)).not.toBeInTheDocument()
  })

  test('rider window via URL: headline swaps, delta arrow hidden (deltas_otp_window mismatch), note renders', async () => {
    // Server echoes it actually computed the rider window (`otp_window:
    // "rider"`), but `deltas_otp_window` stays "official" (NOTES-144) --
    // that mismatch is what must suppress the arrow.
    const { container } = renderDetail('/route/D72?otp_window=rider', {
      otp_window: 'rider',
    })
    await waitFor(() => screen.getByText(/Rider-experience OTP \(−1\/\+3 min\)/))
    const card = findStatLabel(container, 'Rider-experience OTP')
    expect(card).not.toHaveTextContent('▲')
    expect(screen.getByText(/scorecard and system-wide pages/i)).toBeVisible()
  })

  test('rider window with a matching deltas_otp_window still shows the arrow', async () => {
    const { container } = renderDetail('/route/D72?otp_window=rider', {
      otp_window: 'rider',
      deltas_otp_window: 'rider',
    })
    await waitFor(() => screen.getByText(/Rider-experience OTP \(−1\/\+3 min\)/))
    const card = findStatLabel(container, 'Rider-experience OTP')
    expect(card).toHaveTextContent('▲')
    expect(screen.queryByText(/scorecard and system-wide pages/i)).not.toBeInTheDocument()
  })

  test('checking the rider toggle updates the URL and sends otp_window=rider on the detail + OTP trend fetches', async () => {
    const { calls } = renderDetail('/route/D72')
    await waitFor(() => screen.getAllByText('On-Time Performance'))

    fireEvent.click(screen.getByRole('checkbox', { name: /rider-experience otp/i }))

    await waitFor(() => screen.getByText(/Rider-experience OTP \(−1\/\+3 min\)/))
    await waitFor(() =>
      expect(
        calls.some((u) => u.startsWith('/api/routes/D72?') && u.includes('otp_window=rider')),
      ).toBe(true),
    )
    // `calls` accumulates both the pre-toggle (official) and post-toggle
    // (rider) fetches — take the last OTP-trend call, not the first.
    const otpTrendCalls = calls.filter(
      (u) => u.startsWith('/api/routes/D72/trend') && u.includes('metric=otp'),
    )
    expect(otpTrendCalls.at(-1)).toContain('otp_window=rider')
  })

  test('official window (default) fetches carry no otp_window param at all', async () => {
    const { calls } = renderDetail('/route/D72')
    await waitFor(() => screen.getAllByText('On-Time Performance'))
    expect(calls.some((u) => u.includes('otp_window'))).toBe(false)
  })

  test('official window shows the configured OTP target; rider window suppresses it (PR #242 review finding 3)', async () => {
    const official = renderDetail('/route/D72')
    await waitFor(() => screen.getAllByText('On-Time Performance'))
    const officialCard = findStatLabel(official.container, 'On-Time Performance')
    // 70% current vs a 90% target -> a "✗ Target 90%" badge renders.
    expect(officialCard).toHaveTextContent('Target 90%')
    official.unmount()

    const rider = renderDetail('/route/D72?otp_window=rider', { otp_window: 'rider' })
    await waitFor(() => screen.getByText(/Rider-experience OTP \(−1\/\+3 min\)/))
    const riderCard = findStatLabel(rider.container, 'Rider-experience OTP')
    // The configured target is calibrated against the official window --
    // comparing a rider-window OTP value against it would read "below
    // target" purely because the window changed, not performance.
    expect(riderCard).not.toHaveTextContent('Target 90%')
  })
})
