/**
 * RouteDetail error-banner recovery (PR #242 review finding 1).
 *
 * The route-detail fetch effect previously only ever set `error` inside
 * its `.catch` — never cleared it at the top of a new run — so one 404
 * (e.g. switching to Muni on a route_id that only exists in WMATA's DB)
 * latched the error banner permanently: `if (error || !routeData)`
 * short-circuited every subsequent successful fetch, even after switching
 * back to an agency/route that resolves fine. This pins that a 404
 * followed by a 200 clears the banner and renders the route.
 */
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { vi } from 'vitest'
import RouteDetail from '../../src/components/RouteDetail'
import useAgency from '../../src/hooks/useAgency'

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

function jsonResponse(body, ok = true) {
  return Promise.resolve({ ok, status: ok ? 200 : 404, json: () => Promise.resolve(body) })
}

const wmataRouteData = {
  route_id: 'D72',
  route_name: 'D72',
  route_long_name: 'Friendly Shores - Wheaton',
  otp_all_pct: 70,
  service_delivered_ratio: 0.9,
  ewt_seconds: 180,
  bunching_rate: 0.1,
  otp_window: 'official',
  deltas_otp_window: 'official',
  deltas: {},
}

// Stand-in for AgencyToggle (rendered in App.jsx's header, outside
// RouteDetail) so this test can flip `?agency=` without remounting.
function Harness() {
  const [, setAgency] = useAgency()
  return (
    <>
      <button onClick={() => setAgency('wmata')}>switch to wmata</button>
      <Routes>
        <Route path="/route/:routeId" element={<RouteDetail />} />
      </Routes>
    </>
  )
}

function renderHarness(initialPath) {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <Harness />
    </MemoryRouter>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('RouteDetail error-banner recovery', () => {
  test('a 404 followed by a successful fetch clears the error banner', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/D72/trend')) return jsonResponse({ trend_data: [] })
        if (u.startsWith('/api/routes/D72')) {
          if (u.includes('agency=sfmta')) {
            return jsonResponse({ error: 'Route D72 not found' }, false)
          }
          return jsonResponse(wmataRouteData)
        }
        return jsonResponse({})
      }),
    )

    renderHarness('/route/D72?agency=sfmta')

    await waitFor(() =>
      expect(screen.getByText(/Error loading route data/i)).toBeVisible(),
    )

    screen.getByRole('button', { name: /switch to wmata/i }).click()

    await waitFor(() =>
      expect(screen.queryByText(/Error loading route data/i)).not.toBeInTheDocument(),
    )
    await waitFor(() => expect(screen.getAllByText('On-Time Performance')[0]).toBeVisible())
  })
})

/**
 * PR #242 round-2 review finding 4 (cosmetic): the finding-1 fix's
 * unconditional `setLoading(true)` flashed a full-page spinner on every
 * filter change (dayType/period/otpWindow) where it previously swapped
 * data in place. `setLoading(true)` should only fire on a genuine
 * identity change (a different route_id or agency) — a filter change
 * re-fetches the SAME route/agency and should keep stale-while-revalidate
 * behavior: the old content stays on screen (no "Loading route
 * details..." spinner) while the re-sliced data loads in the background.
 */
describe('RouteDetail filter changes keep stale-while-revalidate (no spinner flash)', () => {
  test('changing the day-type filter does not show the full-page loading spinner', async () => {
    let resolveWeekday
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/D72/trend')) return jsonResponse({ trend_data: [] })
        if (u.startsWith('/api/routes/D72')) {
          if (u.includes('day_type=weekday')) {
            return new Promise((resolve) => {
              resolveWeekday = resolve
            })
          }
          return jsonResponse(wmataRouteData)
        }
        return jsonResponse({})
      }),
    )

    render(
      <MemoryRouter initialEntries={['/route/D72']}>
        <Routes>
          <Route path="/route/:routeId" element={<RouteDetail />} />
        </Routes>
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getAllByText('On-Time Performance')[0]).toBeVisible())

    fireEvent.change(screen.getByLabelText('Day-type filter'), { target: { value: 'weekday' } })

    // The weekday fetch is deliberately left pending, but the page must
    // NOT fall back to the full-page spinner — the previously loaded
    // content (and its filter controls) stay on screen the whole time.
    expect(screen.queryByText('Loading route details...')).not.toBeInTheDocument()
    expect(screen.getAllByText('On-Time Performance')[0]).toBeVisible()

    resolveWeekday(jsonResponse(wmataRouteData))
    await waitFor(() => expect(screen.getAllByText('On-Time Performance')[0]).toBeVisible())
  })
})
