/**
 * Overview (NOTES-84) — partial-day disclosure line above the "Biggest
 * drags" contributors table (NOTES-123 review finding 10).
 *
 * The line only renders when `contribData.days_included < contribData.days`;
 * neither `routes_contributors.json` (the e2e fixture) nor any prior test
 * exercised that branch, so a regression there would go unnoticed. `SystemMap`
 * is mocked out — it pulls in `react-leaflet`, which is unrelated to what
 * this test covers and not something any existing suite exercises in jsdom.
 * Mirrors the mock-fetch pattern in AgencyComparison.test.jsx.
 */
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import Overview from '../../src/components/Overview'

vi.mock('../../src/components/SystemMap', () => ({
  default: () => <div data-testid="system-map-stub" />,
}))

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

const baseContributors = {
  metric: 'otp',
  days: 30,
  baseline_value: 73.2,
  system_target_value: 75.0,
  contributors: [
    {
      route_id: 'D72',
      route_short_name: 'D72',
      route_long_name: 'Friendly Shores - Wheaton',
      route_value: 68.4,
      baseline_value: 73.2,
      reference_value: 75.0,
      contribution_score: 1120.4,
      scheduled_trips: 210,
    },
  ],
}

function mockFetch(contributorsPayload) {
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      const u = String(url)
      if (u.startsWith('/api/routes/contributors')) return jsonResponse(contributorsPayload)
      if (u.startsWith('/api/routes')) return jsonResponse({ routes: [] })
      if (u.startsWith('/api/system/trend')) return jsonResponse({ trend_data: [] })
      return jsonResponse({})
    }),
  )
}

function renderOverview() {
  return render(
    <MemoryRouter>
      <Overview />
    </MemoryRouter>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('Overview partial-day disclosure', () => {
  test('renders the days-excluded line when days_included < days', async () => {
    mockFetch({ ...baseContributors, days_included: 27 })
    renderOverview()

    await waitFor(() => expect(screen.getByText(/Based on 27 of 30 days/)).toBeVisible())
    expect(screen.getByText(/3 excluded for partial data collection/)).toBeVisible()
  })

  test('omits the line when days_included equals days', async () => {
    mockFetch({ ...baseContributors, days_included: 30 })
    renderOverview()

    await waitFor(() => expect(screen.getByText('D72')).toBeVisible())
    expect(screen.queryByText(/excluded for partial data collection/)).not.toBeInTheDocument()
  })

  test('omits the line when days_included is absent from the payload', async () => {
    mockFetch(baseContributors)
    renderOverview()

    await waitFor(() => expect(screen.getByText('D72')).toBeVisible())
    expect(screen.queryByText(/excluded for partial data collection/)).not.toBeInTheDocument()
  })
})

describe('Overview cold-load failure retry (NOTES-85)', () => {
  test('clicking Retry re-fetches /api/routes and clears the error banner', async () => {
    let routesCallCount = 0
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/contributors')) return jsonResponse(baseContributors)
        if (u.startsWith('/api/routes')) {
          routesCallCount += 1
          if (routesCallCount === 1) {
            return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve(null) })
          }
          return jsonResponse({ routes: [] })
        }
        if (u.startsWith('/api/system/trend')) return jsonResponse({ trend_data: [] })
        return jsonResponse({})
      }),
    )
    renderOverview()

    await waitFor(() =>
      expect(screen.getByText(/Unable to load system data/)).toBeVisible(),
    )
    const retryBtn = screen.getByRole('button', { name: 'Retry' })

    fireEvent.click(retryBtn)

    await waitFor(() =>
      expect(screen.queryByText(/Unable to load system data/)).not.toBeInTheDocument(),
    )
    expect(routesCallCount).toBe(2)
  })
})
