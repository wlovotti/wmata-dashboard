/**
 * Overview agency-switch guard (PR #242 review finding 2).
 *
 * `useMultiFetch`'s `data` only updates once a fetch resolves, so a
 * genuine cross-agency cache miss (sfmta isn't pre-warmed) previously left
 * WMATA's scorecard rendering under the "Getting worse" movers panel (and
 * feeding the hero) for however long the cold sfmta fetch took. This pins
 * that switching agency immediately stops rendering the stale agency's
 * routes — MoversPanel falls back to its existing `routes == null`
 * (nothing rendered) state, same as a first cold mount — rather than
 * showing WMATA's routes under a switch that's already in flight to sfmta.
 *
 * `SystemMap` is mocked out (pulls in react-leaflet, unrelated here,
 * mirrors Overview.test.jsx's existing convention).
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import Overview from '../../src/components/Overview'
import useAgency from '../../src/hooks/useAgency'

vi.mock('../../src/components/SystemMap', () => ({
  default: () => <div data-testid="system-map-stub" />,
}))

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

// Three routes with a valid, floor-clearing "worse" OTP delta so
// MoversPanel's default "Getting worse" view renders an actual table
// (not its own "not enough history" message) for wmata.
const wmataRoutes = [
  { route_id: 'D72', route_name: 'D72', route_long_name: 'Wisconsin Ave', otp_all_pct: 60, deltas: { otp: { value: -6, valid: true, current_n: 7, prior_n: 7 } } },
  { route_id: 'H8', route_name: 'H8', route_long_name: 'Park Rd-Brookland', otp_all_pct: 55, deltas: { otp: { value: -7, valid: true, current_n: 7, prior_n: 7 } } },
  { route_id: 'X2', route_name: 'X2', route_long_name: 'Benning Rd', otp_all_pct: 50, deltas: { otp: { value: -8, valid: true, current_n: 7, prior_n: 7 } } },
]

function Harness() {
  // Stand-in for AgencyToggle (rendered in App.jsx's header, outside
  // Overview) so this test can flip `?agency=` without remounting Overview.
  const [, setAgency] = useAgency()
  return (
    <>
      <button onClick={() => setAgency('sfmta')}>switch to sfmta</button>
      <Overview />
    </>
  )
}

function renderHarness() {
  return render(
    <MemoryRouter>
      <Harness />
    </MemoryRouter>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('Overview agency-switch guard', () => {
  test('switching agency stops rendering the previous agency\'s routes while the new fetch is cold', async () => {
    let resolveSfmtaRoutes
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.includes('agency=sfmta') && u.startsWith('/api/routes') && !u.includes('contributors')) {
          // Never resolves during this test — simulates an in-flight cold
          // fetch for the newly-selected agency.
          return new Promise((resolve) => {
            resolveSfmtaRoutes = resolve
          })
        }
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        if (u.startsWith('/api/routes')) return jsonResponse({ routes: wmataRoutes })
        if (u.startsWith('/api/system/trend')) return jsonResponse({ trend_data: [] })
        return jsonResponse({})
      }),
    )

    renderHarness()

    // wmata's "Getting worse" movers table renders with real data first.
    await waitFor(() => expect(screen.getByText('X2')).toBeVisible())

    screen.getByRole('button', { name: /switch to sfmta/i }).click()

    // sfmta's /api/routes fetch is deliberately left pending — the stale
    // wmata routes must disappear immediately rather than linger. All
    // three checked inside the same `waitFor` poll (not one `waitFor`
    // followed by synchronous checks) so a transient in-between render
    // can't make this pass on a fluke ordering.
    await waitFor(() => {
      expect(screen.queryByText('X2')).not.toBeInTheDocument()
      expect(screen.queryByText('D72')).not.toBeInTheDocument()
      expect(screen.queryByText('H8')).not.toBeInTheDocument()
    })

    // Clean up the never-resolved promise so it doesn't dangle.
    resolveSfmtaRoutes?.({ ok: true, json: () => Promise.resolve({ routes: [] }) })
  })
})
