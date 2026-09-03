/**
 * Overview agency-switch guard (PR #242 review finding 2, reworked in
 * round-2 review finding 1).
 *
 * `useMultiFetch`'s `data` only updates once a fetch resolves, so a
 * genuine cross-agency cache miss (sfmta isn't pre-warmed) previously left
 * WMATA's scorecard rendering under the "Getting worse" movers panel (and
 * feeding the hero) for however long the cold sfmta fetch took. The FIRST
 * fix derived staleness from each fetch group's `loading` flag via a ref +
 * effect, but that raced `useMultiFetch`'s own effect (`loading` still
 * held its OLD value on the very render `urls` changed) and never actually
 * blanked stale data — verified here with the sfmta fetch left pending and
 * the assertion made with `fireEvent.click` (act-wrapped), not a raw,
 * un-awaited `.click()` that only samples one transient frame.
 *
 * The FIXED version compares `useMultiFetch`'s `dataUrlKey` (the url key
 * the CURRENT `data` actually came from) against the agency encoded in the
 * current url, which changes in the same state update as `data` itself —
 * no race window.
 *
 * `SystemMap` is mocked out (pulls in react-leaflet, unrelated here,
 * mirrors Overview.test.jsx's existing convention).
 */
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import Overview from '../../src/components/Overview'
import useAgency from '../../src/hooks/useAgency'
import useWindowDays from '../../src/hooks/useWindowDays'

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

const sfmtaRoutes = [
  { route_id: '14', route_name: '14', route_long_name: 'Mission', otp_all_pct: 40, deltas: { otp: { value: -9, valid: true, current_n: 7, prior_n: 7 } } },
  { route_id: '38', route_name: '38', route_long_name: 'Geary', otp_all_pct: 42, deltas: { otp: { value: -10, valid: true, current_n: 7, prior_n: 7 } } },
  { route_id: '49', route_name: '49', route_long_name: 'Van Ness-Mission', otp_all_pct: 44, deltas: { otp: { value: -11, valid: true, current_n: 7, prior_n: 7 } } },
]

// Stand-in for AgencyToggle (rendered in App.jsx's header, outside
// Overview) so tests can flip `?agency=` without remounting Overview.
function AgencyHarness() {
  const [, setAgency] = useAgency()
  return (
    <>
      <button onClick={() => setAgency('sfmta')}>switch to sfmta</button>
      <Overview />
    </>
  )
}

function renderAgencyHarness() {
  return render(
    <MemoryRouter>
      <AgencyHarness />
    </MemoryRouter>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('Overview agency-switch guard', () => {
  test('switching agency hides the previous agency\'s routes while the new fetch is cold, then shows the new agency\'s routes once it resolves', async () => {
    let resolveSfmtaRoutes
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.includes('agency=sfmta') && u.startsWith('/api/routes') && !u.includes('contributors')) {
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

    renderAgencyHarness()

    // wmata's "Getting worse" movers table renders with real data first.
    await waitFor(() => expect(screen.getByText('X2')).toBeVisible())

    fireEvent.click(screen.getByRole('button', { name: /switch to sfmta/i }))

    // sfmta's /api/routes fetch is deliberately left pending — the stale
    // wmata routes must disappear, not linger for the duration of the
    // in-flight fetch.
    await waitFor(() => {
      expect(screen.queryByText('X2')).not.toBeInTheDocument()
      expect(screen.queryByText('D72')).not.toBeInTheDocument()
      expect(screen.queryByText('H8')).not.toBeInTheDocument()
    })

    // Resolve the pending sfmta fetch — its own routes now render.
    resolveSfmtaRoutes(jsonResponse({ routes: sfmtaRoutes }))
    await waitFor(() => expect(screen.getByText('49')).toBeVisible())
  })

  test('a cold-load failure on the new agency shows an error state and no stale routes', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.includes('agency=sfmta') && u.startsWith('/api/routes') && !u.includes('contributors')) {
          return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({}) })
        }
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        if (u.startsWith('/api/routes')) return jsonResponse({ routes: wmataRoutes })
        if (u.startsWith('/api/system/trend')) return jsonResponse({ trend_data: [] })
        return jsonResponse({})
      }),
    )

    renderAgencyHarness()
    await waitFor(() => expect(screen.getByText('X2')).toBeVisible())

    fireEvent.click(screen.getByRole('button', { name: /switch to sfmta/i }))

    await waitFor(() => {
      expect(screen.queryByText('X2')).not.toBeInTheDocument()
      expect(screen.queryByText('D72')).not.toBeInTheDocument()
      expect(screen.queryByText('H8')).not.toBeInTheDocument()
      expect(screen.getByRole('alert')).toHaveTextContent(/unable to load system data/i)
    })
  })
})

// Stand-in for WindowPicker (rendered in App.jsx's header, outside
// Overview) so this test can flip `?days=` without remounting Overview.
function DaysHarness() {
  const [, setDays] = useWindowDays()
  return (
    <>
      <button onClick={() => setDays(7)}>switch to 7 days</button>
      <Overview />
    </>
  )
}

describe('Overview window-picker stale-while-revalidate (unaffected by the agency guard)', () => {
  test('switching days keeps the previous window\'s routes on screen while the new fetch is cold (PR #239, round-2 review finding 5)', async () => {
    let resolveNewWindowRoutes
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.includes('days=7') && u.startsWith('/api/routes') && !u.includes('contributors')) {
          return new Promise((resolve) => {
            resolveNewWindowRoutes = resolve
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

    render(
      <MemoryRouter>
        <DaysHarness />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText('X2')).toBeVisible())

    fireEvent.click(screen.getByRole('button', { name: /switch to 7 days/i }))

    // The new (days=7) fetch is cold and pending, but the previously
    // loaded (days=30) wmata routes must stay on screen — a `days` switch
    // is not an agency switch and must not trip the staleness guard.
    expect(screen.getByText('X2')).toBeVisible()

    resolveNewWindowRoutes(jsonResponse({ routes: wmataRoutes }))
    await waitFor(() => expect(screen.getByText('X2')).toBeVisible())
  })
})
