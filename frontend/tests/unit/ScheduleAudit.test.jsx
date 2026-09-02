/**
 * ScheduleAudit URL round-trip (NOTES-142). Filters (route, direction,
 * period, sign, limit) now live in the URL via `useSearchParams`, mirroring
 * `SegmentDiagnostic.jsx`: a non-default filter value lands in the query
 * string, and a key is omitted once its value returns to the default so the
 * URL stays clean. Also pins that the route picker is a `/api/routes`-backed
 * select rather than the old free-text `route_id` input.
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import { vi } from 'vitest'
import ScheduleAudit from '../../src/components/ScheduleAudit'

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

let capturedSearch = ''
function LocationSpy() {
  capturedSearch = useLocation().search
  return null
}

const ROUTES = [
  { route_id: 'D80', route_name: 'D80', route_long_name: 'Fort Totten - Anacostia' },
  { route_id: '70', route_name: '70', route_long_name: 'Georgia Ave-7th St' },
  // route_name differs from route_id (as on some SFMTA routes) — exercises
  // the "short name (id)" option label format.
  { route_id: 'RAPID14', route_name: '14R', route_long_name: 'Mission Rapid' },
]

function mockFetch(segments = []) {
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      const u = String(url)
      if (u.startsWith('/api/routes')) {
        return jsonResponse({ window: null, routes: ROUTES })
      }
      if (u.startsWith('/api/schedule-audit')) {
        return jsonResponse({
          period: 'all',
          sign: 'all',
          lookback_days: 30,
          n_rows: segments.length,
          segments,
        })
      }
      return jsonResponse({})
    }),
  )
}

function renderPage(initialPath = '/schedule-audit') {
  capturedSearch = ''
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <Routes>
        <Route path="/schedule-audit" element={<ScheduleAudit />} />
      </Routes>
      <LocationSpy />
    </MemoryRouter>,
  )
}

describe('ScheduleAudit filters <-> URL', () => {
  test('route picker is a select populated from /api/routes, not a text input', async () => {
    mockFetch()
    renderPage()
    const routeSelect = await screen.findByLabelText('Route filter')
    expect(routeSelect.tagName).toBe('SELECT')
    // route_name === route_id renders as the bare id...
    await waitFor(() =>
      expect(screen.getByRole('option', { name: 'D80' })).toBeInTheDocument(),
    )
    // ...while a differing short name renders as "name (id)".
    expect(screen.getByRole('option', { name: '14R (RAPID14)' })).toBeInTheDocument()
    expect(screen.getByText('All routes')).toBeInTheDocument()
  })

  test('a non-default filter change lands in the URL', async () => {
    mockFetch()
    renderPage()
    await screen.findByLabelText('Route filter')

    fireEvent.change(screen.getByLabelText('Route filter'), { target: { value: 'D80' } })
    await waitFor(() => expect(capturedSearch).toContain('route_id=D80'))

    fireEvent.change(screen.getByLabelText('Direction filter'), { target: { value: '0' } })
    await waitFor(() => expect(capturedSearch).toContain('direction_id=0'))

    fireEvent.change(screen.getByLabelText('Row limit'), { target: { value: '200' } })
    await waitFor(() => expect(capturedSearch).toContain('limit=200'))
  })

  test('returning a filter to its default value removes it from the URL', async () => {
    mockFetch()
    renderPage('/schedule-audit?direction_id=1&limit=200')
    await screen.findByLabelText('Route filter')
    expect(capturedSearch).toContain('direction_id=1')
    expect(capturedSearch).toContain('limit=200')

    fireEvent.change(screen.getByLabelText('Direction filter'), { target: { value: 'all' } })
    await waitFor(() => expect(capturedSearch).not.toContain('direction_id'))
    // Untouched non-default params are preserved.
    expect(capturedSearch).toContain('limit=200')
  })

  test('filters seeded from the URL populate the selects on load', async () => {
    mockFetch()
    renderPage('/schedule-audit?route_id=D80&direction_id=1&period=am_peak&sign=under&limit=50')
    await waitFor(() => expect(screen.getByLabelText('Route filter').value).toBe('D80'))
    expect(screen.getByLabelText('Direction filter').value).toBe('1')
    expect(screen.getByLabelText('Time-of-day period filter').value).toBe('am_peak')
    expect(screen.getByLabelText('Slip sign filter').value).toBe('under')
    expect(screen.getByLabelText('Row limit').value).toBe('50')
  })
})

/**
 * Route-picker honesty (review follow-up on PR #237). `/api/routes` is the
 * known-slow N+1 scorecard endpoint (NOTES-88) — it can be cold or fail
 * outright. When that happens, the picker must still reflect an active
 * `route_id` filter from the URL rather than silently falling back to
 * "All routes" while the table underneath is actually filtered.
 */
describe('ScheduleAudit route picker degraded states', () => {
  function mockRoutesRejecting(segments = []) {
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes')) {
          return Promise.reject(new Error('HTTP 504'))
        }
        if (u.startsWith('/api/schedule-audit')) {
          return jsonResponse({
            period: 'all',
            sign: 'all',
            lookback_days: 30,
            n_rows: segments.length,
            segments,
          })
        }
        return jsonResponse({})
      }),
    )
  }

  test('route select is disabled while /api/routes is still loading', () => {
    mockFetch()
    renderPage()
    // Checked synchronously, before the stubbed fetch promise has a chance
    // to resolve — the select must start disabled, not silently usable
    // against an empty route list.
    expect(screen.getByLabelText('Route filter')).toBeDisabled()
    expect(screen.getByText('Loading routes…')).toBeInTheDocument()
  })

  test('shows a synthetic option for the URL route_id when /api/routes fails', async () => {
    mockRoutesRejecting()
    renderPage('/schedule-audit?route_id=D80')

    await waitFor(() => expect(screen.getByLabelText('Route filter')).not.toBeDisabled())
    // The select still reports the active filter, not "All routes" ...
    expect(screen.getByLabelText('Route filter').value).toBe('D80')
    // ... via a synthetic option, since the real route list never loaded.
    expect(screen.getByRole('option', { name: 'D80' })).toBeInTheDocument()
    // The failure is surfaced rather than swallowed silently.
    expect(screen.getByText(/Route list unavailable/i)).toBeInTheDocument()
  })

  test('the select stays usable (not disabled) after a failed routes fetch', async () => {
    mockRoutesRejecting()
    renderPage('/schedule-audit?route_id=D80')
    await waitFor(() => expect(screen.getByText(/Route list unavailable/i)).toBeInTheDocument())
    expect(screen.getByLabelText('Route filter')).not.toBeDisabled()
  })

  test('no synthetic option is added when no route filter is active', async () => {
    mockRoutesRejecting()
    renderPage()
    await waitFor(() => expect(screen.getByText(/Route list unavailable/i)).toBeInTheDocument())
    // Only "All routes" — no phantom empty-string synthetic option.
    expect(screen.getByLabelText('Route filter').children).toHaveLength(1)
  })
})
