/**
 * RouteList URL-state round trip (NOTES-140).
 *
 * RouteList's search/sort/view/metric filters moved from component-local
 * `useState` to `useUrlState`, and its `/api/routes` fetch now carries the
 * time-window picker's `?days=`. This pins the two directions of the round
 * trip — UI change → URL update, and URL → initial render state — plus the
 * `days` fetch wiring, since a regression in either would silently make
 * "linkable filters" false without breaking any existing assertion (the
 * other RouteList.test.jsx file covers unrelated server-delta rendering).
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import { vi } from 'vitest'
import RouteList from '../../src/components/RouteList'

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

const routes = [
  { route_id: 'D72', route_name: 'D72', route_long_name: 'Friendly Shores - Wheaton' },
  { route_id: 'C51', route_name: 'C51', route_long_name: 'Braddock Road' },
]

function mockFetch() {
  const calls = []
  vi.stubGlobal(
    'fetch',
    vi.fn((url) => {
      const u = String(url)
      calls.push(u)
      if (u.startsWith('/api/routes/contributors')) {
        return jsonResponse({ contributors: [], baseline_value: null })
      }
      if (u.startsWith('/api/routes')) {
        return jsonResponse({ window: null, routes })
      }
      return jsonResponse({})
    }),
  )
  return calls
}

function LocationSpy({ locationRef }) {
  locationRef.current = useLocation()
  return null
}

function renderRouteList(initialEntries = ['/routes']) {
  const locationRef = { current: null }
  const utils = render(
    <MemoryRouter initialEntries={initialEntries}>
      <LocationSpy locationRef={locationRef} />
      <RouteList />
    </MemoryRouter>,
  )
  return { ...utils, locationRef }
}

describe('RouteList URL-state round trip', () => {
  test('typing in the search box writes ?q= to the URL', async () => {
    mockFetch()
    const user = userEvent.setup()
    const { locationRef } = renderRouteList()
    await waitFor(() => screen.getByPlaceholderText('Search routes...'))

    await user.type(screen.getByPlaceholderText('Search routes...'), 'D72')

    await waitFor(() => expect(locationRef.current.search).toContain('q=D72'))
  })

  test('clearing the search box removes ?q= from the URL', async () => {
    mockFetch()
    const user = userEvent.setup()
    const { locationRef } = renderRouteList(['/routes?q=D72'])
    await waitFor(() => screen.getByDisplayValue('D72'))

    await user.clear(screen.getByDisplayValue('D72'))

    await waitFor(() => expect(locationRef.current.search).not.toContain('q='))
  })

  test('a URL with ?q= renders the search box pre-filled (URL -> state)', async () => {
    mockFetch()
    renderRouteList(['/routes?q=D72'])
    await waitFor(() => expect(screen.getByDisplayValue('D72')).toBeInTheDocument())
  })

  test('?days= on the URL is forwarded to the /api/routes fetch', async () => {
    const calls = mockFetch()
    renderRouteList(['/routes?days=7'])
    await waitFor(() =>
      expect(calls.some((u) => u.startsWith('/api/routes?days=7'))).toBe(true),
    )
  })

  test('omitting ?days= defaults the /api/routes fetch to days=30', async () => {
    const calls = mockFetch()
    renderRouteList(['/routes'])
    await waitFor(() =>
      expect(calls.some((u) => u.startsWith('/api/routes?days=30'))).toBe(true),
    )
  })
})
