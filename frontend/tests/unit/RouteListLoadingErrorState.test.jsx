/**
 * RouteList loading/error state regressions (PR #239 delta review).
 *
 * Two bugs introduced by the second commit's out-of-order-response guard:
 *
 *   1. `showLoadingState` included `_cachedDays !== days` unconditionally.
 *      `_cachedDays` is only written on a *successful* fetch, so once a
 *      fetch fails it can never catch up to `days` again — permanently
 *      latching the full-page spinner and hiding the error banner + "Try
 *      Again" button behind it.
 *   2. The `days` effect called `setLoading(true)` unconditionally, which
 *      defeated the stale-while-revalidate lazy `useState` init: remounting
 *      at a window already present in the module-level cache (e.g.
 *      navigating back to /routes) spinner-blocked instead of rendering
 *      the cached rows immediately while a background fetch revalidates.
 *
 * Each test isolates the module-level `_cachedRoutes`/`_cachedDays` cache
 * to its own scenario (a fetch that always fails for #1; a fetch that
 * resolves once then hangs for #2) rather than relying on ordering between
 * tests in this file.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import RouteList from '../../src/components/RouteList'

function jsonResponse(body) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) })
}

function deferred() {
  let resolve
  const promise = new Promise((res) => {
    resolve = res
  })
  return { promise, resolve }
}

function renderRouteList(initialEntries = ['/routes']) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <RouteList />
    </MemoryRouter>,
  )
}

describe('RouteList loading/error state (PR #239 delta review)', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  test('finding 1: a rejected /api/routes fetch shows the error banner and Try Again, not an infinite spinner', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        return Promise.resolve({ ok: false, status: 500 })
      }),
    )

    renderRouteList()

    await waitFor(() => expect(screen.getByText(/Error loading data/)).toBeInTheDocument())
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
    expect(screen.queryByText('Loading routes...')).not.toBeInTheDocument()
  })

  test('finding 2: remounting at an already-cached window renders rows immediately, without waiting for the revalidation fetch', async () => {
    const routes = [
      { route_id: 'D72', route_name: 'D72', route_long_name: 'Friendly Shores - Wheaton' },
    ]

    // First mount: a normal resolving fetch populates the module cache for
    // this window (days defaults to 30 with no `?days=` in the URL).
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        return jsonResponse({ window: null, routes })
      }),
    )
    const first = renderRouteList()
    await waitFor(() => expect(screen.getByText('D72')).toBeInTheDocument())
    first.unmount()

    // Second mount, same window: the revalidation fetch is deliberately
    // left hanging so the only way "D72" can appear before this test ends
    // is via the cache-hit render path, not a resolved fetch.
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        return deferred().promise
      }),
    )
    renderRouteList()

    expect(screen.getByText('D72')).toBeInTheDocument()
    expect(screen.queryByText('Loading routes...')).not.toBeInTheDocument()
  })
})
