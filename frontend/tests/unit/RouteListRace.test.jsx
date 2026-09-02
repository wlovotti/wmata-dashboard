/**
 * RouteList out-of-order window-switch race (PR #239 review finding E).
 *
 * `RouteList`'s `/api/routes` effect refetches whenever the time-window
 * picker's `days` changes. Before this fix, a slower earlier-window
 * response landing after a faster later-window response had already
 * committed could silently overwrite `routes` (and the module-level
 * `_cachedRoutes`/`_cachedDays` cache) with stale, mismatched-window rows.
 * This pins the fix: switching from 30d to 7d before the 30d request
 * resolves, then resolving the 30d request LAST, must not clobber the 7d
 * result that's already on screen.
 *
 * `Harness` exposes a same-page way to change `?days=` (RouteList itself
 * has no window control — that's `WindowPicker`, rendered in the app
 * shell) without needing a full `App` render.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, useSearchParams } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
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

function Harness() {
  const [, setSearchParams] = useSearchParams()
  return (
    <>
      <button onClick={() => setSearchParams({ days: '7' })}>go-7</button>
      <RouteList />
    </>
  )
}

describe('RouteList out-of-order window-switch race', () => {
  test('a slower stale-window response does not overwrite a faster newer-window response', async () => {
    const deferredByUrl = {}
    vi.stubGlobal(
      'fetch',
      vi.fn((url) => {
        const u = String(url)
        if (u.startsWith('/api/routes/contributors')) {
          return jsonResponse({ contributors: [], baseline_value: null })
        }
        const d = deferred()
        deferredByUrl[u] = d
        return d.promise
      }),
    )

    render(
      <MemoryRouter initialEntries={['/routes?days=30']}>
        <Harness />
      </MemoryRouter>,
    )

    // The initial 30-day request is in flight (deliberately unresolved).
    await waitFor(() => expect(deferredByUrl['/api/routes?days=30']).toBeDefined())

    // Switch to 7d before the 30-day request has settled.
    const user = userEvent.setup()
    await user.click(screen.getByText('go-7'))
    await waitFor(() => expect(deferredByUrl['/api/routes?days=7']).toBeDefined())

    // Resolve the NEWER (7d) request first...
    deferredByUrl['/api/routes?days=7'].resolve({
      ok: true,
      json: () =>
        Promise.resolve({
          window: null,
          routes: [{ route_id: 'C51', route_name: 'C51', route_long_name: 'Braddock Road' }],
        }),
    })
    await waitFor(() => expect(screen.getByText('C51')).toBeInTheDocument())

    // ...then the slower, now-superseded 30-day request finally resolves.
    deferredByUrl['/api/routes?days=30'].resolve({
      ok: true,
      json: () =>
        Promise.resolve({
          window: null,
          routes: [
            { route_id: 'D72', route_name: 'D72', route_long_name: 'Friendly Shores - Wheaton' },
          ],
        }),
    })
    // Let the now-ignored promise's .then/.catch/.finally chain flush.
    await new Promise((resolve) => setTimeout(resolve, 0))

    // The stale 30-day response must not have clobbered the 7-day result.
    expect(screen.getByText('C51')).toBeInTheDocument()
    expect(screen.queryByText('D72')).not.toBeInTheDocument()
  })
})
