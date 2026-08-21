/**
 * Characterization tests for hooks/useMultiFetch.js.
 *
 * Tests:
 *   - empty URL array → immediate resolved state with []
 *   - loading → success transition
 *   - loading → error transition
 *   - transform function is applied
 *   - cleanup/abort on unmount (AbortError is swallowed)
 *   - HTTP error (non-ok status) surfaces in error state
 *   - PR #TBD finding 3: a sibling URL that resolves is still cached
 *     even when another URL in the same group fails (Promise.allSettled)
 *   - PR #TBD finding 4: a cache-hit mount with a transform does not
 *     force an extra render before the background revalidate resolves
 *   - PR #TBD finding 5: an empty `urls` array resolves to `data: []`
 *     synchronously on the very first render, not after an effect flush
 */
import React from 'react'
import { renderHook, act, waitFor, render } from '@testing-library/react'
import useMultiFetch from '../../src/hooks/useMultiFetch'
import { setCacheEntry, clearFetchCache } from '../../src/hooks/fetchCache'

// Helper: build a fetch mock that resolves with `data` after an optional delay.
function makeFetchMock(responses) {
  return vi.fn((url) => {
    const resp = responses[url] ?? responses['*'] ?? { status: 200, data: {} }
    return Promise.resolve({
      ok: resp.status >= 200 && resp.status < 300,
      status: resp.status,
      json: () => Promise.resolve(resp.data),
    })
  })
}

afterEach(() => {
  vi.restoreAllMocks()
  clearFetchCache()
})

describe('useMultiFetch', () => {
  test('empty URL array → loading=false, data=[], error=null (no fetch called)', async () => {
    const mockFetch = vi.fn()
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch([]))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).toEqual([])
    expect(result.current.error).toBeNull()
    expect(mockFetch).not.toHaveBeenCalled()
  })

  test('null URL array → loading=false, data=[], error=null', async () => {
    const mockFetch = vi.fn()
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(null))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).toEqual([])
    expect(result.current.error).toBeNull()
  })

  test('loading → success: data is set, loading=false, error=null', async () => {
    const mockFetch = makeFetchMock({
      '/api/foo': { status: 200, data: { foo: 1 } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(['/api/foo']))

    // Initially loading.
    expect(result.current.loading).toBe(true)
    expect(result.current.data).toBeNull()

    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).toEqual([{ foo: 1 }])
    expect(result.current.error).toBeNull()
  })

  test('multiple URLs: data array preserves URL order', async () => {
    const mockFetch = makeFetchMock({
      '/api/a': { status: 200, data: { id: 'a' } },
      '/api/b': { status: 200, data: { id: 'b' } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(['/api/a', '/api/b']))

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data).toEqual([{ id: 'a' }, { id: 'b' }])
  })

  test('transform function is applied to the resolved array', async () => {
    const mockFetch = makeFetchMock({
      '/api/x': { status: 200, data: 42 },
    })
    vi.stubGlobal('fetch', mockFetch)

    const transform = (results) => results[0] * 2
    const { result } = renderHook(() => useMultiFetch(['/api/x'], transform))

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data).toBe(84)
  })

  test('transform applied to empty array when URLs is empty', async () => {
    vi.stubGlobal('fetch', vi.fn())
    const transform = (results) => results.length
    const { result } = renderHook(() => useMultiFetch([], transform))

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data).toBe(0)
  })

  test('HTTP error → error state set, loading=false', async () => {
    const mockFetch = makeFetchMock({
      '/api/bad': { status: 404, data: null },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(['/api/bad']))

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.error).toMatch(/HTTP 404/)
    expect(result.current.data).toBeNull()
  })

  test('network error → error state set, loading=false', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('Network failure'))))

    const { result } = renderHook(() => useMultiFetch(['/api/network']))

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.error).toMatch(/Network failure/)
    expect(result.current.data).toBeNull()
  })

  test('unmount during fetch: AbortError is swallowed (no state update after unmount)', async () => {
    // Simulate a fetch that hangs until aborted.
    vi.stubGlobal(
      'fetch',
      vi.fn((_, { signal }) => {
        return new Promise((_, reject) => {
          signal.addEventListener('abort', () => {
            reject(Object.assign(new Error('AbortError'), { name: 'AbortError' }))
          })
        })
      }),
    )

    const { result, unmount } = renderHook(() => useMultiFetch(['/api/slow']))

    // Still loading before unmount.
    expect(result.current.loading).toBe(true)

    // Unmount fires cleanup → AbortController.abort() → fetch rejects with AbortError.
    unmount()

    // Give a tick for the abort rejection to propagate.
    await act(async () => {
      await Promise.resolve()
    })

    // The hook should have swallowed the AbortError without updating state.
    // loading remains true (the component is unmounted, so it doesn't matter,
    // but the important thing is no error was set).
    expect(result.current.error).toBeNull()
  })
})

describe('useMultiFetch stale-while-revalidate caching (NOTES-122)', () => {
  test('cache miss: no cache entry behaves exactly like the uncached path (loading starts true)', async () => {
    const mockFetch = makeFetchMock({
      '/api/miss': { status: 200, data: { v: 1 } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(['/api/miss']))

    expect(result.current.loading).toBe(true)
    expect(result.current.data).toBeNull()

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data).toEqual([{ v: 1 }])
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  test('cache hit: serves the cached value synchronously (loading=false from the first render), then revalidates in the background', async () => {
    setCacheEntry('/api/hit', { v: 'stale' })

    const mockFetch = makeFetchMock({
      '/api/hit': { status: 200, data: { v: 'fresh' } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result } = renderHook(() => useMultiFetch(['/api/hit']))

    // Served instantly from cache — no spinner, no waiting for the network.
    expect(result.current.loading).toBe(false)
    expect(result.current.data).toEqual([{ v: 'stale' }])

    // A background revalidate request still fires...
    expect(mockFetch).toHaveBeenCalledWith('/api/hit', expect.objectContaining({ signal: expect.anything() }))

    // ...and updates state (and the cache) once it resolves.
    await waitFor(() => expect(result.current.data).toEqual([{ v: 'fresh' }]))
  })

  test('a successful fetch populates the cache for the next mount of the same URL', async () => {
    const mockFetch = makeFetchMock({
      '/api/populate': { status: 200, data: { v: 1 } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result, unmount } = renderHook(() => useMultiFetch(['/api/populate']))
    await waitFor(() => expect(result.current.loading).toBe(false))
    unmount()

    const { result: result2 } = renderHook(() => useMultiFetch(['/api/populate']))
    // Second mount is a cache hit: instant data, no spinner.
    expect(result2.current.loading).toBe(false)
    expect(result2.current.data).toEqual([{ v: 1 }])
  })

  test('background revalidate failure keeps showing the stale cached data instead of surfacing an error', async () => {
    setCacheEntry('/api/stale-on-error', { v: 'stale' })
    vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('Network failure'))))

    const { result } = renderHook(() => useMultiFetch(['/api/stale-on-error']))

    expect(result.current.loading).toBe(false)
    expect(result.current.data).toEqual([{ v: 'stale' }])

    // Give the rejected background revalidate a tick to settle.
    await act(async () => {
      await Promise.resolve()
      await Promise.resolve()
    })

    // Stale data stays on screen; no error banner for a background failure.
    expect(result.current.data).toEqual([{ v: 'stale' }])
    expect(result.current.error).toBeNull()
  })

  test('background revalidate failure sets revalidateError while data stays visible and error stays null (PR #217 review finding 1)', async () => {
    setCacheEntry('/api/revalidate-flag', { v: 'stale' })
    vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('Network failure'))))

    const { result } = renderHook(() => useMultiFetch(['/api/revalidate-flag']))

    expect(result.current.loading).toBe(false)
    expect(result.current.data).toEqual([{ v: 'stale' }])
    expect(result.current.revalidateError).toBeNull()

    await waitFor(() => expect(result.current.revalidateError).not.toBeNull())

    // Stale data is still on screen and `error` (the cold-load field) is
    // still null — the flag is a distinct signal, not a repurposing of
    // the existing error state.
    expect(result.current.data).toEqual([{ v: 'stale' }])
    expect(result.current.error).toBeNull()
  })

  test('a later successful revalidate clears revalidateError', async () => {
    setCacheEntry('/api/revalidate-recover', { v: 'stale' })
    let shouldFail = true
    vi.stubGlobal(
      'fetch',
      vi.fn(() => {
        if (shouldFail) return Promise.reject(new Error('Network failure'))
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ v: 'fresh' }),
        })
      }),
    )

    const { result, rerender } = renderHook(
      ({ urls }) => useMultiFetch(urls),
      { initialProps: { urls: ['/api/revalidate-recover'] } },
    )

    await waitFor(() => expect(result.current.revalidateError).not.toBeNull())

    // A second revalidate succeeds this time. `urlKey` (JSON.stringify(urls))
    // only changes — and thus only re-runs the effect — when the URL set
    // itself changes, so add a second (also-cached) URL to trigger it
    // rather than passing a same-content array by a new reference.
    shouldFail = false
    setCacheEntry('/api/revalidate-recover-2', { v: 'stale-2' })
    rerender({ urls: ['/api/revalidate-recover', '/api/revalidate-recover-2'] })

    await waitFor(() => expect(result.current.revalidateError).toBeNull())
    expect(result.current.data).toEqual([{ v: 'fresh' }, { v: 'fresh' }])
  })

  test('Refresh invalidation: clearFetchCache() makes the next mount a cold miss again', async () => {
    const mockFetch = makeFetchMock({
      '/api/invalidate-me': { status: 200, data: { v: 1 } },
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result, unmount } = renderHook(() => useMultiFetch(['/api/invalidate-me']))
    await waitFor(() => expect(result.current.loading).toBe(false))
    unmount()

    // Simulate the header Refresh button's manual invalidation path.
    clearFetchCache()

    const { result: result2 } = renderHook(() => useMultiFetch(['/api/invalidate-me']))
    // No cache entry survives the clear — back to the cold-load spinner.
    expect(result2.current.loading).toBe(true)
    expect(result2.current.data).toBeNull()

    await waitFor(() => expect(result2.current.loading).toBe(false))
    expect(result2.current.data).toEqual([{ v: 1 }])
  })
})

describe('useMultiFetch sibling caching on partial group failure (PR #TBD finding 3)', () => {
  test('a URL that resolves is cached even when a sibling in the same group fails', async () => {
    const mockFetch = vi.fn((url) => {
      if (url === '/api/sibling-good') {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ v: 'good' }),
        })
      }
      return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve(null) })
    })
    vi.stubGlobal('fetch', mockFetch)

    const { result, unmount } = renderHook(() =>
      useMultiFetch(['/api/sibling-good', '/api/sibling-bad']),
    )

    // The group as a whole still surfaces the failure — partial success
    // does not populate `data`.
    await waitFor(() => expect(result.current.error).not.toBeNull())
    expect(result.current.data).toBeNull()
    unmount()

    // But the sibling that succeeded was cached individually: mounting the
    // hook for just that URL is an instant cache hit, not a cold fetch —
    // pre-fix (Promise.all), the failing sibling would have prevented this
    // URL from ever being cached.
    mockFetch.mockClear()
    const { result: result2 } = renderHook(() => useMultiFetch(['/api/sibling-good']))
    expect(result2.current.loading).toBe(false)
    expect(result2.current.data).toEqual([{ v: 'good' }])
  })
})

describe('useMultiFetch empty-urls contract (PR #TBD finding 5)', () => {
  test('data is [] synchronously on the very first render, not null before an effect flush', () => {
    let capturedOnFirstRender
    function Probe() {
      const { data } = useMultiFetch([])
      // Captured during the render phase itself (not an effect), so this
      // reflects exactly what the lazy useState initializer produced for
      // the very first paint — the docstring promises `data: []`
      // immediately, with no effect flush required to observe it.
      if (capturedOnFirstRender === undefined) capturedOnFirstRender = data
      return null
    }
    render(React.createElement(Probe))
    expect(capturedOnFirstRender).toEqual([])
  })
})

describe('useMultiFetch cache-hit mount avoids a redundant extra render (PR #TBD finding 4)', () => {
  test('a cache hit with a transform does not re-render before the background revalidate resolves', async () => {
    setCacheEntry('/api/render-count', { v: 1 })
    let resolveFetch
    vi.stubGlobal(
      'fetch',
      vi.fn(
        () =>
          new Promise((resolve) => {
            resolveFetch = resolve
          }),
      ),
    )

    let renderCount = 0
    function Probe() {
      renderCount++
      // A transform that returns a fresh object reference every call — the
      // exact shape (`([json]) => ({...})`) Overview's trend fan-out uses.
      // Pre-fix, the effect's `if (hit)` branch called setData again with
      // this transform's new reference on the SAME mount, forcing a second
      // render before the fetch below ever resolves.
      return useMultiFetch(['/api/render-count'], ([json]) => ({ ...json }))
    }

    const { result } = renderHook(() => Probe())

    // Only the initial mount render has happened — the revalidate fetch is
    // still pending, so nothing should have re-rendered the hook yet.
    expect(renderCount).toBe(1)
    expect(result.current.data).toEqual({ v: 1 })

    // Resolve the pending fetch so it doesn't leak into later tests as a
    // hanging promise / act() warning.
    await act(async () => {
      resolveFetch({ ok: true, json: () => Promise.resolve({ v: 1 }) })
      await Promise.resolve()
      await Promise.resolve()
    })
  })
})
