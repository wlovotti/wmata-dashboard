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
 */
import { renderHook, act, waitFor } from '@testing-library/react'
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
