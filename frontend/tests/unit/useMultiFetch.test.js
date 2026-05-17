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
