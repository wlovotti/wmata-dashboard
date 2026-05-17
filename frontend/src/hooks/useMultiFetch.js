import { useEffect, useState } from 'react'

/**
 * Fetch multiple URLs in parallel and return a unified loading/error/data state.
 *
 * @param {string[]} urls - Array of URLs to fetch in parallel via Promise.all.
 *   Re-fetches whenever the array reference changes, so callers should memoize
 *   or compute it outside the component render (e.g. useMemo / top-level const).
 *   An empty array resolves immediately with `data: []`.
 * @param {function} [transform] - Optional transform applied to the resolved
 *   array of JSON responses before storing in state. Receives the array in the
 *   same order as `urls` and must return the value to store in `data`. When
 *   omitted the raw array is stored.
 * @returns {{ data: *, loading: boolean, error: string|null }}
 *   - `data`    – the resolved (and optionally transformed) fetch results, or
 *                 null until the first successful resolution.
 *   - `loading` – true while any fetch is in flight.
 *   - `error`   – stringified error on any failure, null otherwise.
 *
 * Cancellation: an AbortController is created per effect run and its signal is
 * passed to every fetch call. When the component unmounts or `urls` changes the
 * cleanup function calls `controller.abort()`, which causes in-flight fetches to
 * reject with an AbortError. The hook silently swallows AbortErrors so stale
 * responses never update state.
 *
 * Limitation: the hook does not support per-URL transforms mid-Promise.all.
 * Components that need to derive different state from different URLs (e.g.
 * RouteDetail's separate loading spinners per fetch) should keep their own
 * effects.
 */
function useMultiFetch(urls, transform) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Serialize urls to a stable key so the effect only re-runs when the
  // URL set actually changes. JSON.stringify is safe here because the
  // values are plain strings.
  const urlKey = JSON.stringify(urls)

  useEffect(() => {
    if (!urls || urls.length === 0) {
      setData(transform ? transform([]) : [])
      setLoading(false)
      setError(null)
      return
    }

    const controller = new AbortController()
    const { signal } = controller

    setLoading(true)
    setError(null)

    Promise.all(
      urls.map((url) =>
        fetch(url, { signal }).then((res) =>
          res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`)),
        ),
      ),
    )
      .then((results) => {
        setData(transform ? transform(results) : results)
        setLoading(false)
      })
      .catch((err) => {
        if (err.name === 'AbortError') return
        setError(err.message || String(err))
        setLoading(false)
      })

    return () => {
      controller.abort()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlKey])

  return { data, loading, error }
}

export default useMultiFetch
