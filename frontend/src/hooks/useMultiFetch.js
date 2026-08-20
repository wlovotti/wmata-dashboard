import { useEffect, useState } from 'react'
import { getCacheEntry, setCacheEntry } from './fetchCache'

// Look up every URL in the shared fetchCache and report whether all of
// them currently have an entry. `undefined` is the module's cache-miss
// sentinel (see fetchCache.js), so `.every` correctly treats a
// legitimately cached `null` JSON response as a hit.
function readCache(urls) {
  const cached = urls.map((url) => getCacheEntry(url))
  return { cached, hit: cached.every((v) => v !== undefined) }
}

/**
 * Fetch multiple URLs in parallel and return a unified loading/error/data
 * state, with stale-while-revalidate caching (NOTES-122) via the shared
 * `fetchCache` module.
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
 *                 null until the first successful resolution (or the cached
 *                 value, instantly, on a cache hit).
 *   - `loading` – true while a fetch is in flight AND no cached value is
 *                 available to show meanwhile. A cache hit never sets this —
 *                 the stale value is served immediately and the background
 *                 revalidate is invisible to the caller.
 *   - `error`   – stringified error on a failed cold fetch (no cache entry
 *                 to fall back on). A background revalidate failure with a
 *                 cache hit is swallowed instead — the stale data stays on
 *                 screen rather than flashing an error over good content.
 *
 * Caching: every URL is looked up in the shared, module-level `fetchCache`
 * (keyed by URL, no TTL). If every URL in `urls` has a cache entry, that
 * value is served synchronously (no spinner) on mount/re-render, and a
 * background fetch still runs to revalidate — on success the cache and
 * `data` are updated; on failure the stale `data` is left alone and no
 * `error` is surfaced. If any URL is a cache miss, behavior is the
 * original cold-load path: `loading` starts true, `data` starts null,
 * and a fetch failure sets `error`. The app's header Refresh button is
 * the manual invalidation path — it calls `fetchCache`'s
 * `clearFetchCache()` before remounting, so every URL is a cold miss on
 * the next mount instead of an instant replay of stale data.
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
  const hasUrls = !!urls && urls.length > 0
  // Compute the very first render's state from the cache synchronously so a
  // cache-hit mount never paints a spinner frame before the effect runs.
  const initialHit = hasUrls ? readCache(urls) : null

  const [data, setData] = useState(() => {
    if (!hasUrls) return null
    if (!initialHit.hit) return null
    return transform ? transform(initialHit.cached) : initialHit.cached
  })
  const [loading, setLoading] = useState(() => hasUrls && !initialHit.hit)
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

    const { cached, hit } = readCache(urls)
    const controller = new AbortController()
    const { signal } = controller

    if (hit) {
      // Serve the cached value immediately; the fetch below still runs to
      // revalidate it in the background.
      setData(transform ? transform(cached) : cached)
      setLoading(false)
      setError(null)
    } else {
      setLoading(true)
      setError(null)
    }

    Promise.all(
      urls.map((url) =>
        fetch(url, { signal }).then((res) =>
          res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`)),
        ),
      ),
    )
      .then((results) => {
        results.forEach((result, i) => setCacheEntry(urls[i], result))
        setData(transform ? transform(results) : results)
        setLoading(false)
        setError(null)
      })
      .catch((err) => {
        if (err.name === 'AbortError') return
        if (hit) {
          // Background revalidate failed but we already have stale data on
          // screen — keep showing it rather than replacing good content
          // with an error banner.
          return
        }
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
