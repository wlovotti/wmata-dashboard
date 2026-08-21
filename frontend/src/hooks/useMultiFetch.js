import { useEffect, useRef, useState } from 'react'
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
 * @param {string[]} urls - Array of URLs to fetch in parallel. Re-fetches
 *   whenever the *set* of URLs changes (compared by value, not reference —
 *   see "Memoization" below), so callers should still memoize or compute it
 *   outside the component render (e.g. useMemo / top-level const) per the
 *   hook's documented contract, even though a fresh array literal each
 *   render no longer forces a re-fetch. An empty (or null/undefined) array
 *   resolves immediately, on the very first render, with `data: []` — no
 *   effect flush is required to observe that value (PR #TBD finding 5).
 * @param {function} [transform] - Optional transform applied to the resolved
 *   array of JSON responses before storing in state. Receives the array in the
 *   same order as `urls` and must return the value to store in `data`. When
 *   omitted the raw array is stored.
 * @returns {{ data: *, loading: boolean, error: string|null, revalidateError: string|null }}
 *   - `data`    – the resolved (and optionally transformed) fetch results, or
 *                 null until the first successful resolution (or the cached
 *                 value, instantly, on a cache hit).
 *   - `loading` – true while a fetch is in flight AND no cached value is
 *                 available to show meanwhile. A cache hit never sets this —
 *                 the stale value is served immediately and the background
 *                 revalidate is invisible to the caller.
 *   - `error`   – stringified error on a failed cold fetch (no cache entry
 *                 to fall back on). A background revalidate failure with a
 *                 cache hit does NOT set this — the stale data stays on
 *                 screen rather than flashing an error over good content.
 *   - `revalidateError` – stringified error when a background revalidate
 *                 (cache-hit path) fails, or null otherwise. This is the
 *                 staleness signal a cache-hit render is missing without
 *                 it: `error` stays null and `loading` stays false, so
 *                 without this field a downed API (backend restart, a
 *                 dropped tunnel) would leave stale data on screen
 *                 indefinitely with nothing indicating it stopped
 *                 refreshing. Cleared on the next revalidate that
 *                 succeeds. Never set on the cold-load path (a fetch
 *                 failure there sets `error` instead).
 *
 * Caching: every URL is looked up in the shared, module-level `fetchCache`
 * (keyed by URL, no TTL, LRU-capped — see fetchCache.js). If every URL in
 * `urls` has a cache entry, that value is served synchronously (no spinner)
 * on mount/re-render, and a background fetch still runs to revalidate — on
 * success the cache and `data` are updated and `revalidateError` is
 * cleared; on failure the stale `data` is left alone, no `error` is
 * surfaced, but `revalidateError` is set so callers can show a
 * non-blocking staleness note. If any URL is a cache miss, behavior is the
 * original cold-load path: `loading` starts true, `data` starts null, and
 * a fetch failure sets `error`. The app's header Refresh button is the
 * manual invalidation path — it calls `fetchCache`'s `clearFetchCache()`
 * before remounting, so every URL is a cold miss on the next mount instead
 * of an instant replay of stale data.
 *
 * Sibling caching (PR #TBD finding 3): the group fetch uses
 * `Promise.allSettled`, not `Promise.all`. Every URL that resolves
 * successfully is cached individually, even when a sibling in the same
 * `urls` array fails — a single flaky metric (e.g. one `/api/system/trend`
 * call 500ing) no longer poisons caching for the rest of a multi-URL
 * fan-out like Overview's 4-URL trend group. The *group's* `data`/`error`/
 * `revalidateError` state still reflects an all-or-nothing outcome exactly
 * as before: any rejection in the group sets `error` (cold path) or
 * `revalidateError` (cache-hit path) and `data` is only updated when every
 * URL in the group succeeds.
 *
 * Cancellation: an AbortController is created per effect run and its signal is
 * passed to every fetch call. When the component unmounts or `urls` changes the
 * cleanup function calls `controller.abort()`, which causes in-flight fetches to
 * reject with an AbortError. The hook silently swallows AbortErrors so stale
 * responses never update state.
 *
 * Limitation: the hook does not support per-URL transforms mid-fetch.
 * Components that need to derive different state from different URLs (e.g.
 * RouteDetail's separate loading spinners per fetch) should keep their own
 * effects.
 */
function useMultiFetch(urls, transform) {
  const hasUrls = !!urls && urls.length > 0

  // Lazy useState initializers run exactly once, at mount — unlike a plain
  // `readCache(urls)` call inline in the render body (the pre-fix version),
  // which re-ran on every re-render just to feed these once-only values
  // (PR #TBD finding 4). An empty/null `urls` resolves immediately to
  // `data: []` here, matching the docstring's promise without waiting for
  // the effect below to run (PR #TBD finding 5).
  const [data, setData] = useState(() => {
    if (!hasUrls) return transform ? transform([]) : []
    const { cached, hit } = readCache(urls)
    return hit ? (transform ? transform(cached) : cached) : null
  })
  const [loading, setLoading] = useState(() => hasUrls && !readCache(urls).hit)
  const [error, setError] = useState(null)
  const [revalidateError, setRevalidateError] = useState(null)

  // Serialize urls to a stable key so the effect only re-runs when the
  // URL set actually changes, not merely because a caller passed a fresh
  // array literal by reference. JSON.stringify is safe here because the
  // values are plain strings. Callers should still memoize `urls` per the
  // documented contract above — this key comparison is a safety net
  // against the effect re-fetching, not a substitute for memoizing.
  const urlKey = JSON.stringify(urls)

  // True only while processing the very first effect run of this hook
  // instance. The lazy useState initializers above already computed the
  // correct cache-hit state for the first render, so without this guard
  // the effect's `if (hit)` branch below would call setData again with an
  // equivalent-but-new value (transform() runs again, producing a fresh
  // reference) on that same mount — costing an extra render on every
  // cache-hit mount (PR #TBD finding 4).
  const isFirstRun = useRef(true)

  useEffect(() => {
    const firstRun = isFirstRun.current
    isFirstRun.current = false

    if (!urls || urls.length === 0) {
      if (!firstRun) {
        setData(transform ? transform([]) : [])
        setLoading(false)
        setError(null)
      }
      return
    }

    const { cached, hit } = readCache(urls)
    const controller = new AbortController()
    const { signal } = controller

    if (hit) {
      if (!firstRun) {
        // Serve the cached value immediately; the fetch below still runs
        // to revalidate it in the background. On the first run for this
        // hook instance, the lazy useState initializers above already did
        // this synchronously for the first render.
        setData(transform ? transform(cached) : cached)
        setLoading(false)
        setError(null)
      }
    } else {
      setLoading(true)
      setError(null)
    }

    Promise.allSettled(
      urls.map((url) =>
        fetch(url, { signal }).then((res) =>
          res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`)),
        ),
      ),
    ).then((settled) => {
      // Cache every URL that resolved, even if a sibling in this group
      // failed (PR #TBD finding 3) — the next mount of a URL cached here
      // is a cache hit regardless of how the rest of this group fared.
      settled.forEach((outcome, i) => {
        if (outcome.status === 'fulfilled') setCacheEntry(urls[i], outcome.value)
      })

      const firstRejected = settled.find((outcome) => outcome.status === 'rejected')
      if (firstRejected) {
        const err = firstRejected.reason
        if (err && err.name === 'AbortError') return
        if (hit) {
          // Background revalidate failed but we already have stale data on
          // screen — keep showing it rather than replacing good content
          // with an error banner. Still surface it via `revalidateError` so
          // an outage isn't invisible: without this, a downed API leaves
          // stale data on screen forever with no staleness signal anywhere.
          setRevalidateError((err && err.message) || String(err))
          return
        }
        setError((err && err.message) || String(err))
        setLoading(false)
        return
      }

      const results = settled.map((outcome) => outcome.value)
      setData(transform ? transform(results) : results)
      setLoading(false)
      setError(null)
      setRevalidateError(null)
    })

    return () => {
      controller.abort()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlKey])

  return { data, loading, error, revalidateError }
}

export default useMultiFetch
