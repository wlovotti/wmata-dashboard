/**
 * Module-level stale-while-revalidate cache for GET JSON fetches, keyed by
 * URL (NOTES-122). Generalizes the ad-hoc single-endpoint pattern
 * `useGtfsFreshness.js` already used (module-level value, always
 * background-revalidate) so any number of URLs can share it — primarily
 * `useMultiFetch`, so every component that fans its fetches through that
 * hook gets SWR for free without touching its own fetch logic.
 *
 * DESIGN DECISION (NOTES-122): hand-rolled, no TanStack Query / SWR
 * dependency. A cache hit serves the last-known JSON synchronously (no
 * spinner); the caller still fires a normal fetch in the background to
 * revalidate, and updates the cache (and its own state) when the fresh
 * response lands. `clearFetchCache()` is the sole invalidation path,
 * wired to the app's existing header Refresh button so a user-initiated
 * refresh always goes to the network instead of replaying a stale entry.
 *
 * This module intentionally has no TTL/expiry and no request dedup — it
 * is a plain `Map`. Concurrent-mount de-duping (like `useGtfsFreshness`'s
 * `_fetchPromise`) is out of scope here: `useMultiFetch` already dedupes
 * within a single hook instance via `Promise.all`, and cross-instance
 * request coalescing is not something the surfaced issue asked for.
 *
 * SIZE CAP (PR #TBD finding 1): the only removal path used to be
 * `clearFetchCache()`, so a tab left open for days grows the cache
 * monotonically — RouteDetail alone keys three trend URLs per
 * (routeId, dayType, period) combination, alongside the ~200 KB
 * `/api/shapes` entry. `MAX_ENTRIES` bounds that with a simple
 * least-recently-used eviction: both `getCacheEntry` (on a hit) and
 * `setCacheEntry` move the touched key to the "most recently used" end
 * of the `Map`'s iteration order (delete + re-insert), and `setCacheEntry`
 * evicts the oldest entry once the cache exceeds the cap. This changes
 * nothing observable for a session under the cap — semantics only differ
 * once more than `MAX_ENTRIES` distinct URLs have been cached.
 */

const MAX_ENTRIES = 50

const cache = new Map()

/**
 * Look up the cached JSON value for `url`.
 *
 * `undefined` is the miss sentinel (never `null`), so a legitimately
 * cached `null` JSON response is distinguishable from "never fetched".
 * A hit also refreshes `url`'s recency for the LRU eviction in
 * `setCacheEntry`.
 *
 * @param {string} url
 * @returns {*} the cached value, or `undefined` on a cache miss.
 */
export function getCacheEntry(url) {
  if (!cache.has(url)) return undefined
  const value = cache.get(url)
  // Move to the most-recently-used end of the Map's iteration order.
  cache.delete(url)
  cache.set(url, value)
  return value
}

/**
 * Store `data` in the cache under `url`, overwriting any prior entry and
 * marking it most-recently-used. If the cache now holds more than
 * `MAX_ENTRIES` distinct URLs, evicts the single least-recently-used
 * entry (the first key in the `Map`'s iteration order).
 *
 * @param {string} url
 * @param {*} data - parsed JSON response body to cache.
 */
export function setCacheEntry(url, data) {
  cache.delete(url)
  cache.set(url, data)
  if (cache.size > MAX_ENTRIES) {
    const oldestKey = cache.keys().next().value
    cache.delete(oldestKey)
  }
}

/**
 * Clear every cached entry. This is the manual invalidation path: the
 * header Refresh button calls this before remounting the routed subtree
 * so every fetch on the remount is a cold miss instead of an instant
 * replay of stale data.
 */
export function clearFetchCache() {
  cache.clear()
}
