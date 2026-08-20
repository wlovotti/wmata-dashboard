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
 */

const cache = new Map()

/**
 * Look up the cached JSON value for `url`.
 *
 * `undefined` is the miss sentinel (never `null`), so a legitimately
 * cached `null` JSON response is distinguishable from "never fetched".
 *
 * @param {string} url
 * @returns {*} the cached value, or `undefined` on a cache miss.
 */
export function getCacheEntry(url) {
  return cache.has(url) ? cache.get(url) : undefined
}

/**
 * Store `data` in the cache under `url`, overwriting any prior entry.
 *
 * @param {string} url
 * @param {*} data - parsed JSON response body to cache.
 */
export function setCacheEntry(url, data) {
  cache.set(url, data)
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
