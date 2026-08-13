import { useEffect, useState } from 'react'

// Module-level cache so navigating between routes doesn't refetch/flash —
// mirrors the pattern RouteList used before this hook existed (NOTES-90).
let _cachedGtfsFreshness = null

// Module-level in-flight promise so concurrent mounts in the same tick
// (e.g. App.jsx's chrome banner and RouteList's footer both mounting when
// the app loads directly at /routes) share a single request instead of
// each firing its own — the cache alone can't dedupe that case because
// neither mount has committed a result yet when the other's effect runs.
// Always cleared once the fetch settles (success or failure) so the next
// mount/poll starts a fresh request instead of replaying a stale result.
let _fetchPromise = null

// How often a long-lived mount re-polls in the background. App.jsx's
// chrome banner mounts once per session and would otherwise never learn
// the feed expired days into a tab's lifetime. An hour is frequent enough
// to catch that within the review's tolerance and infrequent enough that
// it's not worth debouncing against normal navigation traffic — GTFS
// reloads are a deploy-time event, not something that needs sub-hour
// freshness.
const REFRESH_INTERVAL_MS = 60 * 60 * 1000

/**
 * Fetch `/api/gtfs/freshness`, deduping concurrent requests and sharing the
 * result across every mount via the module-level cache.
 *
 * Never memoizes a failure: a non-ok response or a network error resolves
 * to `null` and clears `_fetchPromise`, so the next call (next mount or
 * poll tick) retries instead of being stuck replaying the failed result
 * for the rest of the session.
 *
 * @returns {Promise<object|null>} The freshness payload, or null on
 *   failure.
 */
function fetchGtfsFreshness() {
  if (!_fetchPromise) {
    _fetchPromise = fetch('/api/gtfs/freshness')
      .then((res) => (res.ok ? res.json() : null))
      .catch(() => null)
      .finally(() => {
        _fetchPromise = null
      })
  }
  return _fetchPromise
}

/**
 * Fetch `/api/gtfs/freshness` and share the result across every component
 * that needs it (the app-chrome expiry banner and RouteList's "GTFS
 * schedule current as of …" footer), with stale-while-revalidate semantics.
 *
 * On mount, the cached value (if any) is returned immediately — no flash —
 * while a background refetch always runs to pick up changes, sharing an
 * in-flight request with any other component mounting in the same tick.
 * Because App.jsx's banner mounts once per SPA session, every mount also
 * re-polls every `REFRESH_INTERVAL_MS` (60 minutes) for as long as it
 * stays mounted, so a tab left open for days still learns the feed
 * expired.
 *
 * Pure observability — failures are silent. A failed fetch just means the
 * banner/footer don't render (or keep showing the last good value); it
 * must never block or error the page.
 *
 * @param {*} [refetchTrigger] - Optional value to include in the effect's
 *   dependency array so a caller can force an immediate refetch (and reset
 *   of the polling interval) by changing it — e.g. App.jsx's header Refresh
 *   button bumps a counter here instead of remounting this hook's owner,
 *   since remounting alone would just replay the module-level cache rather
 *   than hit the network again (the frontend-chrome honesty fixes, PR #204).
 * @returns {object|null} The freshness payload (`snapshot_date`,
 *   `created_at`, `feed_version`, `feed_start_date`, `feed_end_date`,
 *   `status`), or null until the first successful response.
 */
function useGtfsFreshness(refetchTrigger) {
  const [gtfsFreshness, setGtfsFreshness] = useState(_cachedGtfsFreshness)

  useEffect(() => {
    let cancelled = false

    const refresh = () => {
      fetchGtfsFreshness().then((data) => {
        if (data && !cancelled) {
          setGtfsFreshness(data)
          _cachedGtfsFreshness = data
        }
      })
    }

    refresh()
    const intervalId = setInterval(refresh, REFRESH_INTERVAL_MS)
    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [refetchTrigger])

  return gtfsFreshness
}

export default useGtfsFreshness
