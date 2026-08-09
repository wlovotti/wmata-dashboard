import { useEffect, useState } from 'react'

// Module-level cache so navigating between routes doesn't refetch/flash —
// mirrors the pattern RouteList used before this hook existed (NOTES-90).
let _cachedGtfsFreshness = null

/**
 * Fetch `/api/gtfs/freshness` once per session and share the result across
 * every component that needs it (the app-chrome expiry banner and
 * RouteList's "GTFS schedule current as of …" footer).
 *
 * Pure observability — failures are silent. A failed fetch just means the
 * banner/footer don't render; it must never block or error the page.
 *
 * @returns {object|null} The freshness payload (`snapshot_date`,
 *   `created_at`, `feed_version`, `feed_start_date`, `feed_end_date`,
 *   `status`), or null until the first successful response.
 */
function useGtfsFreshness() {
  const [gtfsFreshness, setGtfsFreshness] = useState(_cachedGtfsFreshness)

  useEffect(() => {
    fetch('/api/gtfs/freshness')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data) {
          setGtfsFreshness(data)
          _cachedGtfsFreshness = data
        }
      })
      .catch(() => {
        // Swallow — this is informational; don't surface to the user.
      })
  }, [])

  return gtfsFreshness
}

export default useGtfsFreshness
