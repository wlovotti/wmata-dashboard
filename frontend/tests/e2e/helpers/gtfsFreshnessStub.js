// Shared `/api/gtfs/freshness` stub for visual-regression specs.
//
// `feed_end_date` is far in the future so `status` is always `ok` — the
// NOTES-90 expiry banner (rendered app-wide in App.jsx chrome) must never
// appear in visual baselines. Previously this JSON + comment was
// copy-pasted into overview/routedetail/routelist/segments specs; this
// module is the single source of truth so a future field change (or the
// eventual `snapshot_date`-bearing baseline regen — see routelist.spec.js)
// only needs updating in one place.
export const GTFS_FRESHNESS_OK_STUB = {
  feed_start_date: '20260101',
  feed_end_date: '20991231',
  status: 'ok',
}

/**
 * Fulfill a Playwright route interception for `/api/gtfs/freshness` with
 * the shared "healthy, far-future expiry" stub.
 *
 * @param {import('@playwright/test').Route} route - the intercepted route.
 * @param {object} [overrides] - fields to merge over the base stub (e.g.
 *   `snapshot_date` / `created_at` / `feed_version` for specs that also
 *   exercise RouteList's freshness footer).
 * @returns {Promise<void>}
 */
export function fulfillGtfsFreshness(route, overrides = {}) {
  return route.fulfill({ json: { ...GTFS_FRESHNESS_OK_STUB, ...overrides } })
}
