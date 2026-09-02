import useUrlState from './useUrlState'

// Time-window picker (NOTES-140, wave 1 of the 2026-09 UX program). `?days=`
// on the URL is the single source of truth for "how far back" every page's
// analysis window looks — the app previously scattered `days=30` literals
// (and, for `/api/routes`, no `days` at all) across Overview/RouteList/
// RouteDetail with no way to link a specific window or have it survive
// navigation. 30 is the default so pre-existing links without `?days=`
// render exactly as before.
//
// 90 was dropped in PR #239 review: `/api/routes` clamps `days` to 30
// server-side (api/main.py's `get_routes`), so a 90-day pick rendered a
// 30-day scorecard beside genuinely-90-day trend/contributor panels — an
// internally inconsistent page. Separately, the "prior 90" delta window
// (roughly 2026-03-07..06-04 relative to a 2026-09 "today") mostly predates
// this deployment's production data window (starts 2026-05-02, see
// MEMORY.md) and straddles the pre-cutover old-VM poll-gap era (NOTES-95),
// so a 90-day comparison would be comparing against thin/contaminated
// history far more often than not. A 90-day pick would also have been the
// worst case for `/api/routes/{id}`'s excess-trip-time freshest-day
// lookup, which loops one query per day over `range(days+1)` — moot now
// that that endpoint is deliberately left unwired from this picker (PR
// #239 review finding B), but it was a third reason 90 didn't belong in
// the option set to begin with. 7/30 are the two windows every wired
// endpoint can render faithfully today.
export const WINDOW_DAY_OPTIONS = [7, 30]
export const DEFAULT_WINDOW_DAYS = 30
const WINDOW_DAYS_KEY = 'days'

/**
 * Read/write the app-wide `?days=` window selection. Thin wrapper over
 * `useUrlState` so every page consuming the window shares one key + default
 * instead of repeating the literal `'days'` / `30` at each call site.
 *
 * Validates against `WINDOW_DAY_OPTIONS` (PR #239 review finding F): a
 * hand-edited or stale `?days=` (`7.5`, `1000000`, or anything else not in
 * the allowed set) falls back to the default rather than reaching an API
 * endpoint with an unbounded value or leaving the picker with no active
 * button.
 *
 * @returns {[number, (days: number) => void]}
 */
export function useWindowDays() {
  const [rawDays, setDays] = useUrlState(WINDOW_DAYS_KEY, DEFAULT_WINDOW_DAYS)
  const days = WINDOW_DAY_OPTIONS.includes(rawDays) ? rawDays : DEFAULT_WINDOW_DAYS
  return [days, setDays]
}

/**
 * Append the current `days` window to a path as a query param, omitting it
 * when `days` is the default (30) so unfiltered URLs stay clean. Used by
 * the shell's primary nav and by row-click navigation into RouteDetail, so
 * the selected window survives navigation instead of silently reverting to
 * the default on the next page.
 *
 * @param {string} path - A path, optionally already carrying a query string.
 * @param {number} days - The current window selection.
 * @returns {string} `path`, with `days=<days>` appended when non-default.
 */
export function appendWindowParam(path, days) {
  if (days === DEFAULT_WINDOW_DAYS) return path
  const separator = path.includes('?') ? '&' : '?'
  return `${path}${separator}days=${days}`
}

export default useWindowDays
