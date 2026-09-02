import useUrlState from './useUrlState'

// Time-window picker (NOTES-140, wave 1 of the 2026-09 UX program). `?days=`
// on the URL is the single source of truth for "how far back" every page's
// analysis window looks — the app previously scattered `days=30` literals
// (and, for `/api/routes`, no `days` at all) across Overview/RouteList/
// RouteDetail with no way to link a specific window or have it survive
// navigation. 30 is the default so pre-existing links without `?days=`
// render exactly as before.
export const WINDOW_DAY_OPTIONS = [7, 30, 90]
export const DEFAULT_WINDOW_DAYS = 30
const WINDOW_DAYS_KEY = 'days'

/**
 * Read/write the app-wide `?days=` window selection. Thin wrapper over
 * `useUrlState` so every page consuming the window shares one key + default
 * instead of repeating the literal `'days'` / `30` at each call site.
 *
 * @returns {[number, (days: number) => void]}
 */
export function useWindowDays() {
  return useUrlState(WINDOW_DAYS_KEY, DEFAULT_WINDOW_DAYS)
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
