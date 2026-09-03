import { AGENCIES, DEFAULT_AGENCY } from '../hooks/useAgency'

// One place builds API request URLs (NOTES-143 decision 2): every fetch in
// `frontend/src` (except `/compare`'s agency-comparison components, which
// are deliberately agency-independent — see `CompareStrip.jsx` /
// `AgencyComparison.jsx`) should go through `apiUrl` so the current
// `?agency=` selection reaches the backend without every call site
// re-deriving it.

/**
 * Build an API request URL, merging in the current `agency` (read off the
 * live URL) plus any extra query params, in one place.
 *
 * Reads `agency` directly from `window.location.search` rather than a
 * React hook, since this is a plain function callable from anywhere a URL
 * is needed (module-level `useMultiFetch` URL arrays, event handlers,
 * effects) — mirrors the pattern `useUrlState.js`'s setter already uses
 * for the same cross-call-site reason. Falls back to the default
 * (`wmata`) for a missing or invalid value, and omits `agency` from the
 * built URL entirely at the default so pre-agency-switch URLs, fixtures,
 * and Playwright specs are unaffected.
 *
 * A caller that already knows its own agency (e.g. a hook whose value
 * doesn't come from the current page's URL) can pass it explicitly as an
 * `agency` key in `params` — that value wins over the URL-derived one,
 * and is still omitted from the built URL when it equals the default.
 *
 * @param {string} path - Bare API path, e.g. `/api/routes`.
 * @param {object|URLSearchParams|string|Array} [params] - Additional
 *   query params to merge in. Anything `URLSearchParams`'s constructor
 *   accepts works: a plain object, an array of `[key, value]` pairs, a
 *   query string, or another `URLSearchParams` instance.
 * @returns {string} `path`, with the merged query string appended (or
 *   `path` unchanged when there's nothing to append).
 */
export function apiUrl(path, params) {
  const merged = new URLSearchParams(params)

  let agency = merged.get('agency')
  merged.delete('agency')
  if (agency == null) {
    agency = new URLSearchParams(window.location.search).get('agency')
  }
  if (!AGENCIES.includes(agency)) agency = DEFAULT_AGENCY

  if (agency !== DEFAULT_AGENCY) merged.set('agency', agency)

  const qs = merged.toString()
  return qs ? `${path}?${qs}` : path
}

export default apiUrl
