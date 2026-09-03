import useUrlState from './useUrlState'

// Agency switch (NOTES-143, wave 2 of the 2026-09 UX program). `?agency=`
// on the URL is the single source of truth for which backend database the
// dashboard renders — mirrors `useWindowDays.js`'s `?days=` pattern.
// `wmata` is the default AND the API's own default (`agency` query params
// default to `"wmata"` on every endpoint, NOTES-139), so it's omitted from
// the URL: existing links, fixtures, and Playwright specs that never
// mention `agency` keep rendering exactly as before.
export const AGENCIES = ['wmata', 'sfmta']
export const DEFAULT_AGENCY = 'wmata'
const AGENCY_KEY = 'agency'

// Shared display labels — used by the header title/subtitle, the
// AgencyToggle segmented buttons, and any "not available for this agency"
// note, so the two-agency copy stays consistent in one place.
export const AGENCY_LABELS = {
  wmata: 'WMATA',
  sfmta: 'Muni',
}

/**
 * Read/write the app-wide `?agency=` selection. Thin wrapper over
 * `useUrlState` so every page/component consuming the agency shares one
 * key + default + validation instead of repeating the literal `'agency'`
 * / `'wmata'` at each call site.
 *
 * Validates against `AGENCIES` (mirrors `useWindowDays`'s validation for
 * `?days=`): a hand-edited or stale `?agency=` value not in the allowed
 * set falls back to the default rather than reaching an API endpoint
 * with an unrecognized agency name.
 *
 * @returns {[string, (agency: string) => void]}
 */
export function useAgency() {
  const [rawAgency, setAgency] = useUrlState(AGENCY_KEY, DEFAULT_AGENCY)
  const agency = AGENCIES.includes(rawAgency) ? rawAgency : DEFAULT_AGENCY
  return [agency, setAgency]
}

export default useAgency
