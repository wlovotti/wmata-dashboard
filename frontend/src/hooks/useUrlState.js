import { useSearchParams } from 'react-router-dom'

/**
 * Sync a single piece of UI state with one URL search parameter
 * (NOTES-140). Generalizes the ad-hoc `useSearchParams` + "delete when
 * default" idiom `SegmentDiagnostic.jsx` and `BlockTimeline.jsx` already
 * used, so every page filter can be linkable/back-button-safe without
 * hand-rolling the same read/write dance per component.
 *
 * The URL is the source of truth: reading returns `defaultValue` whenever
 * the param is absent (so a link minted before this param existed, or one
 * a user typed by hand, keeps working unchanged), and writing omits the
 * param entirely when the new value equals `defaultValue` — URLs stay
 * clean instead of accumulating default-valued query strings on every
 * filter touch.
 *
 * Values are coerced to match the type of `defaultValue`: a `number`
 * default parses the raw param with `Number()` (falling back to
 * `defaultValue` when the param is missing, non-numeric, or NaN);
 * anything else is treated as a plain string.
 *
 * Cross-writer note (PR #239 review finding G): each setter merges its
 * change onto the union of the live `window.location.search` and the
 * `searchParams` closed over from the render that created the setter (see
 * `setValue` below for why both are needed), not `searchParams` alone. Two
 * different `useUrlState` instances writing different keys in close
 * succession — even across components, e.g. the header's `WindowPicker`
 * setting `days` right before `RouteDetail` sets `day_type` — merge onto
 * whatever the URL actually holds instead of one clobbering the other's
 * not-yet-rendered change. This doesn't cover a same-tick *deletion* race
 * (a stale `searchParams` can resurrect a key another writer just removed
 * from the real URL) — only additions/updates, which is what the observed
 * bug was. A caller that needs to change more than one key as a single
 * atomic write (e.g. a combined "clear filters" action) should still build
 * one combined `URLSearchParams` update with `useSearchParams` directly,
 * so the change lands as one history entry instead of two.
 *
 * @param {string} key - Search-param name.
 * @param {string|number} defaultValue - Value returned when the param is
 *   absent, and the value that causes the param to be omitted on write.
 * @param {{ replace?: boolean }} [options] - `replace` (default true)
 *   updates the URL by replacing the current history entry, so routine
 *   filter tweaks don't pile up in back/forward history the way a `push`
 *   per keystroke/click would.
 * @returns {[string|number, (value: string|number) => void]} current
 *   value and a setter.
 */
export function useUrlState(key, defaultValue, { replace = true } = {}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const isNumeric = typeof defaultValue === 'number'
  const raw = searchParams.get(key)

  let value = defaultValue
  if (raw !== null) {
    if (isNumeric) {
      const parsed = Number(raw)
      if (Number.isFinite(parsed)) value = parsed
    } else {
      value = raw
    }
  }

  const setValue = (newValue) => {
    // Merge onto the union of the live URL and this render's own
    // `searchParams` (PR #239 review finding G), not `searchParams` alone.
    // React Router 7's `BrowserRouter` wraps the state update that drives
    // a re-render in `startTransition`, but the History API call inside
    // `navigate`/`setSearchParams` mutates `window.location` synchronously
    // — so a key another `useUrlState` instance (or another component
    // entirely) just wrote is already visible on `window.location.search`
    // even before React re-renders this hook with it. Reading
    // `window.location.search` first, and falling back to `searchParams`
    // only for keys it doesn't have, means: under `BrowserRouter` (the
    // only router the app itself renders — `App.jsx`), `window.location`
    // is the authoritative superset, so nothing from `searchParams` is
    // needed; under `MemoryRouter` (tests), `window.location` never
    // reflects the router's in-memory navigation at all, so it
    // contributes nothing and `searchParams` remains the base, exactly as
    // before this fix.
    const next = new URLSearchParams(window.location.search)
    for (const [existingKey, existingValue] of searchParams.entries()) {
      if (!next.has(existingKey)) next.set(existingKey, existingValue)
    }
    if (newValue == null || newValue === defaultValue) {
      next.delete(key)
    } else {
      next.set(key, String(newValue))
    }
    setSearchParams(next, { replace })
  }

  return [value, setValue]
}

export default useUrlState
