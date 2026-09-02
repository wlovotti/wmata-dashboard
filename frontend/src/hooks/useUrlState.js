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
 * Multi-key note: each call reads/writes independently against a fresh
 * `URLSearchParams` snapshot. Firing two different keys' setters
 * synchronously in the same event handler races (react-router computes
 * each new URL from the same pre-update snapshot, so only the last call's
 * change survives) — callers that need to change more than one param
 * atomically (e.g. a combined "clear filters" action) should build one
 * combined `URLSearchParams` update with `useSearchParams` directly
 * instead of composing two `useUrlState` setters.
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
    const next = new URLSearchParams(searchParams)
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
