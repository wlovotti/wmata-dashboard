// 7-day trailing rolling mean for the Overview trend cards (NOTES-84).
// Window length matches the week-over-week framing used everywhere else in
// the app; do not widen it past the post-cutover clean-data window semantics.
export const ROLLING_WINDOW_DAYS = 7

/**
 * Smooth a daily metric series with a trailing rolling mean.
 *
 * @param {Array<{date: string, value: number|null, data_quality?: string}>} series
 *   Daily rows in any order; dates are ISO strings (lexicographic ==
 *   chronological). Rows with `data_quality === 'partial'` or a null value
 *   are excluded from window means but keep their date slot in the output.
 * @param {number} [windowDays] - trailing window length, default 7.
 * @returns {Array<{date: string, value: number|null}>} one row per input
 *   date, ascending; `value` is null when the trailing window contains no
 *   clean day.
 */
export function rollingMean(series, windowDays = ROLLING_WINDOW_DAYS) {
  if (!Array.isArray(series)) return []
  const sorted = [...series].sort((a, b) =>
    a.date < b.date ? -1 : a.date > b.date ? 1 : 0,
  )
  return sorted.map((row, i) => {
    const window = sorted.slice(Math.max(0, i - windowDays + 1), i + 1)
    const clean = window.filter((r) => r.value != null && r.data_quality !== 'partial')
    if (clean.length === 0) return { date: row.date, value: null }
    const mean = clean.reduce((acc, r) => acc + r.value, 0) / clean.length
    return { date: row.date, value: mean }
  })
}
