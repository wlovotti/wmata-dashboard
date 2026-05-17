// Below this many valid days in *either* the recent or prior window we
// suppress the 7-vs-prior-7 delta. With only a day or two of data the delta
// is noise and the up/down arrow is misleading. 3 is the smallest value that
// keeps a missing-day from changing the sign of the comparison.
const DELTA_MIN_DAYS = 3

/**
 * Compute a 7-day-vs-prior-7-day delta from a list of `{date, value}` rows.
 *
 * Drops null values, takes the most-recent 7 valid entries as the "recent"
 * window and the next-most-recent 7 as the "prior" window. Returns
 * `{ delta, recentMean, priorMean, recentN, priorN }` or `null` if either
 * window has fewer than DELTA_MIN_DAYS valid points (the delta would be
 * misleading on thin data).
 */
export function computeWindowDelta(series) {
  if (!series || series.length === 0) return null
  const valid = series.filter((row) => row.value != null)
  if (valid.length < DELTA_MIN_DAYS * 2) return null
  // Sort ascending by date so .slice(-7) gives the most recent week.
  const sorted = [...valid].sort((a, b) =>
    a.date < b.date ? -1 : a.date > b.date ? 1 : 0,
  )
  const recent = sorted.slice(-7)
  const prior = sorted.slice(-14, -7)
  if (recent.length < DELTA_MIN_DAYS || prior.length < DELTA_MIN_DAYS) return null
  const mean = (xs) => xs.reduce((a, b) => a + b.value, 0) / xs.length
  const recentMean = mean(recent)
  const priorMean = mean(prior)
  return {
    delta: recentMean - priorMean,
    recentMean,
    priorMean,
    recentN: recent.length,
    priorN: prior.length,
  }
}
