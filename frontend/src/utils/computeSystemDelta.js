// Suppress the 30-vs-prior-30 delta when either side has too few valid days
// to be meaningful. 30-day windows tolerate more sparsity than the 7-day
// per-route delta — but a single-digit valid-day count is still noise. 10 is
// the smallest threshold that keeps a couple of missing days from flipping
// the sign of a comparison and matches the spirit of the per-route <3 rule
// scaled to a 30-day horizon (≈ 1/3 of the window must be real data).
const SYSTEM_DELTA_MIN_DAYS = 10

/**
 * Compute a 30-vs-prior-30 delta from a current-window series and a single
 * prior-window scalar returned by the system trend endpoint.
 *
 * Returns `{ delta, currentMean, priorMean, currentN }` or `null` when
 * either window is too sparse — `priorMean` from the server is null if the
 * prior window had no valid days, and the current window must have at least
 * `SYSTEM_DELTA_MIN_DAYS` non-null points to avoid noise-driven arrow flips.
 */
export function computeSystemDelta(series, priorWindowValue) {
  if (priorWindowValue == null) return null
  const valid = (series || []).filter((row) => row.value != null)
  if (valid.length < SYSTEM_DELTA_MIN_DAYS) return null
  const currentMean = valid.reduce((a, b) => a + b.value, 0) / valid.length
  return {
    delta: currentMean - priorWindowValue,
    currentMean,
    priorMean: priorWindowValue,
    currentN: valid.length,
  }
}
