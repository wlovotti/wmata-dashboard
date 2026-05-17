// Shared display formatters used across multiple components. Each helper
// was previously duplicated inline; centralizing here keeps unit conventions
// (percent vs ratio vs seconds) and edge-case handling (null → "—")
// consistent across surfaces.

/**
 * Render an absolute deviation in seconds as "M:SS", or "—" for null.
 *
 * Used by the "worst observed deviation" column in BlockList / ActiveBlocks,
 * which already stores the magnitude (abs taken upstream). Pure display:
 * does not annotate early/late — the column header carries that meaning.
 *
 * @param {number|null|undefined} sec
 * @returns {string}
 */
export function formatDeviationMmSs(sec) {
  if (sec == null) return '—'
  const abs = Math.abs(sec)
  const mins = Math.floor(abs / 60)
  const secs = abs % 60
  return `${mins}:${secs.toString().padStart(2, '0')}`
}

/**
 * Render a signed deviation in seconds as "+Ns" / "-Ns" / "0s", or "—" for
 * null. Used by the per-run percentile columns in RecentRuns where the sign
 * itself encodes early vs late and the magnitudes are small enough that
 * raw seconds read more naturally than M:SS.
 *
 * @param {number|null|undefined} sec
 * @returns {string}
 */
export function formatDeviationSignedSec(sec) {
  if (sec == null) return '—'
  if (sec === 0) return '0s'
  const sign = sec > 0 ? '+' : ''
  return `${sign}${sec}s`
}

/**
 * Format a per-route metric value for the contributors / off-target panels
 * on the Overview and RouteList pages. Encodes the canonical units mapping
 * (NOTES-47): OTP is already 0-100 percent, service_delivered and bunching
 * are 0-1 fractions on the scorecard row, EWT is seconds.
 *
 * @param {'otp'|'service_delivered'|'ewt'|'bunching'|string} metric
 * @param {number|null|undefined} value
 * @returns {string}
 */
export function formatContribMetricValue(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${Math.round(value)}%`
  if (metric === 'service_delivered') return `${Math.round(value * 100)}%`
  if (metric === 'ewt') return `${Math.round(value)}s`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

/**
 * Return today's Eastern date as YYYY-MM-DD.
 *
 * The API defines "today" as the current Eastern service date, which can
 * differ from the user's local browser date during the early-morning UTC
 * hours. Use this for any default `service_date` query param so the picker
 * matches the API's notion of today.
 *
 * @returns {string}
 */
export function todayEasternIso() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
  return fmt.format(new Date())
}
