// Formatting helpers for the agency comparison page (PR #198). Split out
// of AgencyComparison.jsx so the pure display logic is unit-testable the
// same way utils/formatters.js is.

// Canonical metric order + labels for the five headline KPIs. Mirrors the
// OTP / service_delivered / SWT / EWT / bunching unit conventions used
// elsewhere (Targets.jsx's formatTarget, formatters.js's
// formatContribMetricValue): OTP is 0-100 percent, service_delivered and
// bunching are 0-1 fractions, SWT and EWT are seconds.
export const METRIC_ORDER = ['otp', 'service_delivered', 'swt', 'ewt', 'bunching']

export const METRIC_LABELS = {
  otp: 'On-time performance',
  service_delivered: 'Service delivered',
  swt: 'Scheduled wait (frequent svc)',
  ewt: 'Excess wait time',
  bunching: 'Bunching rate',
}

// Direction each metric needs to move to count as "improving" — used to
// tint the week-over-week delta pill green/red.
export const HIGHER_IS_BETTER = {
  otp: true,
  service_delivered: true,
  swt: false,
  ewt: false,
  bunching: false,
}

/**
 * Format one metric's window-mean value for display, given its canonical
 * unit. Returns '—' for null (no data in the window for that metric).
 *
 * @param {'otp'|'service_delivered'|'swt'|'ewt'|'bunching'} metric
 * @param {number|null|undefined} value
 * @returns {string}
 */
export function formatMetricValue(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${value.toFixed(1)}%`
  if (metric === 'service_delivered') return `${(value * 100).toFixed(1)}%`
  if (metric === 'ewt' || metric === 'swt') return `${(value / 60).toFixed(1)} min`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

/**
 * Format one metric's week-over-week delta as a signed magnitude plus a
 * green/red/neutral tint, or null when the API reported no delta yet (the
 * matched window doesn't hold a full 14 days for that agency).
 *
 * @param {'otp'|'service_delivered'|'swt'|'ewt'|'bunching'} metric
 * @param {number|null|undefined} delta
 * @returns {{text: string, tint: 'green'|'red'|'neutral'} | null}
 */
export function formatDelta(metric, delta) {
  if (delta == null) return null
  const higherIsBetter = HIGHER_IS_BETTER[metric]
  const improving = higherIsBetter ? delta > 0 : delta < 0
  const neutral = delta === 0

  let magnitude
  if (metric === 'otp') magnitude = `${Math.abs(delta).toFixed(1)} pts`
  else if (metric === 'service_delivered') magnitude = `${Math.abs(delta * 100).toFixed(1)} pts`
  else if (metric === 'ewt' || metric === 'swt') magnitude = `${Math.abs(delta / 60).toFixed(1)} min`
  else magnitude = `${Math.abs(delta * 100).toFixed(1)} pts`

  const sign = delta > 0 ? '+' : delta < 0 ? '−' : '±'
  return {
    text: `${sign}${magnitude} vs prior week`,
    tint: neutral ? 'neutral' : improving ? 'green' : 'red',
  }
}

/**
 * Format the daytime service-level block for its tile. Returns null when
 * the API degraded to the null block (no schedule available), so the tile
 * can render its em-dash empty state.
 *
 * @param {{median_headway_seconds: number|null, pct_at_most_15min: number|null}|null|undefined} serviceLevel
 * @returns {{median: string, share: string|null} | null}
 */
export function formatServiceLevel(serviceLevel) {
  if (!serviceLevel || serviceLevel.median_headway_seconds == null) return null
  const median = `${(serviceLevel.median_headway_seconds / 60).toFixed(1)} min`
  const share =
    serviceLevel.pct_at_most_15min == null
      ? null
      : `${(serviceLevel.pct_at_most_15min * 100).toFixed(0)}% of scheduled service every ≤15 min`
  return { median, share }
}

// ── Route-level distributions (NOTES-141) ───────────────────────────────────
//
// The headline above is one window-mean per metric per agency, which hides
// the spread -- two agencies with identical mean OTP can have very
// different shares of bad routes. `/api/agency-comparison` additively
// carries a `route_distribution` block per agency for these two metrics
// (median/IQR/histogram/threshold-share, computed per-route with every
// route weighted equally -- see the caveat the backend appends for why).

/** Metrics that carry a `route_distribution` block. Mirrors
 * `ROUTE_DISTRIBUTION_METRICS` in `api/aggregations.py`. */
export const ROUTE_DISTRIBUTION_METRICS = ['otp', 'service_delivered']

/** Fixed per-agency series colors for the distribution histogram, so a
 * given agency always reads as the same color across every metric's
 * chart. Keyed by the `agency` field from the API (`wmata` / `sfmta`), not
 * display order -- an agency's color must not shift if the payload's
 * agency array order ever changes. An agency key outside this map (a
 * future third agency) falls back to a neutral gray via
 * {@link agencySeriesColor} rather than an undefined color. */
export const AGENCY_SERIES_COLORS = {
  wmata: '#2a78d6',
  sfmta: '#eb6834',
}

/**
 * Resolve one agency's fixed series color, falling back to a neutral gray
 * for an agency key not in {@link AGENCY_SERIES_COLORS}.
 *
 * @param {string} agencyKey
 * @returns {string} hex color
 */
export function agencySeriesColor(agencyKey) {
  return AGENCY_SERIES_COLORS[agencyKey] ?? '#64748b'
}

/**
 * Format one agency/metric's `route_distribution` entry into display
 * strings for the comparison table's distribution row. Returns null when
 * the block is missing or has no scored routes, so the caller can render
 * an em-dash row instead of "— routes, — – —".
 *
 * @param {'otp'|'service_delivered'} metric
 * @param {{route_count: number, median: number|null, p25: number|null,
 *   p75: number|null, threshold: number|null,
 *   share_at_or_above_threshold: number|null}|null|undefined} distribution
 * @returns {{median: string, iqr: string, share: string|null,
 *   thresholdLabel: string|null, routeCount: number} | null}
 */
export function formatDistributionStats(metric, distribution) {
  if (!distribution || !distribution.route_count) return null
  const median = formatMetricValue(metric, distribution.median)
  const p25 = formatMetricValue(metric, distribution.p25)
  const p75 = formatMetricValue(metric, distribution.p75)
  const thresholdLabel =
    distribution.threshold == null ? null : formatMetricValue(metric, distribution.threshold)
  const share =
    distribution.share_at_or_above_threshold == null
      ? null
      : `${Math.round(distribution.share_at_or_above_threshold * 100)}%`
  return {
    median,
    iqr: `${p25} – ${p75}`,
    share,
    thresholdLabel,
    routeCount: distribution.route_count,
  }
}

/**
 * Build a recharts-ready dataset for the per-metric distribution
 * histogram: one row per bucket label, with one numeric field per agency
 * (keyed by `agency.agency`) holding that agency's route count in the
 * bucket. Bucket labels/order come from whichever agency has a non-empty
 * histogram first -- both agencies share the same fixed bucket edges (see
 * the backend module comment), so any agency's label set is authoritative.
 *
 * @param {Array<{agency: string, route_distribution?: object}>} agencies
 * @param {'otp'|'service_delivered'} metric
 * @returns {Array<Record<string, string|number>>} e.g.
 *   `[{label: '<60', wmata: 12, sfmta: 4}, ...]`
 */
export function buildDistributionHistogramData(agencies, metric) {
  const reference = agencies.find(
    (agency) => agency.route_distribution?.[metric]?.histogram?.length,
  )
  const buckets = reference?.route_distribution?.[metric]?.histogram ?? []
  return buckets.map((bucket, i) => {
    const row = { label: bucket.label }
    for (const agency of agencies) {
      row[agency.agency] = agency.route_distribution?.[metric]?.histogram?.[i]?.count ?? 0
    }
    return row
  })
}
