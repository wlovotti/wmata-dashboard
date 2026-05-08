import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'

const OTP_LINE_COLOR = '#002F6C'
const SD_LINE_COLOR = '#0E8A6F'
// Warm amber for excess-trip-time — distinct from OTP/SD without colliding
// with the EWT thin-data warning red elsewhere on the page.
const EXCESS_LINE_COLOR = '#B45309'

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
function computeWindowDelta(series) {
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

/**
 * Render an inline up/down/flat indicator for a precomputed delta.
 *
 * `format` turns the raw delta into display text (e.g. percentage points).
 * Anything within ±`flatThreshold` shows as flat — avoids arrow flicker on
 * essentially-unchanged metrics.
 */
function DeltaIndicator({ delta, format, flatThreshold = 0.5, title }) {
  if (delta == null) return null
  let arrow = '→'
  let color = '#64748b'
  if (delta > flatThreshold) {
    arrow = '▲'
    color = '#0E8A6F'
  } else if (delta < -flatThreshold) {
    arrow = '▼'
    color = '#C8102E'
  }
  const sign = delta > 0 ? '+' : ''
  return (
    <span
      className="trend-delta"
      style={{
        color,
        fontSize: '0.75rem',
        fontWeight: 600,
        marginLeft: '0.4rem',
      }}
      title={title || '7-day mean vs prior 7-day mean'}
    >
      {arrow} {sign}{format(delta)}
    </span>
  )
}

/**
 * Mini 30-day sparkline rendered with recharts.
 *
 * `data` is an array of `{ date, value }` rows. Rows with `value == null`
 * are dropped defensively (the trend endpoint emits null for days with
 * no observations); without this a sparse early window plots a cliff
 * to zero. Strips axes / grid / legend for a compact card-friendly
 * presentation; tooltip on hover gives the date + value.
 *
 * If only one valid point survives, recharts won't draw a line — fall
 * back to a single dot so the user still sees the measurement.
 */
function Sparkline({ data, color, valueFormat, height = 60 }) {
  const valid = (data || []).filter((row) => row.value != null)
  if (valid.length === 0) {
    return (
      <div
        className="sparkline-empty"
        style={{
          height,
          fontSize: '0.7rem',
          color: '#94a3b8',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        no trend data
      </div>
    )
  }
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={valid} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
        <XAxis dataKey="date" hide />
        <YAxis hide domain={['dataMin', 'dataMax']} />
        <Tooltip
          formatter={(value) => [valueFormat(value), '']}
          labelFormatter={(label) => label}
          contentStyle={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem' }}
          separator=""
        />
        <Line
          type="monotone"
          dataKey="value"
          stroke={color}
          strokeWidth={1.75}
          dot={valid.length === 1}
          connectNulls={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

/**
 * 30-day OTP, service-delivered, and excess-trip-time trend block for a
 * single route.
 *
 * Receives precomputed `{date, value}` series from RouteDetail (which fetches
 * the trend payload once and reuses it for the per-KPI-card deltas above).
 *
 * Closes NOTES-37: surfaces the existing trend payload that was unconsumed
 * by the UI. Service-delivered support was added to the trend endpoint in
 * the same PR. NOTES-43 added the excess-trip-time series.
 */
function RouteTrend({
  otpSeries,
  sdSeries,
  excessSeries,
  otpDelta,
  sdDelta,
  excessDelta,
  loading,
  error,
}) {
  if (loading) {
    return (
      <div className="chart-container">
        <h2>30-Day Trend</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>Loading…</p>
      </div>
    )
  }
  if (error) {
    return (
      <div className="chart-container">
        <h2>30-Day Trend</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>
          Trend unavailable: {error}
        </p>
      </div>
    )
  }
  return (
    <div className="chart-container">
      <h2>30-Day Trend</h2>
      <div className="route-trend-grid">
        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">On-Time Performance</span>
            {otpDelta && (
              <DeltaIndicator
                delta={otpDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
              />
            )}
          </div>
          <Sparkline
            data={otpSeries}
            color={OTP_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
          {otpDelta && (
            <div className="route-trend-meta">
              Last 7 days: {otpDelta.recentMean.toFixed(1)}% · Prior 7:{' '}
              {otpDelta.priorMean.toFixed(1)}%
            </div>
          )}
        </div>
        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">Service Delivered</span>
            {sdDelta && (
              <DeltaIndicator
                delta={sdDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
              />
            )}
          </div>
          <Sparkline
            data={sdSeries}
            color={SD_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
          {sdDelta && (
            <div className="route-trend-meta">
              Last 7 days: {sdDelta.recentMean.toFixed(1)}% · Prior 7:{' '}
              {sdDelta.priorMean.toFixed(1)}%
            </div>
          )}
        </div>
        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">% of Trips Running Long</span>
            {excessDelta && (
              <DeltaIndicator
                delta={excessDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
              />
            )}
          </div>
          <Sparkline
            data={excessSeries}
            color={EXCESS_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
          {excessDelta && (
            <div className="route-trend-meta">
              Last 7 days: {excessDelta.recentMean.toFixed(1)}% · Prior 7:{' '}
              {excessDelta.priorMean.toFixed(1)}%
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export { computeWindowDelta, DeltaIndicator, Sparkline }
export default RouteTrend
