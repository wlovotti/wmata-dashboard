import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceDot,
} from 'recharts'

const OTP_LINE_COLOR = '#002F6C'
const SD_LINE_COLOR = '#0E8A6F'
// Warm amber for excess-trip-time — distinct from OTP/SD without colliding
// with the EWT thin-data warning red elsewhere on the page.
const EXCESS_LINE_COLOR = '#B45309'

/**
 * Render a "vs target" badge for one metric (NOTES-47).
 *
 * Compares a current `value` against a `target`, formats both via the
 * caller-provided `format` (so units stay consistent), and shows a small
 * inline pill colored green when the metric meets or beats its target,
 * red when it misses. `higherIsBetter` flips the comparison for
 * lower-is-better metrics (EWT, bunching).
 *
 * Returns null when either side is null/undefined — the rest of the KPI
 * card renders unchanged, so the target line is purely additive.
 */
function TargetIndicator({
  value,
  target,
  format,
  higherIsBetter = true,
  label = 'Target',
  flatThreshold = 0,
}) {
  if (value == null || target == null) return null
  const gap = higherIsBetter ? value - target : target - value
  let color = '#64748b'
  let arrow = '→'
  if (gap > flatThreshold) {
    color = '#0E8A6F'
    arrow = '✓'
  } else if (gap < -flatThreshold) {
    color = '#C8102E'
    arrow = '✗'
  }
  return (
    <span
      className="trend-target"
      style={{
        color,
        fontSize: '0.7rem',
        fontWeight: 600,
        marginLeft: '0.4rem',
      }}
      title={`Current vs ${label.toLowerCase()}`}
    >
      {arrow} {label} {format(target)}
    </span>
  )
}

/**
 * Render an inline up/down/flat indicator for a precomputed delta.
 *
 * `format` turns the raw delta into display text (e.g. percentage points).
 * Anything within ±`flatThreshold` shows as flat — avoids arrow flicker on
 * essentially-unchanged metrics. `lowerIsBetter=true` flips the color mapping
 * (up = bad, down = good) for metrics like EWT, bunching, and excess-trip-time
 * where a higher value means worse service. The arrow direction always reflects
 * the raw sign of the delta — only the color encodes "good vs bad" — so a
 * tooltip-readable "+5s" EWT delta still points up while the arrow color
 * signals "worse."
 */
function DeltaIndicator({
  delta,
  format,
  flatThreshold = 0.5,
  title,
  lowerIsBetter = false,
}) {
  if (delta == null) return null
  let arrow = '→'
  let color = '#64748b'
  const goodColor = '#0E8A6F'
  const badColor = '#C8102E'
  if (delta > flatThreshold) {
    arrow = '▲'
    color = lowerIsBetter ? badColor : goodColor
  } else if (delta < -flatThreshold) {
    arrow = '▼'
    color = lowerIsBetter ? goodColor : badColor
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
 * Custom dot renderer for the Sparkline component.
 *
 * Renders a grey dot for partial-collection days (data_quality='partial')
 * with a tooltip-readable title attribute. Regular days use a small dot
 * only when it is the sole data point in the series (standard recharts
 * behaviour). Partial-day dots are always rendered even when there are
 * multiple points so the gap is visually explained rather than silent.
 *
 * Props come from recharts' internal dot injection:
 *   cx, cy    — SVG coordinates
 *   payload   — the data row (including `_partial` and `_coveragePct`)
 *   isSingle  — true when the series has only one valid complete point
 */
function SparklineDot({ cx, cy, payload, isSingle }) {
  if (payload && payload._partial) {
    const pct = payload._coveragePct != null
      ? `${Math.round(payload._coveragePct * 100)}%`
      : 'unknown'
    return (
      <circle
        cx={cx}
        cy={cy}
        r={3}
        fill="#94a3b8"
        stroke="white"
        strokeWidth={1}
        style={{ cursor: 'default' }}
      >
        <title>{`Partial collection — ${pct} coverage`}</title>
      </circle>
    )
  }
  // Render a dot when it's the only complete data point in the series.
  if (isSingle) {
    return <circle cx={cx} cy={cy} r={2} fill="#64748b" />
  }
  return null
}

/**
 * Mini 30-day sparkline rendered with recharts.
 *
 * `data` is an array of `{ date, value, data_quality?, coverage_pct? }` rows.
 * Rows where `data_quality === 'partial'` are rendered as grey dots with a
 * hover title "Partial collection — X% coverage"; the line does not connect
 * to them so the gap is visible. Rows with `value == null` and no data_quality
 * info are dropped entirely. Strips axes / grid / legend for compact cards.
 *
 * If only one complete point survives, recharts won't draw a line — fall
 * back to a single dot so the user still sees the measurement.
 */
function Sparkline({ data, color, valueFormat, height = 60 }) {
  // Separate partial rows from complete rows. Partial rows are kept in the
  // dataset with their value (for dot placement) but will be styled grey.
  // We set `_partial` and `_coveragePct` as synthetic fields so the custom
  // dot renderer can read them from `payload`.
  const augmented = (data || []).map((row) => {
    const isPartial = row.data_quality === 'partial'
    return {
      ...row,
      // Null out the value for partial days so recharts draws a gap in the
      // line (connectNulls=false) but keeps the row for the dot renderer.
      value: isPartial ? null : row.value,
      _partial: isPartial,
      _partialValue: isPartial ? row.value : null,
      _coveragePct: row.coverage_pct ?? null,
    }
  })

  // For the "no trend data" guard: check whether any complete OR partial row
  // has a non-null value.
  const hasAnyData = augmented.some(
    (row) => row.value != null || (row._partial && row._partialValue != null),
  )

  if (!hasAnyData) {
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

  // Complete rows that have a value — used for single-point fallback logic.
  const completeCount = augmented.filter((row) => !row._partial && row.value != null).length
  const isSingle = completeCount === 1

  // For partial rows: recharts skips null-value points in the line but still
  // calls the custom dot renderer with cx/cy. We rely on that behaviour plus
  // a synthetic `_partial` field to draw grey dots at the right position.
  // However recharts does NOT call the dot renderer for null-value points by
  // default. Workaround: for partial rows, store the raw value in a second
  // key (`_partialValue`) and set `value` to null so the line doesn't connect,
  // then overlay a separate ReferenceDot per partial row. Using a separate
  // ReferenceDot is simpler and more reliable than fighting the dot renderer.

  // Collect partial rows that have a numeric value for overlay dots.
  const partialDots = augmented.filter(
    (row) => row._partial && row._partialValue != null,
  )

  // For partial-row y-positioning we need the full domain (complete rows
  // only drive the YAxis domain, so a partial value that falls outside the
  // complete range would clip). Use 'dataMin - 5%' / 'dataMax + 5%' padding.
  const completeValues = augmented
    .filter((row) => !row._partial && row.value != null)
    .map((row) => row.value)
  const partialValues = partialDots.map((row) => row._partialValue)
  const allValues = [...completeValues, ...partialValues]
  const domainMin = allValues.length ? Math.min(...allValues) : 0
  const domainMax = allValues.length ? Math.max(...allValues) : 1
  const pad = (domainMax - domainMin) * 0.05 || 1

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={augmented} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
        <XAxis dataKey="date" hide />
        <YAxis
          hide
          domain={[domainMin - pad, domainMax + pad]}
        />
        <Tooltip
          content={({ active, payload, label }) => {
            if (!active || !payload || !payload.length) return null
            const row = payload[0]?.payload
            if (row?._partial) {
              const pct = row._coveragePct != null
                ? `${Math.round(row._coveragePct * 100)}%`
                : 'unknown'
              return (
                <div style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', background: 'white', border: '1px solid #e2e8f0' }}>
                  <div>{label}</div>
                  <div style={{ color: '#64748b' }}>Partial collection — {pct} coverage</div>
                </div>
              )
            }
            const val = row?.value
            return (
              <div style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', background: 'white', border: '1px solid #e2e8f0' }}>
                <div>{label}</div>
                {val != null && <div>{valueFormat(val)}</div>}
              </div>
            )
          }}
        />
        <Line
          type="monotone"
          dataKey="value"
          stroke={color}
          strokeWidth={1.75}
          dot={isSingle ? <SparklineDot isSingle /> : false}
          connectNulls={false}
          isAnimationActive={false}
        />
        {partialDots.map((row) => (
          <ReferenceDot
            key={row.date}
            x={row.date}
            y={row._partialValue}
            r={3}
            fill="#94a3b8"
            stroke="white"
            strokeWidth={1}
          />
        ))}
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
  // Per-route targets (NOTES-47). `null` (or missing) hides the target
  // pill on that card. OTP target is in 0-100, sd target in 0-1 (we
  // render *100 below). Excess-trip-time has no target — operators
  // commit to OTP/SD/EWT/bunching only.
  otpTarget,
  sdTarget,
  // The most-recent observed value for each KPI so we can color the
  // target pill (green = meets, red = misses). Same units as the
  // matching series rows.
  otpCurrent,
  sdCurrent,
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
            <TargetIndicator
              value={otpCurrent}
              target={otpTarget}
              higherIsBetter
              format={(t) => `${t.toFixed(0)}%`}
            />
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
            <TargetIndicator
              value={sdCurrent}
              target={sdTarget != null ? sdTarget * 100 : null}
              higherIsBetter
              format={(t) => `${t.toFixed(0)}%`}
            />
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
                lowerIsBetter
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

export { DeltaIndicator, Sparkline, TargetIndicator }
export default RouteTrend
