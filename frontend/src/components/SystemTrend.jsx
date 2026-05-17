import useMultiFetch from '../hooks/useMultiFetch'
import { computeSystemDelta } from '../utils/computeSystemDelta'
import { DeltaIndicator, Sparkline, TargetIndicator } from './RouteTrend'

const OTP_LINE_COLOR = '#002F6C'
const SD_LINE_COLOR = '#0E8A6F'
const EWT_LINE_COLOR = '#C8102E'
const BUN_LINE_COLOR = '#7C3AED'

// Static URL list — system trend never re-fetches on user interaction so
// this can live outside the component and is stable across renders.
const SYSTEM_TREND_URLS = [
  '/api/system/trend?metric=otp&days=30',
  '/api/system/trend?metric=service_delivered&days=30',
  '/api/system/trend?metric=ewt&days=30',
  '/api/system/trend?metric=bunching&days=30',
]

/**
 * Top-of-page system trend strip: four sparklines (OTP, service-delivered,
 * EWT, bunching) with a 30-vs-prior-30 delta on each.
 *
 * Closes NOTES-36. Reads from `/api/system/trend?metric=<m>&days=30`, which
 * returns the visible 30-day window plus a single scalar `prior_window_value`
 * — option (b) from the NOTES-36 design choice (cleaner than transferring
 * 60 days of points just to compute a delta the server already knows).
 *
 * Uses the `Sparkline` and `DeltaIndicator` primitives factored out of
 * `RouteTrend.jsx` so the visual style stays consistent with the per-route
 * trend block on RouteDetail.
 */
function SystemTrend() {
  const { data: rawData, loading, error } = useMultiFetch(
    SYSTEM_TREND_URLS,
    ([otp, sd, ewt, bun]) => ({
      otp,
      service_delivered: sd,
      ewt,
      bunching: bun,
    }),
  )

  // Use an empty sentinel object while rawData is null so downstream reads
  // are safe without repeated null-guards.
  const data = rawData ?? { otp: null, service_delivered: null, ewt: null, bunching: null }

  if (loading) {
    return (
      <div className="chart-container">
        <h2>30-Day System Trend</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>Loading…</p>
      </div>
    )
  }
  if (error) {
    return (
      <div className="chart-container">
        <h2>30-Day System Trend</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>
          Trend unavailable: {error}
        </p>
      </div>
    )
  }

  // Map the four endpoint payloads to {date, value} series, applying the
  // metric-specific value transform. Service-delivered is stored as 0..1
  // and rendered as percentage points to match RouteTrend's convention.
  // Bunching is stored as 0..1 and rendered as percentage. EWT is rendered
  // in seconds. OTP is already a percentage.
  const otpSeries = (data.otp?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.otp_percentage,
  }))
  const sdSeries = (data.service_delivered?.trend_data || []).map((row) => ({
    date: row.date,
    value:
      row.service_delivered_ratio != null ? row.service_delivered_ratio * 100 : null,
  }))
  const ewtSeries = (data.ewt?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.ewt_seconds,
  }))
  const bunSeries = (data.bunching?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.bunching_rate != null ? row.bunching_rate * 100 : null,
  }))

  const otpDelta = computeSystemDelta(otpSeries, data.otp?.prior_window_value)
  const sdDelta = computeSystemDelta(
    sdSeries,
    data.service_delivered?.prior_window_value != null
      ? data.service_delivered.prior_window_value * 100
      : null,
  )
  const ewtDelta = computeSystemDelta(ewtSeries, data.ewt?.prior_window_value)
  const bunDelta = computeSystemDelta(
    bunSeries,
    data.bunching?.prior_window_value != null
      ? data.bunching.prior_window_value * 100
      : null,
  )

  // For OTP and service-delivered, a positive delta is good (improving). For
  // EWT and bunching, a positive delta is bad (worse waits / more bunching).
  // DeltaIndicator's color logic is good=green/bad=red on positive — flip
  // the sign passed to the indicator for inverted-direction metrics so the
  // colors track the operational reading, not the raw arithmetic. (The
  // displayed magnitude and arrow direction still match the underlying
  // delta — only the color reverses.)
  const otpDeltaTitle = (d) =>
    `Last 30 days mean ${d.currentMean.toFixed(1)}% vs prior 30-day mean ${d.priorMean.toFixed(1)}%`
  const ewtDeltaTitle = (d) =>
    `Last 30 days mean ${d.currentMean.toFixed(1)}s vs prior 30-day mean ${d.priorMean.toFixed(1)}s`
  const bunDeltaTitle = (d) =>
    `Last 30 days mean ${d.currentMean.toFixed(2)}% vs prior 30-day mean ${d.priorMean.toFixed(2)}%`

  // System-default targets (NOTES-47). The trend endpoint emits
  // `target_value` next to `prior_window_value`; units follow each
  // metric's payload (OTP %, service_delivered 0-1, EWT seconds,
  // bunching 0-1). We compare against the current 30-day mean — the
  // pill says "✓ Target X" / "✗ Target X" based on whether the window
  // mean meets the commitment.
  const otpTarget = data.otp?.target_value
  const sdTargetPct =
    data.service_delivered?.target_value != null
      ? data.service_delivered.target_value * 100
      : null
  const ewtTarget = data.ewt?.target_value
  const bunTargetPct =
    data.bunching?.target_value != null
      ? data.bunching.target_value * 100
      : null

  return (
    <div className="chart-container">
      <h2>30-Day System Trend</h2>
      <div className="route-trend-grid">
        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">System OTP</span>
            {otpDelta && (
              <DeltaIndicator
                delta={otpDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
                title={otpDeltaTitle(otpDelta)}
              />
            )}
            <TargetIndicator
              value={otpDelta ? otpDelta.currentMean : null}
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
              30d: {otpDelta.currentMean.toFixed(1)}% · Prior 30:{' '}
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
                title={otpDeltaTitle(sdDelta)}
              />
            )}
            <TargetIndicator
              value={sdDelta ? sdDelta.currentMean : null}
              target={sdTargetPct}
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
              30d: {sdDelta.currentMean.toFixed(1)}% · Prior 30:{' '}
              {sdDelta.priorMean.toFixed(1)}%
            </div>
          )}
        </div>

        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">Excess Wait Time</span>
            {ewtDelta && (
              <DeltaIndicator
                delta={-ewtDelta.delta}
                format={(d) => `${(-d).toFixed(0)}s`}
                title={ewtDeltaTitle(ewtDelta)}
              />
            )}
            <TargetIndicator
              value={ewtDelta ? ewtDelta.currentMean : null}
              target={ewtTarget}
              higherIsBetter={false}
              format={(t) => `${(t / 60).toFixed(1)} min`}
            />
          </div>
          <Sparkline
            data={ewtSeries}
            color={EWT_LINE_COLOR}
            valueFormat={(v) => `${Math.round(v)}s`}
          />
          {ewtDelta && (
            <div className="route-trend-meta">
              30d: {Math.round(ewtDelta.currentMean)}s · Prior 30:{' '}
              {Math.round(ewtDelta.priorMean)}s
            </div>
          )}
        </div>

        <div className="route-trend-card">
          <div className="route-trend-header">
            <span className="route-trend-label">Bunching Rate</span>
            {bunDelta && (
              <DeltaIndicator
                delta={-bunDelta.delta}
                format={(d) => `${(-d).toFixed(2)} pp`}
                title={bunDeltaTitle(bunDelta)}
              />
            )}
            <TargetIndicator
              value={bunDelta ? bunDelta.currentMean : null}
              target={bunTargetPct}
              higherIsBetter={false}
              format={(t) => `${t.toFixed(1)}%`}
            />
          </div>
          <Sparkline
            data={bunSeries}
            color={BUN_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(2)}%`}
          />
          {bunDelta && (
            <div className="route-trend-meta">
              30d: {bunDelta.currentMean.toFixed(2)}% · Prior 30:{' '}
              {bunDelta.priorMean.toFixed(2)}%
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default SystemTrend
