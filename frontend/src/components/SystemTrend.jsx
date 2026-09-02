import { rollingMean } from '../utils/rollingMean'
import { computeSystemDelta } from '../utils/computeSystemDelta'
import { DeltaIndicator, Sparkline, TargetIndicator } from './RouteTrend'

const OTP_LINE_COLOR = '#002F6C'
const SD_LINE_COLOR = '#0E8A6F'
const EWT_LINE_COLOR = '#C8102E'
const BUN_LINE_COLOR = '#7C3AED'

/**
 * Top-of-page system trend strip: four sparklines (OTP, service-delivered,
 * EWT, bunching) with an N-vs-prior-N delta on each, N being the time-window
 * picker's `days` (NOTES-140; 30 by default).
 *
 * Closes NOTES-36. Reads from `/api/system/trend?metric=<m>&days=<days>`,
 * which returns the visible `days`-window plus a single scalar
 * `prior_window_value` summarizing the immediately preceding `days`-window
 * — option (b) from the NOTES-36 design choice (cleaner than transferring
 * twice the points just to compute a delta the server already knows).
 *
 * Uses the `Sparkline` and `DeltaIndicator` primitives factored out of
 * `RouteTrend.jsx` so the visual style stays consistent with the per-route
 * trend block on RouteDetail.
 *
 * NOTES-84: fetching moved up to Overview, which owns the single
 * `useMultiFetch` fan-out for all four metrics and passes the combined
 * payload down as `trendData` (plus `loading`/`error`). Each card's line is
 * now a 7-day trailing mean (`rollingMean`) of the raw daily series, with
 * the raw daily points ghosted underneath via `Sparkline`'s `ghostData` prop
 * — including partial-coverage days, which render as grey ghost dots.
 * Deltas/targets/meta lines are unaffected: they still compute from the raw
 * series since smoothing is presentation-only.
 *
 * @param {object} props
 * @param {{otp: object, service_delivered: object, ewt: object, bunching: object}|null} props.trendData
 *   combined trend payload keyed by metric, or null while loading.
 * @param {boolean} props.loading
 * @param {string|null} props.error
 * @param {number} [props.days] - Time-window picker (NOTES-140) fetch
 *   length, used only to label the N-vs-prior-N deltas below; defaults to
 *   30 for any caller that hasn't been updated to pass it. Below
 *   `computeSystemDelta`'s `SYSTEM_DELTA_MIN_DAYS` (10) the delta itself
 *   goes null and disappears rather than mislabeling — see PR #239.
 * @returns {JSX.Element}
 */
function SystemTrend({ trendData, loading, error, days = 30 }) {
  const data = trendData ?? { otp: null, service_delivered: null, ewt: null, bunching: null }

  if (loading) {
    return (
      <div className="chart-container">
        <h2>System trend — 7-day rolling, daily points ghosted</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>Loading…</p>
      </div>
    )
  }
  if (error) {
    return (
      <div className="chart-container">
        <h2>System trend — 7-day rolling, daily points ghosted</h2>
        <p style={{ color: '#64748b', fontSize: '0.85rem' }}>
          Trend unavailable: {error}
        </p>
      </div>
    )
  }

  // Map the four endpoint payloads to {date, value, data_quality, coverage_pct}
  // series, applying the metric-specific value transform. Service-delivered is
  // stored as 0..1 and rendered as percentage points. Bunching is stored as
  // 0..1 and rendered as percentage. EWT is in seconds. OTP is already %.
  //
  // data_quality and coverage_pct are forwarded so the Sparkline component can
  // render a distinct grey dot with a "Partial collection — X% coverage" hover
  // badge for partial-ingest days, replacing the silent gap the old guard
  // produced.
  const otpSeries = (data.otp?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.otp_percentage,
    data_quality: row.data_quality,
    coverage_pct: row.coverage_pct,
  }))
  const sdSeries = (data.service_delivered?.trend_data || []).map((row) => ({
    date: row.date,
    value:
      row.service_delivered_ratio != null ? row.service_delivered_ratio * 100 : null,
    data_quality: row.data_quality,
    coverage_pct: row.coverage_pct,
  }))
  const ewtSeries = (data.ewt?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.ewt_seconds,
    data_quality: row.data_quality,
    coverage_pct: row.coverage_pct,
  }))
  const bunSeries = (data.bunching?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.bunching_rate != null ? row.bunching_rate * 100 : null,
    data_quality: row.data_quality,
    coverage_pct: row.coverage_pct,
  }))

  // Smoothed lines (NOTES-84): 7-day trailing mean of each raw series. The
  // raw series is passed separately as `ghostData` so the daily points still
  // show, low-opacity, under the smoothed line.
  const otpSmoothed = rollingMean(otpSeries)
  const sdSmoothed = rollingMean(sdSeries)
  const ewtSmoothed = rollingMean(ewtSeries)
  const bunSmoothed = rollingMean(bunSeries)

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
    `Last ${days} days mean ${d.currentMean.toFixed(1)}% vs prior ${days}-day mean ${d.priorMean.toFixed(1)}%`
  const ewtDeltaTitle = (d) =>
    `Last ${days} days mean ${d.currentMean.toFixed(1)}s vs prior ${days}-day mean ${d.priorMean.toFixed(1)}s`
  const bunDeltaTitle = (d) =>
    `Last ${days} days mean ${d.currentMean.toFixed(2)}% vs prior ${days}-day mean ${d.priorMean.toFixed(2)}%`

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
      <h2>System trend — 7-day rolling, daily points ghosted</h2>
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
            data={otpSmoothed}
            ghostData={otpSeries}
            color={OTP_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
          {otpDelta && (
            <div className="route-trend-meta">
              {days}d: {otpDelta.currentMean.toFixed(1)}% · Prior {days}:{' '}
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
            data={sdSmoothed}
            ghostData={sdSeries}
            color={SD_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
          {sdDelta && (
            <div className="route-trend-meta">
              {days}d: {sdDelta.currentMean.toFixed(1)}% · Prior {days}:{' '}
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
                format={(d) => `${Math.abs(d).toFixed(0)}s`}
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
            data={ewtSmoothed}
            ghostData={ewtSeries}
            color={EWT_LINE_COLOR}
            valueFormat={(v) => `${Math.round(v)}s`}
          />
          {ewtDelta && (
            <div className="route-trend-meta">
              {days}d: {Math.round(ewtDelta.currentMean)}s · Prior {days}:{' '}
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
                format={(d) => `${Math.abs(d).toFixed(2)} pp`}
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
            data={bunSmoothed}
            ghostData={bunSeries}
            color={BUN_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(2)}%`}
          />
          {bunDelta && (
            <div className="route-trend-meta">
              {days}d: {bunDelta.currentMean.toFixed(2)}% · Prior {days}:{' '}
              {bunDelta.priorMean.toFixed(2)}%
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default SystemTrend
