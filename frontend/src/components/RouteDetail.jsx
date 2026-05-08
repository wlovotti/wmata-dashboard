import { useState, useEffect, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import RouteMap from './RouteMap'
import PeriodDrilldown from './PeriodDrilldown'
import RecentRuns from './RecentRuns'
import RouteTrend, { computeWindowDelta, DeltaIndicator } from './RouteTrend'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'

function RouteDetail() {
  const { routeId } = useParams()
  const navigate = useNavigate()
  const [routeData, setRouteData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Trend data is fetched here (rather than inside RouteTrend) so the same
  // 30-day series can drive both the sparkline block and the per-KPI-card
  // 7-vs-prior-7-day deltas above. Three fetches: OTP and excess_trip_time
  // come from route_metrics_daily, service_delivered is computed live per
  // service date (NOTES-37 / endpoint extension). NOTES-43 added the
  // excess_trip_time trend.
  const [otpTrend, setOtpTrend] = useState(null)
  const [sdTrend, setSdTrend] = useState(null)
  const [excessTrend, setExcessTrend] = useState(null)
  const [trendLoading, setTrendLoading] = useState(true)
  const [trendError, setTrendError] = useState(null)

  useEffect(() => {
    fetch(`/api/routes/${routeId}`)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .then(data => {
        setRouteData(data)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId])

  useEffect(() => {
    let cancelled = false
    setTrendLoading(true)
    setTrendError(null)
    Promise.all([
      fetch(`/api/routes/${routeId}/trend?metric=otp&days=30`).then((res) =>
        res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`),
      ),
      fetch(`/api/routes/${routeId}/trend?metric=service_delivered&days=30`).then(
        (res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)),
      ),
      fetch(`/api/routes/${routeId}/trend?metric=excess_trip_time&days=30`).then(
        (res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)),
      ),
    ])
      .then(([otp, sd, excess]) => {
        if (cancelled) return
        setOtpTrend(otp)
        setSdTrend(sd)
        setExcessTrend(excess)
        setTrendLoading(false)
      })
      .catch((err) => {
        if (cancelled) return
        setTrendError(err.message || err)
        setTrendLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [routeId])

  // Memoized {date, value} series + deltas. Service-delivered is stored as a
  // 0..1 ratio in the payload but rendered as percentage points on the card,
  // so multiply by 100 here once.
  //
  // The trend endpoint now emits one row per service date in the window
  // with `value: null` for days with no data (so the API caller can
  // distinguish "no observations" from a real zero). Drop those nulls
  // here so the sparkline only plots real points and `computeWindowDelta`
  // sees only valid data — its <3-valid-days suppression rule then
  // actually kicks in for thin-data routes.
  const otpSeries = useMemo(
    () =>
      (otpTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value: row.otp_percentage,
        }))
        .filter((row) => row.value != null),
    [otpTrend],
  )
  const sdSeries = useMemo(
    () =>
      (sdTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value:
            row.service_delivered_ratio != null
              ? row.service_delivered_ratio * 100
              : null,
        }))
        .filter((row) => row.value != null),
    [sdTrend],
  )
  // Excess trip time: % of trips with actual end-to-end duration above 110%
  // of scheduled. Already a percentage in the payload; just pass through.
  const excessSeries = useMemo(
    () =>
      (excessTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value: row.excess_trip_time_pct,
        }))
        .filter((row) => row.value != null),
    [excessTrend],
  )
  const otpDelta = useMemo(() => computeWindowDelta(otpSeries), [otpSeries])
  const sdDelta = useMemo(() => computeWindowDelta(sdSeries), [sdSeries])
  const excessDelta = useMemo(
    () => computeWindowDelta(excessSeries),
    [excessSeries],
  )

  if (loading) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate('/')} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading route details...</p>
        </div>
      </main>
    )
  }

  if (error || !routeData) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate('/')} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading route data:</strong> {error || 'Route not found'}
            <div className="error-actions">
              <button onClick={() => navigate('/')} className="retry-btn">
                Back to Routes
              </button>
            </div>
          </div>
        </div>
      </main>
    )
  }

  const hasMetrics = routeData.otp_all_pct != null
    || routeData.service_delivered_ratio != null
    || routeData.ewt_seconds != null
    || routeData.bunching_rate != null
    || routeData.excess_trip_time_pct != null

  // Subline for the excess-trip-time card: "median trip ran X min,
  // schedule Y min" so a GM can see whether the running-over-110% rate
  // reflects 30% over schedule on a long route or 1% over on a short
  // one. Both come from the freshest daily row inside the 7-day window
  // (NOTES-43, _excess_trip_time_fields in api/aggregations.py).
  const excessActualMin =
    routeData.excess_trip_time_median_actual_sec != null
      ? Math.round(routeData.excess_trip_time_median_actual_sec / 60)
      : null
  const excessSchedMin =
    routeData.excess_trip_time_median_scheduled_sec != null
      ? Math.round(routeData.excess_trip_time_median_scheduled_sec / 60)
      : null
  const excessOverSchedPct =
    routeData.excess_trip_time_median_actual_sec != null &&
    routeData.excess_trip_time_median_scheduled_sec != null &&
    routeData.excess_trip_time_median_scheduled_sec > 0
      ? Math.round(
          ((routeData.excess_trip_time_median_actual_sec -
            routeData.excess_trip_time_median_scheduled_sec) /
            routeData.excess_trip_time_median_scheduled_sec) *
            100,
        )
      : null

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate('/')} className="back-btn">
          ← Back to All Routes
        </button>
        <div className="route-title">
          <h1>
            <span
              className="route-badge-large"
              style={{ backgroundColor: badgeColor(routeData.frequency_class, hasMetrics) }}
              title={FREQUENCY_CLASS_LABELS[routeData.frequency_class] || ''}
            >
              {routeData.route_name}
            </span>
            {routeData.route_long_name}
          </h1>
        </div>
      </div>

      <div className="stats-summary">
        <div className="stat-card">
          <div className="stat-value">
            {routeData.otp_all_pct != null
              ? `${Math.round(routeData.otp_all_pct)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">
            On-Time Performance
            {otpDelta && (
              <DeltaIndicator
                delta={otpDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
                title={`7-day mean ${otpDelta.recentMean.toFixed(1)}% vs prior 7-day mean ${otpDelta.priorMean.toFixed(1)}%`}
              />
            )}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.service_delivered_ratio != null
              ? `${Math.round(routeData.service_delivered_ratio * 100)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">
            Service Delivered
            {sdDelta && (
              <DeltaIndicator
                delta={sdDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
                title={`7-day mean ${sdDelta.recentMean.toFixed(1)}% vs prior 7-day mean ${sdDelta.priorMean.toFixed(1)}%`}
              />
            )}
          </div>
          {routeData.service_delivered_scheduled != null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              ({routeData.service_delivered_delivered} of {routeData.service_delivered_scheduled} trips)
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{ fontSize: '1.5rem' }}>
            {routeData.otp_origin_pct != null
              ? `${Math.round(routeData.otp_origin_pct)}% / ${Math.round(routeData.otp_destination_pct ?? 0)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">OTP Origin / Destination</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.ewt_seconds != null
              ? `${Math.round(routeData.ewt_seconds)}`
              : 'N/A'}
            {routeData.ewt_seconds != null && (
              <span style={{ fontSize: '1.5rem' }}> sec</span>
            )}
            {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
              <span
                className="data-thin-badge"
                title={`Only ${Math.round(routeData.ewt_coverage_ratio * 100)}% of scheduled headways were observed`}
              >
                Thin
              </span>
            )}
          </div>
          <div className="stat-label">Excess Wait Time</div>
          {routeData.ewt_seconds == null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              (frequent service only)
            </div>
          )}
          {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
            <div className="data-thin-note">
              Trip-update coverage {Math.round(routeData.ewt_coverage_ratio * 100)}% — metric unreliable
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.bunching_rate != null
              ? `${(routeData.bunching_rate * 100).toFixed(1)}%`
              : 'N/A'}
            {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
              <span
                className="data-thin-badge"
                title={`Only ${Math.round(routeData.ewt_coverage_ratio * 100)}% of scheduled headways were observed`}
              >
                Thin
              </span>
            )}
          </div>
          <div className="stat-label">Bunching Rate</div>
          {routeData.bunching_total_headways != null && routeData.bunching_total_headways > 0 && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              ({routeData.bunching_count} of {routeData.bunching_total_headways} pairs)
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.excess_trip_time_pct != null
              ? `${Math.round(routeData.excess_trip_time_pct)}%`
              : 'N/A'}
            {excessDelta && (
              <DeltaIndicator
                delta={excessDelta.delta}
                format={(d) => `${d.toFixed(1)} pp`}
                title={`7-day mean ${excessDelta.recentMean.toFixed(1)}% vs prior 7-day mean ${excessDelta.priorMean.toFixed(1)}%`}
              />
            )}
          </div>
          <div className="stat-label">% of Trips Running Long</div>
          {excessActualMin != null && excessSchedMin != null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              median trip {excessActualMin} min, schedule {excessSchedMin} min
              {excessOverSchedPct != null && ` (${excessOverSchedPct >= 0 ? '+' : ''}${excessOverSchedPct}%)`}
            </div>
          )}
          {routeData.excess_trip_time_pct == null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              (no qualifying trips)
            </div>
          )}
        </div>
      </div>

      {hasMetrics && (
        <RouteTrend
          otpSeries={otpSeries}
          sdSeries={sdSeries}
          excessSeries={excessSeries}
          otpDelta={otpDelta}
          sdDelta={sdDelta}
          excessDelta={excessDelta}
          loading={trendLoading}
          error={trendError}
        />
      )}

      {hasMetrics && <PeriodDrilldown routeId={routeId} />}

      <RecentRuns routeId={routeId} />

      <div className="chart-container">
        <h2>Route Map</h2>
        <RouteMap routeId={routeId} />
      </div>

      {!hasMetrics && (
        <div className="no-data-message">
          <div className="no-data-icon">📊</div>
          <h2>No Performance Data Available</h2>
          <p>This route does not have enough data to calculate performance metrics for the latest service date.</p>
        </div>
      )}

      <div className="detail-info">
        <h3>Route Information</h3>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Route ID:</span>
            <span className="info-value">{routeData.route_id}</span>
          </div>
        </div>
      </div>
    </main>
  )
}

export default RouteDetail
