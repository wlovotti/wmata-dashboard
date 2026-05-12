import { useState, useEffect, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import RouteMap from './RouteMap'
import PeriodDrilldown from './PeriodDrilldown'
import RecentRuns from './RecentRuns'
import BlockList from './BlockList'
import RouteTrend, { computeWindowDelta, DeltaIndicator } from './RouteTrend'
import StopDiagnostic from './StopDiagnostic'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'

// Day-type / time-period filter options (NOTES-41). Keys must match the API's
// accepted values (src/time_periods.py: VALID_DAY_TYPES / VALID_PERIOD_KEYS).
const DAY_TYPE_OPTIONS = [
  { key: 'all', label: 'All days' },
  { key: 'weekday', label: 'Weekday' },
  { key: 'saturday', label: 'Saturday' },
  { key: 'sunday', label: 'Sunday' },
]
const PERIOD_OPTIONS = [
  { key: 'all', label: 'All hours' },
  { key: 'am_peak', label: 'AM Peak (6-10am)' },
  { key: 'midday', label: 'Midday (10am-3pm)' },
  { key: 'pm_peak', label: 'PM Peak (3-7pm)' },
  { key: 'evening', label: 'Evening (7-10pm)' },
  { key: 'late', label: 'Late (10pm-6am)' },
]

function _labelFor(options, key) {
  return options.find((o) => o.key === key)?.label || key
}

function RouteDetail() {
  const { routeId } = useParams()
  const navigate = useNavigate()
  const [routeData, setRouteData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Filter state (NOTES-41). Refetch the scorecard + trend whenever either
  // changes. Defaults to no filter so the URL-less (`/routes/:routeId`)
  // initial load preserves the unfiltered cached path.
  const [dayType, setDayType] = useState('all')
  const [period, setPeriod] = useState('all')

  // Recent runs vs Blocks tab (NOTES-45). 'runs' is the default; the user
  // switches to 'blocks' to see the per-vehicle chained-trip view that
  // surfaces cascade lateness.
  const [trailingTab, setTrailingTab] = useState('runs')

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
    const params = new URLSearchParams()
    if (dayType !== 'all') params.set('day_type', dayType)
    if (period !== 'all') params.set('period', period)
    const qs = params.toString()
    const url = `/api/routes/${routeId}${qs ? `?${qs}` : ''}`
    fetch(url)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .then(data => {
        setRouteData(data)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId, dayType, period])

  useEffect(() => {
    let cancelled = false
    setTrendLoading(true)
    setTrendError(null)
    // Build filter querystring fragment shared across the three trend fetches.
    // Period is honored only for the OTP trend (the others are trip-level
    // / daily aggregates that don't decompose by hour); pass it on every
    // call anyway — the API silently ignores it for non-otp metrics.
    const filterParams = []
    if (dayType !== 'all') filterParams.push(`day_type=${encodeURIComponent(dayType)}`)
    if (period !== 'all') filterParams.push(`period=${encodeURIComponent(period)}`)
    const filterQs = filterParams.length ? `&${filterParams.join('&')}` : ''
    Promise.all([
      fetch(`/api/routes/${routeId}/trend?metric=otp&days=30${filterQs}`).then((res) =>
        res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`),
      ),
      fetch(`/api/routes/${routeId}/trend?metric=service_delivered&days=30${filterQs}`).then(
        (res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)),
      ),
      fetch(`/api/routes/${routeId}/trend?metric=excess_trip_time&days=30${filterQs}`).then(
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
  }, [routeId, dayType, period])

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

  // Active-filter chip (NOTES-41) — only shown when at least one filter is
  // non-default. Keeps the unfiltered view chrome-free.
  const filterActive = dayType !== 'all' || period !== 'all'
  const filterChipText = filterActive
    ? `Filter: ${[
        dayType !== 'all' ? _labelFor(DAY_TYPE_OPTIONS, dayType) : null,
        period !== 'all' ? _labelFor(PERIOD_OPTIONS, period) : null,
      ]
        .filter(Boolean)
        .join(' / ')}`
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
            {filterChipText && (
              <span
                className="filter-chip"
                style={{
                  marginLeft: '0.75rem',
                  padding: '0.2rem 0.55rem',
                  fontSize: '0.75rem',
                  borderRadius: '999px',
                  background: 'rgba(0, 100, 200, 0.15)',
                  color: '#0a4a8c',
                  fontWeight: 500,
                  verticalAlign: 'middle',
                }}
                title="Active KPI filter — clear to see all data"
              >
                {filterChipText}
              </span>
            )}
          </h1>
        </div>
      </div>

      <div
        className="route-filter-bar"
        style={{
          display: 'flex',
          gap: '0.75rem',
          alignItems: 'center',
          margin: '0.5rem 0 1rem',
          fontSize: '0.875rem',
        }}
      >
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          <span style={{ opacity: 0.8 }}>Day:</span>
          <select
            value={dayType}
            onChange={(e) => setDayType(e.target.value)}
            aria-label="Day-type filter"
          >
            {DAY_TYPE_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          <span style={{ opacity: 0.8 }}>Time:</span>
          <select
            value={period}
            onChange={(e) => setPeriod(e.target.value)}
            aria-label="Time-period filter"
          >
            {PERIOD_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        {filterActive && (
          <button
            type="button"
            onClick={() => {
              setDayType('all')
              setPeriod('all')
            }}
            style={{
              padding: '0.2rem 0.55rem',
              fontSize: '0.75rem',
              border: '1px solid rgba(0,0,0,0.15)',
              background: 'transparent',
              cursor: 'pointer',
              borderRadius: '4px',
            }}
          >
            Clear filter
          </button>
        )}
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

      {hasMetrics && (
        <StopDiagnostic routeId={routeId} dayType={dayType} period={period} />
      )}

      {hasMetrics && (
        <PeriodDrilldown routeId={routeId} dayType={dayType} period={period} />
      )}

      <div
        style={{
          display: 'flex',
          gap: '0.5rem',
          margin: '1rem 0 0.5rem',
        }}
      >
        <button
          type="button"
          onClick={() => setTrailingTab('runs')}
          className={trailingTab === 'runs' ? 'route-tab-active' : 'route-tab'}
          style={{
            padding: '0.4rem 0.9rem',
            border: '1px solid #cbd5e1',
            background: trailingTab === 'runs' ? '#002F6C' : 'white',
            color: trailingTab === 'runs' ? 'white' : '#1e293b',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '0.875rem',
            fontWeight: 500,
          }}
        >
          Recent runs
        </button>
        <button
          type="button"
          onClick={() => setTrailingTab('blocks')}
          className={trailingTab === 'blocks' ? 'route-tab-active' : 'route-tab'}
          style={{
            padding: '0.4rem 0.9rem',
            border: '1px solid #cbd5e1',
            background: trailingTab === 'blocks' ? '#002F6C' : 'white',
            color: trailingTab === 'blocks' ? 'white' : '#1e293b',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '0.875rem',
            fontWeight: 500,
          }}
        >
          Blocks
        </button>
      </div>

      {trailingTab === 'runs' ? (
        <RecentRuns routeId={routeId} />
      ) : (
        <BlockList routeId={routeId} />
      )}

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
