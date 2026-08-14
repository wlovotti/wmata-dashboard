import { useEffect, useState } from 'react'
import useMultiFetch from '../hooks/useMultiFetch'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { formatContribMetricValue } from '../utils/formatters'
import { latestNonNull } from '../utils/heroSummary'
import OverviewHero from './OverviewHero'
import MoversPanel from './MoversPanel'
import SystemMap from './SystemMap'
import SystemTrend from './SystemTrend'

// Metric options for the "Biggest drags" table. Same 4-entry list as
// RouteList — kept inline per the existing convention.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const CONTRIB_TOP_N = 5

// One page-level fan-out for the four trend payloads; SystemTrend and the
// hero both read from this single fetch (props down — NOTES-84 data flow).
const OVERVIEW_TREND_URLS = [
  '/api/system/trend?metric=otp&days=30',
  '/api/system/trend?metric=service_delivered&days=30',
  '/api/system/trend?metric=ewt&days=30',
  '/api/system/trend?metric=bunching&days=30',
]

/**
 * Overview landing page, rebuilt as an editorial stack (NOTES-84):
 *
 *   1. OverviewHero  — big-number verdict + compare teaser (absorbs the
 *                      retired HealthPulse banner)
 *   2. Fold          — SystemMap + MoversPanel side by side ("getting
 *                      worse" and "where" promoted to the top fold)
 *   3. SystemTrend   — 7-day-smoothed trend cards, daily points ghosted
 *   4. Biggest drags — the contributors table, demoted to the bottom
 *
 * One page-level fetch each for /api/routes and the trend fan-out, passed
 * down as props; the off-target panel moved to the Targets page.
 */
function Overview() {
  const navigate = useNavigate()
  const [scorecard, setScorecard] = useState(null)
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/routes')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setScorecard(json)
      })
      .catch(() => {
        // Hero and movers degrade gracefully without the scorecard.
      })
    return () => {
      cancelled = true
    }
  }, [])

  const {
    data: rawSystemTrendData,
    loading: trendLoading,
    error: trendError,
  } = useMultiFetch(OVERVIEW_TREND_URLS, ([otp, sd, ewt, bun]) => ({
    otp,
    service_delivered: sd,
    ewt,
    bunching: bun,
  }))
  const systemTrendData = rawSystemTrendData ?? null

  useEffect(() => {
    let cancelled = false
    setContribLoading(true)
    setContribError(null)
    fetch(`/api/routes/contributors?metric=${contribMetric}&days=30`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setContribData(json)
          setContribLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setContribError(err.message || String(err))
          setContribLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [contribMetric])

  // The 4-entry worst-of-four input for the hero — same construction the
  // retired HealthPulse used (percent-scaled fractions, trend targets).
  const systemMetrics = [
    {
      key: 'otp',
      label: 'OTP',
      higherIsBetter: true,
      current: latestNonNull(systemTrendData?.otp?.trend_data, 'otp_percentage'),
      target: systemTrendData?.otp?.target_value ?? null,
    },
    {
      key: 'service_delivered',
      label: 'Service Delivered',
      higherIsBetter: true,
      current: (() => {
        const v = latestNonNull(
          systemTrendData?.service_delivered?.trend_data,
          'service_delivered_ratio',
        )
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.service_delivered?.target_value != null
          ? systemTrendData.service_delivered.target_value * 100
          : null,
    },
    {
      key: 'ewt',
      label: 'EWT',
      higherIsBetter: false,
      current: latestNonNull(systemTrendData?.ewt?.trend_data, 'ewt_seconds'),
      target: systemTrendData?.ewt?.target_value ?? null,
    },
    {
      key: 'bunching',
      label: 'Bunching',
      higherIsBetter: false,
      current: (() => {
        const v = latestNonNull(systemTrendData?.bunching?.trend_data, 'bunching_rate')
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.bunching?.target_value != null
          ? systemTrendData.bunching.target_value * 100
          : null,
    },
  ]

  // Daily OTP series for the hero's week-over-week math.
  const otpSeries = (systemTrendData?.otp?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.otp_percentage,
    data_quality: row.data_quality,
  }))

  const visibleContributors = (contribData?.contributors ?? []).slice(0, CONTRIB_TOP_N)

  return (
    <main>
      <OverviewHero
        systemMetrics={systemMetrics}
        scorecardRoutes={scorecard?.routes ?? null}
        otpSeries={otpSeries}
      />

      {/* "Where is it going badly" fold: system map + movers side by side. */}
      <div className="overview-fold overview-fold-with-map">
        <SystemMap scorecardRoutes={scorecard?.routes ?? null} />
        <MoversPanel routes={scorecard?.routes ?? null} />
      </div>

      <SystemTrend trendData={systemTrendData} loading={trendLoading} error={trendError} />

      <div className="table-container">
        <h2>Biggest drags</h2>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Top {CONTRIB_TOP_N} routes ranked by their contribution to system
          underperformance — the routes whose attention would move the
          system the most.
        </p>
        <div className="filters" style={{ marginBottom: '0.75rem' }}>
          <div>
            <label htmlFor="overview-contrib-metric" style={{ marginRight: '0.5rem' }}>
              Metric:
            </label>
            <select
              id="overview-contrib-metric"
              value={contribMetric}
              onChange={(e) => setContribMetric(e.target.value)}
            >
              {CONTRIB_METRICS.map((m) => (
                <option key={m.key} value={m.key}>
                  {m.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {contribError && (
          <p style={{ color: 'var(--color-muted)', padding: '0 1.5rem 1rem' }}>
            Unable to load contributors: {contribError}
          </p>
        )}

        {contribLoading ? (
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading contributors...</p>
          </div>
        ) : contribData == null ? null : visibleContributors.length === 0 ? (
          <p style={{ padding: '0 1.5rem 1rem' }}>
            No routes have enough data to score contribution for this metric yet.
          </p>
        ) : (
          <table className="routes-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Route</th>
                <th>Name</th>
                <th>Route value</th>
                <th title="Per-route target if configured, otherwise system 30-day baseline">
                  Reference
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleContributors.map((c, idx) => (
                <tr
                  key={c.route_id}
                  onClick={() => navigate(`/route/${c.route_id}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <td>{idx + 1}</td>
                  <td className="route-id">
                    <span
                      className="route-badge"
                      style={{ backgroundColor: badgeColor(null, true) }}
                    >
                      {c.route_short_name || c.route_id}
                    </span>
                  </td>
                  <td className="route-name">{c.route_long_name || 'N/A'}</td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.route_value)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.reference_value ?? c.baseline_value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ padding: '1rem 1.5rem 1.5rem' }}>
          <Link to="/routes" className="see-all-link">
            See all routes →
          </Link>
        </div>
      </div>
    </main>
  )
}

export default Overview
