import { useEffect, useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import SystemTrend from './SystemTrend'

// Available metrics for the "Where to look" contributors panel. Mirrors the
// constant in RouteList.jsx — kept inline here to avoid threading a shared
// module just for a 4-row constant. If/when a third surface needs the same
// list, lift to a shared module.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const CONTRIB_TOP_N = 5

/**
 * Format a metric value for the contributors panel — same units mapping as
 * RouteList's contributors table so the rows read identically across surfaces.
 */
function formatContribMetricValue(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${Math.round(value)}%`
  if (metric === 'service_delivered') return `${Math.round(value * 100)}%`
  if (metric === 'ewt') return `${Math.round(value)}s`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

/**
 * Pull the most recent non-null value from a `trend_data` series. Used to
 * derive a current system reading for the health pulse from whichever
 * service date last produced a metric — typically today, but falls back to
 * yesterday on early-morning hits before the daily pipeline has run.
 */
function latestNonNull(series, key) {
  if (!Array.isArray(series)) return null
  for (let i = series.length - 1; i >= 0; i--) {
    const value = series[i]?.[key]
    if (value != null) return value
  }
  return null
}

/**
 * Compute a single metric's "gap to target" expressed as a normalized
 * fraction so the worst metric across the four can be picked apples-to-apples.
 *
 * Returns a positive number when the current value is on the wrong side of
 * the target (worse), negative when on the right side (better), and null
 * when either side is missing. The magnitude is `(gap / target)` so a 10%
 * miss looks the same on OTP (5pp shy of 50% → 0.10) as on EWT (10s over
 * a 100s target → 0.10) — crude but fit for the worst-of-four pick.
 */
function gapFraction({ current, target, higherIsBetter }) {
  if (current == null || target == null || target === 0) return null
  const rawGap = higherIsBetter ? target - current : current - target
  return rawGap / Math.abs(target)
}

/**
 * Health-pulse banner renderer — system-wide single-line status. Tints the
 * banner red / yellow / green based on the worst-of-four normalized gap
 * across OTP, service-delivered, EWT, bunching. The "X routes below
 * target" piece counts routes whose any metric is on the wrong side of
 * its configured/inherited target. Period-over-period deltas are
 * intentionally omitted — NOTES-38 is deferred until ≥14 days of data
 * accumulate, and fabricating a delta would be worse than showing only
 * the level.
 */
function HealthPulse({ systemMetrics, scorecard }) {
  // Pick the worst metric — the one with the largest positive gapFraction.
  // Threshold mapping: < 0 (meeting target) → green, 0..0.1 → yellow,
  // > 0.1 → red. The 10% threshold is the same band the per-route
  // SpectrumBar uses (utils/spectrumBar.js), so visual semantics line up
  // across surfaces.
  let worstGap = null
  for (const m of systemMetrics) {
    const gap = gapFraction({
      current: m.current,
      target: m.target,
      higherIsBetter: m.higherIsBetter,
    })
    if (gap == null) continue
    if (worstGap == null || gap > worstGap) worstGap = gap
  }

  let tint = 'health-pulse-green'
  if (worstGap == null) tint = 'health-pulse-neutral'
  else if (worstGap > 0.1) tint = 'health-pulse-red'
  else if (worstGap > 0) tint = 'health-pulse-yellow'

  // Count routes that miss any of their four targets. A route counts if at
  // least one of its four metrics has both a current value and a target
  // and is on the wrong side of that target. Routes with no live data
  // (no derived stop_events for the window) are excluded — they're not
  // "below target," they're unmeasured.
  let routesBelowTarget = 0
  let routesEvaluated = 0
  for (const r of scorecard?.routes ?? []) {
    const targets = r.targets || {}
    const checks = [
      { current: r.otp_all_pct, target: targets.otp, higherIsBetter: true },
      {
        current:
          r.service_delivered_ratio != null ? r.service_delivered_ratio * 100 : null,
        target:
          targets.service_delivered != null ? targets.service_delivered * 100 : null,
        higherIsBetter: true,
      },
      { current: r.ewt_seconds, target: targets.ewt, higherIsBetter: false },
      {
        current: r.bunching_rate != null ? r.bunching_rate * 100 : null,
        target: targets.bunching != null ? targets.bunching * 100 : null,
        higherIsBetter: false,
      },
    ]
    let hasAnyMeasurement = false
    let isBelow = false
    for (const c of checks) {
      if (c.current == null || c.target == null) continue
      hasAnyMeasurement = true
      const gap = c.higherIsBetter ? c.target - c.current : c.current - c.target
      if (gap > 0) {
        isBelow = true
        break
      }
    }
    if (hasAnyMeasurement) {
      routesEvaluated += 1
      if (isBelow) routesBelowTarget += 1
    }
  }

  // Build the headline. OTP is the system's primary KPI by convention so
  // it's always shown when available; the other metrics only render in
  // the headline if OTP is missing (rare — the system trend backstops
  // even when today's pipeline hasn't run).
  const otpEntry = systemMetrics.find((m) => m.key === 'otp')
  const headlineParts = []
  if (otpEntry?.current != null) {
    headlineParts.push(`OTP ${Math.round(otpEntry.current)}%`)
  } else {
    // Fall back to the first non-null metric so the banner never reads as
    // a bare "0 routes below target."
    const fallback = systemMetrics.find((m) => m.current != null)
    if (fallback) {
      headlineParts.push(
        `${fallback.label} ${formatContribMetricValue(fallback.key, fallback.current)}`,
      )
    }
  }
  if (routesEvaluated > 0) {
    headlineParts.push(
      `${routesBelowTarget} of ${routesEvaluated} routes below target`,
    )
  }

  const text = headlineParts.length > 0 ? headlineParts.join(' · ') : 'System status unavailable'

  return (
    <div className={`health-pulse ${tint}`} role="status">
      <span className="health-pulse-text">{text}</span>
    </div>
  )
}

/**
 * Overview landing page (PR #105). Single screen that answers "are we OK
 * right now, and where should I look?" without parsing the full route
 * table. Composed of:
 *
 *   1. HealthPulse — single-line status banner tinted by worst-of-four gap
 *   2. SystemTrend — reused unchanged from RouteList for the 30-day view
 *   3. Where to look — top-5 contributors for the selected metric
 *   4. Footer — link out to /routes for the full table
 *
 * Pure aggregator: every data source is an existing endpoint
 * (`/api/system/trend`, `/api/routes`, `/api/routes/contributors`). No
 * backend changes for this page.
 */
function Overview() {
  const navigate = useNavigate()
  const [scorecard, setScorecard] = useState(null)
  const [systemTrendData, setSystemTrendData] = useState(null)
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)

  // Fetch scorecard once for the "X routes below target" count. The
  // /api/routes endpoint is cached server-side (60s TTL) so the cost is
  // amortized across this page and /routes.
  useEffect(() => {
    let cancelled = false
    fetch('/api/routes')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setScorecard(json)
      })
      .catch(() => {
        // Health pulse degrades gracefully when scorecard is unavailable —
        // the headline drops the routes count rather than blocking render.
      })
    return () => {
      cancelled = true
    }
  }, [])

  // Fetch the four system trend payloads to read the latest non-null value
  // and target_value for each metric. Same data <SystemTrend> reads, so
  // there's no extra network cost per metric beyond the parallel fan-out
  // that component already does — both calls hit the cached path.
  useEffect(() => {
    let cancelled = false
    Promise.all(
      ['otp', 'service_delivered', 'ewt', 'bunching'].map((metric) =>
        fetch(`/api/system/trend?metric=${metric}&days=30`).then((res) =>
          res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`),
        ),
      ),
    )
      .then(([otp, sd, ewt, bun]) => {
        if (cancelled) return
        setSystemTrendData({ otp, service_delivered: sd, ewt, bunching: bun })
      })
      .catch(() => {
        // Degrade silently — the SystemTrend component below will show
        // its own error if the fan-out fails identically there.
      })
    return () => {
      cancelled = true
    }
  }, [])

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

  // Build the system metrics array the HealthPulse uses. Each entry
  // captures the current reading (most recent non-null value from the
  // trend series), the target_value the trend endpoint already emits, and
  // the metric's higher-is-better orientation so the gap math works.
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

  const visibleContributors = (contribData?.contributors ?? []).slice(0, CONTRIB_TOP_N)

  return (
    <main>
      <HealthPulse systemMetrics={systemMetrics} scorecard={scorecard} />

      <SystemTrend />

      <div className="table-container">
        <h2>Where to look</h2>
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
          <p style={{ color: '#64748b' }}>Unable to load contributors: {contribError}</p>
        )}

        {contribLoading ? (
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading contributors...</p>
          </div>
        ) : contribData == null ? null : visibleContributors.length === 0 ? (
          <p>No routes have enough data to score contribution for this metric yet.</p>
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
                    {formatContribMetricValue(
                      contribMetric,
                      c.reference_value ?? c.baseline_value,
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ marginTop: '1rem' }}>
          <Link to="/routes" className="see-all-link">
            See all routes →
          </Link>
        </div>
      </div>
    </main>
  )
}

export default Overview
