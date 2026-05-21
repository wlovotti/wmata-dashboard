import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { DeltaIndicator } from './RouteTrend'
import { formatContribMetricValue } from '../utils/formatters'

// Metric options for the "What changed" panel selector. Mirrors CONTRIB_METRICS
// in Overview.jsx and RouteList.jsx — kept inline to avoid threading a shared
// module for a 4-entry constant.
const WHAT_CHANGED_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

// Default number of routes to show in each sub-list (improvements / degradations).
const WHAT_CHANGED_TOP_N = 7

/**
 * Return true when an increase in the raw delta value is "good" for the
 * given metric. OTP and service_delivered are higher-is-better; EWT and
 * bunching are lower-is-better.
 *
 * @param {string} metric
 * @returns {boolean}
 */
function isHigherBetter(metric) {
  return metric === 'otp' || metric === 'service_delivered'
}

/**
 * Format a delta value for display, in the metric's natural units.
 *
 * OTP: percentage points (raw value is already pp)
 * service_delivered: the wire value is a 0..1 ratio delta; scale to pp for
 *   readability.
 * ewt: raw seconds
 * bunching: the wire value is a 0..1 ratio delta; scale to pp.
 *
 * @param {string} metric
 * @returns {(delta: number) => string}
 */
function deltaFormatter(metric) {
  if (metric === 'otp') return (d) => `${Math.abs(d).toFixed(1)} pp`
  if (metric === 'service_delivered') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  if (metric === 'ewt') return (d) => `${Math.round(Math.abs(d))}s`
  if (metric === 'bunching') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  return (d) => String(Math.abs(d))
}

/**
 * "What changed" panel (PR #138).
 *
 * Reads the `deltas` block already embedded in each route row from
 * `/api/routes` (shape: `{value, valid, current_n, prior_n}` per metric:
 * otp, service_delivered, ewt, bunching) and ranks week-over-week movers.
 *
 * Props:
 *   routes  – the `routes` array from the `/api/routes` response, or null
 *             while loading. Passed in from Overview so the fetch is shared.
 *
 * Renders two sub-lists:
 *   Improvements  – routes where the delta moved in the favorable direction.
 *   Degradations  – routes where the delta moved in the unfavorable direction.
 *
 * Each row: route badge, long name, current value, delta arrow + magnitude.
 * Click navigates to the RouteDetail page.
 */
function WhatChangedPanel({ routes }) {
  const navigate = useNavigate()
  const [metric, setMetric] = useState('otp')

  // Build sorted mover lists for the selected metric.
  const { improvements, degradations } = (() => {
    if (!routes) return { improvements: [], degradations: [] }

    const higherBetter = isHigherBetter(metric)
    const rows = []

    for (const r of routes) {
      const delta = r.deltas?.[metric]
      if (!delta || !delta.valid || delta.value == null) continue

      // For higher-is-better metrics: positive delta = improvement.
      // For lower-is-better metrics: negative delta = improvement.
      const isImprovement = higherBetter ? delta.value > 0 : delta.value < 0

      rows.push({
        routeId: r.route_id,
        routeShortName: r.route_name,
        routeLongName: r.route_long_name,
        currentValue: getCurrentValue(metric, r),
        deltaValue: delta.value,
        absDelta: Math.abs(delta.value),
        isImprovement,
        currentN: delta.current_n,
        priorN: delta.prior_n,
      })
    }

    // Sort each list by absolute delta magnitude descending (biggest movers first).
    const sorted = [...rows].sort((a, b) => b.absDelta - a.absDelta)
    return {
      improvements: sorted.filter((r) => r.isImprovement).slice(0, WHAT_CHANGED_TOP_N),
      degradations: sorted.filter((r) => !r.isImprovement).slice(0, WHAT_CHANGED_TOP_N),
    }
  })()

  const metricLabel = WHAT_CHANGED_METRICS.find((m) => m.key === metric)?.label ?? metric
  const fmt = deltaFormatter(metric)
  const lowerIsBetter = !isHigherBetter(metric)

  const hasData = improvements.length > 0 || degradations.length > 0
  const noDataMessage = routes == null
    ? null
    : `No routes have valid week-over-week deltas for ${metricLabel} yet.`

  return (
    <div className="table-container">
      <h2>What changed</h2>
      <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
        Week-over-week movers: routes whose {metricLabel} changed most vs the
        prior 7-day window. Positive movement is in the green column;
        setbacks in the red column.
      </p>
      <div className="filters" style={{ marginBottom: '0.75rem' }}>
        <div>
          <label htmlFor="what-changed-metric" style={{ marginRight: '0.5rem' }}>
            Metric:
          </label>
          <select
            id="what-changed-metric"
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
          >
            {WHAT_CHANGED_METRICS.map((m) => (
              <option key={m.key} value={m.key}>
                {m.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {!hasData ? (
        <p style={{ color: '#64748b', padding: '0 1.5rem 1.5rem' }}>{noDataMessage}</p>
      ) : (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: '1.5rem',
            padding: '0 1.5rem 1.5rem',
          }}
        >
          {/* Improvements sub-list */}
          <div>
            <h3
              style={{
                fontSize: '0.9rem',
                fontWeight: 600,
                color: '#15803d',
                marginBottom: '0.5rem',
              }}
            >
              Improvements ({improvements.length})
            </h3>
            {improvements.length === 0 ? (
              <p style={{ color: '#64748b', fontSize: '0.85rem' }}>
                No improvements this week.
              </p>
            ) : (
              <table className="routes-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Name</th>
                    <th>{metricLabel}</th>
                    <th>Change</th>
                  </tr>
                </thead>
                <tbody>
                  {improvements.map((r) => (
                    <tr
                      key={r.routeId}
                      onClick={() => navigate(`/route/${r.routeId}`)}
                      style={{ cursor: 'pointer' }}
                    >
                      <td className="route-id">
                        <span
                          className="route-badge"
                          style={{ backgroundColor: badgeColor(null, true) }}
                        >
                          {r.routeShortName || r.routeId}
                        </span>
                      </td>
                      <td className="route-name">{r.routeLongName || 'N/A'}</td>
                      <td className="metric">
                        {formatContribMetricValue(metric, r.currentValue)}
                      </td>
                      <td className="metric">
                        <DeltaIndicator
                          delta={r.deltaValue}
                          format={fmt}
                          lowerIsBetter={lowerIsBetter}
                          title={`Last 7 days vs prior 7 days (${r.currentN}/${r.priorN} valid days)`}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Degradations sub-list */}
          <div>
            <h3
              style={{
                fontSize: '0.9rem',
                fontWeight: 600,
                color: '#b91c1c',
                marginBottom: '0.5rem',
              }}
            >
              Degradations ({degradations.length})
            </h3>
            {degradations.length === 0 ? (
              <p style={{ color: '#64748b', fontSize: '0.85rem' }}>
                No degradations this week.
              </p>
            ) : (
              <table className="routes-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Name</th>
                    <th>{metricLabel}</th>
                    <th>Change</th>
                  </tr>
                </thead>
                <tbody>
                  {degradations.map((r) => (
                    <tr
                      key={r.routeId}
                      onClick={() => navigate(`/route/${r.routeId}`)}
                      style={{ cursor: 'pointer' }}
                    >
                      <td className="route-id">
                        <span
                          className="route-badge"
                          style={{ backgroundColor: badgeColor(null, true) }}
                        >
                          {r.routeShortName || r.routeId}
                        </span>
                      </td>
                      <td className="route-name">{r.routeLongName || 'N/A'}</td>
                      <td className="metric">
                        {formatContribMetricValue(metric, r.currentValue)}
                      </td>
                      <td className="metric">
                        <DeltaIndicator
                          delta={r.deltaValue}
                          format={fmt}
                          lowerIsBetter={lowerIsBetter}
                          title={`Last 7 days vs prior 7 days (${r.currentN}/${r.priorN} valid days)`}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

/**
 * Extract the current metric value from a route scorecard row in the units
 * that `formatContribMetricValue` expects.
 *
 * @param {string} metric
 * @param {object} row  – a route entry from /api/routes response
 * @returns {number|null}
 */
function getCurrentValue(metric, row) {
  if (!row) return null
  if (metric === 'otp') return row.otp_all_pct ?? null
  if (metric === 'service_delivered') return row.service_delivered_ratio ?? null
  if (metric === 'ewt') return row.ewt_seconds ?? null
  if (metric === 'bunching') return row.bunching_rate ?? null
  return null
}

export default WhatChangedPanel
