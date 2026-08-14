import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { DeltaIndicator } from './RouteTrend'
import { formatContribMetricValue } from '../utils/formatters'

// Metric options — same 4-entry list as Overview/RouteList (kept inline per
// the existing convention comment in those files).
const MOVER_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const MOVERS_TOP_N = 7

// Below this many valid movers in the selected direction, a "ranking" is
// noise wearing a table costume — render a message instead (NOTES-44).
const MIN_VALID_MOVERS = 3

/** True when a larger raw delta is operationally good for `metric`. */
function isHigherBetter(metric) {
  return metric === 'otp' || metric === 'service_delivered'
}

/** Delta display formatter per metric, magnitudes only (sign is the arrow's job). */
function deltaFormatter(metric) {
  if (metric === 'otp') return (d) => `${Math.abs(d).toFixed(1)} pp`
  if (metric === 'service_delivered') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  if (metric === 'ewt') return (d) => `${Math.round(Math.abs(d))}s`
  if (metric === 'bunching') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  return (d) => String(Math.abs(d))
}

/** Current metric value from a scorecard row, in formatter-native units. */
function getCurrentValue(metric, row) {
  if (!row) return null
  if (metric === 'otp') return row.otp_all_pct ?? null
  if (metric === 'service_delivered') return row.service_delivered_ratio ?? null
  if (metric === 'ewt') return row.ewt_seconds ?? null
  if (metric === 'bunching') return row.bunching_rate ?? null
  return null
}

/**
 * "Getting worse" movers panel (NOTES-84) — WhatChangedPanel's degradations
 * list promoted to the top fold, with a "Getting better" toggle preserving
 * the improvements half. Ranks by |week-over-week delta| descending using
 * the `deltas` block on /api/routes rows; only `valid: true` deltas rank.
 *
 * @param {object} props
 * @param {Array<object>|null} props.routes - the `routes` array from
 *   /api/routes, or null while loading.
 * @returns {JSX.Element}
 */
function MoversPanel({ routes }) {
  const navigate = useNavigate()
  const [metric, setMetric] = useState('otp')
  const [direction, setDirection] = useState('worse')

  const movers = (() => {
    if (!routes) return []
    const higherBetter = isHigherBetter(metric)
    const wantImproving = direction === 'better'
    const rows = []
    for (const r of routes) {
      const delta = r.deltas?.[metric]
      if (!delta || !delta.valid || delta.value == null) continue
      // Zero deltas are excluded from both directions — a route that didn't
      // move isn't "worse" or "better," and letting it count toward the
      // 3-mover floor would understate how thin the ranking really is.
      // Sub-noise magnitude floors (e.g. treating |delta| < X as flat too)
      // are a tracked follow-up (NOTES-121), not handled here.
      if (delta.value === 0) continue
      const isImprovement = higherBetter ? delta.value > 0 : delta.value < 0
      if (isImprovement !== wantImproving) continue
      rows.push({
        routeId: r.route_id,
        routeShortName: r.route_name,
        routeLongName: r.route_long_name,
        currentValue: getCurrentValue(metric, r),
        deltaValue: delta.value,
        absDelta: Math.abs(delta.value),
        currentN: delta.current_n,
        priorN: delta.prior_n,
      })
    }
    rows.sort((a, b) => b.absDelta - a.absDelta)
    return rows.slice(0, MOVERS_TOP_N)
  })()

  const metricLabel = MOVER_METRICS.find((m) => m.key === metric)?.label ?? metric
  const fmt = deltaFormatter(metric)
  const lowerIsBetter = !isHigherBetter(metric)

  return (
    <div className="table-container movers-panel">
      <div className="movers-panel-header">
        <h2>{direction === 'worse' ? 'Getting worse' : 'Getting better'}</h2>
        <button
          type="button"
          className="movers-panel-toggle"
          onClick={() => setDirection((d) => (d === 'worse' ? 'better' : 'worse'))}
        >
          {direction === 'worse' ? 'Getting better →' : '← Getting worse'}
        </button>
      </div>
      <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
        Routes whose {metricLabel} moved most vs the prior 7-day window.
      </p>
      <div className="filters" style={{ marginBottom: '0.75rem' }}>
        <div>
          <label htmlFor="movers-metric" style={{ marginRight: '0.5rem' }}>
            Metric:
          </label>
          <select id="movers-metric" value={metric} onChange={(e) => setMetric(e.target.value)}>
            {MOVER_METRICS.map((m) => (
              <option key={m.key} value={m.key}>
                {m.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {routes == null ? null : movers.length < MIN_VALID_MOVERS ? (
        <p style={{ color: 'var(--color-muted)', padding: '0 1.5rem 1.5rem' }}>
          Not enough history this week to rank {direction === 'worse' ? 'worsening' : 'improving'}{' '}
          routes on {metricLabel} — fewer than {MIN_VALID_MOVERS} routes have valid
          week-over-week deltas.
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
            {movers.map((r) => (
              <tr
                key={r.routeId}
                onClick={() => navigate(`/route/${r.routeId}`)}
                style={{ cursor: 'pointer' }}
              >
                <td className="route-id">
                  <span className="route-badge" style={{ backgroundColor: badgeColor(null, true) }}>
                    {r.routeShortName || r.routeId}
                  </span>
                </td>
                <td className="route-name">{r.routeLongName || 'N/A'}</td>
                <td className="metric">{formatContribMetricValue(metric, r.currentValue)}</td>
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
  )
}

export default MoversPanel
