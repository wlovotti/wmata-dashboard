import { useEffect, useMemo, useState } from 'react'

/**
 * Format a signed seconds value as `±M:SS` for slip display.
 *
 * @param {number|null} sec - Seconds to format
 * @returns {string} Formatted string
 */
function formatSignedSeconds(sec) {
  if (sec == null) return '—'
  const sign = sec >= 0 ? '+' : '−'
  const abs = Math.abs(sec)
  const mins = Math.floor(abs / 60)
  const secs = Math.round(abs - mins * 60)
  return `${sign}${mins}:${secs.toString().padStart(2, '0')}`
}

/**
 * Format a minutes-per-trip value with one decimal.
 *
 * @param {number|null} min - Minutes to format
 * @returns {string} Formatted string
 */
function formatMinPerTrip(min) {
  if (min == null) return '—'
  const sign = min >= 0 ? '+' : '−'
  return `${sign}${Math.abs(min).toFixed(1)}`
}

/**
 * Format cumulative weighted slip seconds as a compact human-readable
 * "delay budget" string — minutes when small, hours when large.
 *
 * @param {number|null} sec - Total weighted slip seconds
 * @returns {string} Formatted string
 */
function formatTotalDelay(sec) {
  if (sec == null) return '—'
  const hours = sec / 3600
  if (hours >= 10) return `${hours.toFixed(0)}h`
  if (hours >= 1) return `${hours.toFixed(1)}h`
  const mins = sec / 60
  return `${mins.toFixed(0)}m`
}

const PERIOD_LABELS = {
  all: 'All day',
  am_peak: 'AM Peak (6–10am)',
  midday: 'Midday (10am–3pm)',
  pm_peak: 'PM Peak (3–7pm)',
  evening: 'Evening (7–10pm)',
  late: 'Late (10pm–6am)',
}

const PERIOD_OPTIONS = [
  { value: 'all', label: 'All day' },
  { value: 'am_peak', label: 'AM Peak (6-10am)' },
  { value: 'midday', label: 'Midday (10am-3pm)' },
  { value: 'pm_peak', label: 'PM Peak (3-7pm)' },
  { value: 'evening', label: 'Evening (7-10pm)' },
  { value: 'late', label: 'Late (10pm-6am)' },
]

/**
 * Inline per-route drilldown panel for one stop-pair segment.
 *
 * Renders a light card with a compact table of contributing routes
 * sorted by trip volume.
 *
 * @param {{ routes: Array<{route_id: string, route_short_name: string|null, direction_id: number, mean_slip_sec: number, n_observations: number}> }} props
 * @returns {JSX.Element}
 */
function ContributingRoutesPanel({ routes }) {
  if (!routes || routes.length === 0) {
    return (
      <div className="segment-drilldown-card">
        <p style={{ margin: 0, color: '#64748b', fontSize: '0.85rem' }}>
          No per-route breakdown available.
        </p>
      </div>
    )
  }
  return (
    <div className="segment-drilldown-card">
      <h4>Per-route breakdown</h4>
      <table className="segment-drilldown-table">
        <thead>
          <tr>
            <th className="col-route">Route</th>
            <th className="num" style={{ width: '4rem' }}>Dir</th>
            <th className="num">Mean slip</th>
            <th className="num">Trips</th>
          </tr>
        </thead>
        <tbody>
          {routes.map((r, idx) => (
            <tr key={`${r.route_id}-${r.direction_id}-${idx}`}>
              <td>
                <span className="segment-route-pill">
                  {r.route_short_name || r.route_id}
                </span>
              </td>
              <td className="num" style={{ color: '#64748b' }}>
                {r.direction_id}
              </td>
              <td
                className={`num ${r.mean_slip_sec >= 0 ? 'slip-positive' : 'slip-negative'}`}
              >
                {formatSignedSeconds(r.mean_slip_sec)}
              </td>
              <td className="num" style={{ color: '#475569' }}>
                {r.n_observations.toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/**
 * `/segments` page (NOTES-59). Cross-route segment diagnostic — ranked list
 * of stop-pairs that appear on ≥2 routes, ordered by cumulative
 * trip-volume-weighted slip descending.  Infrastructure-investment
 * candidates: a high total delay on a shared stop-pair indicates a
 * chokepoint that multiple routes traverse, where TSP / queue-jumps /
 * dedicated lanes would benefit the most passengers.
 *
 * V1 uses stop-pair identity matching only — routes sharing the same
 * (from_stop_id, to_stop_id) pair count as traversing the same segment.
 * Shape-aware corridor rollup (NOTES-62) is deferred so this ships without
 * geometric matching infrastructure.
 *
 * Click any row to expand the per-route drilldown panel.
 */
function SegmentDiagnostic() {
  const [period, setPeriod] = useState('all')
  const [limit, setLimit] = useState(100)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expandedIdx, setExpandedIdx] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setExpandedIdx(null)
    const params = new URLSearchParams()
    params.set('period', period)
    params.set('limit', String(limit))
    fetch(`/api/segments?${params.toString()}`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || String(err))
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [period, limit])

  const segments = useMemo(() => data?.segments || [], [data])
  const lookbackDays = data?.lookback_days ?? 30

  const maxImpact = useMemo(() => {
    if (segments.length === 0) return 0
    return Math.max(...segments.map((s) => s.total_weighted_slip_sec || 0))
  }, [segments])

  const handleRowClick = (idx) => {
    setExpandedIdx(expandedIdx === idx ? null : idx)
  }

  const showPeakColumn = period === 'all'
  const totalCols = showPeakColumn ? 7 : 6

  return (
    <main>
      <div className="chart-container">
        <h2>Cross-route segment diagnostic</h2>
        <p className="drilldown-anchor">
          Stop-pairs traversed by ≥2 routes, ranked by cumulative
          trip-volume-weighted delay over the last {lookbackDays} days. A high
          impact value means multiple routes lose time on the same segment —
          a shared infrastructure chokepoint, a candidate for transit signal
          priority, queue-jumps, or dedicated lanes.{' '}
          <strong>V1 note:</strong> stop-pair identity only. Segments where
          routes use different stop IDs on the same street are not yet
          aggregated (deferred to NOTES-62 shape-aware corridor rollup).
        </p>

        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '1rem',
            alignItems: 'center',
            margin: '0.5rem 0 1.25rem',
          }}
        >
          <div className="filter-group">
            <label htmlFor="seg-period">Period</label>
            <select
              id="seg-period"
              className="filter-select"
              value={period}
              onChange={(e) => setPeriod(e.target.value)}
              aria-label="Time-of-day period filter"
            >
              {PERIOD_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
          <div className="filter-group">
            <label htmlFor="seg-limit">Limit</label>
            <select
              id="seg-limit"
              className="filter-select"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              aria-label="Row limit"
            >
              {[50, 100, 200, 500].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>
          {!loading && !error && segments.length > 0 && (
            <div className="results-count">
              {segments.length} stop-pair{segments.length !== 1 ? 's' : ''} • Click a
              row to expand
            </div>
          )}
        </div>

        {loading && <p style={{ color: '#64748b' }}>Loading segment diagnostic…</p>}
        {error && <p style={{ color: '#991b1b' }}>Unable to load segments: {error}</p>}

        {!loading && !error && segments.length === 0 && (
          <p style={{ color: '#64748b' }}>
            No cross-route stop-pairs found for this period. The diagnostic
            pipeline materializes <code>cross_route_segment_rollup</code>{' '}
            nightly via{' '}
            <code>pipelines/refresh_cross_route_segments.py</code> — if this is
            a fresh install, run that pipeline after{' '}
            <code>refresh_route_diagnostic_profile.py</code> has completed.
          </p>
        )}

        {!loading && !error && segments.length > 0 && (
          <div className="table-responsive">
            <table className="routes-table segments-table">
              <thead>
                <tr>
                  <th className="col-rank" title="Rank by total delay">#</th>
                  <th className="col-segment">Segment</th>
                  <th className="col-routes" style={{ textAlign: 'right' }}>Routes</th>
                  <th className="col-impact">Impact (total delay)</th>
                  <th className="col-slip" style={{ textAlign: 'right' }}>
                    Slip/trip
                  </th>
                  <th className="col-obs" style={{ textAlign: 'right' }}>
                    Trips
                  </th>
                  {showPeakColumn && <th className="col-peak">Peak period</th>}
                </tr>
              </thead>
              <tbody>
                {segments.map((seg, idx) => {
                  const isExpanded = expandedIdx === idx
                  const impactPct =
                    maxImpact > 0 ? (seg.total_weighted_slip_sec / maxImpact) * 100 : 0
                  return (
                    <>
                      <tr
                        key={`${seg.from_stop_id}-${seg.to_stop_id}`}
                        className={`expand-row${isExpanded ? ' is-expanded' : ''}`}
                        onClick={() => handleRowClick(idx)}
                        aria-expanded={isExpanded}
                        title="Click to expand per-route breakdown"
                      >
                        <td className="col-rank">{idx + 1}</td>
                        <td className="col-segment">
                          <span className="chevron">▸</span>
                          <span className="segment-from-to">
                            {seg.from_stop_name || seg.from_stop_id}
                            <span className="segment-arrow">→</span>
                            {seg.to_stop_name || seg.to_stop_id}
                          </span>
                          <div className="segment-routes-line">
                            {seg.route_short_names.map((rsn) => (
                              <span key={rsn} className="segment-route-pill">
                                {rsn}
                              </span>
                            ))}
                          </div>
                        </td>
                        <td className="col-routes">{seg.n_routes}</td>
                        <td className="col-impact">
                          <div className="impact-bar">
                            <div className="impact-bar-track">
                              <div
                                className="impact-bar-fill"
                                style={{ width: `${impactPct}%` }}
                              />
                            </div>
                            <span className="impact-bar-label">
                              {formatTotalDelay(seg.total_weighted_slip_sec)}
                            </span>
                          </div>
                        </td>
                        <td
                          className={`col-slip ${
                            seg.slip_min_per_trip >= 0 ? 'slip-positive' : 'slip-negative'
                          }`}
                        >
                          {formatMinPerTrip(seg.slip_min_per_trip)}
                        </td>
                        <td className="col-obs" style={{ color: '#475569' }}>
                          {seg.n_total_observations.toLocaleString()}
                        </td>
                        {showPeakColumn && (
                          <td className="col-peak">
                            {seg.peak_period
                              ? PERIOD_LABELS[seg.peak_period] || seg.peak_period
                              : '—'}
                          </td>
                        )}
                      </tr>
                      {isExpanded && (
                        <tr
                          key={`${seg.from_stop_id}-${seg.to_stop_id}-detail`}
                          className="segment-drilldown-row"
                        >
                          <td colSpan={totalCols}>
                            <ContributingRoutesPanel routes={seg.contributing_routes} />
                          </td>
                        </tr>
                      )}
                    </>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </main>
  )
}

export default SegmentDiagnostic
