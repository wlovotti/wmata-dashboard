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
  return `${sign}${Math.abs(min).toFixed(1)} min/trip`
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
 * Renders a compact table of contributing routes sorted by trip volume.
 *
 * @param {{ routes: Array<{route_id: string, route_short_name: string|null, direction_id: number, mean_slip_sec: number, n_observations: number}> }} props
 * @returns {JSX.Element}
 */
function ContributingRoutesPanel({ routes }) {
  if (!routes || routes.length === 0) {
    return <p style={{ color: '#64748b', fontSize: '0.8rem' }}>No route breakdown available.</p>
  }
  return (
    <table
      style={{
        width: '100%',
        fontSize: '0.8rem',
        borderCollapse: 'collapse',
        marginTop: '0.5rem',
      }}
    >
      <thead>
        <tr style={{ borderBottom: '1px solid #334155' }}>
          <th style={{ textAlign: 'left', padding: '0.25rem 0.5rem', fontWeight: 600 }}>Route</th>
          <th style={{ textAlign: 'right', padding: '0.25rem 0.5rem', fontWeight: 600 }}>Dir</th>
          <th style={{ textAlign: 'right', padding: '0.25rem 0.5rem', fontWeight: 600 }}>
            Mean slip
          </th>
          <th style={{ textAlign: 'right', padding: '0.25rem 0.5rem', fontWeight: 600 }}>Trips</th>
        </tr>
      </thead>
      <tbody>
        {routes.map((r, idx) => (
          <tr
            key={`${r.route_id}-${r.direction_id}-${idx}`}
            style={{ borderBottom: '1px solid #1e293b' }}
          >
            <td style={{ padding: '0.25rem 0.5rem' }}>
              {r.route_short_name || r.route_id}
            </td>
            <td style={{ textAlign: 'right', padding: '0.25rem 0.5rem', color: '#94a3b8' }}>
              {r.direction_id}
            </td>
            <td
              style={{
                textAlign: 'right',
                padding: '0.25rem 0.5rem',
                color: r.mean_slip_sec >= 0 ? '#f87171' : '#4ade80',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {formatSignedSeconds(r.mean_slip_sec)}
            </td>
            <td
              style={{
                textAlign: 'right',
                padding: '0.25rem 0.5rem',
                color: '#94a3b8',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {r.n_observations.toLocaleString()}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/**
 * `/segments` page (NOTES-59). Cross-route segment diagnostic — ranked list
 * of stop-pairs that appear on ≥2 routes, ordered by total trip-volume-
 * weighted slip descending.  Infrastructure-investment candidates: a high
 * total weighted slip on a shared stop-pair indicates a chokepoint that
 * multiple routes traverse and where TSP / queue-jumps / dedicated lanes
 * would benefit the most passengers.
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

  const handleRowClick = (idx) => {
    setExpandedIdx(expandedIdx === idx ? null : idx)
  }

  return (
    <main>
      <div className="chart-container">
        <h2>Cross-route segment diagnostic</h2>
        <p className="drilldown-anchor">
          Stop-pairs traversed by ≥2 routes, ranked by total trip-volume-weighted
          slip. A high value means multiple routes lose time on the same segment —
          a shared infrastructure chokepoint that is a candidate for transit signal
          priority, queue-jumps, or dedicated lanes. Based on the last{' '}
          {lookbackDays} days of observed stop events.{' '}
          <strong>V1 note:</strong> uses stop-pair identity only — routes sharing
          the same stop IDs count as sharing a segment. Segments where routes use
          different stop IDs on the same street are not yet aggregated (deferred to
          NOTES-62 shape-aware corridor rollup).
        </p>

        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.75rem',
            alignItems: 'center',
            margin: '0.5rem 0 1rem',
            fontSize: '0.875rem',
          }}
        >
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <span style={{ opacity: 0.8 }}>Period:</span>
            <select
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
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <span style={{ opacity: 0.8 }}>Limit:</span>
            <select
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
          </label>
        </div>

        {loading && <p style={{ color: '#64748b' }}>Loading segment diagnostic…</p>}
        {error && <p style={{ color: '#64748b' }}>Unable to load segments: {error}</p>}

        {!loading && !error && segments.length === 0 && (
          <p style={{ color: '#64748b' }}>
            No cross-route stop-pairs found for this period. The diagnostic
            pipeline materializes <code>cross_route_segment_rollup</code>{' '}
            nightly via{' '}
            <code>pipelines/refresh_cross_route_segments.py</code> — if this
            is a fresh install, run that pipeline after{' '}
            <code>refresh_route_diagnostic_profile.py</code> has completed.
          </p>
        )}

        {!loading && !error && segments.length > 0 && (
          <>
            <p style={{ color: '#94a3b8', fontSize: '0.8rem', marginBottom: '0.5rem' }}>
              {segments.length} stop-pair{segments.length !== 1 ? 's' : ''} •{' '}
              {period !== 'all' ? PERIOD_LABELS[period] : 'All day'} • Click a row
              to expand the per-route breakdown.
            </p>
            <div className="table-responsive">
              <table>
                <thead>
                  <tr>
                    <th style={{ width: '2rem' }}>#</th>
                    <th>Segment (from → to)</th>
                    <th style={{ textAlign: 'right' }}>Routes</th>
                    <th style={{ textAlign: 'right' }}>Avg slip/trip</th>
                    <th style={{ textAlign: 'right' }}>Total obs.</th>
                    {period === 'all' && <th style={{ textAlign: 'right' }}>Peak period</th>}
                  </tr>
                </thead>
                <tbody>
                  {segments.map((seg, idx) => (
                    <>
                      <tr
                        key={`${seg.from_stop_id}-${seg.to_stop_id}`}
                        onClick={() => handleRowClick(idx)}
                        style={{
                          cursor: 'pointer',
                          backgroundColor: expandedIdx === idx ? '#1e293b' : undefined,
                        }}
                        title="Click to expand per-route breakdown"
                      >
                        <td style={{ color: '#64748b', fontSize: '0.8rem' }}>{idx + 1}</td>
                        <td>
                          <span style={{ fontWeight: 500 }}>
                            {seg.from_stop_name || seg.from_stop_id}
                          </span>
                          <span style={{ color: '#64748b', margin: '0 0.3rem' }}>→</span>
                          <span style={{ fontWeight: 500 }}>
                            {seg.to_stop_name || seg.to_stop_id}
                          </span>
                          <br />
                          <span style={{ color: '#64748b', fontSize: '0.75rem' }}>
                            {seg.route_short_names.join(', ')}
                          </span>
                        </td>
                        <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                          {seg.n_routes}
                        </td>
                        <td
                          style={{
                            textAlign: 'right',
                            fontVariantNumeric: 'tabular-nums',
                            color: seg.slip_min_per_trip >= 0 ? '#f87171' : '#4ade80',
                          }}
                        >
                          {formatMinPerTrip(seg.slip_min_per_trip)}
                        </td>
                        <td
                          style={{
                            textAlign: 'right',
                            fontVariantNumeric: 'tabular-nums',
                            color: '#94a3b8',
                          }}
                        >
                          {seg.n_total_observations.toLocaleString()}
                        </td>
                        {period === 'all' && (
                          <td
                            style={{
                              textAlign: 'right',
                              color: '#94a3b8',
                              fontSize: '0.8rem',
                            }}
                          >
                            {seg.peak_period ? PERIOD_LABELS[seg.peak_period] || seg.peak_period : '—'}
                          </td>
                        )}
                      </tr>
                      {expandedIdx === idx && (
                        <tr key={`${seg.from_stop_id}-${seg.to_stop_id}-detail`}>
                          <td />
                          <td
                            colSpan={period === 'all' ? 5 : 4}
                            style={{ padding: '0.5rem 0.5rem 1rem', background: '#0f172a' }}
                          >
                            <ContributingRoutesPanel routes={seg.contributing_routes} />
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </main>
  )
}

export default SegmentDiagnostic
