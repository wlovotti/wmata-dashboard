import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'

// NOTES-44: Marginal-bus EWT ranking page. Renders the API's per-(route,
// period) SWT-reduction estimates as a sortable / filterable table — "where
// would my next scheduled trip help most?"
//
// The page-level modeling-caveats banner is deliberately surfaced above the
// table (not in a tooltip) per the NOTES-44 framing: the absolute number is
// less reliable than the relative ranking, so the disclaimer should never
// be skimmable-past.

const DAY_TYPE_OPTIONS = [
  { value: '', label: 'Today (auto)' },
  { value: 'weekday', label: 'Weekday' },
  { value: 'saturday', label: 'Saturday' },
  { value: 'sunday', label: 'Sunday' },
]

const PERIOD_FILTER_OPTIONS = [
  { value: 'all', label: 'All periods' },
  { value: 'AM Peak (6-9)', label: 'AM Peak' },
  { value: 'Midday (9-15)', label: 'Midday' },
  { value: 'PM Peak (15-19)', label: 'PM Peak' },
  { value: 'Evening (19-24)', label: 'Evening' },
  { value: 'Night (0-6)', label: 'Night' },
]

// Drop everything from " (" onwards so the table cell stays compact.
function shortPeriodLabel(label) {
  const idx = label.indexOf(' (')
  return idx === -1 ? label : label.slice(0, idx)
}

function MarginalBus() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [dayType, setDayType] = useState('')
  const [periodFilter, setPeriodFilter] = useState('all')
  const [topN, setTopN] = useState(25)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const url = dayType
      ? `/api/marginal-ewt?day_type=${dayType}`
      : '/api/marginal-ewt'
    fetch(url)
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
  }, [dayType])

  const allRows = data?.rankings ?? []
  const filteredRows = periodFilter === 'all'
    ? allRows
    : allRows.filter((r) => r.time_period === periodFilter)
  const visibleRows = filteredRows.slice(0, topN)

  return (
    <main className="marginal-bus-page" style={{ maxWidth: '1400px', margin: '0 auto', padding: '2rem' }}>
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/" style={{ color: '#002F6C', textDecoration: 'none', fontSize: '0.875rem' }}>
          ← Back to routes
        </Link>
      </div>

      <h1 style={{ fontSize: '1.75rem', marginBottom: '0.5rem' }}>
        Where would the next bus help most?
      </h1>
      <p style={{ color: '#64748b', marginBottom: '1.5rem' }}>
        Per-(route, period) ranking of the SWT reduction predicted from
        adding one scheduled trip. Sorted by largest absolute reduction first.
      </p>

      <div
        className="modeling-disclaimer"
        style={{
          background: '#fef3c7',
          border: '1px solid #fcd34d',
          borderRadius: '0.5rem',
          padding: '1rem',
          marginBottom: '1.5rem',
          fontSize: '0.875rem',
          color: '#78350f',
        }}
      >
        <strong>Modeling caveats.</strong>{' '}
        Reduction is the closed-form{' '}
        <code style={{ background: 'rgba(0,0,0,0.06)', padding: '0 0.25rem', borderRadius: '0.25rem' }}>
          period_minutes / (2 N (N + 1))
        </code>{' '}
        — the scheduled-wait-time drop from adding one trip to a period that
        currently has N evenly-spaced trips, assuming riders arrive uniformly.
        Real AWT depends on observed variance and where in the schedule the
        new trip lands; the absolute reduction here is a uniform-arrival
        upper-bound proxy.{' '}
        <strong>The relative ranking — which (route, period) cells gain the
        most — is the defensible artifact.</strong>{' '}
        Trip counts come from the per-route trunk-stop hourly buckets in
        <code style={{ background: 'rgba(0,0,0,0.06)', padding: '0 0.25rem', borderRadius: '0.25rem' }}>
          route_service_profile
        </code>
        ; they may differ from "buses operating in this period" for trips
        crossing the period boundary.
      </div>

      <div
        className="filters"
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '1rem',
          alignItems: 'center',
          marginBottom: '1rem',
        }}
      >
        <label style={{ fontSize: '0.875rem', color: '#475569' }}>
          Day type:{' '}
          <select
            value={dayType}
            onChange={(e) => setDayType(e.target.value)}
            style={{ marginLeft: '0.25rem', padding: '0.25rem 0.5rem' }}
          >
            {DAY_TYPE_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </label>

        <label style={{ fontSize: '0.875rem', color: '#475569' }}>
          Period:{' '}
          <select
            value={periodFilter}
            onChange={(e) => setPeriodFilter(e.target.value)}
            style={{ marginLeft: '0.25rem', padding: '0.25rem 0.5rem' }}
          >
            {PERIOD_FILTER_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </label>

        <label style={{ fontSize: '0.875rem', color: '#475569' }}>
          Show top:{' '}
          <select
            value={topN}
            onChange={(e) => setTopN(Number(e.target.value))}
            style={{ marginLeft: '0.25rem', padding: '0.25rem 0.5rem' }}
          >
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={999}>All</option>
          </select>
        </label>

        {data?.day_type && (
          <span style={{ fontSize: '0.875rem', color: '#64748b', marginLeft: 'auto' }}>
            Day type: <strong>{data.day_type}</strong>
          </span>
        )}
      </div>

      {loading && <p style={{ color: '#64748b' }}>Loading…</p>}

      {error && !loading && (
        <p style={{ color: '#C8102E' }}>
          Unable to load marginal-bus ranking: {error}
        </p>
      )}

      {!loading && !error && filteredRows.length === 0 && (
        <p style={{ color: '#64748b' }}>
          No (route, period) cells with scheduled service for the selected filters.
        </p>
      )}

      {!loading && !error && filteredRows.length > 0 && (
        <>
          <p style={{ fontSize: '0.875rem', color: '#64748b', marginBottom: '0.5rem' }}>
            Showing {visibleRows.length} of {filteredRows.length} ranked cells.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
                background: 'white',
                boxShadow: '0 1px 3px rgba(0, 0, 0, 0.06)',
                borderRadius: '0.5rem',
                overflow: 'hidden',
              }}
            >
              <thead>
                <tr style={{ background: '#f1f5f9', textAlign: 'left' }}>
                  <th style={{ padding: '0.75rem' }}>#</th>
                  <th style={{ padding: '0.75rem' }}>Route</th>
                  <th style={{ padding: '0.75rem' }}>Period</th>
                  <th style={{ padding: '0.75rem', textAlign: 'right' }}>Current trips</th>
                  <th style={{ padding: '0.75rem', textAlign: 'right' }}>Current SWT</th>
                  <th style={{ padding: '0.75rem', textAlign: 'right' }}>
                    SWT reduction (one more trip)
                  </th>
                  <th style={{ padding: '0.75rem', textAlign: 'right' }}>% drop</th>
                </tr>
              </thead>
              <tbody>
                {visibleRows.map((row, idx) => (
                  <tr
                    key={`${row.route_id}-${row.time_period}`}
                    style={{ borderTop: '1px solid #e2e8f0' }}
                  >
                    <td style={{ padding: '0.75rem', color: '#94a3b8' }}>{idx + 1}</td>
                    <td style={{ padding: '0.75rem' }}>
                      <Link
                        to={`/route/${row.route_id}`}
                        style={{ color: '#002F6C', textDecoration: 'none', fontWeight: 600 }}
                      >
                        {row.route_short_name || row.route_id}
                      </Link>
                      {row.route_long_name && (
                        <div style={{ fontSize: '0.75rem', color: '#64748b' }}>
                          {row.route_long_name}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: '0.75rem', color: '#475569' }}>
                      {shortPeriodLabel(row.time_period)}
                    </td>
                    <td style={{ padding: '0.75rem', textAlign: 'right' }}>
                      {row.current_trip_count}
                    </td>
                    <td style={{ padding: '0.75rem', textAlign: 'right' }}>
                      {row.current_swt_minutes != null
                        ? `${row.current_swt_minutes.toFixed(1)} min`
                        : '—'}
                    </td>
                    <td
                      style={{
                        padding: '0.75rem',
                        textAlign: 'right',
                        fontWeight: 600,
                        color: '#0E8A6F',
                      }}
                    >
                      {row.marginal_swt_reduction_minutes != null
                        ? `−${row.marginal_swt_reduction_minutes.toFixed(2)} min`
                        : '—'}
                    </td>
                    <td style={{ padding: '0.75rem', textAlign: 'right', color: '#64748b' }}>
                      {row.marginal_swt_reduction_pct != null
                        ? `${(row.marginal_swt_reduction_pct * 100).toFixed(1)}%`
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p style={{ fontSize: '0.75rem', color: '#94a3b8', marginTop: '1rem' }}>
            <strong>SWT</strong> = scheduled wait time = half the headway.{' '}
            <strong>Reduction</strong> = closed-form drop from N → N+1 trips
            in the period. <strong>% drop</strong> = reduction / current SWT
            = 1 / (N+1).
          </p>
        </>
      )}
    </main>
  )
}

export default MarginalBus
