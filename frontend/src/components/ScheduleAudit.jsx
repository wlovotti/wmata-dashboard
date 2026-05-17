import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'

/**
 * Format signed seconds as `±M:SS` (minutes:seconds). Used for the per-row
 * mean slip column so an under-padded "+1:12" reads at the same glance as
 * an over-padded "−0:30".
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
 * Format signed minutes-per-day with one decimal — the headline "would
 * save/recover X min/day for the average bus" column.
 */
function formatSignedMinutes(min) {
  if (min == null) return '—'
  const sign = min >= 0 ? '+' : '−'
  return `${sign}${Math.abs(min).toFixed(1)} min/day`
}

const PERIOD_OPTIONS = [
  { value: 'all', label: 'All day' },
  { value: 'am_peak', label: 'AM Peak (6-10am)' },
  { value: 'midday', label: 'Midday (10am-3pm)' },
  { value: 'pm_peak', label: 'PM Peak (3-7pm)' },
  { value: 'evening', label: 'Evening (7-10pm)' },
  { value: 'late', label: 'Late (10pm-6am)' },
]

const SIGN_OPTIONS = [
  { value: 'all', label: 'All segments' },
  { value: 'under', label: 'Under-padded only (positive slip)' },
  { value: 'over', label: 'Over-padded only (negative slip)' },
]

const DIRECTION_OPTIONS = [
  { value: 'all', label: 'Both directions' },
  { value: '0', label: 'Direction 0' },
  { value: '1', label: 'Direction 1' },
]

/**
 * `/schedule-audit` page (NOTES-60). System-wide ranked table of
 * under-padded and over-padded segments — direct input to schedule
 * revisions. Populates from `/api/schedule-audit`, which reads
 * `route_diagnostic_segment` rows materialized nightly by PR #107's
 * pipeline.
 *
 * Sign convention (mirrors `src/route_diagnostics.py`):
 *  - positive `mean_slip_sec` → observed > scheduled → bus runs longer
 *    than the schedule allots → UNDER-padded (revisions add time)
 *  - negative `mean_slip_sec` → observed < scheduled → bus is faster
 *    than scheduled → OVER-padded (revisions recover service-hours)
 *
 * Default sort is absolute `minutes_per_day` (slip × daily trips,
 * "biggest leverage first"), enforced server-side. Route, direction,
 * period, and sign filters are all server-side query parameters; the
 * route filter is a free-text input (typed against the route_id) so an
 * operator can drill into one route without an extra route-picker
 * round-trip.
 */
function ScheduleAudit() {
  const [routeIdInput, setRouteIdInput] = useState('')
  const [routeId, setRouteId] = useState('')
  const [direction, setDirection] = useState('all')
  const [period, setPeriod] = useState('all')
  const [sign, setSign] = useState('all')
  const [limit, setLimit] = useState(100)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Debounced route_id push from the text input to the query. Keeps the
  // fetch from firing on every keystroke — only on a 300ms pause.
  useEffect(() => {
    const trimmed = routeIdInput.trim().toUpperCase()
    const t = setTimeout(() => setRouteId(trimmed), 300)
    return () => clearTimeout(t)
  }, [routeIdInput])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const params = new URLSearchParams()
    if (routeId) params.set('route_id', routeId)
    if (direction !== 'all') params.set('direction_id', direction)
    if (period) params.set('period', period)
    if (sign) params.set('sign', sign)
    if (limit) params.set('limit', String(limit))
    fetch(`/api/schedule-audit?${params.toString()}`)
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
  }, [routeId, direction, period, sign, limit])

  const segments = useMemo(() => data?.segments || [], [data])
  const lookbackDays = data?.lookback_days ?? 30

  // Total minutes/day across the displayed rows — useful at-a-glance
  // headline for "how much would resolving these segments yield."
  const totalMinutesPerDay = useMemo(() => {
    return segments.reduce((acc, s) => acc + (s.minutes_per_day || 0), 0)
  }, [segments])

  return (
    <main>
      <div className="chart-container">
        <h2>Schedule audit</h2>
        <p className="drilldown-anchor">
          Per-segment slip ranked by leverage — the biggest leverage on
          schedule revision, regardless of route. Each row shows one
          consecutive segment (from-stop → to-stop) on one route /
          direction / period over the last {lookbackDays} days. Positive
          slip means the bus runs <strong>slower</strong> than the
          schedule allots (under-padded — add time); negative means the
          bus runs <strong>faster</strong> (over-padded — recoverable
          service-hours). "Min/day for the average bus" multiplies the
          per-trip slip by the daily trip count, so segments with both a
          large slip and high volume rise to the top.
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
            <span style={{ opacity: 0.8 }}>Route:</span>
            <input
              type="text"
              value={routeIdInput}
              onChange={(e) => setRouteIdInput(e.target.value)}
              placeholder="e.g. D80"
              aria-label="Route id filter"
              style={{ width: '6rem' }}
            />
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <span style={{ opacity: 0.8 }}>Direction:</span>
            <select
              value={direction}
              onChange={(e) => setDirection(e.target.value)}
              aria-label="Direction filter"
            >
              {DIRECTION_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </label>
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
            <span style={{ opacity: 0.8 }}>Sign:</span>
            <select
              value={sign}
              onChange={(e) => setSign(e.target.value)}
              aria-label="Slip sign filter"
            >
              {SIGN_OPTIONS.map((opt) => (
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

        {loading && <p style={{ color: '#64748b' }}>Loading schedule audit…</p>}
        {error && <p style={{ color: '#64748b' }}>Unable to load schedule audit: {error}</p>}

        {!loading && !error && segments.length === 0 && (
          <p style={{ color: '#64748b' }}>
            No segments match the current filters. The diagnostic
            pipeline materializes <code>route_diagnostic_segment</code>{' '}
            nightly — if this is a fresh install, the pipeline may not
            have run yet.
          </p>
        )}

        {!loading && !error && segments.length > 0 && (
          <>
            <p
              style={{
                color: '#64748b',
                fontSize: '0.875rem',
                marginBottom: '0.5rem',
              }}
            >
              Showing {segments.length} of {data.n_rows} matching
              segments. Total leverage across the displayed rows:{' '}
              <strong>{formatSignedMinutes(totalMinutesPerDay)}</strong>.
            </p>
            <div className="recent-runs-table-wrapper">
              <table className="recent-runs-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Dir</th>
                    <th>Segment</th>
                    <th>Period</th>
                    <th>Mean slip</th>
                    <th>Trips/day</th>
                    <th>Min/day for average bus</th>
                  </tr>
                </thead>
                <tbody>
                  {segments.map((s, idx) => (
                    <tr
                      key={`${s.route_id}-${s.direction_id}-${s.period}-${s.from_stop_id}-${s.to_stop_id}-${idx}`}
                    >
                      <td className="route-id">
                        <Link to={`/route/${s.route_id}`}>
                          {s.route_short_name || s.route_id}
                        </Link>
                      </td>
                      <td>{s.direction_id}</td>
                      <td>
                        {s.from_stop_name || s.from_stop_id} →{' '}
                        {s.to_stop_name || s.to_stop_id}
                      </td>
                      <td>{s.period}</td>
                      <td
                        style={{
                          color: s.mean_slip_sec >= 0 ? '#b91c1c' : '#15803d',
                          fontWeight: 600,
                        }}
                      >
                        {formatSignedSeconds(s.mean_slip_sec)}
                      </td>
                      <td>{(s.daily_trip_count ?? 0).toFixed(1)}</td>
                      <td
                        style={{
                          color: s.minutes_per_day >= 0 ? '#b91c1c' : '#15803d',
                          fontWeight: 600,
                        }}
                      >
                        {formatSignedMinutes(s.minutes_per_day)}
                      </td>
                    </tr>
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

export default ScheduleAudit
