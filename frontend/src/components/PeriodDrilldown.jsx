import { useState, useEffect } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'

const EWT_BAR_COLOR = '#002F6C'
const EWT_BAR_COLOR_THIN = '#94a3b8' // de-saturated for low-coverage periods
const BUNCHING_BAR_COLOR = '#C8102E'

// Below this observed/scheduled-headway ratio, EWT (and bunching, which
// shares the trip_update observation source) is unreliable — surfaced as a
// "data thin" badge so the EWT clamp at 0 doesn't silently mask the gap.
const COVERAGE_THIN_THRESHOLD = 0.5

// NOTES-42: cause-decomposition stacked-bar segment colors. Distinct hues so
// a glance at the bar gives the operator the dominant cause.
const CAUSE_COLORS = {
  leader_late_only: '#C8102E', // running-time / recovery problem (red — same hue as bunching)
  trailer_early_only: '#F59E0B', // dispatch / departure-discipline problem (amber)
  both_off: '#7c3aed', // compounding — both interventions apply (violet)
  neither_off: '#94a3b8', // OTP-window-internal (de-saturated gray)
  unknown: '#cbd5e1', // missing schedule match (paler gray)
}

const CAUSE_LABELS = {
  leader_late_only: 'Running-time failure (leader late)',
  trailer_early_only: 'Dispatch failure (trailer early)',
  both_off: 'Compounding (both off)',
  neither_off: 'Within OTP window',
  unknown: 'Unknown (missing schedule)',
}

// Display order for the stacked bar (left → right) and the legend list
// underneath. Run the actionable buckets first; the residual buckets last.
const CAUSE_ORDER = [
  'leader_late_only',
  'trailer_early_only',
  'both_off',
  'neither_off',
  'unknown',
]

function shortPeriodLabel(label) {
  const idx = label.indexOf(' (')
  return idx === -1 ? label : label.slice(0, idx)
}

function isThin(coverageRatio) {
  return coverageRatio != null && coverageRatio < COVERAGE_THIN_THRESHOLD
}

function ewtTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  const thin = isThin(row.coverage_ratio)
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">
        {row.time_period}
        {thin && <span className="data-thin-badge">Thin</span>}
      </div>
      <div>EWT: {row.ewt_seconds != null ? `${row.ewt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div>AWT: {row.awt_seconds != null ? `${row.awt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div>SWT: {row.swt_seconds != null ? `${row.swt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div className="chart-tooltip-meta">
        {row.n_observed_headways} observed / {row.n_scheduled_headways} scheduled headways
        {row.coverage_ratio != null
          && ` (${Math.round(row.coverage_ratio * 100)}% coverage)`}
      </div>
    </div>
  )
}

function bunchingTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  const thin = isThin(row.coverage_ratio)
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">
        {row.time_period}
        {thin && <span className="data-thin-badge">Thin</span>}
      </div>
      <div>
        Bunching:{' '}
        {row.bunching_rate != null ? `${(row.bunching_rate * 100).toFixed(1)}%` : 'N/A'}
      </div>
      <div className="chart-tooltip-meta">
        {row.bunching_count} of {row.total_headways} headway pairs
        {row.coverage_ratio != null
          && ` (${Math.round(row.coverage_ratio * 100)}% EWT coverage)`}
      </div>
    </div>
  )
}

// NOTES-42: bunching-cause stacked horizontal bar.
//
// Renders the breakdown payload from /api/routes/{route_id}/bunching-causes
// as a single horizontal bar with one segment per category. Categories with
// zero count drop out of the bar. Category list below the bar shows
// count + pct for every non-zero category in display order.
//
// The disclaimer above the bar is small text on purpose — the tooltip
// carries the technical detail. Per NOTES-42 framing: the mechanism is
// textbook (late leaders pick up more passengers, trailers run light), but
// the five-bucket decomposition is this dashboard's, not industry-standard.
function BunchingCauseBar({ data }) {
  if (!data || data.n_bunched_pairs === 0) {
    return (
      <div className="bunching-cause-block">
        <h3>Bunching cause decomposition</h3>
        <p className="drilldown-empty">
          No bunched pairs in the selected window.
        </p>
      </div>
    )
  }

  const segments = CAUSE_ORDER
    .map((key) => ({
      key,
      label: CAUSE_LABELS[key],
      color: CAUSE_COLORS[key],
      count: data.breakdown[key]?.count ?? 0,
      pct: data.breakdown[key]?.pct ?? 0,
    }))
    .filter((s) => s.count > 0)

  return (
    <div className="bunching-cause-block">
      <h3>Bunching cause decomposition</h3>
      <p className="bunching-cause-disclaimer">
        Internal diagnostic — the mechanism is well-established (late leaders
        pick up more passengers, trailers run light), but the breakdown shown
        here is this dashboard&apos;s decomposition, not an industry-standard
        metric.
      </p>
      <div
        className="bunching-cause-bar"
        title="Dispatch failure: trailer ran more than 2 minutes ahead of schedule. Running-time failure: leader ran more than 7 minutes behind schedule. Compounding: both off. Threshold matches the WMATA OTP window."
      >
        {segments.map((s) => (
          <div
            key={s.key}
            className="bunching-cause-segment"
            style={{ width: `${s.pct * 100}%`, background: s.color }}
            aria-label={`${s.label}: ${s.count} pairs (${(s.pct * 100).toFixed(1)}%)`}
          />
        ))}
      </div>
      <ul className="bunching-cause-legend">
        {segments.map((s) => (
          <li key={s.key}>
            <span
              className="bunching-cause-swatch"
              style={{ background: s.color }}
              aria-hidden="true"
            />
            <span className="bunching-cause-legend-label">{s.label}</span>
            <span className="bunching-cause-legend-value">
              {s.count} ({(s.pct * 100).toFixed(1)}%)
            </span>
          </li>
        ))}
      </ul>
      <p className="bunching-cause-meta">
        {data.n_bunched_pairs} bunched pair{data.n_bunched_pairs === 1 ? '' : 's'}{' '}
        over the past {data.days} day{data.days === 1 ? '' : 's'}.
      </p>
    </div>
  )
}

function PeriodDrilldown({ routeId, dayType = 'all', period = 'all' }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [causeData, setCauseData] = useState(null)
  const [causeError, setCauseError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`/api/routes/${routeId}/period-drilldown`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || err)
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [routeId])

  useEffect(() => {
    let cancelled = false
    setCauseError(null)
    const params = new URLSearchParams()
    if (dayType && dayType !== 'all') params.set('day_type', dayType)
    if (period && period !== 'all') params.set('period', period)
    const qs = params.toString()
    const url = `/api/routes/${routeId}/bunching-causes${qs ? `?${qs}` : ''}`
    fetch(url)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setCauseData(json)
      })
      .catch((err) => {
        if (!cancelled) setCauseError(err.message || err)
      })
    return () => {
      cancelled = true
    }
  }, [routeId, dayType, period])

  if (loading) {
    return (
      <div className="chart-container">
        <h2>Performance by Time of Day</h2>
        <p style={{ color: '#64748b' }}>Loading…</p>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="chart-container">
        <h2>Performance by Time of Day</h2>
        <p style={{ color: '#64748b' }}>
          Unable to load drilldown: {error || 'no data'}
        </p>
      </div>
    )
  }

  const ewtRows = (data.ewt || [])
    .filter((r) => r.frequent_cell_hours > 0)
    .map((r) => ({ ...r, _label: shortPeriodLabel(r.time_period) }))
  // EWT coverage_ratio is the observed-vs-scheduled-headways gauge; bunching
  // shares the same observation source so the same threshold applies. Thread
  // it onto bunching rows by time_period so the tooltip and bar shading
  // reflect it consistently.
  const ewtCoverageByPeriod = Object.fromEntries(
    (data.ewt || []).map((r) => [r.time_period, r.coverage_ratio]),
  )
  const bunchingRows = (data.bunching || [])
    .filter((r) => r.total_headways > 0)
    .map((r) => ({
      ...r,
      _label: shortPeriodLabel(r.time_period),
      coverage_ratio: ewtCoverageByPeriod[r.time_period] ?? null,
    }))
  const anyThin = ewtRows.some((r) => isThin(r.coverage_ratio))
    || bunchingRows.some((r) => isThin(r.coverage_ratio))

  return (
    <div className="chart-container">
      <h2>Performance by Time of Day</h2>
      {data.service_date && (
        <p className="drilldown-anchor">
          Service date: {data.service_date} ({data.day_type})
        </p>
      )}
      <div className="drilldown-grid">
        <div className="drilldown-chart">
          <h3>Excess Wait Time (seconds)</h3>
          {ewtRows.length === 0 ? (
            <p className="drilldown-empty">
              No frequent-service periods on this date.
            </p>
          ) : (
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={ewtRows} margin={{ top: 8, right: 8, left: 0, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="_label" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip content={ewtTooltip} />
                <Bar dataKey="ewt_seconds" fill={EWT_BAR_COLOR}>
                  {ewtRows.map((row) => {
                    let fill = EWT_BAR_COLOR
                    if (row.ewt_seconds == null) {
                      fill = '#cbd5e1'
                    } else if (isThin(row.coverage_ratio)) {
                      fill = EWT_BAR_COLOR_THIN
                    }
                    return <Cell key={row.time_period} fill={fill} />
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
        <div className="drilldown-chart">
          <h3>Bunching Rate (%)</h3>
          {bunchingRows.length === 0 ? (
            <p className="drilldown-empty">No observed headway pairs on this date.</p>
          ) : (
            <ResponsiveContainer width="100%" height={260}>
              <BarChart
                data={bunchingRows.map((r) => ({
                  ...r,
                  bunching_pct: r.bunching_rate != null ? r.bunching_rate * 100 : null,
                }))}
                margin={{ top: 8, right: 8, left: 0, bottom: 8 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="_label" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} unit="%" />
                <Tooltip content={bunchingTooltip} />
                <Bar dataKey="bunching_pct" fill={BUNCHING_BAR_COLOR} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
      {anyThin && (
        <p className="data-thin-note">
          <span className="data-thin-badge">Thin</span> periods had under 50%
          observed-vs-scheduled headway coverage — the trip-update derivation
          missed enough arrivals that the metric is unreliable.
        </p>
      )}
      {causeError ? (
        <div className="bunching-cause-block">
          <h3>Bunching cause decomposition</h3>
          <p className="drilldown-empty">
            Unable to load cause breakdown: {causeError}
          </p>
        </div>
      ) : (
        <BunchingCauseBar data={causeData} />
      )}
    </div>
  )
}

export default PeriodDrilldown
