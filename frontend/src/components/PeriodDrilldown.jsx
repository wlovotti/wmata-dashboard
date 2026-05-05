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
const BUNCHING_BAR_COLOR = '#C8102E'

function shortPeriodLabel(label) {
  const idx = label.indexOf(' (')
  return idx === -1 ? label : label.slice(0, idx)
}

function ewtTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">{row.time_period}</div>
      <div>EWT: {row.ewt_seconds != null ? `${row.ewt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div>AWT: {row.awt_seconds != null ? `${row.awt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div>SWT: {row.swt_seconds != null ? `${row.swt_seconds.toFixed(0)} sec` : 'N/A'}</div>
      <div className="chart-tooltip-meta">
        {row.n_observed_headways} observed / {row.n_scheduled_headways} scheduled headways
      </div>
    </div>
  )
}

function bunchingTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">{row.time_period}</div>
      <div>
        Bunching:{' '}
        {row.bunching_rate != null ? `${(row.bunching_rate * 100).toFixed(1)}%` : 'N/A'}
      </div>
      <div className="chart-tooltip-meta">
        {row.bunching_count} of {row.total_headways} headway pairs
      </div>
    </div>
  )
}

function PeriodDrilldown({ routeId }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

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
  const bunchingRows = (data.bunching || [])
    .filter((r) => r.total_headways > 0)
    .map((r) => ({ ...r, _label: shortPeriodLabel(r.time_period) }))

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
                  {ewtRows.map((row) => (
                    <Cell
                      key={row.time_period}
                      fill={row.ewt_seconds == null ? '#cbd5e1' : EWT_BAR_COLOR}
                    />
                  ))}
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
    </div>
  )
}

export default PeriodDrilldown
