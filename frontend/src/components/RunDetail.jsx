import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ReferenceArea,
  ResponsiveContainer,
} from 'recharts'

// WMATA on-time band: -2 min early to +7 min late. Mirrors `src/otp_constants.py`.
const ON_TIME_LOWER_SEC = -120
const ON_TIME_UPPER_SEC = 420

const DEVIATION_LINE_COLOR = '#E7872B'

function formatTimeOnly(iso) {
  /**
   * Render an Eastern ISO8601 timestamp from the API as HH:MM:SS local time.
   *
   * The API already converts naive-UTC storage to Eastern before serialization,
   * so we just slice the time component off the string. Returns "—" for null
   * (a stop with no observed `stop_event`).
   */
  if (!iso) return '—'
  const t = iso.slice(11, 19)
  return t || '—'
}

function deviationTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  const dev = row.deviation_sec
  let devLabel = 'no data'
  if (dev != null) {
    const sign = dev > 0 ? '+' : ''
    const lateOrEarly = dev > 0 ? 'late' : dev < 0 ? 'early' : 'on time'
    devLabel = `${sign}${dev}s (${lateOrEarly})`
  }
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">{row.stop_name}</div>
      <div>Stop sequence: {row.stop_sequence}</div>
      <div>Scheduled: {formatTimeOnly(row.scheduled)}</div>
      <div>Actual: {formatTimeOnly(row.actual)}</div>
      <div>Deviation: {devLabel}</div>
    </div>
  )
}

function RunDetail() {
  const { runId } = useParams()
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`/api/runs/${runId}/deviations`)
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
  }, [runId])

  const backToRoute = () => {
    if (data?.route_id) {
      navigate(`/route/${data.route_id}`)
    } else {
      navigate(-1)
    }
  }

  if (loading) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(-1)} className="back-btn">
            ← Back
          </button>
        </div>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading run details...</p>
        </div>
      </main>
    )
  }

  if (error || !data) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(-1)} className="back-btn">
            ← Back
          </button>
        </div>
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading run data:</strong> {error || 'Run not found'}
          </div>
        </div>
      </main>
    )
  }

  // Recharts renders `null` y-values as gaps — exactly what we want for stops
  // without an observed stop_event. The line skips them, the on-time band
  // still spans the full sequence range.
  const chartData = (data.deviations || []).map((d) => ({
    ...d,
    deviation_sec: d.deviation_sec, // explicit so null stays null
  }))

  const seqValues = chartData.map((d) => d.stop_sequence)
  const seqMin = seqValues.length ? Math.min(...seqValues) : 0
  const seqMax = seqValues.length ? Math.max(...seqValues) : 0

  const directionLabel = data.direction_id === 0 ? 'Outbound (0)' : 'Inbound (1)'
  const startTime = formatTimeOnly(data.first_obs_ts)
  const endTime = formatTimeOnly(data.last_obs_ts)

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={backToRoute} className="back-btn">
          ← Back to Route {data.route_id}
        </button>
        <div className="route-title">
          <h1>
            Run on Route {data.route_id}
            {data.trip_headsign && (
              <span style={{ fontWeight: 400, color: '#475569' }}>
                {' '}— {data.trip_headsign}
              </span>
            )}
          </h1>
          <p style={{ color: '#64748b', marginTop: '0.25rem' }}>
            Service date {data.service_date} · Trip {data.trip_id} ·{' '}
            {directionLabel} · Source {data.source}
            {data.vehicle_id && ` · Vehicle ${data.vehicle_id}`}
          </p>
        </div>
      </div>

      <div className="stats-summary">
        <div className="stat-card">
          <div className="stat-value">{data.stops_observed ?? 0}<span style={{ fontSize: '1.5rem' }}> / {data.stops_scheduled ?? 0}</span></div>
          <div className="stat-label">Stops observed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{startTime}</div>
          <div className="stat-label">First observation</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{endTime}</div>
          <div className="stat-label">Last observation</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {data.dev_p50_sec != null ? `${data.dev_p50_sec}s` : 'N/A'}
          </div>
          <div className="stat-label">Median deviation</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {data.dev_p95_sec != null ? `${data.dev_p95_sec}s` : 'N/A'}
          </div>
          <div className="stat-label">p95 deviation</div>
        </div>
      </div>

      <div className="chart-container">
        <h2>Schedule deviation across the trip</h2>
        <p className="drilldown-anchor">
          Positive = late, negative = early. Translucent green band is the
          WMATA on-time window (−2 min to +7 min). Gaps in the line are stops
          with no observed event.
        </p>
        {chartData.length === 0 ? (
          <p className="drilldown-empty">No scheduled stops found for this trip.</p>
        ) : (
          <ResponsiveContainer width="100%" height={360}>
            <LineChart
              data={chartData}
              margin={{ top: 16, right: 24, left: 0, bottom: 16 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis
                dataKey="stop_sequence"
                type="number"
                domain={[seqMin, seqMax]}
                tick={{ fontSize: 12 }}
                label={{
                  value: 'stop_sequence',
                  position: 'insideBottomRight',
                  offset: -4,
                  fontSize: 12,
                }}
              />
              <YAxis
                tick={{ fontSize: 12 }}
                label={{
                  value: 'deviation (sec, +late / -early)',
                  angle: -90,
                  position: 'insideLeft',
                  style: { fontSize: 12, textAnchor: 'middle' },
                }}
              />
              <Tooltip content={deviationTooltip} />
              <ReferenceArea
                y1={ON_TIME_LOWER_SEC}
                y2={ON_TIME_UPPER_SEC}
                fill="#16a34a"
                fillOpacity={0.12}
                stroke="none"
              />
              <ReferenceLine y={0} stroke="#0f172a" strokeWidth={1} />
              <Line
                type="monotone"
                dataKey="deviation_sec"
                stroke={DEVIATION_LINE_COLOR}
                strokeWidth={2}
                dot={{ r: 3, fill: DEVIATION_LINE_COLOR }}
                activeDot={{ r: 5 }}
                connectNulls={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </main>
  )
}

export default RunDetail
