import { useState, useEffect, useMemo } from 'react'

// Stop-level diagnostic strip chart (NOTES-40).
//
// Renders one horizontal heatmap row per direction along the route's
// canonical stop sequence (longest trip per direction, picked server-side).
// Each cell is one stop; color encodes the selected metric.
//
// The (route_id, direction_id, stop_id) grouping rule is enforced
// server-side — termini that share a stop_id across directions surface as
// two cells (one per direction row) rather than one collapsed cell, so
// the diagnostic doesn't quietly halve apparent deviation magnitudes.

const METRIC_OPTIONS = [
  { key: 'median_deviation_sec', label: 'Median deviation' },
  { key: 'p95_deviation_sec', label: 'P95 deviation' },
  { key: 'otp_pct', label: 'OTP %' },
  { key: 'skip_pct', label: 'Skip %' },
]

// Diverging-deviation gradient: blue (early) → white (on-time) → red (late).
// The on-time band corresponds to the OTP_EARLY_SEC / OTP_LATE_SEC window
// (-120s / +420s); we treat anything in that range as ~white.
function deviationColor(devSec) {
  if (devSec == null) return null
  // Cap absolute deviation at 600s for the gradient — outliers all read
  // as full-saturation. Beyond the cap the human eye can't distinguish.
  const cap = 600
  const clamped = Math.max(-cap, Math.min(cap, devSec))
  if (clamped < -120) {
    // Early — blue. Map [-cap, -120] → [1.0, 0.0] saturation.
    const t = (-120 - clamped) / (cap - 120)
    return `rgb(${Math.round(255 - 180 * t)}, ${Math.round(255 - 100 * t)}, 255)`
  }
  if (clamped > 420) {
    // Late — red. Map [420, cap] → [0.0, 1.0] saturation.
    const t = (clamped - 420) / (cap - 420)
    return `rgb(255, ${Math.round(255 - 180 * t)}, ${Math.round(255 - 200 * t)})`
  }
  // On-time band — near-white.
  return 'rgb(245, 245, 245)'
}

// Sequential green→yellow→red for OTP%. 100% = full green, 0% = full red.
function otpColor(pct) {
  if (pct == null) return null
  // pct is 0..1
  // Hue 120 (green) → 0 (red) by way of 60 (yellow).
  const hue = Math.round(120 * pct)
  return `hsl(${hue}, 65%, 65%)`
}

// Sequential white→red for skip rate. 0% = white, ≥10% = saturated red.
function skipColor(pct) {
  if (pct == null) return null
  const cap = 0.1 // a 10% skip rate is severe; anything higher still reads max-red
  const t = Math.min(1, pct / cap)
  // White (245,245,245) → red (200,30,30).
  const r = Math.round(245 - (245 - 200) * t)
  const g = Math.round(245 - (245 - 30) * t)
  const b = Math.round(245 - (245 - 30) * t)
  return `rgb(${r}, ${g}, ${b})`
}

function colorForMetric(metricKey, value) {
  if (value == null) return '#e5e5e5' // null/no-data — gray
  if (metricKey === 'median_deviation_sec' || metricKey === 'p95_deviation_sec') {
    return deviationColor(value)
  }
  if (metricKey === 'otp_pct') return otpColor(value)
  if (metricKey === 'skip_pct') return skipColor(value)
  return '#e5e5e5'
}

function formatValueForMetric(metricKey, value) {
  if (value == null) return 'N/A'
  if (metricKey === 'median_deviation_sec' || metricKey === 'p95_deviation_sec') {
    return `${value > 0 ? '+' : ''}${value} sec`
  }
  if (metricKey === 'otp_pct' || metricKey === 'skip_pct') {
    return `${(value * 100).toFixed(1)}%`
  }
  return String(value)
}

function StopDiagnostic({ routeId, dayType, period }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [metric, setMetric] = useState('median_deviation_sec')
  const [hoveredStop, setHoveredStop] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const params = new URLSearchParams()
    if (dayType !== 'all') params.set('day_type', dayType)
    if (period !== 'all') params.set('period', period)
    const qs = params.toString()
    const url = `/api/routes/${routeId}/stops${qs ? `?${qs}` : ''}`
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
          setError(err.message || err)
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [routeId, dayType, period])

  // Group rows by direction so each direction renders as its own strip.
  const stopsByDirection = useMemo(() => {
    if (!data?.stops) return {}
    const out = {}
    for (const s of data.stops) {
      if (!out[s.direction_id]) out[s.direction_id] = []
      out[s.direction_id].push(s)
    }
    return out
  }, [data])

  if (loading) {
    return (
      <div className="chart-container">
        <h2>Stop Diagnostic</h2>
        <p>Loading stop-level metrics...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="chart-container">
        <h2>Stop Diagnostic</h2>
        <p style={{ color: '#a00' }}>Error loading stop diagnostic: {error}</p>
      </div>
    )
  }

  if (!data?.stops?.length) {
    return null
  }

  const directions = Object.keys(stopsByDirection)
    .map((d) => Number(d))
    .sort((a, b) => a - b)

  return (
    <div className="chart-container">
      <h2>Stop Diagnostic</h2>
      <div
        style={{
          fontSize: '0.85rem',
          opacity: 0.75,
          marginBottom: '0.75rem',
        }}
      >
        Per-stop metrics over the last {data.days} days. Each cell is one
        stop along the route, ordered origin → destination per direction.
        Hover for details.
      </div>

      <div
        style={{
          display: 'flex',
          gap: '0.75rem',
          alignItems: 'center',
          marginBottom: '0.75rem',
          fontSize: '0.875rem',
        }}
      >
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          <span style={{ opacity: 0.8 }}>Metric:</span>
          <select
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
            aria-label="Stop diagnostic metric"
          >
            {METRIC_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      {directions.map((dir) => {
        const stops = stopsByDirection[dir]
        return (
          <div key={dir} style={{ marginBottom: '1rem' }}>
            <div
              style={{
                fontSize: '0.85rem',
                fontWeight: 500,
                marginBottom: '0.35rem',
                opacity: 0.85,
              }}
            >
              Direction {dir} ({stops.length} stops)
            </div>
            <div
              style={{
                display: 'flex',
                width: '100%',
                height: '32px',
                border: '1px solid #ccc',
                borderRadius: '3px',
                overflow: 'hidden',
              }}
              role="img"
              aria-label={`Stop diagnostic strip for direction ${dir}`}
            >
              {stops.map((s, i) => {
                const value = s[metric]
                const bg = colorForMetric(metric, value)
                const isHovered =
                  hoveredStop &&
                  hoveredStop.direction_id === s.direction_id &&
                  hoveredStop.stop_id === s.stop_id &&
                  hoveredStop.stop_sequence === s.stop_sequence
                return (
                  <div
                    key={`${s.direction_id}-${s.stop_id}-${s.stop_sequence}-${i}`}
                    onMouseEnter={() => setHoveredStop(s)}
                    onMouseLeave={() => setHoveredStop(null)}
                    style={{
                      flex: 1,
                      backgroundColor: bg,
                      borderRight: i === stops.length - 1 ? 'none' : '1px solid rgba(0,0,0,0.05)',
                      cursor: 'pointer',
                      outline: isHovered ? '2px solid #002F6C' : 'none',
                      outlineOffset: '-2px',
                    }}
                    title={`${s.stop_name} (#${s.stop_sequence}): ${formatValueForMetric(
                      metric,
                      value,
                    )}${
                      s.n_observations > 0 ? ` — ${s.n_observations} obs` : ' — no data'
                    }`}
                  />
                )
              })}
            </div>
          </div>
        )
      })}

      {hoveredStop && (
        <div
          style={{
            marginTop: '0.5rem',
            padding: '0.5rem 0.75rem',
            background: '#f5f7fa',
            border: '1px solid #d0d7de',
            borderRadius: '4px',
            fontSize: '0.85rem',
          }}
        >
          <div style={{ fontWeight: 600 }}>
            {hoveredStop.stop_name} (seq #{hoveredStop.stop_sequence}, dir {hoveredStop.direction_id})
          </div>
          <div style={{ marginTop: '0.25rem', display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
            <span>
              Median dev:{' '}
              {formatValueForMetric('median_deviation_sec', hoveredStop.median_deviation_sec)}
            </span>
            <span>
              P95 dev: {formatValueForMetric('p95_deviation_sec', hoveredStop.p95_deviation_sec)}
            </span>
            <span>OTP: {formatValueForMetric('otp_pct', hoveredStop.otp_pct)}</span>
            <span>Skip: {formatValueForMetric('skip_pct', hoveredStop.skip_pct)}</span>
            <span style={{ opacity: 0.7 }}>
              {hoveredStop.n_observations} obs / {hoveredStop.n_scheduled} TU rows
            </span>
          </div>
        </div>
      )}
    </div>
  )
}

export default StopDiagnostic
