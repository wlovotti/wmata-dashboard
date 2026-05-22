/**
 * Route diagnosis panel — slip trajectory chart + timepoint behavior table
 * + LLM narrative (the route diagnosis narrative, PR #141).
 *
 * Surfaces the route_diagnostic_profile materialized by
 * pipelines/refresh_route_diagnostic_profile.py (PR #107) for one route and
 * time-of-day period. Three sub-panels:
 *
 *   1. Slip trajectory chart — per-direction ComposedChart with a line of
 *      cumulative slip vs stop_sequence (cumulative delay picture) and a bar
 *      overlay of per-segment mean slip (red = late, green = recovery).
 *      Timepoint stops are annotated with a dot and label on the cumulative
 *      line so the viewer can see where WMATA schedule checkpoints land on
 *      the trajectory.
 *
 *   2. Timepoint behavior table — one row per timepoint on the route with a
 *      classification badge (recovery / leaky / underpowered / neutral) and
 *      the distribution summaries (median entering, median leaving, p10
 *      spread change) that justify the label.
 *
 *   3. Narrative — cached LLM interpretation of the diagnostic profile
 *      (generated offline by scripts/generate_route_diagnosis.py; the
 *      public API never calls Claude). Shows a stale-data banner when the
 *      underlying profile has changed since the narrative was generated.
 *
 * Terminology tooltip definitions for "slip" and "timepoint" appear inline
 * on first use to make the panel readable for a transit-interested public.
 *
 * Period filtering reuses the RouteDetail `period=` prop — no new selector
 * is added here; the parent controls the period and passes it as a prop.
 *
 * Data sources:
 *   GET /api/routes/{routeId}/diagnostic_profile?period=...
 *   GET /api/routes/{routeId}/diagnosis?period=...
 *
 * Props:
 *   routeId  — string route identifier (e.g. 'D80')
 *   period   — time-of-day period key ('all' | 'am_peak' | ...)
 */

import { useState, useEffect, useMemo } from 'react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from 'recharts'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CLASSIFICATION_STYLES = {
  recovery: { bg: '#dcfce7', border: '#16a34a', text: '#14532d', label: 'Recovery' },
  leaky: { bg: '#fef9c3', border: '#ca8a04', text: '#713f12', label: 'Leaky' },
  underpowered: { bg: '#fee2e2', border: '#dc2626', text: '#7f1d1d', label: 'Underpowered' },
  neutral: { bg: '#f1f5f9', border: '#94a3b8', text: '#334155', label: 'Neutral' },
}

const CLASSIFICATION_TOOLTIPS = {
  recovery:
    'The bus typically arrives late but departs much closer to schedule — the timepoint is absorbing delay.',
  leaky:
    'A meaningful share of buses depart ahead of schedule — early-departure bleed that increases passenger wait time downstream.',
  underpowered:
    'The bus arrives late and the timepoint provides little or no recovery — a schedule-revision candidate.',
  neutral: 'Well-behaved timepoint; no notable distribution shift across the checkpoint.',
}

/** Tooltip glossary for "slip". */
const SLIP_DEFINITION =
  'Slip is the difference between observed and scheduled segment travel time. ' +
  'Positive slip (red) means the bus takes longer than the schedule budgets; ' +
  'negative slip (green) means the bus runs faster — often at a recovery timepoint.'

/** Tooltip glossary for "timepoint". */
const TIMEPOINT_DEFINITION =
  'A timepoint is a WMATA-designated schedule checkpoint — a stop where buses ' +
  'are expected to hold until their scheduled departure time. Timepoints are where ' +
  'the schedule tries to absorb accumulated delay.'

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/**
 * Format deviation in seconds as ±Xm Ys or ±Xs.
 * @param {number|null} sec
 * @returns {string}
 */
function fmtSec(sec) {
  if (sec == null) return 'N/A'
  const abs = Math.abs(sec)
  const sign = sec < 0 ? '−' : '+'
  if (abs >= 60) {
    const m = Math.floor(abs / 60)
    const s = Math.round(abs % 60)
    return `${sign}${m}m ${s}s`
  }
  return `${sign}${Math.round(abs)}s`
}

/**
 * Inline tooltip anchor — renders a "?" superscript that shows `text` on hover.
 * @param {{ text: string }} props
 */
function InfoTip({ text }) {
  return (
    <span
      title={text}
      style={{
        display: 'inline-block',
        marginLeft: '0.3em',
        width: '1em',
        height: '1em',
        lineHeight: '1em',
        textAlign: 'center',
        borderRadius: '50%',
        background: '#e2e8f0',
        color: '#475569',
        fontSize: '0.65em',
        cursor: 'help',
        fontWeight: 700,
        verticalAlign: 'super',
        flexShrink: 0,
      }}
    >
      ?
    </span>
  )
}

/**
 * Classification badge for the timepoint behavior table.
 * @param {{ classification: string }} props
 */
function ClassificationBadge({ classification }) {
  const style = CLASSIFICATION_STYLES[classification] || CLASSIFICATION_STYLES.neutral
  const tip = CLASSIFICATION_TOOLTIPS[classification] || ''
  return (
    <span
      title={tip}
      style={{
        display: 'inline-block',
        padding: '0.15rem 0.5rem',
        borderRadius: '999px',
        border: `1px solid ${style.border}`,
        background: style.bg,
        color: style.text,
        fontSize: '0.75rem',
        fontWeight: 600,
        whiteSpace: 'nowrap',
        cursor: 'help',
      }}
    >
      {style.label}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Custom recharts tooltip for the slip chart
// ---------------------------------------------------------------------------

/**
 * Custom tooltip shown when hovering a segment on the slip trajectory chart.
 * Displays the stop names for from/to, per-segment slip, and cumulative slip.
 *
 * Recharts passes active, payload, and label automatically.
 */
function SlipTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null

  const from = d.from_stop_name || d.from_stop_id || '?'
  const to = d.to_stop_name || d.to_stop_id || '?'

  return (
    <div
      style={{
        background: 'white',
        border: '1px solid #cbd5e1',
        borderRadius: '6px',
        padding: '0.5rem 0.75rem',
        fontSize: '0.8rem',
        maxWidth: '220px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: '0.25rem', color: '#1e293b' }}>
        Seq {d.from_seq} → {d.to_seq}
        {d.is_timepoint && (
          <span
            style={{
              marginLeft: '0.4em',
              fontSize: '0.7rem',
              background: '#dbeafe',
              color: '#1d4ed8',
              padding: '0.1rem 0.35rem',
              borderRadius: '4px',
            }}
          >
            Timepoint
          </span>
        )}
      </div>
      <div style={{ color: '#64748b', marginBottom: '0.25rem' }}>
        {from} → {to}
      </div>
      <div>
        Per-segment slip:{' '}
        <strong style={{ color: d.mean_slip_sec > 0 ? '#dc2626' : '#16a34a' }}>
          {fmtSec(d.mean_slip_sec)}
        </strong>
      </div>
      <div>
        Cumulative slip:{' '}
        <strong>{fmtSec(d.cum_slip_sec)}</strong>
      </div>
      <div style={{ color: '#94a3b8', marginTop: '0.25rem', fontSize: '0.7rem' }}>
        {d.n_observations} observations
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Per-direction slip chart
// ---------------------------------------------------------------------------

/**
 * Slip trajectory chart for one direction.
 *
 * Renders a recharts ComposedChart with:
 *   - Bar layer: per-segment mean_slip_sec (red = positive / late, green = negative / recovery)
 *   - Line layer: cum_slip_sec trajectory with timepoint markers
 *
 * Only consecutive-edge rows (the min to_seq per from_seq) participate in the
 * cumulative trajectory line — this mirrors the materialisation logic. The
 * bar overlay uses all rows (consecutive + skip-N).
 *
 * @param {{ segments: Array<object>, directionLabel: string }} props
 */
function SlipChart({ segments, directionLabel }) {
  // The materialized data already has cum_slip_sec computed correctly by the
  // pipeline. For the trajectory line we only plot consecutive-edge rows
  // (min to_seq per from_seq); skip-N rows still render as bars.
  const consecutiveByFromSeq = useMemo(() => {
    const map = {}
    for (const s of segments) {
      if (!(s.from_seq in map) || s.to_seq < map[s.from_seq].to_seq) {
        map[s.from_seq] = s
      }
    }
    return map
  }, [segments])

  // Chart data: one entry per (from_seq, to_seq) pair for the bars.
  // The line only connects consecutive-edge rows in from_seq order.
  const chartData = useMemo(() => {
    return [...segments].sort((a, b) => a.from_seq - b.from_seq || a.to_seq - b.to_seq)
  }, [segments])

  // Line data — consecutive edges only, in from_seq order.
  const lineData = useMemo(() => {
    return Object.values(consecutiveByFromSeq).sort((a, b) => a.from_seq - b.from_seq)
  }, [consecutiveByFromSeq])

  const allValues = [
    ...chartData.map((d) => d.mean_slip_sec_min),
    ...lineData.map((d) => d.cum_slip_sec_min),
    0,
  ]
  const yMin = Math.min(...allValues)
  const yMax = Math.max(...allValues)
  const yDomain = [Math.floor(yMin - 0.5), Math.ceil(yMax + 0.5)]

  // Timepoint positions for reference lines
  const timepointSeqs = chartData.filter((d) => d.is_timepoint).map((d) => d.to_seq)

  return (
    <div>
      <div
        style={{
          fontSize: '0.8rem',
          fontWeight: 600,
          color: '#475569',
          marginBottom: '0.3rem',
        }}
      >
        {directionLabel}
      </div>
      <ResponsiveContainer width="100%" height={200}>
        <ComposedChart
          data={chartData}
          margin={{ top: 8, right: 16, left: 0, bottom: 4 }}
        >
          <XAxis
            dataKey="from_seq"
            tick={{ fontSize: 10 }}
            tickLine={false}
            label={{
              value: 'stop sequence →',
              position: 'insideBottomRight',
              offset: -4,
              fontSize: 9,
              fill: '#94a3b8',
            }}
          />
          <YAxis
            domain={yDomain}
            tick={{ fontSize: 10 }}
            tickFormatter={(v) => `${v}m`}
            width={36}
          />
          <RechartsTooltip content={<SlipTooltip />} />
          <ReferenceLine y={0} stroke="#94a3b8" strokeWidth={1} />
          {timepointSeqs.map((seq) => (
            <ReferenceLine
              key={seq}
              x={seq}
              stroke="#3b82f6"
              strokeDasharray="3 3"
              strokeWidth={1}
            />
          ))}
          <Bar dataKey="mean_slip_sec_min" name="Per-seg slip" barSize={6} radius={1}>
            {chartData.map((entry, idx) => (
              <Cell
                key={idx}
                fill={entry.mean_slip_sec > 0 ? '#ef4444' : '#22c55e'}
                fillOpacity={0.75}
              />
            ))}
          </Bar>
          <Line
            data={lineData}
            type="monotone"
            dataKey="cum_slip_sec_min"
            stroke="#1e293b"
            strokeWidth={2}
            dot={(props) => {
              const entry = props.payload
              if (!entry?.is_timepoint) {
                return <circle key={props.key} cx={props.cx} cy={props.cy} r={2} fill="#1e293b" />
              }
              return (
                <circle
                  key={props.key}
                  cx={props.cx}
                  cy={props.cy}
                  r={5}
                  fill="#3b82f6"
                  stroke="white"
                  strokeWidth={1.5}
                />
              )
            }}
            activeDot={{ r: 4 }}
            name="Cumulative slip"
          />
        </ComposedChart>
      </ResponsiveContainer>
      {/* Timepoint label strip below chart */}
      {lineData.some((d) => d.is_timepoint) && (
        <div
          style={{
            fontSize: '0.65rem',
            color: '#475569',
            marginTop: '0.15rem',
            paddingLeft: 36,
          }}
        >
          <span style={{ color: '#3b82f6', fontWeight: 600 }}>● </span>
          Timepoints:{' '}
          {lineData
            .filter((d) => d.is_timepoint)
            .map((d) => d.to_stop_name || d.to_stop_id)
            .join(' · ')}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Timepoint behavior table
// ---------------------------------------------------------------------------

/**
 * Per-timepoint behavior table for one direction.
 *
 * Columns: stop name, stop_sequence, classification badge, median dev
 * entering, median dev leaving, p10 spread change (p10_entering − p10_leaving).
 *
 * @param {{ timepoints: Array<object>, directionLabel: string }} props
 */
function TimepointTable({ timepoints, directionLabel }) {
  if (!timepoints.length) {
    return (
      <div style={{ color: '#94a3b8', fontSize: '0.8rem', padding: '0.5rem 0' }}>
        No timepoint data for this direction.
      </div>
    )
  }

  return (
    <div style={{ marginBottom: '0.75rem' }}>
      <div
        style={{
          fontSize: '0.8rem',
          fontWeight: 600,
          color: '#475569',
          marginBottom: '0.4rem',
        }}
      >
        {directionLabel}
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table
          style={{
            width: '100%',
            borderCollapse: 'collapse',
            fontSize: '0.8rem',
          }}
        >
          <thead>
            <tr style={{ borderBottom: '1px solid #e2e8f0' }}>
              <th style={thStyle}>Timepoint</th>
              <th style={{ ...thStyle, textAlign: 'center' }}>
                Classification
                <InfoTip
                  text={
                    'How the timepoint behaves:\n' +
                    'Recovery — absorbs delay.\n' +
                    'Leaky — early-departure bleed.\n' +
                    'Underpowered — late buses, no recovery.\n' +
                    'Neutral — well-behaved.'
                  }
                />
              </th>
              <th style={{ ...thStyle, textAlign: 'right' }}>
                Median entering
                <InfoTip text="Median schedule deviation at the stop just before this timepoint." />
              </th>
              <th style={{ ...thStyle, textAlign: 'right' }}>
                Median leaving
                <InfoTip text="Median schedule deviation at the timepoint itself." />
              </th>
              <th style={{ ...thStyle, textAlign: 'right' }}>
                p10 spread change
                <InfoTip
                  text={
                    'p10 deviation entering minus p10 deviation leaving. ' +
                    'A large positive value means the early-running tail is moving further ahead of schedule downstream of this timepoint (leaky sign).'
                  }
                />
              </th>
            </tr>
          </thead>
          <tbody>
            {timepoints.map((tp, i) => {
              const p10Change =
                tp.p10_dev_entering != null && tp.p10_dev_leaving != null
                  ? tp.p10_dev_entering - tp.p10_dev_leaving
                  : null
              return (
                <tr
                  key={tp.timepoint_stop_id}
                  style={{
                    borderBottom: '1px solid #f1f5f9',
                    background: i % 2 === 0 ? 'white' : '#f8fafc',
                  }}
                >
                  <td style={tdStyle}>
                    <span style={{ fontWeight: 500 }}>{tp.stop_name || tp.timepoint_stop_id}</span>
                    <span style={{ color: '#94a3b8', marginLeft: '0.4em', fontSize: '0.7rem' }}>
                      #{tp.timepoint_stop_id}
                    </span>
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'center' }}>
                    <ClassificationBadge classification={tp.classification} />
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    <span style={{ color: tp.median_dev_entering > 0 ? '#dc2626' : tp.median_dev_entering < -30 ? '#2563eb' : '#374151' }}>
                      {fmtSec(tp.median_dev_entering)}
                    </span>
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    <span style={{ color: tp.median_dev_leaving > 0 ? '#dc2626' : tp.median_dev_leaving < -30 ? '#2563eb' : '#374151' }}>
                      {fmtSec(tp.median_dev_leaving)}
                    </span>
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {p10Change != null ? (
                      <span style={{ color: p10Change > 60 ? '#b45309' : '#374151' }}>
                        {fmtSec(p10Change)}
                      </span>
                    ) : (
                      <span style={{ color: '#94a3b8' }}>N/A</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const thStyle = {
  padding: '0.3rem 0.5rem',
  textAlign: 'left',
  fontWeight: 600,
  color: '#64748b',
  whiteSpace: 'nowrap',
}

const tdStyle = {
  padding: '0.35rem 0.5rem',
  verticalAlign: 'middle',
}

// ---------------------------------------------------------------------------
// Narrative section (route diagnosis narrative, PR #141)
// ---------------------------------------------------------------------------

/**
 * Cached LLM narrative sub-section.
 *
 * Fetches GET /api/routes/{routeId}/diagnosis?period={period}. Shows:
 *   - The narrative text when cached.
 *   - A stale-data banner when is_stale=true (profile changed since generation).
 *   - A "not generated yet" message when the endpoint returns 404.
 *
 * @param {{ routeId: string, period: string }} props
 */
function NarrativeSection({ routeId, period }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [notFound, setNotFound] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setNotFound(false)
    setData(null)
    const params = new URLSearchParams()
    if (period && period !== 'all') params.set('period', period)
    const qs = params.toString()
    const url = `/api/routes/${routeId}/diagnosis${qs ? `?${qs}` : ''}`
    fetch(url)
      .then((res) => {
        if (res.status === 404) {
          if (!cancelled) { setNotFound(true); setLoading(false) }
          return null
        }
        if (!res.ok) return Promise.reject(`HTTP ${res.status}`)
        return res.json()
      })
      .then((json) => {
        if (json !== null && !cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err?.message || String(err))
          setLoading(false)
        }
      })
    return () => { cancelled = true }
  }, [routeId, period])

  const headerStyle = {
    fontSize: '0.95rem',
    marginBottom: '0.4rem',
    color: '#1e293b',
  }

  if (loading) {
    return (
      <div style={{ marginTop: '1.5rem' }}>
        <h3 style={headerStyle}>Narrative</h3>
        <p style={{ fontSize: '0.85rem', color: '#64748b' }}>Loading narrative…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div style={{ marginTop: '1.5rem' }}>
        <h3 style={headerStyle}>Narrative</h3>
        <p style={{ color: '#a00', fontSize: '0.85rem' }}>Error: {error}</p>
      </div>
    )
  }

  if (notFound) {
    return (
      <div style={{ marginTop: '1.5rem' }}>
        <h3 style={headerStyle}>Narrative</h3>
        <p style={{ fontSize: '0.85rem', color: '#94a3b8' }}>
          No narrative generated yet for this route and period.
          Run:{' '}
          <code
            style={{
              background: '#f1f5f9',
              padding: '0.1rem 0.35rem',
              borderRadius: '4px',
              fontSize: '0.8rem',
            }}
          >
            scripts/generate_route_diagnosis.py --route {routeId}
            {period && period !== 'all' ? ` --period ${period}` : ''}
          </code>
        </p>
      </div>
    )
  }

  if (!data) return null

  return (
    <div style={{ marginTop: '1.5rem' }}>
      <h3 style={headerStyle}>
        Narrative
        <InfoTip
          text={
            'AI-generated interpretation of this route\'s diagnostic profile. ' +
            'Generated offline from the materialized slip and timepoint data — ' +
            'Claude is never called when you load this page.'
          }
        />
      </h3>

      {data.is_stale && (
        <div
          style={{
            background: '#fefce8',
            border: '1px solid #facc15',
            borderRadius: '6px',
            padding: '0.5rem 0.75rem',
            marginBottom: '0.75rem',
            fontSize: '0.8rem',
            color: '#713f12',
          }}
        >
          <strong>Diagnosis is out of date.</strong> The diagnostic profile has changed
          since this narrative was generated. Re-run:{' '}
          <code
            style={{
              background: '#fef9c3',
              padding: '0.1rem 0.3rem',
              borderRadius: '3px',
              fontSize: '0.78rem',
            }}
          >
            scripts/generate_route_diagnosis.py --route {routeId}
            {period && period !== 'all' ? ` --period ${period}` : ''}
          </code>
        </div>
      )}

      <p
        style={{
          fontSize: '0.85rem',
          color: '#1e293b',
          lineHeight: 1.65,
          marginBottom: '0.5rem',
          whiteSpace: 'pre-wrap',
        }}
      >
        {data.narrative}
      </p>
      <div style={{ fontSize: '0.7rem', color: '#94a3b8' }}>
        Generated {data.generated_at ? data.generated_at.slice(0, 10) : 'unknown'} ·{' '}
        {data.model_id} · prompt {data.prompt_version}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Direction label helper
// ---------------------------------------------------------------------------

/**
 * Produce a human-readable direction label given direction_id and asymmetry data.
 * @param {number} directionId
 * @param {object|undefined} asymmetry
 * @returns {string}
 */
function directionLabel(directionId, asymmetry) {
  const dir = directionId === 0 ? 'Outbound (dir 0)' : 'Inbound (dir 1)'
  if (!asymmetry) return dir
  const sigMap = {
    early_dominant: '— mostly early',
    late_dominant: '— mostly late',
    balanced: '— balanced',
  }
  return `${dir} ${sigMap[asymmetry.signature] || ''}`
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

/**
 * Route diagnosis panel — slip trajectory chart + timepoint behavior table.
 *
 * Fetches GET /api/routes/{routeId}/diagnostic_profile?period={period} and
 * renders both sub-panels per direction. Returns null when there is no
 * materialized data for the route (normal before the pipeline has run).
 *
 * @param {{ routeId: string, period: string }} props
 */
function RouteDiagnosisPanel({ routeId, period }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setData(null)
    const params = new URLSearchParams()
    if (period && period !== 'all') params.set('period', period)
    const qs = params.toString()
    const url = `/api/routes/${routeId}/diagnostic_profile${qs ? `?${qs}` : ''}`
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
          setError(err?.message || String(err))
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [routeId, period])

  // Group segments and timepoints by direction
  const byDirection = useMemo(() => {
    if (!data) return {}
    const dirs = new Set([
      ...(data.segments || []).map((s) => s.direction_id),
      ...(data.timepoints || []).map((t) => t.direction_id),
    ])
    const out = {}
    for (const d of dirs) {
      out[d] = {
        segments: (data.segments || []).filter((s) => s.direction_id === d),
        timepoints: (data.timepoints || []).filter((t) => t.direction_id === d),
        asymmetry: (data.direction_asymmetry || []).find((a) => a.direction_id === d),
      }
    }
    return out
  }, [data])

  // Inject computed minute fields into segment rows so recharts can reference them
  // as dataKey strings — recharts can't call functions inside dataKey.
  const byDirectionWithMinutes = useMemo(() => {
    const out = {}
    for (const [dir, val] of Object.entries(byDirection)) {
      out[dir] = {
        ...val,
        segments: val.segments.map((s) => ({
          ...s,
          mean_slip_sec_min: s.mean_slip_sec / 60,
          cum_slip_sec_min: s.cum_slip_sec / 60,
        })),
      }
    }
    return out
  }, [byDirection])

  const directions = Object.keys(byDirectionWithMinutes)
    .map(Number)
    .sort((a, b) => a - b)

  const hasAnyData =
    data &&
    ((data.segments && data.segments.length > 0) ||
      (data.timepoints && data.timepoints.length > 0))

  if (loading) {
    return (
      <div className="chart-container">
        <h2>
          Diagnosis
          <InfoTip text="Slip trajectory and timepoint behavior — materialized from the last 30 days of stop_events." />
        </h2>
        <p style={{ fontSize: '0.85rem', color: '#64748b' }}>Loading diagnostic profile…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="chart-container">
        <h2>Diagnosis</h2>
        <p style={{ color: '#a00', fontSize: '0.85rem' }}>
          Error loading diagnostic profile: {error}
        </p>
      </div>
    )
  }

  if (!hasAnyData) {
    return (
      <div className="chart-container">
        <h2>Diagnosis</h2>
        <p style={{ fontSize: '0.85rem', color: '#94a3b8' }}>
          No diagnostic profile available for this route and period. The profile is
          materialized nightly by the batch pipeline — check back after the next run.
        </p>
      </div>
    )
  }

  return (
    <div className="chart-container">
      <h2>
        Diagnosis
        <InfoTip text="Slip trajectory and timepoint behavior — materialized from the last 30 days of stop_events." />
      </h2>

      {/* Slip chart section */}
      <div>
        <h3 style={{ fontSize: '0.95rem', marginBottom: '0.4rem', color: '#1e293b' }}>
          Slip
          <InfoTip text={SLIP_DEFINITION} />
          {' '}trajectory
        </h3>
        <p
          style={{
            fontSize: '0.8rem',
            color: '#64748b',
            marginBottom: '0.75rem',
            lineHeight: 1.5,
          }}
        >
          Bars show per-segment slip (red = late, green = recovery). Line shows cumulative
          slip from origin — a rising line means the bus is accumulating lateness; a drop at
          a{' '}
          <span title={TIMEPOINT_DEFINITION} style={{ textDecoration: 'underline dotted', cursor: 'help' }}>
            timepoint
          </span>{' '}
          (blue dot) means the schedule is absorbing delay there.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: directions.length > 1 ? '1fr 1fr' : '1fr',
            gap: '1.25rem',
          }}
        >
          {directions.map((d) => {
            const val = byDirectionWithMinutes[d]
            if (!val.segments.length) return null
            return (
              <SlipChart
                key={d}
                segments={val.segments}
                directionLabel={directionLabel(d, val.asymmetry)}
              />
            )
          })}
        </div>
      </div>

      {/* Timepoint behavior section */}
      <div style={{ marginTop: '1.5rem' }}>
        <h3 style={{ fontSize: '0.95rem', marginBottom: '0.4rem', color: '#1e293b' }}>
          Timepoint
          <InfoTip text={TIMEPOINT_DEFINITION} />
          {' '}behavior
        </h3>
        <p
          style={{
            fontSize: '0.8rem',
            color: '#64748b',
            marginBottom: '0.75rem',
            lineHeight: 1.5,
          }}
        >
          How each WMATA schedule checkpoint behaves in practice. Hover a badge
          for its definition. "Median entering" is the typical deviation arriving
          at the checkpoint; "median leaving" is after any hold.
        </p>
        {directions.map((d) => {
          const val = byDirectionWithMinutes[d]
          return (
            <TimepointTable
              key={d}
              timepoints={val.timepoints}
              directionLabel={directionLabel(d, val.asymmetry)}
            />
          )
        })}
      </div>

      {/* LLM narrative section (route diagnosis narrative, PR #141) */}
      <NarrativeSection routeId={routeId} period={period} />
    </div>
  )
}

export default RouteDiagnosisPanel
