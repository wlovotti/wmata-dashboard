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
import useAgency, { AGENCY_LABELS, DEFAULT_AGENCY } from '../hooks/useAgency'
import { apiUrl } from '../utils/apiUrl'
import AgencyUnavailable from './AgencyUnavailable'
import { CHART_MARGIN, SERIES_COLOR } from '../charts/theme'
import './RouteDiagnosisPanel.css'

// Timepoint (WMATA schedule-checkpoint) marker color. Distinct blue, not one
// of the six semantic tokens (it marks a stop's role, not a status), reused
// across the slip chart and the "●" legend swatch below it.
const TIMEPOINT_COLOR = '#3b82f6'

// Deviation-table accent for "notably early" (below -30s) — distinct from
// the ordinary early/late gray so a meaningfully-early median stands out.
const DEV_EARLY_COLOR = '#2563eb'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CLASSIFICATION_LABELS = {
  recovery: 'Recovery',
  leaky: 'Leaky',
  underpowered: 'Underpowered',
  neutral: 'Neutral',
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
 * Color for a "median deviation" table cell — genuinely data-driven (three
 * bands off the numeric value), so this stays a computed value rather than
 * a CSS class.
 * @param {number|null} sec
 * @returns {string} CSS color value
 */
function deviationColor(sec) {
  if (sec == null) return 'var(--text-secondary)'
  if (sec > 0) return 'var(--color-bad)'
  if (sec < -30) return DEV_EARLY_COLOR
  return 'var(--text-secondary)'
}

/**
 * Inline tooltip anchor — renders a "?" superscript that shows `text` on hover.
 * @param {{ text: string }} props
 */
function InfoTip({ text }) {
  return (
    <span title={text} className="info-tip">
      ?
    </span>
  )
}

/**
 * Classification badge for the timepoint behavior table.
 * @param {{ classification: string }} props
 */
function ClassificationBadge({ classification }) {
  const key = CLASSIFICATION_LABELS[classification] ? classification : 'neutral'
  const label = CLASSIFICATION_LABELS[key]
  const tip = CLASSIFICATION_TOOLTIPS[key] || ''
  return (
    <span title={tip} className={`classification-badge classification-${key}`}>
      {label}
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
    <div className="slip-tooltip">
      <div className="slip-tooltip-header">
        Seq {d.from_seq} → {d.to_seq}
        {d.is_timepoint && <span className="slip-tooltip-timepoint-tag">Timepoint</span>}
      </div>
      <div className="slip-tooltip-stops">
        {from} → {to}
      </div>
      <div>
        Per-segment slip:{' '}
        <strong style={{ color: d.mean_slip_sec > 0 ? 'var(--color-bad)' : 'var(--color-good)' }}>
          {fmtSec(d.mean_slip_sec)}
        </strong>
      </div>
      <div>
        Cumulative slip:{' '}
        <strong>{fmtSec(d.cum_slip_sec)}</strong>
      </div>
      <div className="slip-tooltip-meta">{d.n_observations} observations</div>
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
      <div className="direction-label">{directionLabel}</div>
      <ResponsiveContainer width="100%" height={200}>
        <ComposedChart data={chartData} margin={CHART_MARGIN}>
          <XAxis
            dataKey="from_seq"
            tick={{ fontSize: 10 }}
            tickLine={false}
            label={{
              value: 'stop sequence →',
              position: 'insideBottomRight',
              offset: -4,
              fontSize: 9,
              fill: SERIES_COLOR.neutral,
            }}
          />
          <YAxis
            domain={yDomain}
            tick={{ fontSize: 10 }}
            tickFormatter={(v) => `${v}m`}
            width={36}
          />
          <RechartsTooltip content={<SlipTooltip />} />
          <ReferenceLine y={0} stroke={SERIES_COLOR.neutral} strokeWidth={1} />
          {timepointSeqs.map((seq) => (
            <ReferenceLine
              key={seq}
              x={seq}
              stroke={TIMEPOINT_COLOR}
              strokeDasharray="3 3"
              strokeWidth={1}
            />
          ))}
          <Bar dataKey="mean_slip_sec_min" name="Per-seg slip" barSize={6} radius={1}>
            {chartData.map((entry, idx) => (
              <Cell
                key={idx}
                fill={entry.mean_slip_sec > 0 ? SERIES_COLOR.bad : SERIES_COLOR.good}
                fillOpacity={0.75}
              />
            ))}
          </Bar>
          <Line
            data={lineData}
            type="monotone"
            dataKey="cum_slip_sec_min"
            stroke="var(--text-primary)"
            strokeWidth={2}
            dot={(props) => {
              const entry = props.payload
              if (!entry?.is_timepoint) {
                return (
                  <circle
                    key={props.key}
                    cx={props.cx}
                    cy={props.cy}
                    r={2}
                    fill="var(--text-primary)"
                  />
                )
              }
              return (
                <circle
                  key={props.key}
                  cx={props.cx}
                  cy={props.cy}
                  r={5}
                  fill={TIMEPOINT_COLOR}
                  stroke="var(--surface-card)"
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
        <div className="timepoint-label-strip">
          <span style={{ color: TIMEPOINT_COLOR }} className="font-semibold">● </span>
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
    return <div className="panel-note-xs">No timepoint data for this direction.</div>
  }

  return (
    <div className="timepoint-table-wrap">
      <div className="direction-label">{directionLabel}</div>
      <div className="table-scroll-x">
        <table className="timepoint-table">
          <thead>
            <tr>
              <th>Timepoint</th>
              <th className="text-center">
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
              <th className="text-right">
                Median entering
                <InfoTip text="Median schedule deviation at the stop just before this timepoint." />
              </th>
              <th className="text-right">
                Median leaving
                <InfoTip text="Median schedule deviation at the timepoint itself." />
              </th>
              <th className="text-right">
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
                <tr key={tp.timepoint_stop_id} className={i % 2 === 1 ? 'row-odd' : undefined}>
                  <td>
                    <span className="font-medium">{tp.stop_name || tp.timepoint_stop_id}</span>
                    <span className="timepoint-stop-id">#{tp.timepoint_stop_id}</span>
                  </td>
                  <td className="text-center">
                    <ClassificationBadge classification={tp.classification} />
                  </td>
                  <td className="text-right nums">
                    <span style={{ color: deviationColor(tp.median_dev_entering) }}>
                      {fmtSec(tp.median_dev_entering)}
                    </span>
                  </td>
                  <td className="text-right nums">
                    <span style={{ color: deviationColor(tp.median_dev_leaving) }}>
                      {fmtSec(tp.median_dev_leaving)}
                    </span>
                  </td>
                  <td className="text-right nums">
                    {p10Change != null ? (
                      <span
                        style={{ color: p10Change > 60 ? 'var(--color-warn)' : 'var(--text-secondary)' }}
                      >
                        {fmtSec(p10Change)}
                      </span>
                    ) : (
                      <span className="text-muted">N/A</span>
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
function NarrativeSection({ routeId, period, agency }) {
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
    const params = {}
    if (period && period !== 'all') params.period = period
    const url = apiUrl(`/api/routes/${routeId}/diagnosis`, params)
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
  }, [routeId, period, agency])

  if (loading) {
    return (
      <div className="mt-6">
        <h3 className="diagnosis-section-heading">Narrative</h3>
        <p className="panel-loading-text">Loading narrative…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="mt-6">
        <h3 className="diagnosis-section-heading">Narrative</h3>
        <p className="panel-error-text">Error: {error}</p>
      </div>
    )
  }

  if (notFound) {
    return (
      <div className="mt-6">
        <h3 className="diagnosis-section-heading">Narrative</h3>
        <p className="panel-note-xs">
          No narrative generated yet for this route and period.
          Run:{' '}
          <code className="narrative-code">
            scripts/generate_route_diagnosis.py --route {routeId}
            {period && period !== 'all' ? ` --period ${period}` : ''}
          </code>
        </p>
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="mt-6">
      <h3 className="diagnosis-section-heading">
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
        <div className="narrative-stale-banner">
          <strong>Diagnosis is out of date.</strong> The diagnostic profile has changed
          since this narrative was generated. Re-run:{' '}
          <code>
            scripts/generate_route_diagnosis.py --route {routeId}
            {period && period !== 'all' ? ` --period ${period}` : ''}
          </code>
        </div>
      )}

      <p className="narrative-text">{data.narrative}</p>
      <div className="narrative-meta">
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
 * WMATA-only (NOTES-143): for any other agency, skips the fetch entirely
 * and renders a short "not available for this agency" card instead —
 * see `unavailable` below.
 *
 * @param {{ routeId: string, period: string }} props
 */
function RouteDiagnosisPanel({ routeId, period }) {
  const [agency] = useAgency()
  // WMATA-only (NOTES-143): the diagnostic profile is materialized from
  // `timepoints`, which uses GTFS-Plus internal stop_ids WMATA publishes
  // and SFMTA does not (see CLAUDE.md's "timepoints uses GTFS-Plus
  // internal stop_ids" gotcha). Rather than let a non-wmata request 200
  // with an empty profile and render the generic "no diagnostic profile"
  // message — which reads as "not generated yet for this WMATA route,"
  // not "not available for this agency" — skip the fetch entirely and
  // show a dedicated unavailable card below.
  const unavailable = agency !== DEFAULT_AGENCY

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (unavailable) {
      setLoading(false)
      return
    }
    let cancelled = false
    setLoading(true)
    setError(null)
    setData(null)
    const params = {}
    if (period && period !== 'all') params.period = period
    const url = apiUrl(`/api/routes/${routeId}/diagnostic_profile`, params)
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
  }, [routeId, period, agency, unavailable])

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

  if (unavailable) {
    return (
      <div className="chart-container">
        <h2>Diagnosis</h2>
        <AgencyUnavailable
          agencyLabel={AGENCY_LABELS[agency] || agency}
          reason="The slip trajectory and timepoint behavior panels are built from WMATA's GTFS-Plus timepoint data, which this agency doesn't publish."
        />
      </div>
    )
  }

  if (loading) {
    return (
      <div className="chart-container">
        <h2>
          Diagnosis
          <InfoTip text="Slip trajectory and timepoint behavior — materialized from the last 30 days of stop_events." />
        </h2>
        <p className="panel-loading-text">Loading diagnostic profile…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="chart-container">
        <h2>Diagnosis</h2>
        <p className="panel-error-text">Error loading diagnostic profile: {error}</p>
      </div>
    )
  }

  if (!hasAnyData) {
    return (
      <div className="chart-container">
        <h2>Diagnosis</h2>
        <p className="panel-note-xs">
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
        <h3 className="diagnosis-section-heading">
          Slip
          <InfoTip text={SLIP_DEFINITION} />
          {' '}trajectory
        </h3>
        <p className="diagnosis-section-note">
          Bars show per-segment slip (red = late, green = recovery). Line shows cumulative
          slip from origin — a rising line means the bus is accumulating lateness; a drop at
          a{' '}
          <span title={TIMEPOINT_DEFINITION} className="diagnosis-glossary-term">
            timepoint
          </span>{' '}
          (blue dot) means the schedule is absorbing delay there.
        </p>
        <div className={`slip-chart-grid ${directions.length > 1 ? 'slip-chart-grid-2up' : ''}`}>
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
      <div className="mt-6">
        <h3 className="diagnosis-section-heading">
          Timepoint
          <InfoTip text={TIMEPOINT_DEFINITION} />
          {' '}behavior
        </h3>
        <p className="diagnosis-section-note">
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
      <NarrativeSection routeId={routeId} period={period} agency={agency} />
    </div>
  )
}

export default RouteDiagnosisPanel
