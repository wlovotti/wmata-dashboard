import { useEffect, useState } from 'react'
import useMultiFetch from '../hooks/useMultiFetch'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { formatContribMetricValue } from '../utils/formatters'
import SystemTrend from './SystemTrend'
import WhatChangedPanel from './WhatChangedPanel'

// Available metrics for the "Where to look" contributors panel. Mirrors the
// constant in RouteList.jsx — kept inline here to avoid threading a shared
// module just for a 4-row constant. If/when a third surface needs the same
// list, lift to a shared module.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const CONTRIB_TOP_N = 5

// Off-target panel (NOTES-53). The panel ranks routes by their gap to a
// configured per-route target — distinct from "Where to look," which is
// volume-weighted contribution. We only include routes that appear in
// `/api/targets`'s `routes` block (i.e. with an explicit override),
// otherwise the panel would just re-rank the same routes the contributors
// panel ranks, against the shared system default.
const OFF_TARGET_TOP_N = 10

/**
 * For a given metric, pull the current per-route value from the scorecard
 * row in the same canonical units that `route_targets.yaml` exposes via the
 * `targets` block on /api/routes. Returns null when the underlying live
 * field is missing.
 *
 * OTP is already in percent (0-100). service_delivered and bunching live
 * as 0-1 fractions on the scorecard row; the matching target on `r.targets`
 * is the same 0-1 fraction (NOTES-47 canonical units). EWT is seconds on
 * both sides.
 */
function currentForMetric(metric, row) {
  if (!row) return null
  if (metric === 'otp') return row.otp_all_pct ?? null
  if (metric === 'service_delivered') return row.service_delivered_ratio ?? null
  if (metric === 'ewt') return row.ewt_seconds ?? null
  if (metric === 'bunching') return row.bunching_rate ?? null
  return null
}

/**
 * Format a route's gap to target with units appropriate to the metric. For
 * OTP / service_delivered / bunching we report percentage points (pp); for
 * EWT we report seconds. The sign + suffix communicates direction:
 * "below target" for the wrong-side, "above target" for the right-side.
 *
 * Returns the suffixed string only — callers tint / format the whole row.
 */
function formatGap(metric, current, target) {
  if (current == null || target == null) return null
  // Convert to display units (percent points for the 0-1 metrics) so the gap
  // reads "13 pp below" instead of "0.13 below."
  let currentDisp = current
  let targetDisp = target
  let unit = ''
  let higherIsBetter = true
  if (metric === 'otp') {
    unit = 'pp'
    higherIsBetter = true
  } else if (metric === 'service_delivered') {
    currentDisp = current * 100
    targetDisp = target * 100
    unit = 'pp'
    higherIsBetter = true
  } else if (metric === 'ewt') {
    unit = 's'
    higherIsBetter = false
  } else if (metric === 'bunching') {
    currentDisp = current * 100
    targetDisp = target * 100
    unit = 'pp'
    higherIsBetter = false
  }
  // Signed gap from the route's perspective: negative = below target,
  // positive = above target. For lower-is-better metrics we flip so the
  // "below target" label always means "underperforming."
  const signedGap = higherIsBetter ? currentDisp - targetDisp : targetDisp - currentDisp
  const magnitude = Math.abs(signedGap)
  const rounded = unit === 's' ? Math.round(magnitude) : magnitude.toFixed(1)
  const direction = signedGap < 0 ? 'below target' : 'above target'
  const sign = signedGap < 0 ? '-' : '+'
  return { text: `${sign}${rounded} ${unit} ${direction}`, isBelow: signedGap < 0 }
}

/**
 * Pull the most recent non-null value from a `trend_data` series. Used to
 * derive a current system reading for the health pulse from whichever
 * service date last produced a metric — typically today, but falls back to
 * yesterday on early-morning hits before the daily pipeline has run.
 */
function latestNonNull(series, key) {
  if (!Array.isArray(series)) return null
  for (let i = series.length - 1; i >= 0; i--) {
    const value = series[i]?.[key]
    if (value != null) return value
  }
  return null
}

/**
 * Compute a single metric's "gap to target" expressed as a normalized
 * fraction so the worst metric across the four can be picked apples-to-apples.
 *
 * Returns a positive number when the current value is on the wrong side of
 * the target (worse), negative when on the right side (better), and null
 * when either side is missing. The magnitude is `(gap / target)` so a 10%
 * miss looks the same on OTP (5pp shy of 50% → 0.10) as on EWT (10s over
 * a 100s target → 0.10) — crude but fit for the worst-of-four pick.
 */
function gapFraction({ current, target, higherIsBetter }) {
  if (current == null || target == null || target === 0) return null
  const rawGap = higherIsBetter ? target - current : current - target
  return rawGap / Math.abs(target)
}

/**
 * Health-pulse banner renderer — system-wide single-line status. Tints the
 * banner red / yellow / green based on the worst-of-four normalized gap
 * across OTP, service-delivered, EWT, bunching. The "X routes below
 * target" piece counts routes whose any metric is on the wrong side of
 * its configured/inherited target. Period-over-period deltas are
 * intentionally omitted from this banner — the `deltas` block on the
 * scorecard payload (PR #125) carries them per-route for RouteList /
 * RouteDetail; surfacing them here is the What changed panel (PR #138).
 */
function HealthPulse({ systemMetrics, scorecard }) {
  // Pick the worst metric — the one with the largest positive gapFraction.
  // Threshold mapping: < 0 (meeting target) → green, 0..0.1 → yellow,
  // > 0.1 → red. The 10% threshold is the same band the per-route
  // SpectrumBar uses (utils/spectrumBar.js), so visual semantics line up
  // across surfaces.
  let worstGap = null
  for (const m of systemMetrics) {
    const gap = gapFraction({
      current: m.current,
      target: m.target,
      higherIsBetter: m.higherIsBetter,
    })
    if (gap == null) continue
    if (worstGap == null || gap > worstGap) worstGap = gap
  }

  let tint = 'health-pulse-green'
  if (worstGap == null) tint = 'health-pulse-neutral'
  else if (worstGap > 0.1) tint = 'health-pulse-red'
  else if (worstGap > 0) tint = 'health-pulse-yellow'

  // Count routes that miss any of their four targets. A route counts if at
  // least one of its four metrics has both a current value and a target
  // and is on the wrong side of that target. Routes with no live data
  // (no derived stop_events for the window) are excluded — they're not
  // "below target," they're unmeasured.
  let routesBelowTarget = 0
  let routesEvaluated = 0
  for (const r of scorecard?.routes ?? []) {
    const targets = r.targets || {}
    const checks = [
      { current: r.otp_all_pct, target: targets.otp, higherIsBetter: true },
      {
        current:
          r.service_delivered_ratio != null ? r.service_delivered_ratio * 100 : null,
        target:
          targets.service_delivered != null ? targets.service_delivered * 100 : null,
        higherIsBetter: true,
      },
      { current: r.ewt_seconds, target: targets.ewt, higherIsBetter: false },
      {
        current: r.bunching_rate != null ? r.bunching_rate * 100 : null,
        target: targets.bunching != null ? targets.bunching * 100 : null,
        higherIsBetter: false,
      },
    ]
    let hasAnyMeasurement = false
    let isBelow = false
    for (const c of checks) {
      if (c.current == null || c.target == null) continue
      hasAnyMeasurement = true
      const gap = c.higherIsBetter ? c.target - c.current : c.current - c.target
      if (gap > 0) {
        isBelow = true
        break
      }
    }
    if (hasAnyMeasurement) {
      routesEvaluated += 1
      if (isBelow) routesBelowTarget += 1
    }
  }

  // Build the headline. OTP is the system's primary KPI by convention so
  // it's always shown when available; the other metrics only render in
  // the headline if OTP is missing (rare — the system trend backstops
  // even when today's pipeline hasn't run).
  const otpEntry = systemMetrics.find((m) => m.key === 'otp')
  const headlineParts = []
  if (otpEntry?.current != null) {
    headlineParts.push(`OTP ${Math.round(otpEntry.current)}%`)
  } else {
    // Fall back to the first non-null metric so the banner never reads as
    // a bare "0 routes below target."
    const fallback = systemMetrics.find((m) => m.current != null)
    if (fallback) {
      headlineParts.push(
        `${fallback.label} ${formatContribMetricValue(fallback.key, fallback.current)}`,
      )
    }
  }
  if (routesEvaluated > 0) {
    headlineParts.push(
      `${routesBelowTarget} of ${routesEvaluated} routes below target`,
    )
  }

  const text = headlineParts.length > 0 ? headlineParts.join(' · ') : 'System status unavailable'

  return (
    <div className={`health-pulse ${tint}`} role="status">
      <span className="health-pulse-text">{text}</span>
    </div>
  )
}

// Static URL list for the four system trend metrics. Defined outside Overview
// so the array reference is stable and useMultiFetch doesn't re-fetch on
// every render.
const OVERVIEW_TREND_URLS = [
  '/api/system/trend?metric=otp&days=30',
  '/api/system/trend?metric=service_delivered&days=30',
  '/api/system/trend?metric=ewt&days=30',
  '/api/system/trend?metric=bunching&days=30',
]

/**
 * Overview landing page (PR #105). Single screen that answers "are we OK
 * right now, and where should I look?" without parsing the full route
 * table. Composed of:
 *
 *   1. HealthPulse — single-line status banner tinted by worst-of-four gap
 *   2. SystemTrend — reused unchanged from RouteList for the 30-day view
 *   3. Where to look — top-5 contributors for the selected metric
 *   4. Footer — link out to /routes for the full table
 *
 * Pure aggregator: every data source is an existing endpoint
 * (`/api/system/trend`, `/api/routes`, `/api/routes/contributors`). No
 * backend changes for this page.
 */
function Overview() {
  const navigate = useNavigate()
  const [scorecard, setScorecard] = useState(null)
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)
  // Off-target panel (NOTES-53). One fetch of `/api/targets` gives us the
  // override set; per-route current values come from `scorecard` so we can
  // share a single network round-trip with the health pulse and the "X
  // routes below target" count.
  const [targetsData, setTargetsData] = useState(null)

  // Fetch scorecard once for the "X routes below target" count. The
  // /api/routes endpoint is cached server-side (60s TTL) so the cost is
  // amortized across this page and /routes.
  useEffect(() => {
    let cancelled = false
    fetch('/api/routes')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setScorecard(json)
      })
      .catch(() => {
        // Health pulse degrades gracefully when scorecard is unavailable —
        // the headline drops the routes count rather than blocking render.
      })
    return () => {
      cancelled = true
    }
  }, [])

  // Fetch the four system trend payloads to read the latest non-null value
  // and target_value for each metric. Same data <SystemTrend> reads, so
  // there's no extra network cost per metric beyond the parallel fan-out
  // that component already does — both calls hit the cached path.
  // AbortController cancellation is handled inside useMultiFetch.
  const { data: rawSystemTrendData } = useMultiFetch(
    OVERVIEW_TREND_URLS,
    ([otp, sd, ewt, bun]) => ({ otp, service_delivered: sd, ewt, bunching: bun }),
  )
  const systemTrendData = rawSystemTrendData ?? null

  // One-shot fetch for the off-target panel's override set. The endpoint is
  // a static-ish YAML read on the backend (mtime-cached in
  // `src/route_targets.py`), so no need to refetch on metric switch — the
  // panel filters the cached payload client-side.
  useEffect(() => {
    let cancelled = false
    fetch('/api/targets')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setTargetsData(json)
      })
      .catch(() => {
        // Off-target panel falls back to its error / empty state. Don't
        // block the rest of Overview on a missing targets endpoint.
      })
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    setContribLoading(true)
    setContribError(null)
    fetch(`/api/routes/contributors?metric=${contribMetric}&days=30`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setContribData(json)
          setContribLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setContribError(err.message || String(err))
          setContribLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [contribMetric])

  // Build the system metrics array the HealthPulse uses. Each entry
  // captures the current reading (most recent non-null value from the
  // trend series), the target_value the trend endpoint already emits, and
  // the metric's higher-is-better orientation so the gap math works.
  const systemMetrics = [
    {
      key: 'otp',
      label: 'OTP',
      higherIsBetter: true,
      current: latestNonNull(systemTrendData?.otp?.trend_data, 'otp_percentage'),
      target: systemTrendData?.otp?.target_value ?? null,
    },
    {
      key: 'service_delivered',
      label: 'Service Delivered',
      higherIsBetter: true,
      current: (() => {
        const v = latestNonNull(
          systemTrendData?.service_delivered?.trend_data,
          'service_delivered_ratio',
        )
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.service_delivered?.target_value != null
          ? systemTrendData.service_delivered.target_value * 100
          : null,
    },
    {
      key: 'ewt',
      label: 'EWT',
      higherIsBetter: false,
      current: latestNonNull(systemTrendData?.ewt?.trend_data, 'ewt_seconds'),
      target: systemTrendData?.ewt?.target_value ?? null,
    },
    {
      key: 'bunching',
      label: 'Bunching',
      higherIsBetter: false,
      current: (() => {
        const v = latestNonNull(systemTrendData?.bunching?.trend_data, 'bunching_rate')
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.bunching?.target_value != null
          ? systemTrendData.bunching.target_value * 100
          : null,
    },
  ]

  const visibleContributors = (contribData?.contributors ?? []).slice(0, CONTRIB_TOP_N)

  // Off-target rows for the selected metric (NOTES-53). Build by joining
  // the override set from `/api/targets` against the per-route current
  // values + targets on the scorecard. `targetsData.routes` keys are
  // route_ids; each value is a partial-override block (subset of the four
  // metrics). We filter to routes that override the *selected* metric so
  // the panel is target-driven rather than re-ranking the entire scorecard.
  const offTargetRows = (() => {
    if (!targetsData || !scorecard) return []
    const overrides = targetsData.routes || {}
    const byRouteId = new Map(
      (scorecard.routes || []).map((r) => [r.route_id, r]),
    )
    const rows = []
    for (const [routeId, overrideBlock] of Object.entries(overrides)) {
      // Only routes whose override block names the selected metric. A
      // route with an override for OTP but not EWT shouldn't appear under
      // the EWT cut — its EWT target is the inherited system default.
      if (!overrideBlock || overrideBlock[contribMetric] == null) continue
      const row = byRouteId.get(routeId)
      if (!row) continue
      const target = overrideBlock[contribMetric]
      const current = currentForMetric(contribMetric, row)
      const gap = formatGap(contribMetric, current, target)
      if (gap == null) continue
      // signedGap (route-perspective) for sorting: most-below first.
      // For higher-is-better, "below target" is current < target → (current - target) < 0;
      // for lower-is-better we flipped the sign in formatGap, so the magnitude is the
      // same but we need a sortable scalar. Recompute here for clarity.
      const higherIsBetter = contribMetric === 'otp' || contribMetric === 'service_delivered'
      const cDisp =
        contribMetric === 'service_delivered' || contribMetric === 'bunching'
          ? current * 100
          : current
      const tDisp =
        contribMetric === 'service_delivered' || contribMetric === 'bunching'
          ? target * 100
          : target
      const signedGap = higherIsBetter ? cDisp - tDisp : tDisp - cDisp
      rows.push({
        routeId,
        routeShortName: row.route_name,
        routeLongName: row.route_long_name,
        current,
        target,
        gapText: gap.text,
        isBelow: gap.isBelow,
        signedGap,
      })
    }
    // Sort most-below-target first (ascending signedGap — most negative first).
    rows.sort((a, b) => a.signedGap - b.signedGap)
    return rows.slice(0, OFF_TARGET_TOP_N)
  })()

  // Distinguish "no overrides configured at all" (the spec's empty-state
  // message) from "overrides exist but none for this metric" (more honest
  // sub-message). `targetsData.routes` can be `{}` when YAML is the
  // out-of-box default.
  const hasAnyOverrides =
    targetsData != null &&
    targetsData.routes &&
    Object.keys(targetsData.routes).length > 0

  return (
    <main>
      <HealthPulse systemMetrics={systemMetrics} scorecard={scorecard} />

      <SystemTrend />

      <div className="table-container">
        <h2>Where to look</h2>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Top {CONTRIB_TOP_N} routes ranked by their contribution to system
          underperformance — the routes whose attention would move the
          system the most.
        </p>
        <div className="filters" style={{ marginBottom: '0.75rem' }}>
          <div>
            <label htmlFor="overview-contrib-metric" style={{ marginRight: '0.5rem' }}>
              Metric:
            </label>
            <select
              id="overview-contrib-metric"
              value={contribMetric}
              onChange={(e) => setContribMetric(e.target.value)}
            >
              {CONTRIB_METRICS.map((m) => (
                <option key={m.key} value={m.key}>
                  {m.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {contribError && (
          <p style={{ color: '#64748b' }}>Unable to load contributors: {contribError}</p>
        )}

        {contribLoading ? (
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading contributors...</p>
          </div>
        ) : contribData == null ? null : visibleContributors.length === 0 ? (
          <p>No routes have enough data to score contribution for this metric yet.</p>
        ) : (
          <table className="routes-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Route</th>
                <th>Name</th>
                <th>Route value</th>
                <th title="Per-route target if configured, otherwise system 30-day baseline">
                  Reference
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleContributors.map((c, idx) => (
                <tr
                  key={c.route_id}
                  onClick={() => navigate(`/route/${c.route_id}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <td>{idx + 1}</td>
                  <td className="route-id">
                    <span
                      className="route-badge"
                      style={{ backgroundColor: badgeColor(null, true) }}
                    >
                      {c.route_short_name || c.route_id}
                    </span>
                  </td>
                  <td className="route-name">{c.route_long_name || 'N/A'}</td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.route_value)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(
                      contribMetric,
                      c.reference_value ?? c.baseline_value,
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ marginTop: '1rem' }}>
          <Link to="/routes" className="see-all-link">
            See all routes →
          </Link>
        </div>
      </div>

      <div className="table-container">
        <h2>Off target</h2>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Routes with a configured per-route target in{' '}
          <code>config/route_targets.yaml</code>, ranked by gap to that
          target on the metric selected above. Complementary to "Where to
          look" — a small-volume route can be far off target without
          showing up as a big system contributor.
        </p>

        {!hasAnyOverrides ? (
          <p>
            Set per-route targets in <code>config/route_targets.yaml</code>{' '}
            to populate this view.
          </p>
        ) : offTargetRows.length === 0 ? (
          <p>
            No per-route overrides configured for{' '}
            {CONTRIB_METRICS.find((m) => m.key === contribMetric)?.label ??
              contribMetric}
            .
          </p>
        ) : (
          <table className="routes-table">
            <thead>
              <tr>
                <th>Route</th>
                <th>Name</th>
                <th>Route value</th>
                <th>Target</th>
                <th>Gap</th>
              </tr>
            </thead>
            <tbody>
              {offTargetRows.map((r) => (
                <tr
                  key={r.routeId}
                  onClick={() => navigate(`/route/${r.routeId}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <td className="route-id">
                    <span
                      className="route-badge"
                      style={{ backgroundColor: badgeColor(null, true) }}
                    >
                      {r.routeShortName || r.routeId}
                    </span>
                  </td>
                  <td className="route-name">{r.routeLongName || 'N/A'}</td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, r.current)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, r.target)}
                  </td>
                  <td
                    className="metric"
                    style={{ color: r.isBelow ? '#b91c1c' : '#15803d' }}
                  >
                    {r.gapText}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* What changed panel (PR #138). Reuses the scorecard fetch already
          issued for HealthPulse and Off-target — no additional network
          round-trip. */}
      <WhatChangedPanel routes={scorecard?.routes ?? null} />

    </main>
  )
}

export default Overview
