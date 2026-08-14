import { useEffect, useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { formatContribMetricValue } from '../utils/formatters'

/**
 * Format a target value for display, given the canonical metric key. Mirrors
 * the unit conventions used everywhere else: OTP percent, service-delivered
 * fraction (rendered as %), EWT seconds (rendered as minutes for operator
 * readability), bunching fraction (rendered as %).
 */
function formatTarget(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${value.toFixed(0)}%`
  if (metric === 'service_delivered') return `${(value * 100).toFixed(0)}%`
  if (metric === 'ewt') return `${(value / 60).toFixed(1)} min`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

const METRIC_ORDER = ['otp', 'service_delivered', 'ewt', 'bunching']
const METRIC_LABELS = {
  otp: 'OTP',
  service_delivered: 'Service Delivered',
  ewt: 'EWT',
  bunching: 'Bunching',
}

// Off-target panel (NOTES-53, re-homed from Overview.jsx by NOTES-84). The
// panel ranks routes by their gap to a configured per-route target —
// distinct from Overview's "Biggest drags," which is volume-weighted
// contribution. We only include routes that appear in this page's `routes`
// block (i.e. with an explicit override), otherwise the panel would just
// re-rank the same routes against the shared system default.
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
 * Off-target panel body when there are no rows to rank (the frontend-chrome
 * honesty fixes, PR #204).
 *
 * The panel only ever shows routes with a hand-edited override in
 * `config/route_targets.yaml` (`routes:` block) — it is empty out of the
 * box (`routes: {}`) and stays empty until someone commits an override, not
 * because of a bug or missing data. Distinguishes that "nothing has been
 * configured at all" case from "overrides exist, just none for the
 * currently-selected metric," since the latter is a narrower, more
 * reassuring claim (the config isn't empty, this metric just isn't in it).
 *
 * A third, distinct case: `targetsData` hasn't loaded yet (fetch still in
 * flight) or failed (swallowed by the page's `.catch`, which degrades to
 * the empty state rather than blocking render). Neither of those is "the
 * config file is empty" — asserting that while we don't actually know the
 * config's contents would be its own dishonesty, so this case gets a
 * neutral message instead of the confident claim about
 * `route_targets.yaml`.
 *
 * @param {boolean} hasAnyOverrides - whether this page's `routes` block
 *   has at least one entry, for any metric. Only meaningful when
 *   `targetsLoaded` is true.
 * @param {boolean} targetsLoaded - whether `/api/targets` has resolved
 *   (successfully or not) — i.e. `data != null` on the page. False
 *   while the fetch is in flight, or forever if it failed.
 * @param {string} metricLabel - display label of the currently-selected
 *   metric (e.g. "EWT"), used only in the narrower sub-message.
 */
export function OffTargetEmptyState({ hasAnyOverrides, targetsLoaded, metricLabel }) {
  if (!targetsLoaded) {
    return (
      <p style={{ padding: '0 1.5rem 1.5rem' }}>
        Per-route target configuration isn't available right now, so this
        panel can't say whether any overrides are configured.
      </p>
    )
  }
  if (!hasAnyOverrides) {
    return (
      <p style={{ padding: '0 1.5rem 1.5rem' }}>
        This panel is empty because no route has a per-route target — the
        default <code>config/route_targets.yaml</code> ships with an empty{' '}
        <code>routes:</code> block, so every route currently just inherits
        the same system-wide target already summarized in the Overview
        page's "Biggest drags" table. It only populates after someone
        hand-edits that file to add per-route overrides (see the file's
        header comment for the schema).
      </p>
    )
  }
  return (
    <p style={{ padding: '0 1.5rem 1.5rem' }}>
      No per-route overrides configured for {metricLabel}.
    </p>
  )
}

/**
 * `/targets` page (PR #105). Read-only renderer over
 * `config/route_targets.yaml` — the system defaults plus per-route
 * overrides — so an operator can see what the dashboard's "vs target"
 * comparisons compare against without opening the YAML.
 *
 * Editing stays git-only. NOTES-47's design explicitly allowed the
 * read-only-in-UI / edit-in-yaml split because the targets are a
 * commitment artifact (low edit frequency, want the change in version
 * control with the rest of the config).
 *
 * Also hosts the off-target panel (NOTES-53, re-homed here from Overview
 * by NOTES-84): routes with a per-route override ranked by gap to that
 * target on a selected metric, using a second fetch of `/api/routes` for
 * each route's current live value.
 */
function Targets() {
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  // Off-target panel (NOTES-53). Per-route current values come from the
  // scorecard so the panel can compute each override's gap.
  const [scorecard, setScorecard] = useState(null)
  const [offTargetMetric, setOffTargetMetric] = useState('otp')

  useEffect(() => {
    let cancelled = false
    fetch('/api/targets')
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
  }, [])

  // Fetch scorecard once for the off-target panel's current-value column.
  // The /api/routes endpoint is cached server-side (60s TTL) so the cost is
  // amortized across this page and Overview/RouteList.
  useEffect(() => {
    let cancelled = false
    fetch('/api/routes')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setScorecard(json)
      })
      .catch(() => {
        // Off-target panel degrades gracefully when scorecard is
        // unavailable — its rows are simply empty rather than an error.
      })
    return () => {
      cancelled = true
    }
  }, [])

  if (loading) {
    return (
      <main>
        <div className="chart-container">
          <h2>Performance targets</h2>
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading targets...</p>
          </div>
        </div>
      </main>
    )
  }

  if (error) {
    return (
      <main>
        <div className="chart-container">
          <h2>Performance targets</h2>
          <p style={{ color: '#64748b' }}>Unable to load targets: {error}</p>
        </div>
      </main>
    )
  }

  const systemDefault = data?.system_default || {}
  const routes = data?.routes || {}
  const sortedRouteIds = Object.keys(routes).sort()

  // Off-target rows for the selected metric (NOTES-53). Build by joining
  // the override set from this page's own `/api/targets` fetch (`routes`,
  // above) against the per-route current values + targets on the
  // scorecard. `routes` keys are route_ids; each value is a partial-override
  // block (subset of the four metrics). We filter to routes that override
  // the *selected* metric so the panel is target-driven rather than
  // re-ranking the entire scorecard.
  const offTargetRows = (() => {
    if (!data || !scorecard) return []
    const overrides = data.routes || {}
    const byRouteId = new Map((scorecard.routes || []).map((r) => [r.route_id, r]))
    const rows = []
    for (const [routeId, overrideBlock] of Object.entries(overrides)) {
      // Only routes whose override block names the selected metric. A
      // route with an override for OTP but not EWT shouldn't appear under
      // the EWT cut — its EWT target is the inherited system default.
      if (!overrideBlock || overrideBlock[offTargetMetric] == null) continue
      const row = byRouteId.get(routeId)
      if (!row) continue
      const target = overrideBlock[offTargetMetric]
      const current = currentForMetric(offTargetMetric, row)
      const gap = formatGap(offTargetMetric, current, target)
      if (gap == null) continue
      // signedGap (route-perspective) for sorting: most-below first.
      // For higher-is-better, "below target" is current < target → (current - target) < 0;
      // for lower-is-better we flipped the sign in formatGap, so the magnitude is the
      // same but we need a sortable scalar. Recompute here for clarity.
      const higherIsBetter = offTargetMetric === 'otp' || offTargetMetric === 'service_delivered'
      const cDisp =
        offTargetMetric === 'service_delivered' || offTargetMetric === 'bunching'
          ? current * 100
          : current
      const tDisp =
        offTargetMetric === 'service_delivered' || offTargetMetric === 'bunching'
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
  // sub-message). `data.routes` can be `{}` when YAML is the out-of-box
  // default.
  const hasAnyOverrides = data != null && data.routes && Object.keys(data.routes).length > 0

  return (
    <main>
      <div className="chart-container">
        <h2>Performance targets</h2>
        <p className="drilldown-anchor">
          Read-only view of <code>config/route_targets.yaml</code>. Edits land
          via git — the dashboard reloads automatically on the next request
          after the file mtime advances. Per-route overrides apply per metric;
          missing entries inherit the system default.
        </p>

        <h3 style={{ marginTop: '1.5rem' }}>System defaults</h3>
        <table className="routes-table" style={{ marginTop: '0.5rem' }}>
          <thead>
            <tr>
              {METRIC_ORDER.map((m) => (
                <th key={m}>{METRIC_LABELS[m]}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr>
              {METRIC_ORDER.map((m) => (
                <td key={m} className="metric">
                  {formatTarget(m, systemDefault[m])}
                </td>
              ))}
            </tr>
          </tbody>
        </table>

        <h3 style={{ marginTop: '1.5rem' }}>Off target</h3>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Routes with a configured per-route target above, ranked by gap to
          that target on the metric selected below. Complementary to the
          Overview page's "Biggest drags" table — a small-volume route can
          be far off target without showing up as a big system contributor.
        </p>
        <div className="filters" style={{ marginBottom: '0.75rem' }}>
          <div>
            <label htmlFor="off-target-metric" style={{ marginRight: '0.5rem' }}>
              Metric:
            </label>
            <select
              id="off-target-metric"
              value={offTargetMetric}
              onChange={(e) => setOffTargetMetric(e.target.value)}
            >
              {METRIC_ORDER.map((m) => (
                <option key={m} value={m}>
                  {METRIC_LABELS[m]}
                </option>
              ))}
            </select>
          </div>
        </div>

        {offTargetRows.length === 0 ? (
          <OffTargetEmptyState
            hasAnyOverrides={hasAnyOverrides}
            targetsLoaded={data != null}
            metricLabel={METRIC_LABELS[offTargetMetric] ?? offTargetMetric}
          />
        ) : (
          <table className="routes-table" style={{ marginTop: '0.5rem' }}>
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
                    {formatContribMetricValue(offTargetMetric, r.current)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(offTargetMetric, r.target)}
                  </td>
                  <td className="metric" style={{ color: r.isBelow ? '#b91c1c' : '#15803d' }}>
                    {r.gapText}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <h3 style={{ marginTop: '1.5rem' }}>Per-route overrides</h3>
        {sortedRouteIds.length === 0 ? (
          <p style={{ color: '#64748b' }}>
            No per-route overrides configured. Every route inherits the system
            defaults above. Add overrides by editing{' '}
            <code>config/route_targets.yaml</code>.
          </p>
        ) : (
          <table className="routes-table" style={{ marginTop: '0.5rem' }}>
            <thead>
              <tr>
                <th>Route</th>
                {METRIC_ORDER.map((m) => (
                  <th key={m}>{METRIC_LABELS[m]}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedRouteIds.map((rid) => {
                const block = routes[rid] || {}
                return (
                  <tr key={rid}>
                    <td className="route-id">
                      <Link to={`/route/${rid}`}>{rid}</Link>
                    </td>
                    {METRIC_ORDER.map((m) => (
                      <td key={m} className="metric">
                        {block[m] != null ? (
                          formatTarget(m, block[m])
                        ) : (
                          <span style={{ color: '#94a3b8' }} title="Inherits system default">
                            {formatTarget(m, systemDefault[m])}
                          </span>
                        )}
                      </td>
                    ))}
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </main>
  )
}

export default Targets
