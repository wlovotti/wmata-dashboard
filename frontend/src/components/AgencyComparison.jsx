import { Fragment, useCallback, useEffect, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import './AgencyComparison.css'
import {
  METRIC_ORDER,
  METRIC_LABELS,
  ROUTE_DISTRIBUTION_METRICS,
  formatMetricValue,
  formatDelta,
  formatServiceLevel,
  formatDistributionStats,
  buildDistributionHistogramData,
  agencySeriesColor,
} from '../utils/agencyComparison'

/**
 * One headline-KPI cell: big number, week-over-week delta pill, and a
 * small partial-day disclosure when any of the window's days for this
 * metric were flagged `data_quality='partial'` (NOTES-104). Rendered as
 * a `<td>` so a metric's values line up across agencies on one table
 * row (the comparison-table reformat, PR #211).
 */
function MetricCell({ metric, metricData }) {
  const delta = metricData ? formatDelta(metric, metricData.wow_delta) : null
  return (
    <td className="agency-compare-cell">
      <div className="agency-metric-value">
        {formatMetricValue(metric, metricData?.window_mean)}
      </div>
      {delta && (
        <div className={`agency-metric-delta agency-metric-delta-${delta.tint}`}>{delta.text}</div>
      )}
      {metricData && metricData.partial_days > 0 && (
        <div className="agency-metric-partial">
          {metricData.partial_days} of {metricData.days_included} day
          {metricData.days_included === 1 ? '' : 's'} flagged partial
        </div>
      )}
    </td>
  )
}

/**
 * Schedule-promise cell (NOTES-115): trip-weighted median daytime
 * scheduled headway + share of service at ≤15-min headways, computed
 * from the current GTFS — no week-over-week delta by design (it only
 * changes when an agency ships a new schedule).
 */
function ServiceLevelCell({ serviceLevel }) {
  const formatted = formatServiceLevel(serviceLevel)
  return (
    <td className="agency-compare-cell">
      <div className="agency-metric-value">{formatted ? formatted.median : '—'}</div>
      {formatted?.share && <div className="agency-metric-partial">{formatted.share}</div>}
      <div className="agency-metric-partial">
        Median scheduled headway · weekday 7:00–19:00 · current schedule
      </div>
    </td>
  )
}

/**
 * One agency's route-level distribution stats for one metric: median,
 * interquartile range, route count, and the share of routes at or above
 * the configured system target (NOTES-141). Renders an em-dash cell when
 * the agency has no scored routes in the window rather than a blank one,
 * so a reader can tell "zero routes" from "not loaded."
 */
function RouteDistributionStatsCell({ metric, distribution }) {
  const stats = formatDistributionStats(metric, distribution)
  if (!stats) {
    return (
      <td className="agency-compare-cell">
        <span className="agency-distribution-empty">No routes scored in this window</span>
      </td>
    )
  }
  return (
    <td className="agency-compare-cell">
      <div className="agency-distribution-stats">
        <dl>
          <dt>Median</dt>
          <dd>{stats.median}</dd>
          <dt>IQR (p25–p75)</dt>
          <dd>{stats.iqr}</dd>
          <dt>Routes</dt>
          <dd>{stats.routeCount}</dd>
          {stats.share != null && stats.thresholdLabel != null && (
            <>
              <dt>≥ {stats.thresholdLabel}</dt>
              <dd>{stats.share} of routes</dd>
            </>
          )}
        </dl>
      </div>
    </td>
  )
}

/**
 * Tooltip content for the distribution histogram — one line per agency
 * present in the hovered bucket, using each agency's display name and
 * fixed series color as a color swatch so the tooltip stays legible even
 * though the legend below already names the series.
 */
function HistogramTooltip({ active, payload, label, agencies }) {
  if (!active || !payload?.length) return null
  const nameByKey = Object.fromEntries(agencies.map((a) => [a.agency, a.display_name]))
  return (
    <div className="agency-distribution-tooltip">
      <div className="agency-distribution-tooltip-label">{label}%</div>
      {payload.map((entry) => (
        <div
          key={entry.dataKey}
          className="agency-distribution-tooltip-row"
          style={{ color: entry.color }}
        >
          {nameByKey[entry.dataKey] ?? entry.dataKey}: {entry.value} route
          {entry.value === 1 ? '' : 's'}
        </div>
      ))}
    </div>
  )
}

/**
 * Compact side-by-side histogram of route counts per bucket for one
 * metric, one grouped bar per agency so the two agencies' shapes read
 * directly against each other rather than as two separate charts
 * (NOTES-141). Buckets are the shared fixed percentage-scale edges the
 * backend computes with (`<60/60-70/70-80/80-90/90+`) — identical cut
 * points for every agency and both metrics, per the dataviz skill's "color
 * follows the entity" rule each agency keeps one fixed color across every
 * chart on the page (`agencySeriesColor`).
 */
function RouteDistributionHistogram({ metric, agencies }) {
  const data = buildDistributionHistogramData(agencies, metric)
  if (data.length === 0 || data.every((row) => agencies.every((a) => !row[a.agency]))) {
    return null
  }
  return (
    <div className="agency-distribution-histogram">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 0 }} barGap={2}>
          <CartesianGrid vertical={false} stroke="#e1e0d9" />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 11, fill: '#898781' }}
            axisLine={{ stroke: '#c3c2b7' }}
            tickLine={false}
          />
          <YAxis
            allowDecimals={false}
            tick={{ fontSize: 11, fill: '#898781' }}
            axisLine={false}
            tickLine={false}
            width={24}
          />
          <Tooltip content={<HistogramTooltip agencies={agencies} />} cursor={{ fill: '#f1f5f9' }} />
          <Legend
            wrapperStyle={{ fontSize: '0.75rem' }}
            formatter={(value) =>
              agencies.find((a) => a.agency === value)?.display_name ?? value
            }
          />
          {agencies.map((agency) => (
            <Bar
              key={agency.agency}
              dataKey={agency.agency}
              name={agency.agency}
              fill={agencySeriesColor(agency.agency)}
              radius={[2, 2, 0, 0]}
              maxBarSize={22}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * `/compare` page (PR #198 — "the north star"). One row per headline
 * metric (OTP, service-delivered, scheduled wait, EWT, bunching, plus
 * the daytime service-level row), one column per agency (WMATA vs
 * SFMTA), over the matched window that began 2026-07-23 — a table
 * layout so comparing one metric across agencies reads left-to-right
 * on a single row instead of jumping between per-agency columns of
 * tiles (the comparison-table reformat, PR #211). Reads a single endpoint (`/api/agency-comparison`)
 * that already computed the window means, week-over-week deltas, and
 * comparability caveats — this component is a plain renderer over that
 * payload.
 *
 * OTP and service_delivered additionally carry a `route_distribution`
 * block (NOTES-141, wave 1 of the 2026-09 UX program): the headline mean
 * collapses each metric to one number, which hides the spread between
 * routes. Two extra sub-rows per metric — stats (median/IQR/share/count)
 * and a compact histogram — surface that spread using the same matched
 * window and the same per-route data the mean is built from. Both
 * sub-rows render only when at least one agency's payload carries the
 * block, so an older cached payload (or a payload from before this
 * change) degrades to exactly the pre-existing table.
 *
 * Deliberately plain per the item's scope decision: reuse the app's
 * existing `routes-table` styling rather than a new palette, defer
 * further visual polish to NOTES-85. The comparability caveats are
 * rendered in the page body (not a tooltip) per the PR #198 scope
 * decision — a comparison that hides its caveats is worse than none.
 */
function AgencyComparison() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryToken, setRetryToken] = useState(0)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch('/api/agency-comparison')
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
  }, [retryToken])

  const handleRetry = useCallback(() => setRetryToken((t) => t + 1), [])

  if (loading) {
    return (
      <main>
        <div className="chart-container">
          <h2>Agency comparison</h2>
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading agency comparison...</p>
          </div>
        </div>
      </main>
    )
  }

  if (error) {
    return (
      <main>
        <div className="chart-container">
          <h2>Agency comparison</h2>
          <p style={{ color: '#64748b' }}>Unable to load agency comparison: {error}</p>
          <div className="agency-comparison-error-actions">
            <button onClick={handleRetry} className="retry-btn">
              Try Again
            </button>
          </div>
        </div>
      </main>
    )
  }

  const agencies = data?.agencies ?? []
  const caveats = data?.caveats ?? []
  const windowStart = data?.window_start
  const windowEnd = data?.window_end
  const columnCount = agencies.length + 1

  return (
    <main>
      <div className="chart-container">
        <h2>Agency comparison</h2>
        <p className="drilldown-anchor">
          WMATA vs SFMTA (Muni) — matched window since {windowStart}
          {windowEnd ? `, through ${windowEnd}` : ' (no data collected in either agency yet)'}.
          Each metric is computed identically per agency from that agency&rsquo;s own data.
        </p>

        {agencies.length === 0 ? (
          <p style={{ color: '#64748b' }}>
            No agency database is currently configured. Set{' '}
            <code>DATABASE_URL</code> / <code>SFMTA_DATABASE_URL</code> and reload.
          </p>
        ) : (
          <div className="agency-compare-table-wrapper">
            <table className="routes-table agency-compare-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  {agencies.map((agency) => (
                    <th key={agency.agency}>{agency.display_name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {METRIC_ORDER.map((metric) => {
                  const hasDistribution =
                    ROUTE_DISTRIBUTION_METRICS.includes(metric) &&
                    agencies.some((agency) => agency.route_distribution?.[metric])
                  return (
                    <Fragment key={metric}>
                      <tr>
                        <td className="agency-compare-metric-label">{METRIC_LABELS[metric]}</td>
                        {agencies.map((agency) => (
                          <MetricCell
                            key={agency.agency}
                            metric={metric}
                            metricData={agency.metrics?.[metric]}
                          />
                        ))}
                      </tr>
                      {hasDistribution && (
                        <tr className="agency-distribution-row">
                          <td className="agency-distribution-sublabel">Route distribution</td>
                          {agencies.map((agency) => (
                            <RouteDistributionStatsCell
                              key={agency.agency}
                              metric={metric}
                              distribution={agency.route_distribution?.[metric]}
                            />
                          ))}
                        </tr>
                      )}
                      {hasDistribution && (
                        <tr className="agency-distribution-row">
                          <td className="agency-distribution-sublabel">Distribution shape</td>
                          <td
                            className="agency-distribution-histogram-cell"
                            colSpan={columnCount - 1}
                          >
                            <RouteDistributionHistogram metric={metric} agencies={agencies} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
                <tr>
                  <td className="agency-compare-metric-label">Daytime service level</td>
                  {agencies.map((agency) => (
                    <ServiceLevelCell key={agency.agency} serviceLevel={agency.service_level} />
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        )}

        {caveats.length > 0 && (
          <div className="agency-comparison-caveats">
            <h3>Comparability caveats</h3>
            <ul>
              {caveats.map((caveat) => (
                <li key={caveat.slice(0, 40)}>{caveat}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </main>
  )
}

export default AgencyComparison
