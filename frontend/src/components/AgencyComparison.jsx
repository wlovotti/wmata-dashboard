import { useEffect, useState } from 'react'
import { METRIC_ORDER, METRIC_LABELS, formatMetricValue, formatDelta, formatServiceLevel } from '../utils/agencyComparison'

/**
 * One headline-KPI tile: big number, week-over-week delta pill, and a
 * small partial-day disclosure when any of the window's days for this
 * metric were flagged `data_quality='partial'` (NOTES-104).
 */
function MetricTile({ metric, metricData }) {
  const delta = metricData ? formatDelta(metric, metricData.wow_delta) : null
  return (
    <div className="agency-metric-tile">
      <div className="agency-metric-label">{METRIC_LABELS[metric]}</div>
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
    </div>
  )
}

/**
 * Schedule-promise tile (NOTES-115): trip-weighted median daytime
 * scheduled headway + share of service at ≤15-min headways, computed
 * from the current GTFS — no week-over-week delta by design (it only
 * changes when an agency ships a new schedule).
 */
function ServiceLevelTile({ serviceLevel }) {
  const formatted = formatServiceLevel(serviceLevel)
  return (
    <div className="agency-metric-tile">
      <div className="agency-metric-label">Daytime service level</div>
      <div className="agency-metric-value">{formatted ? formatted.median : '—'}</div>
      {formatted?.share && <div className="agency-metric-partial">{formatted.share}</div>}
      <div className="agency-metric-partial">
        Median scheduled headway · weekday 7:00–19:00 · current schedule
      </div>
    </div>
  )
}

/**
 * One agency's column: display name heading + its headline-KPI tiles
 * (OTP, service-delivered, scheduled wait, EWT, bunching) plus the
 * daytime service-level tile.
 */
function AgencyColumn({ agency }) {
  return (
    <div className="agency-column">
      <h3>{agency.display_name}</h3>
      <div className="agency-metric-grid">
        {METRIC_ORDER.map((metric) => (
          <MetricTile key={metric} metric={metric} metricData={agency.metrics?.[metric]} />
        ))}
        <ServiceLevelTile serviceLevel={agency.service_level} />
      </div>
    </div>
  )
}

/**
 * `/compare` page (PR #198 — "the north star"). Two columns, WMATA vs
 * SFMTA, side by side on the headline KPIs (OTP, service-delivered,
 * scheduled wait, EWT, bunching) plus the daytime service-level tile,
 * over the matched window that began 2026-07-23. Reads a
 * single endpoint (`/api/agency-comparison`) that already computed the
 * window means, week-over-week deltas, and comparability caveats — this
 * component is a plain renderer over that payload.
 *
 * Deliberately plain per the item's scope decision: ship ugly-but-honest
 * now, defer visual polish to the Overview editorial redesign (PR #209)
 * and NOTES-85. The comparability caveats are rendered in the page body
 * (not a tooltip) per the same scope decision — a comparison that hides
 * its caveats is worse than none.
 */
function AgencyComparison() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
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
  }, [])

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
        </div>
      </main>
    )
  }

  const agencies = data?.agencies ?? []
  const caveats = data?.caveats ?? []
  const windowStart = data?.window_start
  const windowEnd = data?.window_end

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
          <div className="agency-comparison-grid">
            {agencies.map((agency) => (
              <AgencyColumn key={agency.agency} agency={agency} />
            ))}
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
