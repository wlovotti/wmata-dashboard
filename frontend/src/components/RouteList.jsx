import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'
import SystemTrend from './SystemTrend'

// Module-level cache so navigating back from RouteDetail doesn't show the
// loading spinner — we render last-known data immediately while refetching
// in the background. The API itself is also cached server-side (60s TTL),
// so the background fetch is cheap when warm.
let _cachedRoutes = null
let _cachedLastUpdated = null
let _cachedGtfsFreshness = null

// "Biggest contributors" view (NOTES-39): ranks routes by their absolute
// contribution to system underperformance instead of raw worst percentage.
// The contribution score is `(baseline - route_value) * scheduled_trips`
// for higher-is-better metrics (OTP, service-delivered), sign-flipped for
// lower-is-better metrics (EWT, bunching) so positive always means
// "dragging the system down." Volume proxy is GTFS scheduled trips over
// the window — ridership is not in the data.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

function formatContribMetricValue(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${Math.round(value)}%`
  if (metric === 'service_delivered') return `${Math.round(value * 100)}%`
  if (metric === 'ewt') return `${Math.round(value)}s`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

function formatContribScore(score) {
  if (score == null) return '—'
  // The score's units depend on the metric (percent-points × trips,
  // seconds × trips, etc.) so a raw rounded integer is the right level
  // of precision; the rank order matters more than the absolute value.
  const rounded = Math.round(score)
  return rounded.toLocaleString('en-US')
}

// Format a naive-UTC ISO timestamp as a service date in Eastern. The
// backend stores datetimes as naive UTC (see CLAUDE.md); appending 'Z'
// makes the Date constructor treat the string as UTC instead of local.
function formatSnapshotDate(isoString) {
  if (!isoString) return null
  const d = new Date(isoString.endsWith('Z') ? isoString : `${isoString}Z`)
  if (Number.isNaN(d.getTime())) return null
  return d.toLocaleDateString('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

function formatLoadedAt(isoString) {
  if (!isoString) return null
  const d = new Date(isoString.endsWith('Z') ? isoString : `${isoString}Z`)
  if (Number.isNaN(d.getTime())) return null
  return d.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  })
}

function RouteList() {
  const navigate = useNavigate()
  const [routes, setRoutes] = useState(_cachedRoutes ?? [])
  const [loading, setLoading] = useState(_cachedRoutes === null)
  const [error, setError] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [sortConfig, setSortConfig] = useState({ key: 'route_name', direction: 'asc' })
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(_cachedLastUpdated)
  const [gtfsFreshness, setGtfsFreshness] = useState(_cachedGtfsFreshness)
  // Mode toggle: 'default' (current scorecard) vs 'contributors' (NOTES-39).
  // Pure additive — toggling away leaves the default view unchanged.
  const [viewMode, setViewMode] = useState('default')
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)

  const fetchRoutes = () => {
    return fetch('/api/routes')
      .then(res => {
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`)
        }
        return res.json()
      })
      .then(data => {
        setRoutes(data)
        const now = new Date()
        setLastUpdated(now)
        _cachedRoutes = data
        _cachedLastUpdated = now
        setError(null)
      })
      .catch(err => {
        setError(err.message)
      })
  }

  // Pure observability — failures are silent; the footer just doesn't render.
  const fetchGtfsFreshness = () => {
    return fetch('/api/gtfs/freshness')
      .then(res => (res.ok ? res.json() : null))
      .then(data => {
        if (data) {
          setGtfsFreshness(data)
          _cachedGtfsFreshness = data
        }
      })
      .catch(() => {
        // Swallow — this is informational; don't surface to the user.
      })
  }

  useEffect(() => {
    fetchRoutes().finally(() => setLoading(false))
    fetchGtfsFreshness()
  }, [])

  // Fetch contributors only while the contributors mode is selected, and
  // refetch when the metric changes. Pure additive — never blocks the
  // default view.
  useEffect(() => {
    if (viewMode !== 'contributors') return
    setContribLoading(true)
    setContribError(null)
    fetch(`/api/routes/contributors?metric=${contribMetric}&days=30`)
      .then(res => {
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`)
        }
        return res.json()
      })
      .then(data => {
        setContribData(data)
      })
      .catch(err => {
        setContribError(err.message)
      })
      .finally(() => setContribLoading(false))
  }, [viewMode, contribMetric])

  const handleRefresh = () => {
    setRefreshing(true)
    fetchRoutes().finally(() => setRefreshing(false))
  }

  // Filter and sort routes
  const filteredAndSortedRoutes = routes
    .filter(route => {
      const matchesSearch = route.route_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                           route.route_long_name?.toLowerCase().includes(searchTerm.toLowerCase())
      return matchesSearch
    })
    .sort((a, b) => {
      const aValue = a[sortConfig.key]
      const bValue = b[sortConfig.key]

      // Null/undefined sort last regardless of direction
      if (aValue == null) return 1
      if (bValue == null) return -1

      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue
      }

      const aStr = String(aValue).toLowerCase()
      const bStr = String(bValue).toLowerCase()
      if (sortConfig.direction === 'asc') {
        return aStr < bStr ? -1 : aStr > bStr ? 1 : 0
      } else {
        return bStr < aStr ? -1 : bStr > aStr ? 1 : 0
      }
    })

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }))
  }

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) return '⇅'
    return sortConfig.direction === 'asc' ? '↑' : '↓'
  }

  const handleRouteClick = (routeId) => {
    navigate(`/route/${routeId}`)
  }

  if (loading) {
    return (
      <main>
        <SystemTrend />
        <div className="stats-summary">
          <div className="stat-card skeleton">
            <div className="skeleton-line skeleton-stat-value"></div>
            <div className="skeleton-line skeleton-stat-label"></div>
          </div>
          <div className="stat-card skeleton">
            <div className="skeleton-line skeleton-stat-value"></div>
            <div className="skeleton-line skeleton-stat-label"></div>
          </div>
          <div className="stat-card skeleton">
            <div className="skeleton-line skeleton-stat-value"></div>
            <div className="skeleton-line skeleton-stat-label"></div>
          </div>
        </div>
        <div className="table-container">
          <h2>Route Performance Scorecard</h2>
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading routes...</p>
          </div>
        </div>
      </main>
    )
  }

  return (
    <main>
      {error && (
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading data:</strong> {error}
            <div className="error-actions">
              <button onClick={handleRefresh} className="retry-btn">
                Try Again
              </button>
            </div>
          </div>
        </div>
      )}

      <SystemTrend />

      <div className="stats-summary">
        <div className="stat-card">
          <div className="stat-value">{routes.length}</div>
          <div className="stat-label">Total Routes</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routes.filter(r => r.otp_all_pct !== null && r.otp_all_pct !== undefined).length}
          </div>
          <div className="stat-label">Routes with Data</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {(() => {
              const withData = routes.filter(r => r.otp_all_pct !== null && r.otp_all_pct !== undefined)
              if (withData.length === 0) return 'N/A'
              return `${Math.round(withData.reduce((sum, r) => sum + r.otp_all_pct, 0) / withData.length)}%`
            })()}
          </div>
          <div className="stat-label">System-wide OTP</div>
        </div>
      </div>

      <div className="table-container">
        <h2>Route Performance Scorecard</h2>

        <div className="mode-toggle" style={{ marginBottom: '1rem' }}>
          <button
            type="button"
            onClick={() => setViewMode('default')}
            className={viewMode === 'default' ? 'mode-active' : 'mode-inactive'}
            style={{
              marginRight: '0.5rem',
              padding: '0.4rem 0.9rem',
              fontWeight: viewMode === 'default' ? 'bold' : 'normal',
            }}
          >
            Default
          </button>
          <button
            type="button"
            onClick={() => setViewMode('contributors')}
            className={viewMode === 'contributors' ? 'mode-active' : 'mode-inactive'}
            style={{
              padding: '0.4rem 0.9rem',
              fontWeight: viewMode === 'contributors' ? 'bold' : 'normal',
            }}
            title="Rank routes by contribution to system underperformance instead of raw worst percentage"
          >
            Biggest contributors
          </button>
        </div>

        {viewMode === 'contributors' ? (
          <div className="contributors-view">
            <div className="filters" style={{ marginBottom: '0.75rem' }}>
              <div>
                <label htmlFor="contrib-metric" style={{ marginRight: '0.5rem' }}>
                  Metric:
                </label>
                <select
                  id="contrib-metric"
                  value={contribMetric}
                  onChange={e => setContribMetric(e.target.value)}
                >
                  {CONTRIB_METRICS.map(m => (
                    <option key={m.key} value={m.key}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </div>
              {contribData && contribData.baseline_value != null && (
                <div className="results-count">
                  System baseline (30d):{' '}
                  <strong>
                    {formatContribMetricValue(contribMetric, contribData.baseline_value)}
                  </strong>
                </div>
              )}
            </div>

            {contribError && (
              <div className="error-banner">
                <div className="error-content">
                  <strong>Error loading contributors:</strong> {contribError}
                </div>
              </div>
            )}

            {contribLoading ? (
              <div className="loading-spinner">
                <div className="spinner"></div>
                <p>Loading contributors...</p>
              </div>
            ) : contribData == null ? null : contribData.baseline_value == null ? (
              <p>
                System baseline unavailable for this metric in the last 30 days — cannot rank
                contributors yet. Once the daily metrics pipeline writes a row to{' '}
                <code>system_metrics_daily</code> the table will populate.
              </p>
            ) : contribData.contributors.length === 0 ? (
              <p>No routes have enough data to score contribution for this metric.</p>
            ) : (
              <table className="routes-table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Route</th>
                    <th>Name</th>
                    <th>Route value</th>
                    <th>Baseline</th>
                    <th>Scheduled trips (30d)</th>
                    <th>Contribution score</th>
                  </tr>
                </thead>
                <tbody>
                  {contribData.contributors.map((c, idx) => {
                    // Width of the score bar relative to the top contributor;
                    // anchored on the largest absolute score so negative scores
                    // (routes outperforming baseline) still get a visible bar.
                    const maxAbs = Math.max(
                      ...contribData.contributors.map(x => Math.abs(x.contribution_score || 0)),
                      1
                    )
                    const barPct = (Math.abs(c.contribution_score || 0) / maxAbs) * 100
                    const barColor = (c.contribution_score || 0) >= 0 ? '#d97706' : '#16a34a'
                    return (
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
                          {formatContribMetricValue(contribMetric, c.baseline_value)}
                        </td>
                        <td className="metric">{c.scheduled_trips.toLocaleString('en-US')}</td>
                        <td className="metric">
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            <div
                              style={{
                                width: `${barPct}%`,
                                minWidth: '2px',
                                maxWidth: '120px',
                                height: '10px',
                                backgroundColor: barColor,
                                borderRadius: '2px',
                              }}
                              title={`Contribution magnitude: ${formatContribScore(
                                c.contribution_score
                              )}`}
                            />
                            <span>{formatContribScore(c.contribution_score)}</span>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
            <p style={{ marginTop: '1rem', fontSize: '0.85em', color: '#666' }}>
              Contribution = (baseline − route value) × scheduled trips, sign-flipped for
              lower-is-better metrics. Baseline is the system 30-day window mean; per-route
              targets land with NOTES-47.
            </p>
          </div>
        ) : (
        <>
        <div className="filters">
          <div className="search-box">
            <input
              type="text"
              placeholder="Search routes..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="search-input"
            />
          </div>

          <div className="results-count">
            Showing {filteredAndSortedRoutes.length} of {routes.length} routes
          </div>
        </div>

        <table className="routes-table">
          <thead>
            <tr>
              <th onClick={() => handleSort('route_name')} className="sortable">
                Route {getSortIcon('route_name')}
              </th>
              <th onClick={() => handleSort('route_long_name')} className="sortable">
                Name {getSortIcon('route_long_name')}
              </th>
              <th onClick={() => handleSort('otp_all_pct')} className="sortable">
                On-Time % {getSortIcon('otp_all_pct')}
              </th>
              <th onClick={() => handleSort('service_delivered_ratio')} className="sortable">
                Service Delivered {getSortIcon('service_delivered_ratio')}
              </th>
              <th onClick={() => handleSort('ewt_seconds')} className="sortable">
                EWT {getSortIcon('ewt_seconds')}
              </th>
              <th onClick={() => handleSort('bunching_rate')} className="sortable">
                Bunching {getSortIcon('bunching_rate')}
              </th>
            </tr>
          </thead>
          <tbody>
            {filteredAndSortedRoutes.length === 0 ? (
              <tr>
                <td colSpan="6" className="empty-state">
                  <div className="empty-state-content">
                    <div className="empty-state-icon">🔍</div>
                    <p>No routes match your filters</p>
                    <button
                      onClick={() => setSearchTerm('')}
                      className="clear-filters-btn"
                    >
                      Clear Filters
                    </button>
                  </div>
                </td>
              </tr>
            ) : (
              filteredAndSortedRoutes.map(route => (
              <tr
                key={route.route_id}
                className={route.otp_all_pct == null ? 'no-data' : ''}
                onClick={() => handleRouteClick(route.route_id)}
                style={{ cursor: 'pointer' }}
              >
                <td className="route-id">
                  <span
                    className="route-badge"
                    style={{
                      backgroundColor: badgeColor(route.frequency_class, route.otp_all_pct != null),
                    }}
                    title={FREQUENCY_CLASS_LABELS[route.frequency_class] || ''}
                  >
                    {route.route_name}
                  </span>
                </td>
                <td className="route-name">{route.route_long_name || 'N/A'}</td>
                <td className="metric">
                  {route.otp_all_pct != null
                    ? `${Math.round(route.otp_all_pct)}%`
                    : '—'}
                </td>
                <td className="metric">
                  {route.service_delivered_ratio != null
                    ? `${Math.round(route.service_delivered_ratio * 100)}%`
                    : '—'}
                </td>
                <td className="metric">
                  {route.ewt_seconds != null
                    ? `${Math.round(route.ewt_seconds)}s`
                    : '—'}
                  {route.ewt_coverage_ratio != null && route.ewt_coverage_ratio < 0.5 && (
                    <span
                      className="data-thin-badge"
                      title={`Only ${Math.round(route.ewt_coverage_ratio * 100)}% of scheduled headways were observed — metric unreliable`}
                    >
                      Thin
                    </span>
                  )}
                </td>
                <td className="metric">
                  {route.bunching_rate != null
                    ? `${(route.bunching_rate * 100).toFixed(1)}%`
                    : '—'}
                  {route.ewt_coverage_ratio != null && route.ewt_coverage_ratio < 0.5 && (
                    <span
                      className="data-thin-badge"
                      title={`Only ${Math.round(route.ewt_coverage_ratio * 100)}% of scheduled headways were observed — metric unreliable`}
                    >
                      Thin
                    </span>
                  )}
                </td>
              </tr>
              ))
            )}
          </tbody>
        </table>
        </>
        )}
      </div>

      {gtfsFreshness && gtfsFreshness.snapshot_date && (
        <footer className="gtfs-freshness-footer">
          {(() => {
            const snapshot = formatSnapshotDate(gtfsFreshness.snapshot_date)
            const loaded = formatLoadedAt(gtfsFreshness.created_at)
            if (!snapshot) return null
            return (
              <span>
                GTFS schedule current as of {snapshot}
                {loaded && ` (loaded ${loaded})`}
                {gtfsFreshness.feed_version && ` · feed ${gtfsFreshness.feed_version}`}
              </span>
            )
          })()}
        </footer>
      )}
    </main>
  )
}

export default RouteList
