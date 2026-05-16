import { useState, useEffect } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'
import { computeSpectrumBar } from '../utils/spectrumBar'
import SystemTrend from './SystemTrend'

// Module-level cache so navigating back from RouteDetail doesn't show the
// loading spinner — we render last-known data immediately while refetching
// in the background. The API itself is also cached server-side (60s TTL),
// so the background fetch is cheap when warm.
let _cachedRoutes = null
let _cachedWindow = null
let _cachedLastUpdated = null
let _cachedGtfsFreshness = null

// Inline target subline for the scorecard table cells (NOTES-47).
// `current` and `target` should already be in the same units the cell
// renders (percent for OTP/SD/bunching, seconds for EWT). Returns null
// when no target is set so the cell stays unchanged for unconfigured
// metrics. Color tracks "meets target" semantics: green when current
// is at/beyond target in the favorable direction, red otherwise.
function TargetSubline({ current, target, format, higherIsBetter = true }) {
  if (target == null) return null
  let color = '#94a3b8'
  if (current != null) {
    const meets = higherIsBetter ? current >= target : current <= target
    color = meets ? '#0E8A6F' : '#C8102E'
  }
  return (
    <div
      className="metric-target-subline"
      style={{
        fontSize: '0.7rem',
        color,
        marginTop: '0.1rem',
      }}
      title="Per-route target (config/route_targets.yaml)"
    >
      tgt {format(target)}
    </div>
  )
}

// Spectrum bar for the scorecard table cells (NOTES-55). A thin
// red/yellow/green track under each numeric value lets the eye
// pre-classify "needs attention" before parsing digits, eliminating the
// number-then-target comparison the original `TargetSubline` required on
// every cell. Returns null when there's no target or no current value
// (the cell falls back to the bare numeric / em-dash). Color mapping
// lives in `utils/spectrumBar.js` so it can be reused if another table
// adopts the pattern; the same ±10% yellow band applies to all metrics
// to keep the visual language consistent across the row.
function SpectrumBar({ current, target, higherIsBetter }) {
  const result = computeSpectrumBar({ current, target, higherIsBetter })
  if (result == null) return null
  const { color, fillPct } = result
  return (
    <div
      className="spectrum-bar-track"
      style={{
        marginTop: '0.25rem',
        width: '100%',
        height: '5px',
        backgroundColor: '#e5e7eb',
        borderRadius: '2px',
        overflow: 'hidden',
      }}
      aria-hidden="true"
    >
      <div
        className="spectrum-bar-fill"
        style={{
          width: `${fillPct}%`,
          height: '100%',
          backgroundColor: color,
          transition: 'width 0.2s ease',
        }}
      />
    </div>
  )
}

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

// NOTES-51 step 4: contributors table caps at the top N by default, with
// a "Show all (M)" expander revealing the rest. Ten is enough to surface
// the routes worth investigating without overwhelming the first viewport.
const CONTRIB_TOP_N = 10

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

// Format the scorecard's pooled-window range as a subtle one-line annotation.
// Inputs are ISO date strings (YYYY-MM-DD); both null when the DB has no
// derived stop_events yet, in which case we render nothing.
function formatWindowRange(startIso, endIso) {
  if (!startIso || !endIso) return null
  const parse = (iso) => {
    const [y, m, d] = iso.split('-').map(Number)
    return new Date(y, m - 1, d)
  }
  const monthDay = (date) =>
    date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  const start = parse(startIso)
  const end = parse(endIso)
  const sameYear = start.getFullYear() === end.getFullYear()
  const startLabel = sameYear ? monthDay(start) : `${monthDay(start)}, ${start.getFullYear()}`
  return `${startLabel} – ${monthDay(end)}, ${end.getFullYear()}`
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
  const [scorecardWindow, setScorecardWindow] = useState(_cachedWindow)
  const [loading, setLoading] = useState(_cachedRoutes === null)
  const [error, setError] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [sortConfig, setSortConfig] = useState({ key: 'route_name', direction: 'asc' })
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(_cachedLastUpdated)
  const [gtfsFreshness, setGtfsFreshness] = useState(_cachedGtfsFreshness)
  // Mode toggle: 'contributors' (NOTES-39, default after NOTES-51) vs
  // 'default' (full alphabetic scorecard table, now collapsed behind a
  // <details> disclosure). Clicking the "Default" toggle still flips the
  // disclosure open; the disclosure can also be expanded independently
  // without leaving contributors mode.
  const [viewMode, setViewMode] = useState('contributors')
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)
  // NOTES-51 step 4: top-10 contributors by default with a "Show all"
  // expander; reset to top-10 when the metric changes so the user sees
  // the new metric's top movers, not a previously-expanded list.
  const [showAllContributors, setShowAllContributors] = useState(false)
  useEffect(() => {
    setShowAllContributors(false)
  }, [contribMetric])

  const fetchRoutes = () => {
    return fetch('/api/routes')
      .then(res => {
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`)
        }
        return res.json()
      })
      .then(data => {
        // Response shape: `{window: {start, end, days}, routes: [...]}`. The
        // window block lets us label the pooled date range under the heading.
        const routesList = data.routes ?? []
        const window = data.window ?? null
        setRoutes(routesList)
        setScorecardWindow(window)
        const now = new Date()
        setLastUpdated(now)
        _cachedRoutes = routesList
        _cachedWindow = window
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

  // NOTES-51: the lifted search input applies to both views. Filter
  // contributors by the same `searchTerm` against route short/long name
  // so name lookup works regardless of which mode is active. The
  // expander then slices the filtered list — search wins over the cap.
  const lowerSearch = searchTerm.toLowerCase()
  const filteredContributors = (contribData?.contributors ?? []).filter(c => {
    if (!lowerSearch) return true
    return (
      c.route_short_name?.toLowerCase().includes(lowerSearch) ||
      c.route_long_name?.toLowerCase().includes(lowerSearch) ||
      c.route_id?.toLowerCase().includes(lowerSearch)
    )
  })
  const visibleContributors = showAllContributors
    ? filteredContributors
    : filteredContributors.slice(0, CONTRIB_TOP_N)

  if (loading) {
    return (
      <main>
        <SystemTrend />
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

      {/* NOTES-44: entry point to the marginal-bus ranking. Decision-support
          surface is shallow (one page) for now; a top-level nav (NOTES-52)
          would absorb this link into a "Decision support" section. */}
      <div
        className="decision-support-strip"
        style={{
          margin: '1rem 0',
          padding: '0.75rem 1rem',
          background: '#eff6ff',
          border: '1px solid #bfdbfe',
          borderRadius: '0.5rem',
          fontSize: '0.875rem',
          color: '#1e40af',
        }}
      >
        Operator decision support:{' '}
        <Link
          to="/marginal-bus"
          style={{ color: '#002F6C', fontWeight: 600, textDecoration: 'underline' }}
        >
          Where would the next bus help most? →
        </Link>
      </div>

      <div className="table-container">
        <h2>Route Performance Scorecard</h2>
        {(() => {
          const range = formatWindowRange(
            scorecardWindow?.start,
            scorecardWindow?.end,
          )
          if (!range) return null
          const days = scorecardWindow?.days
          return (
            <p className="scorecard-window-note">
              Pooled over {days}-day window · {range}
            </p>
          )
        })()}

        {/* NOTES-51: search lifted above both views so name lookup is one
            keystroke regardless of which mode is active. The same
            `searchTerm` filters the contributors panel and the full table. */}
        <div className="filters" style={{ marginBottom: '1rem' }}>
          <div className="search-box">
            <input
              type="text"
              placeholder="Search routes..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="search-input"
            />
          </div>
        </div>

        <div className="mode-toggle" style={{ marginBottom: '1rem' }}>
          <button
            type="button"
            onClick={() => setViewMode('contributors')}
            className={viewMode === 'contributors' ? 'mode-active' : 'mode-inactive'}
            style={{
              marginRight: '0.5rem',
              padding: '0.4rem 0.9rem',
              fontWeight: viewMode === 'contributors' ? 'bold' : 'normal',
            }}
            title="Rank routes by contribution to system underperformance instead of raw worst percentage"
          >
            Biggest contributors
          </button>
          <button
            type="button"
            onClick={() => setViewMode('default')}
            className={viewMode === 'default' ? 'mode-active' : 'mode-inactive'}
            style={{
              padding: '0.4rem 0.9rem',
              fontWeight: viewMode === 'default' ? 'bold' : 'normal',
            }}
            title="Alphabetic full-route table (collapses behind a disclosure when not active)"
          >
            All routes
          </button>
        </div>

        {viewMode === 'contributors' && (
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
              {contribData && (
                <div className="results-count">
                  {contribData.system_target_value != null ? (
                    <>
                      System target:{' '}
                      <strong>
                        {formatContribMetricValue(
                          contribMetric,
                          contribData.system_target_value,
                        )}
                      </strong>
                      {contribData.baseline_value != null && (
                        <>
                          {' '}· baseline (30d):{' '}
                          <strong>
                            {formatContribMetricValue(
                              contribMetric,
                              contribData.baseline_value,
                            )}
                          </strong>
                        </>
                      )}
                    </>
                  ) : contribData.baseline_value != null ? (
                    <>
                      System baseline (30d):{' '}
                      <strong>
                        {formatContribMetricValue(
                          contribMetric,
                          contribData.baseline_value,
                        )}
                      </strong>
                    </>
                  ) : null}
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
            ) : filteredContributors.length === 0 ? (
              <p>
                {searchTerm
                  ? 'No contributors match your search.'
                  : 'No routes have enough data to score contribution for this metric.'}
              </p>
            ) : (
              <>
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
                      <th>Scheduled trips (30d)</th>
                      <th>Contribution score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleContributors.map((c, idx) => {
                      // Width of the score bar relative to the top contributor;
                      // anchored on the largest absolute score so negative scores
                      // (routes outperforming baseline) still get a visible bar.
                      // Scale to the full filtered list, not just the visible
                      // slice, so the "Show all" toggle doesn't re-scale bars.
                      const maxAbs = Math.max(
                        ...filteredContributors.map(x => Math.abs(x.contribution_score || 0)),
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
                            {formatContribMetricValue(
                              contribMetric,
                              c.reference_value ?? c.baseline_value,
                            )}
                            {c.reference_source && (
                              <div
                                style={{
                                  fontSize: '0.7rem',
                                  color: '#64748b',
                                  marginTop: '0.1rem',
                                }}
                              >
                                {c.reference_source === 'target'
                                  ? 'route target'
                                  : 'system baseline'}
                              </div>
                            )}
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
                {filteredContributors.length > CONTRIB_TOP_N && (
                  <div style={{ marginTop: '0.75rem' }}>
                    <button
                      type="button"
                      onClick={() => setShowAllContributors(v => !v)}
                      className="show-all-contributors"
                    >
                      {showAllContributors
                        ? `Show top ${CONTRIB_TOP_N}`
                        : `Show all (${filteredContributors.length})`}
                    </button>
                  </div>
                )}
              </>
            )}
            <p style={{ marginTop: '1rem', fontSize: '0.85em', color: '#666' }}>
              Contribution = (reference − route value) × scheduled trips, sign-flipped for
              lower-is-better metrics. Reference is the route&apos;s configured target when set
              (config/route_targets.yaml), otherwise the system 30-day window mean.
            </p>
          </div>
        )}

        {/* NOTES-51 step 3: full alphabetic table lives inside a disclosure
            below the contributors view. Clicking the "All routes" toggle
            flips the disclosure open; the user can also expand it manually
            without switching modes. */}
        <details
          className="all-routes-disclosure"
          open={viewMode === 'default'}
          style={{ marginTop: '1.5rem' }}
        >
          <summary>
            See all routes ({filteredAndSortedRoutes.length}
            {searchTerm ? ` of ${routes.length}` : ''})
          </summary>
          <div style={{ marginTop: '1rem' }}>
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
                  <SpectrumBar
                    current={route.otp_all_pct}
                    target={route.targets?.otp}
                    higherIsBetter
                  />
                  <TargetSubline
                    current={route.otp_all_pct}
                    target={route.targets?.otp}
                    higherIsBetter
                    format={(t) => `${t.toFixed(0)}%`}
                  />
                </td>
                <td className="metric">
                  {route.service_delivered_ratio != null
                    ? `${Math.round(route.service_delivered_ratio * 100)}%`
                    : '—'}
                  <SpectrumBar
                    current={
                      route.service_delivered_ratio != null
                        ? route.service_delivered_ratio * 100
                        : null
                    }
                    target={
                      route.targets?.service_delivered != null
                        ? route.targets.service_delivered * 100
                        : null
                    }
                    higherIsBetter
                  />
                  <TargetSubline
                    current={
                      route.service_delivered_ratio != null
                        ? route.service_delivered_ratio * 100
                        : null
                    }
                    target={
                      route.targets?.service_delivered != null
                        ? route.targets.service_delivered * 100
                        : null
                    }
                    higherIsBetter
                    format={(t) => `${t.toFixed(0)}%`}
                  />
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
                  <SpectrumBar
                    current={route.ewt_seconds}
                    target={route.targets?.ewt}
                    higherIsBetter={false}
                  />
                  <TargetSubline
                    current={route.ewt_seconds}
                    target={route.targets?.ewt}
                    higherIsBetter={false}
                    format={(t) => `${(t / 60).toFixed(1)}m`}
                  />
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
                  <SpectrumBar
                    current={
                      route.bunching_rate != null
                        ? route.bunching_rate * 100
                        : null
                    }
                    target={
                      route.targets?.bunching != null
                        ? route.targets.bunching * 100
                        : null
                    }
                    higherIsBetter={false}
                  />
                  <TargetSubline
                    current={
                      route.bunching_rate != null
                        ? route.bunching_rate * 100
                        : null
                    }
                    target={
                      route.targets?.bunching != null
                        ? route.targets.bunching * 100
                        : null
                    }
                    higherIsBetter={false}
                    format={(t) => `${t.toFixed(1)}%`}
                  />
                </td>
              </tr>
              ))
            )}
          </tbody>
        </table>
          </div>
        </details>
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
