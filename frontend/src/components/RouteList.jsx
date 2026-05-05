import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'

// Module-level cache so navigating back from RouteDetail doesn't show the
// loading spinner — we render last-known data immediately while refetching
// in the background. The API itself is also cached server-side (60s TTL),
// so the background fetch is cheap when warm.
let _cachedRoutes = null
let _cachedLastUpdated = null

function RouteList() {
  const navigate = useNavigate()
  const [routes, setRoutes] = useState(_cachedRoutes ?? [])
  const [loading, setLoading] = useState(_cachedRoutes === null)
  const [error, setError] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [sortConfig, setSortConfig] = useState({ key: 'route_name', direction: 'asc' })
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(_cachedLastUpdated)

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

  useEffect(() => {
    fetchRoutes().finally(() => setLoading(false))
  }, [])

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
                </td>
                <td className="metric">
                  {route.bunching_rate != null
                    ? `${(route.bunching_rate * 100).toFixed(1)}%`
                    : '—'}
                </td>
              </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </main>
  )
}

export default RouteList
