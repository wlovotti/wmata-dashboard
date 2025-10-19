import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

function RouteList() {
  const navigate = useNavigate()
  const [routes, setRoutes] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [gradeFilter, setGradeFilter] = useState('all')
  const [sortConfig, setSortConfig] = useState({ key: 'route_name', direction: 'asc' })
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(null)

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
        setLastUpdated(new Date())
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

  const getGradeColor = (grade) => {
    const colors = {
      'A': '#00BFB3',  // WMATA cyan (excellent)
      'B': '#67823A',  // WMATA green (good)
      'C': '#FFA300',  // WMATA orange (fair)
      'D': '#FA4616',  // WMATA bright orange (poor)
      'F': '#C8102E',  // WMATA red (failing)
      'N/A': '#919D9D' // WMATA gray
    }
    return colors[grade] || colors['N/A']
  }

  // Filter and sort routes
  const filteredAndSortedRoutes = routes
    .filter(route => {
      const matchesSearch = route.route_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                           route.route_long_name?.toLowerCase().includes(searchTerm.toLowerCase())
      const matchesGrade = gradeFilter === 'all' || route.grade === gradeFilter
      return matchesSearch && matchesGrade
    })
    .sort((a, b) => {
      const aValue = a[sortConfig.key]
      const bValue = b[sortConfig.key]

      // Handle null values
      if (aValue === null) return 1
      if (bValue === null) return -1

      // Handle numeric vs string comparison
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue
      }

      // String comparison
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
            {routes.filter(r => r.otp_percentage !== null).length}
          </div>
          <div className="stat-label">Routes with Data</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routes.filter(r => r.otp_percentage !== null).length > 0
              ? Math.round(
                  routes
                    .filter(r => r.otp_percentage !== null)
                    .reduce((sum, r) => sum + r.otp_percentage, 0) /
                  routes.filter(r => r.otp_percentage !== null).length
                )
              : 'N/A'}
            {routes.filter(r => r.otp_percentage !== null).length > 0 && '%'}
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

          <div className="filter-group">
            <label htmlFor="grade-filter">Grade:</label>
            <select
              id="grade-filter"
              value={gradeFilter}
              onChange={(e) => setGradeFilter(e.target.value)}
              className="filter-select"
            >
              <option value="all">All Grades</option>
              <option value="A">A</option>
              <option value="B">B</option>
              <option value="C">C</option>
              <option value="D">D</option>
              <option value="F">F</option>
              <option value="N/A">N/A</option>
            </select>
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
              <th onClick={() => handleSort('grade')} className="sortable">
                Grade {getSortIcon('grade')}
              </th>
              <th onClick={() => handleSort('otp_percentage')} className="sortable">
                On-Time % {getSortIcon('otp_percentage')}
              </th>
              <th onClick={() => handleSort('avg_headway_minutes')} className="sortable">
                Avg Headway {getSortIcon('avg_headway_minutes')}
              </th>
              <th onClick={() => handleSort('avg_speed_mph')} className="sortable">
                Avg Speed {getSortIcon('avg_speed_mph')}
              </th>
              <th onClick={() => handleSort('total_observations')} className="sortable">
                Observations {getSortIcon('total_observations')}
              </th>
              <th onClick={() => handleSort('data_updated_at')} className="sortable">
                Last Updated {getSortIcon('data_updated_at')}
              </th>
            </tr>
          </thead>
          <tbody>
            {filteredAndSortedRoutes.length === 0 ? (
              <tr>
                <td colSpan="8" className="empty-state">
                  <div className="empty-state-content">
                    <div className="empty-state-icon">🔍</div>
                    <p>No routes match your filters</p>
                    <button
                      onClick={() => {
                        setSearchTerm('')
                        setGradeFilter('all')
                      }}
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
                className={route.otp_percentage === null ? 'no-data' : ''}
                onClick={() => handleRouteClick(route.route_id)}
                style={{ cursor: 'pointer' }}
              >
                <td className="route-id">
                  <span className="route-badge" style={{
                    backgroundColor: route.otp_percentage !== null ? '#002F6C' : '#919D9D'
                  }}>
                    {route.route_name}
                  </span>
                </td>
                <td className="route-name">{route.route_long_name || 'N/A'}</td>
                <td>
                  <span
                    className="grade-badge"
                    style={{ backgroundColor: getGradeColor(route.grade) }}
                  >
                    {route.grade}
                  </span>
                </td>
                <td className="metric">
                  {route.otp_percentage !== null
                    ? `${Math.round(route.otp_percentage)}%`
                    : '—'}
                </td>
                <td className="metric">
                  {route.avg_headway_minutes !== null
                    ? `${Math.round(route.avg_headway_minutes)} min`
                    : '—'}
                </td>
                <td className="metric">
                  {route.avg_speed_mph !== null
                    ? `${Math.round(route.avg_speed_mph)} mph`
                    : '—'}
                </td>
                <td className="metric">
                  {route.total_observations?.toLocaleString() || '0'}
                </td>
                <td className="timestamp">
                  {route.data_updated_at
                    ? new Date(route.data_updated_at).toLocaleDateString()
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
