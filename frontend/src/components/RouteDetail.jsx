import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import RouteMap from './RouteMap'

function RouteDetail() {
  const { routeId } = useParams()
  const navigate = useNavigate()
  const [routeData, setRouteData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch(`/api/routes/${routeId}`)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .then(data => {
        setRouteData(data)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId])

  if (loading) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate('/')} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading route details...</p>
        </div>
      </main>
    )
  }

  if (error || !routeData) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate('/')} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading route data:</strong> {error || 'Route not found'}
            <div className="error-actions">
              <button onClick={() => navigate('/')} className="retry-btn">
                Back to Routes
              </button>
            </div>
          </div>
        </div>
      </main>
    )
  }

  const hasMetrics = routeData.otp_all_pct != null
    || routeData.service_delivered_ratio != null
    || routeData.ewt_seconds != null
    || routeData.bunching_rate != null

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate('/')} className="back-btn">
          ← Back to All Routes
        </button>
        <div className="route-title">
          <h1>
            <span className="route-badge-large" style={{
              backgroundColor: hasMetrics ? '#002F6C' : '#919D9D'
            }}>
              {routeData.route_name}
            </span>
            {routeData.route_long_name}
          </h1>
        </div>
      </div>

      <div className="stats-summary">
        <div className="stat-card">
          <div className="stat-value">
            {routeData.otp_all_pct != null
              ? `${Math.round(routeData.otp_all_pct)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">On-Time Performance</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.service_delivered_ratio != null
              ? `${Math.round(routeData.service_delivered_ratio * 100)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">Service Delivered</div>
          {routeData.service_delivered_scheduled != null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              ({routeData.service_delivered_delivered} of {routeData.service_delivered_scheduled} trips)
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{ fontSize: '1.5rem' }}>
            {routeData.otp_origin_pct != null
              ? `${Math.round(routeData.otp_origin_pct)}% / ${Math.round(routeData.otp_destination_pct ?? 0)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">OTP Origin / Destination</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.ewt_seconds != null
              ? `${Math.round(routeData.ewt_seconds)}`
              : 'N/A'}
            {routeData.ewt_seconds != null && (
              <span style={{ fontSize: '1.5rem' }}> sec</span>
            )}
          </div>
          <div className="stat-label">Excess Wait Time</div>
          {routeData.ewt_seconds == null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              (frequent service only)
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.bunching_rate != null
              ? `${(routeData.bunching_rate * 100).toFixed(1)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">Bunching Rate</div>
          {routeData.bunching_total_headways != null && routeData.bunching_total_headways > 0 && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              ({routeData.bunching_count} of {routeData.bunching_total_headways} pairs)
            </div>
          )}
        </div>
      </div>

      <div className="chart-container">
        <h2>Route Map</h2>
        <RouteMap routeId={routeId} />
      </div>

      {!hasMetrics && (
        <div className="no-data-message">
          <div className="no-data-icon">📊</div>
          <h2>No Performance Data Available</h2>
          <p>This route does not have enough data to calculate performance metrics for the latest service date.</p>
        </div>
      )}

      <div className="detail-info">
        <h3>Route Information</h3>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Route ID:</span>
            <span className="info-value">{routeData.route_id}</span>
          </div>
        </div>
      </div>
    </main>
  )
}

export default RouteDetail
