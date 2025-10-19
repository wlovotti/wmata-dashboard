import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import RouteMap from './RouteMap'

function RouteDetail() {
  const { routeId } = useParams()
  const navigate = useNavigate()
  const [routeData, setRouteData] = useState(null)
  const [timePeriods, setTimePeriods] = useState([])
  const [otpTrends, setOtpTrends] = useState([])
  const [headwayTrends, setHeadwayTrends] = useState([])
  const [speedTrends, setSpeedTrends] = useState([])
  const [earlyLateTrends, setEarlyLateTrends] = useState([])
  const [headwayRegularityTrends, setHeadwayRegularityTrends] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    // Fetch route detail
    const routePromise = fetch(`/api/routes/${routeId}`)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))

    // Fetch time period data
    const timePeriodsPromise = fetch(`/api/routes/${routeId}/time-periods`)
      .then(res => res.ok ? res.json() : { time_periods: [] })
      .catch(() => ({ time_periods: [] }))

    // Fetch OTP trend data
    const otpTrendsPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=otp`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    // Fetch headway trend data
    const headwayTrendsPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=headway`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    // Fetch speed trend data
    const speedTrendsPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=speed`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    // Fetch early trend data
    const earlyTrendsPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=early`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    // Fetch late trend data
    const lateTrendsPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=late`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    // Fetch headway std dev trend data
    const headwayStdDevPromise = fetch(`/api/routes/${routeId}/trend?days=30&metric=headway_std_dev`)
      .then(res => res.ok ? res.json() : { trend_data: [] })
      .catch(() => ({ trend_data: [] }))

    Promise.all([
      routePromise,
      timePeriodsPromise,
      otpTrendsPromise,
      headwayTrendsPromise,
      speedTrendsPromise,
      earlyTrendsPromise,
      lateTrendsPromise,
      headwayStdDevPromise
    ])
      .then(([route, timePeriodData, otpData, headwayData, speedData, earlyData, lateData, headwayStdDevData]) => {
        setRouteData(route)
        setTimePeriods(timePeriodData.time_periods || [])
        setOtpTrends(otpData.trend_data || [])
        setHeadwayTrends(headwayData.trend_data || [])
        setSpeedTrends(speedData.trend_data || [])

        // Combine early and late data into single array for dual-line chart
        const earlyTrendData = earlyData.trend_data || []
        const lateTrendData = lateData.trend_data || []
        const combined = earlyTrendData.map((earlyPoint, idx) => ({
          date: earlyPoint.date,
          early_percentage: earlyPoint.early_percentage,
          late_percentage: lateTrendData[idx]?.late_percentage || null
        }))
        setEarlyLateTrends(combined)

        setHeadwayRegularityTrends(headwayStdDevData.trend_data || [])
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId])

  const getGradeColor = (grade) => {
    const colors = {
      'A': '#00BFB3',
      'B': '#67823A',
      'C': '#FFA300',
      'D': '#FA4616',
      'F': '#C8102E',
      'N/A': '#919D9D'
    }
    return colors[grade] || colors['N/A']
  }

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

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate('/')} className="back-btn">
          ← Back to All Routes
        </button>
        <div className="route-title">
          <h1>
            <span className="route-badge-large" style={{
              backgroundColor: routeData.otp_percentage !== null ? '#002F6C' : '#919D9D'
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
            <span
              className="grade-badge-large"
              style={{ backgroundColor: getGradeColor(routeData.grade) }}
            >
              {routeData.grade}
            </span>
          </div>
          <div className="stat-label">Performance Grade</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.otp_percentage !== null
              ? `${Math.round(routeData.otp_percentage)}%`
              : 'N/A'}
          </div>
          <div className="stat-label">On-Time Performance</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.avg_headway_minutes !== null
              ? `${Math.round(routeData.avg_headway_minutes)}`
              : 'N/A'}
            {routeData.avg_headway_minutes !== null && <span style={{ fontSize: '1.5rem' }}> min</span>}
          </div>
          <div className="stat-label">Average Headway</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.headway_std_dev_minutes !== null
              ? `${Math.round(routeData.headway_std_dev_minutes * 10) / 10}`
              : 'N/A'}
            {routeData.headway_std_dev_minutes !== null && <span style={{ fontSize: '1.5rem' }}> min</span>}
          </div>
          <div className="stat-label">Headway Regularity</div>
          {routeData.headway_std_dev_minutes !== null && (
            <div style={{ fontSize: '0.75rem', marginTop: '0.25rem', opacity: 0.7 }}>
              (lower = more regular)
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {routeData.avg_speed_mph !== null
              ? `${Math.round(routeData.avg_speed_mph)}`
              : 'N/A'}
            {routeData.avg_speed_mph !== null && <span style={{ fontSize: '1.5rem' }}> mph</span>}
          </div>
          <div className="stat-label">Average Speed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {(routeData.total_positions || 0).toLocaleString()}
          </div>
          <div className="stat-label">Position Records</div>
        </div>
      </div>

      <div className="chart-container">
        <h2>Route Map</h2>
        <RouteMap routeId={routeId} />
      </div>

      {routeData.otp_percentage === null ? (
        <div className="no-data-message">
          <div className="no-data-icon">📊</div>
          <h2>No Performance Data Available</h2>
          <p>This route does not have enough data to calculate performance metrics.</p>
          <p>Data collection may still be in progress.</p>
        </div>
      ) : (
        <>
          {routeData.early_percentage !== null && routeData.late_percentage !== null && (
            <div className="chart-container">
              <h2>On-Time Performance Breakdown</h2>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={[{
                  name: 'Current Period',
                  early: Math.round(routeData.early_percentage),
                  onTime: Math.round(routeData.otp_percentage),
                  late: Math.round(routeData.late_percentage)
                }]}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis domain={[0, 100]} label={{ value: 'Percentage', angle: -90, position: 'insideLeft' }} />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="early" name="Early %" fill="#67823A" stackId="a" />
                  <Bar dataKey="onTime" name="On-Time %" fill="#00BFB3" stackId="a" />
                  <Bar dataKey="late" name="Late %" fill="#C8102E" stackId="a" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {timePeriods && timePeriods.length > 0 && (
            <div className="chart-container">
              <h2>Performance by Time of Day</h2>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={timePeriods}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time_period" />
                  <YAxis domain={[0, 100]} />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="otp_percentage" name="On-Time %" fill="#002F6C" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {otpTrends && otpTrends.length > 0 && (
            <div className="chart-container">
              <h2>On-Time Performance Trend (30 Days)</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={otpTrends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis domain={[0, 100]} />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="otp_percentage"
                    name="On-Time %"
                    stroke="#002F6C"
                    strokeWidth={2}
                    dot={{ fill: '#002F6C' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {headwayTrends && headwayTrends.length > 0 && (
            <div className="chart-container">
              <h2>Average Headway Trend (30 Days)</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={headwayTrends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="avg_headway_minutes"
                    name="Avg Headway (min)"
                    stroke="#00BFB3"
                    strokeWidth={2}
                    dot={{ fill: '#00BFB3' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {speedTrends && speedTrends.length > 0 && (
            <div className="chart-container">
              <h2>Average Speed Trend (30 Days)</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={speedTrends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="avg_speed_mph"
                    name="Avg Speed (mph)"
                    stroke="#FFA300"
                    strokeWidth={2}
                    dot={{ fill: '#FFA300' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {earlyLateTrends && earlyLateTrends.length > 0 && (
            <div className="chart-container">
              <h2>Early vs Late Percentage Trends (30 Days)</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={earlyLateTrends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis domain={[0, 100]} label={{ value: 'Percentage', angle: -90, position: 'insideLeft' }} />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="early_percentage"
                    name="Early %"
                    stroke="#67823A"
                    strokeWidth={2}
                    dot={{ fill: '#67823A' }}
                  />
                  <Line
                    type="monotone"
                    dataKey="late_percentage"
                    name="Late %"
                    stroke="#C8102E"
                    strokeWidth={2}
                    dot={{ fill: '#C8102E' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {headwayRegularityTrends && headwayRegularityTrends.length > 0 && (
            <div className="chart-container">
              <h2>Headway Regularity Trend (30 Days)</h2>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={headwayRegularityTrends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis label={{ value: 'Std Dev (minutes)', angle: -90, position: 'insideLeft' }} />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="headway_std_dev_minutes"
                    name="Headway Std Dev (lower = more regular)"
                    stroke="#002F6C"
                    strokeWidth={2}
                    dot={{ fill: '#002F6C' }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          <div className="detail-info">
            <h3>Route Information</h3>
            <div className="info-grid">
              <div className="info-item">
                <span className="info-label">Route ID:</span>
                <span className="info-value">{routeData.route_id}</span>
              </div>
              <div className="info-item">
                <span className="info-label">Last Updated:</span>
                <span className="info-value">
                  {routeData.data_updated_at
                    ? new Date(routeData.data_updated_at).toLocaleString()
                    : 'N/A'}
                </span>
              </div>
            </div>
          </div>
        </>
      )}
    </main>
  )
}

export default RouteDetail
