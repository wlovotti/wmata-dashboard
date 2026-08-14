import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { MapContainer, TileLayer, Polyline, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { routeLineColor } from '../utils/mapColors'

/** Fit the map viewport to the full network extent once bounds are known. */
function FitBounds({ bounds }) {
  const map = useMap()

  useEffect(() => {
    if (bounds && bounds.length > 0) {
      map.fitBounds(bounds, { padding: [20, 20] })
    }
  }, [bounds, map])

  return null
}

/**
 * Overview system map (NOTES-84): every current route's representative
 * polyline from /api/shapes, colored by OTP vs target via routeLineColor —
 * the most direct answer to "where is it going badly". Routes without a
 * scorecard row render neutral grey (the network stays visible; unmeasured
 * is not hidden). Click a route to open its detail page.
 *
 * Failure posture: the map is an enhancement, not load-bearing — a shapes
 * fetch failure renders a quiet inline note and the fold's movers panel
 * carries the answer alone (Overview's CSS lets this cell collapse).
 *
 * Props:
 *   scorecardRoutes – `routes` array from /api/routes, or null while loading.
 */
function SystemMap({ scorecardRoutes }) {
  const navigate = useNavigate()
  const [shapes, setShapes] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/shapes')
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((json) => {
        if (!cancelled) setShapes(json?.routes ?? [])
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || String(err))
      })
    return () => {
      cancelled = true
    }
  }, [])

  const byRouteId = useMemo(
    () => new Map((scorecardRoutes || []).map((r) => [r.route_id, r])),
    [scorecardRoutes],
  )

  if (error) {
    return (
      <div className="chart-container system-map-error">
        <h2>System map</h2>
        <p style={{ color: 'var(--color-muted)', fontSize: '0.85rem' }}>
          Map unavailable right now ({error}) — the movers list still tells
          you where to look.
        </p>
      </div>
    )
  }

  if (shapes == null) {
    return (
      <div className="chart-container">
        <h2>System map</h2>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading map...</p>
        </div>
      </div>
    )
  }

  const bounds = shapes.flatMap((route) => route.points)
  const defaultCenter = [38.9072, -77.0369]

  return (
    <div className="chart-container system-map">
      <h2>System map</h2>
      <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
        Routes colored by on-time performance vs target — grey routes have
        no measured data this week. Click a route for detail.
      </p>
      <MapContainer
        center={defaultCenter}
        zoom={11}
        style={{ height: '420px', width: '100%', borderRadius: '0.75rem' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {shapes.map((route) => (
          <Polyline
            key={route.route_id}
            positions={route.points}
            pathOptions={{
              color: routeLineColor(byRouteId.get(route.route_id)),
              weight: 3,
              opacity: 0.85,
            }}
            eventHandlers={{ click: () => navigate(`/route/${route.route_id}`) }}
          />
        ))}
        {bounds.length > 0 && <FitBounds bounds={bounds} />}
      </MapContainer>
    </div>
  )
}

export default SystemMap
