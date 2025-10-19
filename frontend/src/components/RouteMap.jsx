import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, Polyline, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

function FitBounds({ bounds }) {
  const map = useMap()

  useEffect(() => {
    if (bounds && bounds.length > 0) {
      map.fitBounds(bounds, { padding: [50, 50] })
    }
  }, [bounds, map])

  return null
}

function RouteMap({ routeId, showSpeedSegments = false }) {
  const [shapes, setShapes] = useState([])
  const [segments, setSegments] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    // Fetch shapes (always)
    const shapesPromise = fetch(`/api/routes/${routeId}/shapes`)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .catch(() => ({ shapes: [] }))

    // Only fetch segments if explicitly requested (disabled by default for performance)
    const segmentsPromise = showSpeedSegments
      ? fetch(`/api/routes/${routeId}/segments`)
          .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
          .catch(() => ({ segments: [] }))
      : Promise.resolve({ segments: [] })

    Promise.all([shapesPromise, segmentsPromise])
      .then(([shapesData, segmentsData]) => {
        setShapes(shapesData.shapes || [])
        setSegments(segmentsData.segments || [])
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId, showSpeedSegments])

  if (loading) {
    return (
      <div className="map-container">
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading map...</p>
        </div>
      </div>
    )
  }

  if (error || (shapes.length === 0 && segments.length === 0)) {
    return (
      <div className="map-container">
        <div className="no-data-message" style={{ padding: '2rem' }}>
          <p>Map data not available for this route</p>
        </div>
      </div>
    )
  }

  // Function to get color based on speed
  const getSpeedColor = (speed) => {
    if (speed === null || speed === undefined) return '#919D9D' // Gray for no data
    if (speed < 5) return '#C8102E'   // Red - very slow
    if (speed < 10) return '#FA4616'  // Orange-red - slow
    if (speed < 15) return '#FFA300'  // Orange - moderate
    if (speed < 20) return '#67823A'  // Yellow-green - good
    return '#00BFB3'                  // Teal - fast
  }

  // Calculate bounds from all points
  const allPoints = segments.length > 0
    ? segments.flatMap(seg => seg.points.map(p => [p.lat, p.lon]))
    : shapes.flatMap(shape => shape.points.map(p => [p.lat, p.lon]))

  const bounds = allPoints.length > 0 ? allPoints : null

  // Default center (DC area)
  const defaultCenter = [38.9072, -77.0369]

  return (
    <div className="map-container" style={{ position: 'relative' }}>
      {segments.length > 0 && (
        <div style={{
          position: 'absolute',
          top: '10px',
          right: '10px',
          zIndex: 1000,
          backgroundColor: 'white',
          padding: '10px',
          borderRadius: '5px',
          boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
          fontSize: '0.75rem'
        }}>
          <div style={{ fontWeight: 'bold', marginBottom: '5px' }}>Speed (mph)</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '3px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#00BFB3' }}></div>
            <span>20+</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '3px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#67823A' }}></div>
            <span>15-20</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '3px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#FFA300' }}></div>
            <span>10-15</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '3px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#FA4616' }}></div>
            <span>5-10</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '3px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#C8102E' }}></div>
            <span>&lt;5</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
            <div style={{ width: '20px', height: '4px', backgroundColor: '#919D9D' }}></div>
            <span>No data</span>
          </div>
        </div>
      )}
      <MapContainer
        center={defaultCenter}
        zoom={13}
        style={{ height: '400px', width: '100%', borderRadius: '0.75rem' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {/* Show speed segments if available, otherwise show simple route shapes */}
        {segments.length > 0 ? (
          segments.map((segment, idx) => (
            <Polyline
              key={`segment-${idx}`}
              positions={segment.points.map(p => [p.lat, p.lon])}
              pathOptions={{
                color: getSpeedColor(segment.avg_speed_mph),
                weight: 5,
                opacity: 0.9
              }}
            />
          ))
        ) : (
          shapes.map((shape, idx) => (
            <Polyline
              key={shape.shape_id}
              positions={shape.points.map(p => [p.lat, p.lon])}
              pathOptions={{
                color: '#C8102E',
                weight: 4,
                opacity: 0.9
              }}
            />
          ))
        )}
        {bounds && <FitBounds bounds={bounds} />}
      </MapContainer>
    </div>
  )
}

export default RouteMap
