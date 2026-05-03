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

function RouteMap({ routeId }) {
  const [shapes, setShapes] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch(`/api/routes/${routeId}/shapes`)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .then(shapesData => {
        setShapes(shapesData.shapes || [])
        setLoading(false)
      })
      .catch(err => {
        setError(err.message || err)
        setLoading(false)
      })
  }, [routeId])

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

  if (error || shapes.length === 0) {
    return (
      <div className="map-container">
        <div className="no-data-message" style={{ padding: '2rem' }}>
          <p>Map data not available for this route</p>
        </div>
      </div>
    )
  }

  const allPoints = shapes.flatMap(shape => shape.points.map(p => [p.lat, p.lon]))
  const bounds = allPoints.length > 0 ? allPoints : null
  const defaultCenter = [38.9072, -77.0369]

  return (
    <div className="map-container" style={{ position: 'relative' }}>
      <MapContainer
        center={defaultCenter}
        zoom={13}
        style={{ height: '400px', width: '100%', borderRadius: '0.75rem' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {shapes.map((shape) => (
          <Polyline
            key={shape.shape_id}
            positions={shape.points.map(p => [p.lat, p.lon])}
            pathOptions={{
              color: '#C8102E',
              weight: 4,
              opacity: 0.9
            }}
          />
        ))}
        {bounds && <FitBounds bounds={bounds} />}
      </MapContainer>
    </div>
  )
}

export default RouteMap
