import { useEffect } from 'react'
import { MapContainer, TileLayer, Polyline, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

import { parseLineStringWkt } from '../utils/wkt.js'

/**
 * Imperative bounds fit — react-leaflet's declarative `bounds` prop is
 * applied once on mount; this hook re-fits whenever the polyline changes
 * (e.g. when a user expands a different corridor row).
 *
 * @param {{ bounds: Array<[number, number]> }} props
 * @returns {null}
 */
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
 * Renders a single corridor's LineString geometry on a Leaflet map.
 *
 * Built for the inline expansion panel on `/segments?level=corridor` —
 * compact (240px tall), no controls beyond pan/zoom, OSM tiles. Hidden
 * entirely if the WKT can't be parsed so a stale or unexpected payload
 * doesn't break the page.
 *
 * @param {{ geometryWkt: string, displayName?: string }} props
 * @returns {JSX.Element|null}
 */
function CorridorMap({ geometryWkt, displayName }) {
  const points = parseLineStringWkt(geometryWkt)
  if (points.length < 2) {
    return (
      <div className="corridor-map-placeholder" style={{ color: '#64748b', fontSize: '0.85rem' }}>
        Corridor geometry unavailable.
      </div>
    )
  }

  const center = points[Math.floor(points.length / 2)]

  return (
    <div className="corridor-map">
      <MapContainer
        center={center}
        zoom={14}
        style={{ height: '240px', width: '100%', borderRadius: '0.5rem' }}
        aria-label={displayName ? `Map: ${displayName}` : 'Corridor map'}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <Polyline
          positions={points}
          pathOptions={{ color: '#C8102E', weight: 5, opacity: 0.9 }}
        />
        <FitBounds bounds={points} />
      </MapContainer>
    </div>
  )
}

export default CorridorMap
