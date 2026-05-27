/**
 * Parse a WKT LINESTRING into an array of [lat, lon] tuples.
 *
 * WKT stores coordinates as `(lon lat, lon lat, ...)` per the standard;
 * Leaflet expects `[lat, lon]`, so we swap on the way out.
 *
 * Returns an empty array on any parse failure so callers can render a
 * "geometry unavailable" state instead of crashing.
 *
 * @param {string} wkt - WKT LineString, e.g. `LINESTRING(-77.03 38.91, -77.03 38.90)`
 * @returns {Array<[number, number]>}
 */
export function parseLineStringWkt(wkt) {
  if (!wkt || typeof wkt !== 'string') return []
  const match = wkt.match(/^LINESTRING\s*\(([^)]+)\)\s*$/i)
  if (!match) return []
  const points = []
  for (const pair of match[1].split(',')) {
    const [lonStr, latStr] = pair.trim().split(/\s+/)
    const lon = Number(lonStr)
    const lat = Number(latStr)
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      points.push([lat, lon])
    }
  }
  return points
}
