import { useEffect, useMemo, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { MapContainer, TileLayer, Polyline, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { routeLineColor } from '../utils/mapColors'
import useMultiFetch from '../hooks/useMultiFetch'
import useAgency from '../hooks/useAgency'
import useWindowDays, { appendWindowParam } from '../hooks/useWindowDays'
import { apiUrl } from '../utils/apiUrl'

/**
 * Fit the map viewport to the full network extent once bounds are known —
 * but only once per mount (PR #217 review finding 2). A `useMultiFetch`
 * cache hit paints instantly from stale shapes and then background-
 * revalidates; the revalidated response is a freshly parsed, reference-
 * distinct `routes` array even when the polylines are unchanged, so the
 * `bounds` memo in SystemMap recomputes and would re-fire this effect on
 * every revalidate without the `hasFit` guard below — snapping the
 * viewport back to full extent ~0.5-2s after paint and discarding any
 * pan/zoom the user did in that window. The guard is per-FitBounds-
 * instance (a `useRef`), so a genuine remount (e.g. the header Refresh
 * button, which clears the cache and remounts the routed subtree) still
 * gets a fresh fit.
 */
export function FitBounds({ bounds }) {
  const map = useMap()
  const hasFit = useRef(false)

  useEffect(() => {
    if (!hasFit.current && bounds && bounds.length > 0) {
      map.fitBounds(bounds, { padding: [20, 20] })
      hasFit.current = true
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
 * /api/shapes is the largest single payload Overview pulls (~200 KB) and
 * changes only on a GTFS reload, making it the prime candidate for the
 * stale-while-revalidate fetch cache (NOTES-122) — routed through
 * `useMultiFetch` so returning to Overview paints the map instantly from
 * the cached shapes instead of blocking on a spinner. The fetch/parse of
 * that ~200 KB still happens on every visit (SWR revalidates in the
 * background by design, it doesn't skip the request) — what the cache
 * buys is removing that round-trip from the critical path of the first
 * paint, not eliminating it.
 *
 * Props:
 *   scorecardRoutes – `routes` array from /api/routes, or null while loading.
 */
function SystemMap({ scorecardRoutes }) {
  const navigate = useNavigate()
  const [agency] = useAgency()
  // Carry the current time-window selection into RouteDetail navigation
  // (PR #242 review finding 14) — read-only here, WindowPicker in the app
  // shell owns writes.
  const [days] = useWindowDays()
  // Memoized on `agency` (NOTES-143) so the URL array recomputes on an
  // agency switch instead of replaying the previous agency's shapes —
  // `useMultiFetch`'s documented contract asks callers to memoize `urls`.
  const shapesUrls = useMemo(() => [apiUrl('/api/shapes', { agency })], [agency])
  const {
    data: shapes,
    error,
  } = useMultiFetch(shapesUrls, ([json]) => json?.routes ?? [])

  const byRouteId = useMemo(
    () => new Map((scorecardRoutes || []).map((r) => [r.route_id, r])),
    [scorecardRoutes],
  )

  // Memoized on `shapes` alone so its identity is stable across unrelated
  // Overview re-renders (e.g. the "Biggest drags" metric select) — an
  // inline flatMap in the render body would hand FitBounds a new array
  // every render, whose effect deps would then re-fire and snap a
  // panned/zoomed map back to full extent. Hooks must run unconditionally,
  // so this sits above the early returns below; `shapes` can still be null
  // pre-load, hence the `?? []`.
  const bounds = useMemo(() => (shapes ?? []).flatMap((route) => route.points), [shapes])

  if (error) {
    return (
      <div className="chart-container system-map-error">
        <h2>System map</h2>
        <p className="status-text">
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

  const defaultCenter = [38.9072, -77.0369]

  return (
    <div className="chart-container system-map">
      <h2>System map</h2>
      <p className="drilldown-anchor mb-3">
        Routes colored by on-time performance vs target — grey routes have
        no measured data this week. Click a route for detail.
      </p>
      <MapContainer
        center={defaultCenter}
        zoom={11}
        className="map-canvas map-canvas--lg"
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
            eventHandlers={{
              click: () =>
                navigate(appendWindowParam(`/route/${route.route_id}`, days, agency)),
            }}
          />
        ))}
        {bounds.length > 0 && <FitBounds bounds={bounds} />}
      </MapContainer>
    </div>
  )
}

export default SystemMap
