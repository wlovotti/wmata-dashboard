/**
 * Regression test for `FitBounds` (PR #217 review finding 2).
 *
 * The rest of SystemMap.jsx pulls in react-leaflet/leaflet map internals
 * (MapContainer, TileLayer, tile loading, DOM measurement) that aren't
 * meaningfully exercisable in jsdom — see the note in Overview.test.jsx,
 * which mocks SystemMap out entirely for that reason. `FitBounds` itself
 * is a small, self-contained component whose only dependency is the
 * `useMap` hook, so it's tested in isolation here by mocking just that
 * hook rather than mounting a real Leaflet map.
 *
 * Bug being guarded against: a `useMultiFetch` cache-hit revalidate hands
 * SystemMap a freshly parsed (reference-distinct) `routes` array even when
 * the shapes are unchanged, so the `bounds` memo recomputes and produces a
 * new array on every revalidate. Before the fix, FitBounds's
 * `useEffect(..., [bounds, map])` re-fired on every such revalidate and
 * called `map.fitBounds()` again, snapping a panned/zoomed viewport back
 * to full network extent ~0.5-2s after the cached paint. The fix guards
 * the call with a `useRef` so it only fires once per FitBounds mount.
 */
import { render } from '@testing-library/react'
import { vi } from 'vitest'
import { FitBounds } from '../../src/components/SystemMap'

const fitBounds = vi.fn()

vi.mock('react-leaflet', () => ({
  useMap: () => ({ fitBounds }),
  MapContainer: () => null,
  TileLayer: () => null,
  Polyline: () => null,
}))

beforeEach(() => {
  fitBounds.mockClear()
})

describe('FitBounds', () => {
  test('fits the map once on mount when bounds are non-empty', () => {
    render(<FitBounds bounds={[[38.9, -77.0], [38.91, -77.01]]} />)
    expect(fitBounds).toHaveBeenCalledTimes(1)
  })

  test('does not fit when bounds is empty', () => {
    render(<FitBounds bounds={[]} />)
    expect(fitBounds).not.toHaveBeenCalled()
  })

  test('a later bounds update (e.g. a background revalidate reference change) does NOT re-fit — the pan/zoom-snap regression', () => {
    const { rerender } = render(<FitBounds bounds={[[38.9, -77.0]]} />)
    expect(fitBounds).toHaveBeenCalledTimes(1)

    // A reference-distinct-but-equivalent bounds array, exactly what a
    // revalidated /api/shapes response produces via the `bounds` useMemo
    // in SystemMap even when the underlying shapes are unchanged.
    rerender(<FitBounds bounds={[[38.9, -77.0]]} />)
    rerender(<FitBounds bounds={[[38.91, -77.02]]} />)

    expect(fitBounds).toHaveBeenCalledTimes(1)
  })
})
