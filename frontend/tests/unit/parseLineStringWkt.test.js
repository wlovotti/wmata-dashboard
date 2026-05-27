import { describe, expect, it } from 'vitest'

import { parseLineStringWkt } from '../../src/utils/wkt.js'

describe('parseLineStringWkt', () => {
  it('parses a basic LINESTRING into [lat, lon] pairs (WKT lon-lat -> Leaflet lat-lon)', () => {
    const wkt = 'LINESTRING(-77.032 38.917, -77.032 38.910, -77.030 38.902)'
    expect(parseLineStringWkt(wkt)).toEqual([
      [38.917, -77.032],
      [38.910, -77.032],
      [38.902, -77.030],
    ])
  })

  it('accepts mixed whitespace and case', () => {
    expect(parseLineStringWkt('linestring ( -1 2 , -3 4 )')).toEqual([
      [2, -1],
      [4, -3],
    ])
  })

  it('returns empty array on null / undefined / wrong shape', () => {
    expect(parseLineStringWkt(null)).toEqual([])
    expect(parseLineStringWkt(undefined)).toEqual([])
    expect(parseLineStringWkt('')).toEqual([])
    expect(parseLineStringWkt('POINT(-77 38)')).toEqual([])
    expect(parseLineStringWkt('LINESTRING(garbage)')).toEqual([])
  })

  it('skips coordinate pairs that fail to parse to numbers', () => {
    expect(parseLineStringWkt('LINESTRING(-77 38, abc def, -76 37)')).toEqual([
      [38, -77],
      [37, -76],
    ])
  })
})
