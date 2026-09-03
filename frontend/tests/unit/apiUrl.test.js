/**
 * utils/apiUrl.js (NOTES-143) — the single place every fetch in
 * `frontend/src` (except the deliberately agency-independent `/compare`
 * components) builds its request URL. Covers the three behaviors called
 * out by the design: the default agency (wmata) is never sent, a
 * non-default agency (sfmta) read off the live URL is appended, and an
 * explicit `agency` in `params` overrides the URL-derived value.
 */
import { apiUrl } from '../../src/utils/apiUrl'

afterEach(() => {
  window.history.pushState({}, '', '/')
})

describe('apiUrl', () => {
  test('omits agency at the default (wmata), with no other params', () => {
    expect(apiUrl('/api/routes')).toBe('/api/routes')
  })

  test('omits agency at the default even when other params are given', () => {
    expect(apiUrl('/api/routes', { days: 7 })).toBe('/api/routes?days=7')
  })

  test('appends agency read off the current URL when non-default', () => {
    window.history.pushState({}, '', '/routes?agency=sfmta')
    expect(apiUrl('/api/routes', { days: 7 })).toBe('/api/routes?days=7&agency=sfmta')
  })

  test('falls back to the default for an unrecognized agency on the URL', () => {
    window.history.pushState({}, '', '/routes?agency=cta')
    expect(apiUrl('/api/routes')).toBe('/api/routes')
  })

  test('an explicit agency in params overrides the URL-derived value', () => {
    window.history.pushState({}, '', '/routes?agency=sfmta')
    expect(apiUrl('/api/routes', { agency: 'wmata' })).toBe('/api/routes')
    expect(apiUrl('/api/gtfs/freshness', { agency: 'sfmta' })).toBe(
      '/api/gtfs/freshness?agency=sfmta',
    )
  })

  test('accepts a URLSearchParams instance as params', () => {
    const params = new URLSearchParams()
    params.set('level', 'corridor')
    params.set('period', 'am_peak')
    expect(apiUrl('/api/segments', params)).toBe('/api/segments?level=corridor&period=am_peak')
  })
})
