/**
 * hooks/useAgency.js (NOTES-143) — the `?agency=` URL-state hook the
 * agency switch UI is built on. Thin wrapper over `useUrlState`, mirroring
 * `useWindowDays.test.jsx`'s coverage of the `?days=` equivalent: default
 * fallback, reading a valid value, and validating against the allowed set.
 */
import { renderHook, act } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import useAgency, { AGENCIES, DEFAULT_AGENCY } from '../../src/hooks/useAgency'

function LocationSpy({ locationRef }) {
  locationRef.current = useLocation()
  return null
}

function renderUseAgency(initialEntries = ['/']) {
  const locationRef = { current: null }
  const { result } = renderHook(() => useAgency(), {
    wrapper: ({ children }) => (
      <MemoryRouter initialEntries={initialEntries}>
        <LocationSpy locationRef={locationRef} />
        {children}
      </MemoryRouter>
    ),
  })
  return { result, locationRef }
}

afterEach(() => {
  window.history.pushState({}, '', '/')
})

describe('useAgency', () => {
  test('defaults to wmata when the param is absent', () => {
    const { result } = renderUseAgency()
    expect(result.current[0]).toBe('wmata')
    expect(DEFAULT_AGENCY).toBe('wmata')
  })

  test('reads a valid non-default agency from the URL', () => {
    const { result } = renderUseAgency(['/?agency=sfmta'])
    expect(result.current[0]).toBe('sfmta')
  })

  test('falls back to the default for an unrecognized agency value', () => {
    const { result } = renderUseAgency(['/?agency=cta'])
    expect(result.current[0]).toBe('wmata')
  })

  test('setting a non-default agency writes it to the URL', () => {
    const { result, locationRef } = renderUseAgency()
    act(() => result.current[1]('sfmta'))
    expect(locationRef.current.search).toBe('?agency=sfmta')
    expect(result.current[0]).toBe('sfmta')
  })

  test('setting the default agency omits the param from the URL', () => {
    const { result, locationRef } = renderUseAgency(['/?agency=sfmta'])
    act(() => result.current[1]('wmata'))
    expect(locationRef.current.search).toBe('')
    expect(result.current[0]).toBe('wmata')
  })

  test('AGENCIES lists exactly the two supported agencies', () => {
    expect(AGENCIES).toEqual(['wmata', 'sfmta'])
  })
})
