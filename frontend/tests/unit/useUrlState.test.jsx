/**
 * hooks/useUrlState.js (NOTES-140) — the generic "sync one piece of state
 * with one URL search param" primitive every filter migrated in this PR
 * (WindowPicker's `days`, RouteDetail's `day_type`/`period`, RouteList's
 * `q`/`sort`/`view`/`metric`, StopDiagnostic's `metric`/`direction_id`,
 * Overview's contributors `metric`) is built on.
 *
 * `LocationSpy` renders alongside the hook under test purely to expose the
 * `MemoryRouter`'s current `location` to assertions — the hook itself never
 * returns the raw URL, only the derived value.
 */
import { renderHook, act } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import useUrlState from '../../src/hooks/useUrlState'

function LocationSpy({ locationRef }) {
  locationRef.current = useLocation()
  return null
}

function renderUseUrlState(key, defaultValue, initialEntries = ['/']) {
  const locationRef = { current: null }
  const { result } = renderHook(() => useUrlState(key, defaultValue), {
    wrapper: ({ children }) => (
      <MemoryRouter initialEntries={initialEntries}>
        <LocationSpy locationRef={locationRef} />
        {children}
      </MemoryRouter>
    ),
  })
  return { result, locationRef }
}

describe('useUrlState', () => {
  test('returns defaultValue when the param is absent from the URL', () => {
    const { result } = renderUseUrlState('days', 30)
    expect(result.current[0]).toBe(30)
  })

  test('reads and coerces a numeric param present in the URL', () => {
    const { result } = renderUseUrlState('days', 30, ['/?days=7'])
    expect(result.current[0]).toBe(7)
  })

  test('falls back to the numeric default for a non-numeric param value', () => {
    const { result } = renderUseUrlState('days', 30, ['/?days=banana'])
    expect(result.current[0]).toBe(30)
  })

  test('reads a string param verbatim', () => {
    const { result } = renderUseUrlState('metric', 'otp', ['/?metric=ewt'])
    expect(result.current[0]).toBe('ewt')
  })

  test('setting a non-default value writes it to the URL', () => {
    const { result, locationRef } = renderUseUrlState('days', 30)
    act(() => result.current[1](7))
    expect(locationRef.current.search).toBe('?days=7')
    expect(result.current[0]).toBe(7)
  })

  test('setting the default value omits the param from the URL', () => {
    const { result, locationRef } = renderUseUrlState('days', 30, ['/?days=7'])
    act(() => result.current[1](30))
    expect(locationRef.current.search).toBe('')
    expect(result.current[0]).toBe(30)
  })

  test('writing one key preserves other existing params', () => {
    const { result, locationRef } = renderUseUrlState('metric', 'otp', ['/?other=x'])
    act(() => result.current[1]('ewt'))
    const params = new URLSearchParams(locationRef.current.search)
    expect(params.get('metric')).toBe('ewt')
    expect(params.get('other')).toBe('x')
  })

  test('setting null clears the param, same as the default', () => {
    const { result, locationRef } = renderUseUrlState('q', '', ['/?q=D72'])
    act(() => result.current[1](null))
    expect(locationRef.current.search).toBe('')
  })
})
