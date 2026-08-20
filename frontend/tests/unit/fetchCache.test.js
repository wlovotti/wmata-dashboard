/**
 * Unit tests for hooks/fetchCache.js — the module-level stale-while-
 * revalidate cache underlying useMultiFetch (NOTES-122).
 *
 * Tests:
 *   - miss: getCacheEntry returns undefined for an unknown URL
 *   - hit: setCacheEntry followed by getCacheEntry returns the stored value
 *   - a cached `null` JSON value is distinguishable from a miss
 *   - overwrite: setCacheEntry replaces a prior entry for the same URL
 *   - independence: entries for different URLs don't collide
 *   - clearFetchCache empties every entry
 */
import { getCacheEntry, setCacheEntry, clearFetchCache } from '../../src/hooks/fetchCache'

afterEach(() => {
  clearFetchCache()
})

describe('fetchCache', () => {
  test('miss: unknown URL returns undefined', () => {
    expect(getCacheEntry('/api/never-fetched')).toBeUndefined()
  })

  test('hit: a stored value round-trips through getCacheEntry', () => {
    setCacheEntry('/api/foo', { foo: 1 })
    expect(getCacheEntry('/api/foo')).toEqual({ foo: 1 })
  })

  test('a cached null JSON response is distinguishable from a miss', () => {
    setCacheEntry('/api/null-body', null)
    expect(getCacheEntry('/api/null-body')).toBeNull()
    expect(getCacheEntry('/api/never-set')).toBeUndefined()
  })

  test('overwrite: a second set for the same URL replaces the first', () => {
    setCacheEntry('/api/foo', { v: 1 })
    setCacheEntry('/api/foo', { v: 2 })
    expect(getCacheEntry('/api/foo')).toEqual({ v: 2 })
  })

  test('independence: entries for different URLs do not collide', () => {
    setCacheEntry('/api/a', { id: 'a' })
    setCacheEntry('/api/b', { id: 'b' })
    expect(getCacheEntry('/api/a')).toEqual({ id: 'a' })
    expect(getCacheEntry('/api/b')).toEqual({ id: 'b' })
  })

  test('clearFetchCache empties every entry', () => {
    setCacheEntry('/api/a', { id: 'a' })
    setCacheEntry('/api/b', { id: 'b' })
    clearFetchCache()
    expect(getCacheEntry('/api/a')).toBeUndefined()
    expect(getCacheEntry('/api/b')).toBeUndefined()
  })
})
