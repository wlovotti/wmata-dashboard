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
 *   - LRU size cap (PR #TBD finding 1): the oldest untouched entry is
 *     evicted once the cache exceeds MAX_ENTRIES (50), and reading an
 *     entry via getCacheEntry refreshes its recency so it survives an
 *     eviction sweep that would otherwise have dropped it
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

  test('LRU cap: caching a 51st distinct URL evicts the oldest untouched entry', () => {
    for (let i = 0; i < 50; i++) {
      setCacheEntry(`/api/entry-${i}`, { i })
    }
    // No reads in between — /api/entry-0 is still the least-recently-used
    // entry when the 51st URL pushes the cache over MAX_ENTRIES (50).
    setCacheEntry('/api/entry-50', { i: 50 })

    expect(getCacheEntry('/api/entry-0')).toBeUndefined()
    // The newest entry, and everything in between, survives.
    expect(getCacheEntry('/api/entry-50')).toEqual({ i: 50 })
    expect(getCacheEntry('/api/entry-1')).toEqual({ i: 1 })
  })

  test('LRU cap: reading an entry refreshes its recency so it survives an eviction that would otherwise drop it', () => {
    for (let i = 0; i < 50; i++) {
      setCacheEntry(`/api/lru-${i}`, { i })
    }
    // Touch the oldest entry via a read — this should move it to the
    // most-recently-used end, making /api/lru-1 the new eviction target.
    getCacheEntry('/api/lru-0')

    setCacheEntry('/api/lru-50', { i: 50 })

    expect(getCacheEntry('/api/lru-0')).toEqual({ i: 0 })
    expect(getCacheEntry('/api/lru-1')).toBeUndefined()
  })
})
