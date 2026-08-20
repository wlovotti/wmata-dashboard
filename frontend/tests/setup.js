import '@testing-library/jest-dom'
import { afterEach } from 'vitest'
import { clearFetchCache } from '../src/hooks/fetchCache'

// The stale-while-revalidate fetch cache (NOTES-122) is a module-level
// singleton shared across every test in a file. Without a reset, a test
// that renders a component after an earlier test already populated the
// cache for the same URL would see stale data flash before the mocked
// fetch for that specific test resolves. Clearing after every test keeps
// each test's fetch mock the sole source of truth.
afterEach(() => {
  clearFetchCache()
})
