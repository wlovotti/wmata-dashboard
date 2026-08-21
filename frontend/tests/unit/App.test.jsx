/**
 * App.jsx refresh invalidation (PR #218 finding 6).
 *
 * `clearFetchCache()` in App.jsx's `handleRefresh` is the sole manual
 * invalidation path for the stale-while-revalidate fetch cache (NOTES-122)
 * — the header Refresh button is the only UI affordance that forces a cold
 * refetch instead of replaying cached data. Every existing test exercising
 * this exercised `clearFetchCache()` directly (fetchCache.test.js,
 * useMultiFetch.test.js) or the button's own spinner behavior
 * (RefreshButton.test.jsx) — none of them actually click the real Refresh
 * button wired into `App` and assert the cache gets cleared. Deleting the
 * `clearFetchCache()` call from `handleRefresh` would leave every existing
 * test green; this test pins the integration point so that regression is
 * caught.
 *
 * Every routed page component and useGtfsFreshness are mocked out — this
 * test is only about the refresh wiring, not any individual page's own
 * data fetching (already covered by that page's own test file, e.g.
 * Overview.test.jsx). The mocks also sidestep App.jsx eagerly importing
 * every route module (including ones that pull in react-leaflet) even
 * though only the default "/" route actually renders here.
 */
import { act, render, fireEvent, screen } from '@testing-library/react'
import { vi } from 'vitest'
import App from '../../src/App'

const mockClearFetchCache = vi.fn()

function stub(name) {
  return { default: () => <div data-testid={name} /> }
}

vi.mock('../../src/hooks/fetchCache', () => ({
  clearFetchCache: () => mockClearFetchCache(),
}))
vi.mock('../../src/hooks/useGtfsFreshness', () => ({ default: () => null }))
vi.mock('../../src/components/Overview', () => stub('overview-stub'))
vi.mock('../../src/components/RouteList', () => stub('routelist-stub'))
vi.mock('../../src/components/RouteDetail', () => stub('routedetail-stub'))
vi.mock('../../src/components/RunDetail', () => stub('rundetail-stub'))
vi.mock('../../src/components/BlockTimeline', () => stub('blocktimeline-stub'))
vi.mock('../../src/components/ActiveBlocks', () => stub('activeblocks-stub'))
vi.mock('../../src/components/Targets', () => stub('targets-stub'))
vi.mock('../../src/components/ScheduleAudit', () => stub('scheduleaudit-stub'))
vi.mock('../../src/components/SegmentDiagnostic', () => stub('segmentdiagnostic-stub'))
vi.mock('../../src/components/AgencyComparison', () => stub('agencycomparison-stub'))
vi.mock('../../src/components/DiagnosticsIndex', () => stub('diagnosticsindex-stub'))

describe('App refresh invalidation', () => {
  beforeEach(() => {
    mockClearFetchCache.mockClear()
  })

  test('clicking the header Refresh button calls clearFetchCache', () => {
    render(<App />)

    act(() => {
      fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    })

    expect(mockClearFetchCache).toHaveBeenCalledTimes(1)
  })
})
