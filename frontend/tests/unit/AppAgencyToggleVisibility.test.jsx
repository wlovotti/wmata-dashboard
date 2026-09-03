/**
 * App.jsx agency-toggle visibility rule (PR #242 review finding 4).
 *
 * The header AgencyToggle is hidden on `/compare` (agency-independent by
 * design) and on id-scoped pages whose path parameter is an opaque,
 * per-database identifier with no cross-agency meaning: `/runs/:runId`
 * (a per-DB autoincrement integer) and `/blocks/:blockId` (a GTFS
 * `block_id` string that can coincidentally exist in both agencies'
 * schedules). `/blocks` (the list, no id) and `/route/:routeId` (route_ids
 * are the one identifier users deliberately compare across agencies) keep
 * the toggle.
 *
 * Uses `window.history.pushState` before rendering `<App />` (which owns
 * its own `BrowserRouter`) to control the initial path, mirroring the
 * pattern `useUrlState.test.jsx` uses for the same reason.
 */
import { render, screen } from '@testing-library/react'
import { vi } from 'vitest'
import App from '../../src/App'

function stub(name) {
  return { default: () => <div data-testid={name} /> }
}

vi.mock('../../src/hooks/fetchCache', () => ({ clearFetchCache: () => {} }))
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

function renderAt(path) {
  window.history.pushState({}, '', path)
  return render(<App />)
}

afterEach(() => {
  window.history.pushState({}, '', '/')
})

describe('App agency-toggle visibility', () => {
  test.each([
    ['/', 'overview-stub'],
    ['/routes', 'routelist-stub'],
    ['/route/D72', 'routedetail-stub'],
    ['/blocks', 'activeblocks-stub'],
    ['/targets', 'targets-stub'],
    ['/schedule-audit', 'scheduleaudit-stub'],
    ['/segments', 'segmentdiagnostic-stub'],
    ['/diagnostics', 'diagnosticsindex-stub'],
  ])('shows the toggle on %s', (path, testId) => {
    renderAt(path)
    expect(screen.getByTestId(testId)).toBeInTheDocument()
    expect(screen.getByRole('group', { name: 'Agency' })).toBeInTheDocument()
  })

  test.each([
    ['/compare', 'agencycomparison-stub'],
    ['/runs/123', 'rundetail-stub'],
    ['/blocks/6601_M11', 'blocktimeline-stub'],
  ])('hides the toggle on %s', (path, testId) => {
    renderAt(path)
    expect(screen.getByTestId(testId)).toBeInTheDocument()
    expect(screen.queryByRole('group', { name: 'Agency' })).not.toBeInTheDocument()
  })
})

/**
 * PR #242 review finding 10: Overview → Muni → Compare → Overview must not
 * silently reset to WMATA. The Compare nav link now carries the current
 * agency (for the round trip), and — while actually on /compare — the
 * OTHER nav links must still reflect the real `?agency=` on the URL, not
 * the header's agency-independent pinned value, so navigating away from
 * Compare restores the previously-selected agency.
 */
describe('App nav links preserve agency through /compare', () => {
  test('the Compare nav link carries the current agency', () => {
    renderAt('/route/D72?agency=sfmta')
    expect(screen.getByRole('link', { name: 'Compare' })).toHaveAttribute(
      'href',
      '/compare?agency=sfmta',
    )
  })

  test('while on /compare, the other nav links still carry the agency selected before arriving', () => {
    renderAt('/compare?agency=sfmta')
    expect(screen.getByRole('link', { name: 'Overview' })).toHaveAttribute(
      'href',
      '/?agency=sfmta',
    )
    expect(screen.getByRole('link', { name: 'Routes' })).toHaveAttribute(
      'href',
      '/routes?agency=sfmta',
    )
    expect(screen.getByRole('link', { name: 'Diagnostics' })).toHaveAttribute(
      'href',
      '/diagnostics?agency=sfmta',
    )
  })

  test('the header copy stays pinned to wmata on /compare even with ?agency=sfmta in the URL', () => {
    renderAt('/compare?agency=sfmta')
    expect(
      screen.getByRole('heading', { name: 'WMATA Performance Dashboard' }),
    ).toBeInTheDocument()
  })
})
