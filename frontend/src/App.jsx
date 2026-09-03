import { BrowserRouter as Router, Routes, Route, NavLink, useLocation } from 'react-router-dom'
import { useEffect, useRef, useState } from 'react'
import Overview from './components/Overview'
import RouteList from './components/RouteList'
import RouteDetail from './components/RouteDetail'
import RunDetail from './components/RunDetail'
import BlockTimeline from './components/BlockTimeline'
import ActiveBlocks from './components/ActiveBlocks'
import Targets from './components/Targets'
import ScheduleAudit from './components/ScheduleAudit'
import SegmentDiagnostic from './components/SegmentDiagnostic'
import AgencyComparison from './components/AgencyComparison'
import DiagnosticsIndex from './components/DiagnosticsIndex'
import useGtfsFreshness from './hooks/useGtfsFreshness'
import { clearFetchCache } from './hooks/fetchCache'
import useWindowDays, { appendWindowParam } from './hooks/useWindowDays'
import useAgency, { DEFAULT_AGENCY } from './hooks/useAgency'
import WindowPicker from './components/WindowPicker'
import AgencyToggle from './components/AgencyToggle'
import './App.css'

// Format a raw GTFS YYYYMMDD string (e.g. `feed_end_date`) as a
// human-readable date, mirroring RouteList's `formatSnapshotDate` (which
// formats ISO timestamps, not this bare-digits GTFS convention). Falls
// back to the raw string if it doesn't parse as an 8-digit date so the
// banner degrades gracefully instead of hiding the value.
function formatFeedDate(yyyymmdd) {
  if (!yyyymmdd || !/^\d{8}$/.test(yyyymmdd)) return yyyymmdd
  const year = Number(yyyymmdd.slice(0, 4))
  const month = Number(yyyymmdd.slice(4, 6))
  const day = Number(yyyymmdd.slice(6, 8))
  const d = new Date(year, month - 1, day)
  if (Number.isNaN(d.getTime())) return yyyymmdd
  return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
}

// Refresh button (the frontend-chrome honesty fixes, PR #204). Previously a
// bare `window.location.reload()` masquerading as an in-app refresh — it
// worked, but paid for it with a full browser navigation (re-downloads the
// JS bundle, flashes white, drops in-memory state) to do something React
// can do by just remounting the current page. `onRefresh` is expected to
// force that remount (see `handleRefresh` in `App`); this component only
// owns the button's own transient spinner state, since it has no way to
// observe when the remounted page's fetches actually resolve — the spin
// duration is a fixed visual cue, not a completion signal.
//
// Deliberately rendered OUTSIDE the keyed subtree that `App` remounts on
// refresh (see `App`'s render): if this button were inside that subtree,
// clicking it would remount RefreshButton itself in the same render that
// `onRefresh` fires, wiping the `refreshing` state we just set before the
// spinner/disabled feedback ever paints.
const REFRESH_SPIN_MS = 600

export function RefreshButton({ onRefresh }) {
  const [refreshing, setRefreshing] = useState(false)
  const timeoutRef = useRef(null)

  useEffect(() => {
    // Clear any in-flight spin timer on unmount so it doesn't fire
    // setState on an unmounted component.
    return () => {
      if (timeoutRef.current != null) clearTimeout(timeoutRef.current)
    }
  }, [])

  const handleClick = () => {
    if (refreshing) return
    setRefreshing(true)
    onRefresh()
    timeoutRef.current = setTimeout(() => {
      timeoutRef.current = null
      setRefreshing(false)
    }, REFRESH_SPIN_MS)
  }

  return (
    <button onClick={handleClick} disabled={refreshing} className="refresh-btn" title="Refresh data">
      <span className={refreshing ? 'refresh-icon spinning' : 'refresh-icon'}>↻</span>
      {refreshing ? 'Refreshing...' : 'Refresh'}
    </button>
  )
}

// Feed-expiry alarm (NOTES-90): renders only when the schedule is already
// expired or expiring within 7 days — silent (returns null) for `ok` or an
// unknown status (no feed_info row yet), so it never shows on a healthy or
// freshly-initialized DB.
function GtfsExpiryBanner({ freshness }) {
  if (!freshness || (freshness.status !== 'expired' && freshness.status !== 'expiring_soon')) {
    return null
  }
  const tint = freshness.status === 'expired' ? 'gtfs-expiry-banner-red' : 'gtfs-expiry-banner-yellow'
  const feedEndDate = formatFeedDate(freshness.feed_end_date)
  const message =
    freshness.status === 'expired'
      ? `GTFS schedule EXPIRED as of ${feedEndDate}`
      : `GTFS schedule expires soon (${feedEndDate})`
  return (
    <div className={`gtfs-expiry-banner ${tint}`} role="status">
      {message}
    </div>
  )
}

// Shell content rendered *inside* `<Router>` (App.jsx below only creates the
// Router — a component isn't a descendant of the elements it returns, so
// `useWindowDays`/`useSearchParams` can't be called directly in `App` itself;
// it needs a child component that Router actually wraps). Everything that
// reads or writes route/search-param state — the primary nav's `?days=`
// links, `<Routes>` — has to live here.
function AppShell({ refreshKey, handleRefresh }) {
  // Time-window picker (NOTES-140): `?days=` is the source of truth for
  // every page's analysis window. Read here (not written — WindowPicker
  // owns the write) so the primary nav links below can carry the current
  // selection forward, and it survives a plain click into another tab.
  const [days] = useWindowDays()

  // Agency switch (NOTES-143): `?agency=` is the source of truth for which
  // backend database the dashboard renders. Read here (not written —
  // AgencyToggle owns the write) for the same reason as `days` above.
  const [agencyParam] = useAgency()

  // Only Overview (`/`), RouteList (`/routes`), and RouteDetail
  // (`/route/:id`) actually read `?days=` (PR #239 review finding H) — show
  // the picker only there so it doesn't imply pages like Compare or
  // Diagnostics respond to it when they don't.
  const location = useLocation()
  const isCompare = location.pathname === '/compare'
  // Id-scoped pages whose path parameter is an opaque, per-database
  // identifier with no cross-agency meaning (PR #242 review finding 4):
  // `runs.id` is a per-DB autoincrement integer, and a GTFS `block_id`
  // string can coincidentally exist in both agencies' schedules pointing
  // at an unrelated block. Unlike `/route/:routeId` (route_ids are the
  // one identifier users deliberately compare across agencies, and an
  // agency switch there either loads that agency's own route or 404s
  // honestly), switching agency on these pages would silently swap in a
  // wrong-but-plausible-looking record from the other database instead of
  // erroring — so the toggle is hidden rather than left to produce that.
  // `/blocks` (the list) is NOT id-scoped; only `/blocks/:blockId` is.
  const isIdScopedRoute =
    location.pathname.startsWith('/runs/') ||
    (location.pathname.startsWith('/blocks/') && location.pathname !== '/blocks')
  const showWindowPicker =
    location.pathname === '/' ||
    location.pathname === '/routes' ||
    location.pathname.startsWith('/route/')

  // `/compare` is deliberately agency-independent (NOTES-143 decision 2) —
  // it renders both agencies side by side, so the header/title stay at the
  // default (wmata) copy even if `?agency=` is present on the URL (e.g.
  // carried over from another page — see the nav links below, which now
  // preserve it through the round trip per PR #242 review finding 10),
  // rather than flipping to Muni copy on a page that isn't scoped to
  // either agency. This pinned value drives ONLY the header copy and the
  // GTFS-freshness fetch below — nav links use `agencyParam` (the real
  // URL value) directly so navigating away from Compare restores whatever
  // agency was selected before, instead of silently resetting to wmata.
  const agency = isCompare ? DEFAULT_AGENCY : agencyParam
  const showAgencyToggle = !isCompare && !isIdScopedRoute

  // `useGtfsFreshness` needs the current agency (NOTES-143), which comes
  // from `useUrlState`/`useSearchParams` — only available inside `Router`.
  // Fetched here (not in `App` below, which renders outside `Router`) so
  // this hook has router context; App only owns `refreshKey`/`handleRefresh`.
  const gtfsFreshness = useGtfsFreshness(refreshKey, agency)

  const headerTitle =
    agency === 'sfmta' ? 'SFMTA (Muni) Performance Dashboard' : 'WMATA Performance Dashboard'
  const headerSubtitle =
    agency === 'sfmta'
      ? 'Daily Muni bus network performance metrics'
      : 'Daily bus network performance metrics'

  return (
    <div className="app">
      <header>
        <div className="header-content">
          <div>
            <h1>{headerTitle}</h1>
            <p className="subtitle">{headerSubtitle}</p>
          </div>
          <div className="header-actions">
            <div className="header-actions-row">
              {showAgencyToggle && <AgencyToggle />}
              {showWindowPicker && <WindowPicker />}
              <RefreshButton onRefresh={handleRefresh} />
            </div>
          </div>
        </div>
        <GtfsExpiryBanner freshness={gtfsFreshness} />
        <nav className="primary-nav" aria-label="Primary">
          {/* Nav links carry the current `?days=` window and `?agency=`
              selection (NOTES-140, NOTES-143) so both survive navigation
              instead of silently reverting to the default on the next page;
              appendWindowParam omits each param entirely at its default
              (30 / wmata) so unfiltered URLs stay clean. These use
              `agencyParam` (the real URL value), not the header's pinned
              `agency`, so navigating away from Compare restores whatever
              agency was selected before rather than resetting to wmata
              (PR #242 review finding 10) — Compare's own link now also
              carries `agency` for the same round-trip reason, even though
              Compare's fetches and header copy stay agency-independent. */}
          <NavLink
            to={appendWindowParam('/', days, agencyParam)}
            end
            className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
          >
            Overview
          </NavLink>
          <NavLink
            to={appendWindowParam('/routes', days, agencyParam)}
            className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
          >
            Routes
          </NavLink>
          <NavLink
            to={appendWindowParam('/compare', days, agencyParam)}
            className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
          >
            Compare
          </NavLink>
          <NavLink
            to={appendWindowParam('/diagnostics', days, agencyParam)}
            className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
          >
            Diagnostics
          </NavLink>
        </nav>
      </header>

      <div key={refreshKey}>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/routes" element={<RouteList />} />
          <Route path="/route/:routeId" element={<RouteDetail />} />
          <Route path="/runs/:runId" element={<RunDetail />} />
          <Route path="/blocks" element={<ActiveBlocks />} />
          <Route path="/blocks/:blockId" element={<BlockTimeline />} />
          <Route path="/targets" element={<Targets />} />
          <Route path="/schedule-audit" element={<ScheduleAudit />} />
          <Route path="/segments" element={<SegmentDiagnostic />} />
          <Route path="/diagnostics" element={<DiagnosticsIndex />} />
          {/* Agency comparison page (PR #198), promoted to the nav by the
              nav collapse (PR #208). */}
          <Route path="/compare" element={<AgencyComparison />} />
        </Routes>
      </div>
    </div>
  )
}

function App() {
  // Bumping this key remounts only the routed subtree below (wrapped
  // separately, not the whole `.app`), which re-runs each page's
  // data-fetching effects — a real in-place refetch instead of the old
  // `window.location.reload()` (the frontend-chrome honesty fixes, PR #204).
  // Kept outside the keyed subtree: RefreshButton itself (so its own
  // `refreshing` spinner state survives the remount it triggers — see
  // RefreshButton's comment) and the GTFS-expiry banner (refetched instead
  // via `refreshKey` threaded into `useGtfsFreshness`, since remounting it
  // would just replay the cached value rather than force a real refetch).
  // `useGtfsFreshness` itself now lives in `AppShell` (NOTES-143 — it needs
  // the current `?agency=`, which requires `Router` context this `App`
  // component doesn't have).
  const [refreshKey, setRefreshKey] = useState(0)
  // Manual invalidation path for the stale-while-revalidate fetch cache
  // (NOTES-122): clear every cached entry before remounting the routed
  // subtree so the remount's fetches are all cold misses instead of an
  // instant replay of whatever was cached before the click.
  const handleRefresh = () => {
    clearFetchCache()
    setRefreshKey((k) => k + 1)
  }

  return (
    <Router>
      <AppShell refreshKey={refreshKey} handleRefresh={handleRefresh} />
    </Router>
  )
}

export default App
