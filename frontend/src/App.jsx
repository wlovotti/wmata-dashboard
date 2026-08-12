import { BrowserRouter as Router, Routes, Route, NavLink } from 'react-router-dom'
import { useState } from 'react'
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
import useGtfsFreshness from './hooks/useGtfsFreshness'
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

function App() {
  const [refreshing, setRefreshing] = useState(false)
  const gtfsFreshness = useGtfsFreshness()

  const handleRefresh = () => {
    setRefreshing(true)
    // Trigger refresh - this will be handled by child components
    window.location.reload()
  }

  return (
    <Router>
      <div className="app">
        <header>
          <div className="header-content">
            <div>
              <h1>WMATA Performance Dashboard</h1>
              <p className="subtitle">Real-time transit performance metrics</p>
            </div>
            <div className="header-actions">
              <button
                onClick={handleRefresh}
                disabled={refreshing}
                className="refresh-btn"
                title="Refresh data"
              >
                <span className={refreshing ? 'refresh-icon spinning' : 'refresh-icon'}>↻</span>
                {refreshing ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          </div>
          <GtfsExpiryBanner freshness={gtfsFreshness} />
          <nav className="primary-nav" aria-label="Primary">
            <NavLink to="/" end className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Overview
            </NavLink>
            <NavLink to="/routes" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Routes
            </NavLink>
            <NavLink to="/blocks" end className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Blocks
            </NavLink>
            <NavLink to="/targets" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Targets
            </NavLink>
            <NavLink to="/schedule-audit" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Schedule audit
            </NavLink>
            <NavLink to="/segments" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Segments
            </NavLink>
          </nav>
        </header>

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
          {/* Agency comparison page (PR #TBD): reachable by URL only for now — no nav link, so the
              baselined pages' shared header chrome (Overview / RouteList /
              RouteDetail-D72 Playwright visual regressions) is untouched.
              Add a nav entry once NOTES-84 revisits the header. */}
          <Route path="/compare" element={<AgencyComparison />} />
        </Routes>
      </div>
    </Router>
  )
}

export default App
