import { useState, useEffect } from 'react'
import { useNavigate, useSearchParams, Link } from 'react-router-dom'
import { formatDeviationMmSs, todayEasternIso } from '../utils/formatters'
import ErrorState from './ErrorState.jsx'
import { apiUrl } from '../utils/apiUrl'
import useAgency from '../hooks/useAgency'
import { DEFAULT_WINDOW_DAYS, appendWindowParam } from '../hooks/useWindowDays'
import './ActiveBlocks.css'

const LIMIT_OPTIONS = [25, 50, 100, 200, 500]

/**
 * System-level `/blocks` index page (PR #105). Lists the active blocks
 * for the selected service date, ranked by trip count desc and worst
 * observed deviation desc. Each row links to the existing
 * `BlockTimeline` (`/blocks/:blockId`) so the cascade view is one click
 * away. Populates from `/api/blocks/active`.
 *
 * Until this page existed, blocks were only reachable from the
 * `RouteDetail` Blocks tab — operators had to know the route to find
 * the block. The system-level rank surfaces the chains worth
 * investigating first regardless of route.
 *
 * `service_date` and `limit` round-trip through the URL (same
 * `useSearchParams` omit-default pattern as `ScheduleAudit.jsx` /
 * `SegmentDiagnostic.jsx`) so navigating to a block's timeline and back
 * restores the filtered view instead of resetting to today.
 */
function ActiveBlocks() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const defaultServiceDate = todayEasternIso()
  const serviceDate = searchParams.get('service_date') || defaultServiceDate
  const limitParam = Number(searchParams.get('limit'))
  const limit = Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 100

  const updateParam = (key, value) => {
    const next = new URLSearchParams(searchParams)
    if (value == null || value === '') {
      next.delete(key)
    } else {
      next.set(key, String(value))
    }
    setSearchParams(next, { replace: false })
  }

  const setServiceDate = (newDate) =>
    updateParam('service_date', newDate === defaultServiceDate ? null : newDate)
  const setLimit = (newLimit) => updateParam('limit', newLimit === 100 ? null : newLimit)

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryTick, setRetryTick] = useState(0)
  const [agency] = useAgency()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(apiUrl('/api/blocks/active', { service_date: serviceDate, limit }))
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || String(err))
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [serviceDate, limit, retryTick, agency])

  const blocks = data?.blocks || []

  return (
    <main>
      <div className="chart-container">
        <p className="breadcrumb-link-row">
          <Link
            to={appendWindowParam('/diagnostics', DEFAULT_WINDOW_DAYS, agency)}
            className="breadcrumb-link"
          >
            ← Diagnostics
          </Link>
        </p>
        <h2>Active blocks</h2>
        <p className="drilldown-anchor">
          A block chains a vehicle's consecutive trips through the day.
          When a trip falls behind, the next trip on the same block
          typically inherits the lateness. Rows are ranked by trip count
          and worst observed deviation — the longest, most cascade-prone
          chains land at the top.
        </p>

        <div className="filter-bar active-blocks-filter-row">
          <label className="filter-label-flex">
            <span className="opacity-80">Service date:</span>
            <input
              type="date"
              value={serviceDate}
              onChange={(e) => setServiceDate(e.target.value)}
              aria-label="Service date for active blocks"
            />
          </label>
          <label className="filter-label-flex">
            <span className="opacity-80">Limit:</span>
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              aria-label="Row limit"
            >
              {LIMIT_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </div>

        {loading && <p className="panel-loading-text">Loading blocks…</p>}
        {error && (
          <ErrorState
            title="Unable to load blocks"
            message={error}
            onRetry={() => setRetryTick((t) => t + 1)}
          />
        )}

        {!loading && !error && blocks.length === 0 && (
          <p className="text-muted">
            No active blocks found for {serviceDate}.
          </p>
        )}

        {!loading && !error && blocks.length > 0 && (
          <div className="recent-runs-table-wrapper">
            <table className="recent-runs-table">
              <thead>
                <tr>
                  <th>Block ID</th>
                  <th>First trip start</th>
                  <th>Trips in block</th>
                  <th>Routes</th>
                  <th>Worst dev (m:ss)</th>
                  <th>Observed?</th>
                </tr>
              </thead>
              <tbody>
                {blocks.map((b) => (
                  <tr
                    key={b.block_id}
                    className="recent-runs-row"
                    title="View block timeline"
                    onClick={() =>
                      navigate(
                        appendWindowParam(
                          `/blocks/${encodeURIComponent(b.block_id)}?service_date=${encodeURIComponent(serviceDate)}`,
                          DEFAULT_WINDOW_DAYS,
                          agency,
                        ),
                      )
                    }
                  >
                    <td>
                      <Link
                        to={appendWindowParam(
                          `/blocks/${encodeURIComponent(b.block_id)}?service_date=${encodeURIComponent(serviceDate)}`,
                          DEFAULT_WINDOW_DAYS,
                          agency,
                        )}
                        onClick={(e) => e.stopPropagation()}
                      >
                        {b.block_id}
                      </Link>
                    </td>
                    <td>
                      {b.scheduled_start ? b.scheduled_start.slice(11, 16) : '—'}
                    </td>
                    <td>{b.trip_count}</td>
                    <td>{(b.routes || []).join(', ')}</td>
                    <td>{formatDeviationMmSs(b.worst_deviation_seconds)}</td>
                    <td>{b.any_observed ? 'yes' : 'no'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </main>
  )
}

export default ActiveBlocks
