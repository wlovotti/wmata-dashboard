import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { formatDeviationMmSs, todayEasternIso } from '../utils/formatters'

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
 */
function ActiveBlocks() {
  const navigate = useNavigate()
  const [serviceDate, setServiceDate] = useState(todayEasternIso())
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`/api/blocks/active?service_date=${encodeURIComponent(serviceDate)}`)
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
  }, [serviceDate])

  const blocks = data?.blocks || []

  return (
    <main>
      <div className="chart-container">
        <h2>Active blocks</h2>
        <p className="drilldown-anchor">
          A block chains a vehicle's consecutive trips through the day.
          When a trip falls behind, the next trip on the same block
          typically inherits the lateness. Rows are ranked by trip count
          and worst observed deviation — the longest, most cascade-prone
          chains land at the top.
        </p>

        <div
          style={{
            display: 'flex',
            gap: '0.75rem',
            alignItems: 'center',
            margin: '0.5rem 0 1rem',
            fontSize: '0.875rem',
          }}
        >
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <span style={{ opacity: 0.8 }}>Service date:</span>
            <input
              type="date"
              value={serviceDate}
              onChange={(e) => setServiceDate(e.target.value)}
              aria-label="Service date for active blocks"
            />
          </label>
        </div>

        {loading && <p style={{ color: '#64748b' }}>Loading blocks…</p>}
        {error && <p style={{ color: '#64748b' }}>Unable to load blocks: {error}</p>}

        {!loading && !error && blocks.length === 0 && (
          <p style={{ color: '#64748b' }}>
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
                        `/blocks/${encodeURIComponent(b.block_id)}?service_date=${encodeURIComponent(serviceDate)}`,
                      )
                    }
                    style={{ cursor: 'pointer' }}
                  >
                    <td>{b.block_id}</td>
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
