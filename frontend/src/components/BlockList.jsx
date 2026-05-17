import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { formatDeviationMmSs, todayEasternIso } from '../utils/formatters'

function BlockList({ routeId }) {
  /**
   * "Blocks" tab on RouteDetail (NOTES-45). Lists every block_id that
   * touches the route on the selected service_date — clicking a row
   * navigates to `/blocks/:blockId` with the date as a query param.
   *
   * The date picker defaults to today (Eastern); blocks with no
   * observations (early in the day, or off-schedule service dates) still
   * render so the user can see the planned chain.
   */
  const navigate = useNavigate()
  const [serviceDate, setServiceDate] = useState(todayEasternIso())
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`/api/routes/${routeId}/blocks?service_date=${encodeURIComponent(serviceDate)}`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || err)
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [routeId, serviceDate])

  const blocks = data?.blocks || []

  return (
    <div className="chart-container">
      <h2>Blocks</h2>
      <p className="drilldown-anchor">
        A block chains a vehicle's consecutive trips through the day. When a
        trip falls behind, the next trip on the same block typically inherits
        the lateness — picking a block surfaces that cascade.
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
            aria-label="Service date for block list"
          />
        </label>
      </div>

      {loading && <p style={{ color: '#64748b' }}>Loading blocks…</p>}
      {error && <p style={{ color: '#64748b' }}>Unable to load blocks: {error}</p>}

      {!loading && !error && blocks.length === 0 && (
        <p style={{ color: '#64748b' }}>
          No blocks found for this route on {serviceDate}.
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
                <th>Trips on this route</th>
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
                >
                  <td>{b.block_id}</td>
                  <td>
                    {b.scheduled_start
                      ? b.scheduled_start.slice(11, 16)
                      : '—'}
                  </td>
                  <td>{b.trip_count}</td>
                  <td>{b.trips_on_route}</td>
                  <td>{formatDeviationMmSs(b.worst_deviation_seconds)}</td>
                  <td>{b.any_observed ? 'yes' : 'no'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default BlockList
