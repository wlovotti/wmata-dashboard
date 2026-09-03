import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { formatDeviationSignedSec } from '../utils/formatters'
import { apiUrl } from '../utils/apiUrl'
import useAgency from '../hooks/useAgency'
import { DEFAULT_WINDOW_DAYS, appendWindowParam } from '../hooks/useWindowDays'

function RecentRuns({ routeId }) {
  /**
   * "Recent runs" list on RouteDetail. Renders the runs returned by
   * `/api/routes/{route_id}/recent-runs`; clicking a row navigates to the
   * per-run drill-down page (`/runs/:runId`).
   *
   * The endpoint anchors on today's service_date if any runs exist for it,
   * otherwise the most recent service_date with runs — so this section is
   * non-empty for any route with aggregated runs in the system.
   */
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [agency] = useAgency()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(apiUrl(`/api/routes/${routeId}/recent-runs`))
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
  }, [routeId, agency])

  if (loading) {
    return (
      <div className="chart-container">
        <h2>Recent runs</h2>
        <p className="text-muted">Loading…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="chart-container">
        <h2>Recent runs</h2>
        <p className="text-muted">Unable to load runs: {error}</p>
      </div>
    )
  }

  const runs = data?.runs || []
  const serviceDate = data?.service_date

  return (
    <div className="chart-container">
      <h2>Recent runs</h2>
      {serviceDate ? (
        <p className="drilldown-anchor">
          Service date: {serviceDate} ({runs.length} run{runs.length === 1 ? '' : 's'})
        </p>
      ) : (
        <p className="drilldown-anchor">No aggregated runs yet for this route.</p>
      )}
      {runs.length > 0 && (
        <div className="recent-runs-table-wrapper">
          <table className="recent-runs-table">
            <thead>
              <tr>
                <th>Start</th>
                <th>End</th>
                <th>Headsign</th>
                <th>Direction</th>
                <th>Stops obs / sched</th>
                <th>Median dev</th>
                <th>p95 dev</th>
                <th>Vehicle</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <tr
                  key={r.run_id}
                  onClick={() =>
                    navigate(appendWindowParam(`/runs/${r.run_id}`, DEFAULT_WINDOW_DAYS, agency))
                  }
                  className="recent-runs-row"
                  title="View per-stop deviation chart"
                >
                  <td>{r.start_time || '—'}</td>
                  <td>{r.end_time || '—'}</td>
                  <td>{r.headsign || '—'}</td>
                  <td>{r.direction_id === 0 ? 'Out' : 'In'}</td>
                  <td>
                    {r.stops_observed ?? 0} / {r.stops_scheduled ?? 0}
                  </td>
                  <td>{formatDeviationSignedSec(r.dev_p50_sec)}</td>
                  <td>{formatDeviationSignedSec(r.dev_p95_sec)}</td>
                  <td>{r.vehicle_id || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default RecentRuns
