import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'

/**
 * Format a target value for display, given the canonical metric key. Mirrors
 * the unit conventions used everywhere else: OTP percent, service-delivered
 * fraction (rendered as %), EWT seconds (rendered as minutes for operator
 * readability), bunching fraction (rendered as %).
 */
function formatTarget(metric, value) {
  if (value == null) return '—'
  if (metric === 'otp') return `${value.toFixed(0)}%`
  if (metric === 'service_delivered') return `${(value * 100).toFixed(0)}%`
  if (metric === 'ewt') return `${(value / 60).toFixed(1)} min`
  if (metric === 'bunching') return `${(value * 100).toFixed(1)}%`
  return String(value)
}

const METRIC_ORDER = ['otp', 'service_delivered', 'ewt', 'bunching']
const METRIC_LABELS = {
  otp: 'OTP',
  service_delivered: 'Service Delivered',
  ewt: 'EWT',
  bunching: 'Bunching',
}

/**
 * `/targets` page (PR #105). Read-only renderer over
 * `config/route_targets.yaml` — the system defaults plus per-route
 * overrides — so an operator can see what the dashboard's "vs target"
 * comparisons compare against without opening the YAML.
 *
 * Editing stays git-only. NOTES-47's design explicitly allowed the
 * read-only-in-UI / edit-in-yaml split because the targets are a
 * commitment artifact (low edit frequency, want the change in version
 * control with the rest of the config).
 */
function Targets() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/targets')
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
  }, [])

  if (loading) {
    return (
      <main>
        <div className="chart-container">
          <h2>Performance targets</h2>
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading targets...</p>
          </div>
        </div>
      </main>
    )
  }

  if (error) {
    return (
      <main>
        <div className="chart-container">
          <h2>Performance targets</h2>
          <p style={{ color: '#64748b' }}>Unable to load targets: {error}</p>
        </div>
      </main>
    )
  }

  const systemDefault = data?.system_default || {}
  const routes = data?.routes || {}
  const sortedRouteIds = Object.keys(routes).sort()

  return (
    <main>
      <div className="chart-container">
        <h2>Performance targets</h2>
        <p className="drilldown-anchor">
          Read-only view of <code>config/route_targets.yaml</code>. Edits land
          via git — the dashboard reloads automatically on the next request
          after the file mtime advances. Per-route overrides apply per metric;
          missing entries inherit the system default.
        </p>

        <h3 style={{ marginTop: '1.5rem' }}>System defaults</h3>
        <table className="routes-table" style={{ marginTop: '0.5rem' }}>
          <thead>
            <tr>
              {METRIC_ORDER.map((m) => (
                <th key={m}>{METRIC_LABELS[m]}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr>
              {METRIC_ORDER.map((m) => (
                <td key={m} className="metric">
                  {formatTarget(m, systemDefault[m])}
                </td>
              ))}
            </tr>
          </tbody>
        </table>

        <h3 style={{ marginTop: '1.5rem' }}>Per-route overrides</h3>
        {sortedRouteIds.length === 0 ? (
          <p style={{ color: '#64748b' }}>
            No per-route overrides configured. Every route inherits the system
            defaults above. Add overrides by editing{' '}
            <code>config/route_targets.yaml</code>.
          </p>
        ) : (
          <table className="routes-table" style={{ marginTop: '0.5rem' }}>
            <thead>
              <tr>
                <th>Route</th>
                {METRIC_ORDER.map((m) => (
                  <th key={m}>{METRIC_LABELS[m]}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedRouteIds.map((rid) => {
                const block = routes[rid] || {}
                return (
                  <tr key={rid}>
                    <td className="route-id">
                      <Link to={`/route/${rid}`}>{rid}</Link>
                    </td>
                    {METRIC_ORDER.map((m) => (
                      <td key={m} className="metric">
                        {block[m] != null ? (
                          formatTarget(m, block[m])
                        ) : (
                          <span style={{ color: '#94a3b8' }} title="Inherits system default">
                            {formatTarget(m, systemDefault[m])}
                          </span>
                        )}
                      </td>
                    ))}
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </main>
  )
}

export default Targets
