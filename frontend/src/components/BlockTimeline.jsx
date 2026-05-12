import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'

// WMATA on-time band: -2 min early to +7 min late. Mirrors `src/otp_constants.py`
// and the per-run chart in RunDetail. Same thresholds drive the card color
// coding here so the visual language is consistent across the dashboard.
const ON_TIME_LOWER_SEC = -120
const ON_TIME_UPPER_SEC = 420

// Cascade carry threshold (NOTES-45). When a trip ends ≥5 min late AND the
// next trip on the same block starts ≥5 min late AND there was no vehicle
// swap between them, we flag the trailing trip as carrying the leader's
// lateness. 5 min is a deliberately conservative threshold — it's well past
// the on-time band's upper edge (7 min late) on the early side, so we won't
// false-positive on minor running-time slop.
const CASCADE_CARRY_SEC = 300

function devColor(deviationSec) {
  /**
   * Color a deviation card by WMATA on-time band.
   *
   * - green:  within band or early (≤ +7 min late)
   * - yellow: late beyond band, ≤ 14 min (twice the on-time threshold)
   * - red:    > 14 min late
   * - gray:   no observation
   *
   * Returns a hex string suitable for inline `borderLeftColor`.
   */
  if (deviationSec == null) return '#cbd5e1'
  if (deviationSec <= ON_TIME_UPPER_SEC) return '#16a34a'
  if (deviationSec <= ON_TIME_UPPER_SEC * 2) return '#ca8a04'
  return '#dc2626'
}

function formatDeviation(deviationSec) {
  /**
   * Render an integer deviation_sec as ±M:SS with "late"/"early" suffix.
   *
   * Used in the per-card deviation chips. Returns "no data" for null so the
   * UI never renders an ambiguous "0s" when the observation was missing.
   */
  if (deviationSec == null) return 'no data'
  const sign = deviationSec > 0 ? '+' : deviationSec < 0 ? '-' : ''
  const abs = Math.abs(deviationSec)
  const mins = Math.floor(abs / 60)
  const secs = abs % 60
  const mmss = `${mins}:${secs.toString().padStart(2, '0')}`
  if (deviationSec === 0) return 'on time'
  const verbal = deviationSec > 0 ? 'late' : 'early'
  return `${sign}${mmss} (${verbal})`
}

function formatTimeOnly(iso) {
  /**
   * Render an Eastern ISO timestamp as HH:MM. Returns "—" for null.
   */
  if (!iso) return '—'
  return iso.slice(11, 16) || '—'
}

function BlockTimeline() {
  /**
   * Block timeline view (NOTES-45). Renders the scheduled chain of trips
   * for one block on one service date, with origin/destination deviation
   * shown as color-coded cards and inter-card adornments for vehicle
   * swaps and lateness carry-over.
   *
   * The service_date is read from a `?service_date=YYYY-MM-DD` query param,
   * defaulting to today (Eastern) on the API side.
   */
  const { blockId } = useParams()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const serviceDateParam = searchParams.get('service_date')

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const qs = serviceDateParam
      ? `?service_date=${encodeURIComponent(serviceDateParam)}`
      : ''
    fetch(`/api/blocks/${encodeURIComponent(blockId)}${qs}`)
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
  }, [blockId, serviceDateParam])

  if (loading) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(-1)} className="back-btn">
            ← Back
          </button>
        </div>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading block timeline…</p>
        </div>
      </main>
    )
  }

  if (error || !data) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(-1)} className="back-btn">
            ← Back
          </button>
        </div>
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading block timeline:</strong>{' '}
            {error || 'Block not found'}
          </div>
        </div>
      </main>
    )
  }

  const trips = data.trips || []

  // Pre-compute per-trip status flags consumed by the inter-card adornments.
  // Done at render time (not on the server) because both cues depend on
  // pairwise relationships between consecutive cards; the server returns
  // each card independently.
  const annotated = trips.map((t, i) => {
    if (i === 0) return { ...t, swapFromPrev: false, carryFromPrev: false }
    const prev = trips[i - 1]
    const swap =
      prev.observed_vehicle_id != null &&
      t.observed_vehicle_id != null &&
      prev.observed_vehicle_id !== t.observed_vehicle_id
    const carry =
      !swap &&
      prev.destination_deviation_seconds != null &&
      prev.destination_deviation_seconds >= CASCADE_CARRY_SEC &&
      t.origin_deviation_seconds != null &&
      t.origin_deviation_seconds >= CASCADE_CARRY_SEC
    return { ...t, swapFromPrev: swap, carryFromPrev: carry }
  })

  const observedCount = trips.filter((t) => t.trip_status !== 'not_observed').length
  const firstStart = trips.length > 0 ? formatTimeOnly(trips[0].scheduled_start) : '—'
  const lastEnd =
    trips.length > 0
      ? formatTimeOnly(trips[trips.length - 1].scheduled_end)
      : '—'
  const routeIds = Array.from(new Set(trips.map((t) => t.route_id)))

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate(-1)} className="back-btn">
          ← Back
        </button>
        <div className="route-title">
          <h1>Block {data.block_id}</h1>
          <p style={{ color: '#64748b', marginTop: '0.25rem' }}>
            Service date {data.service_date} · {trips.length} trip
            {trips.length === 1 ? '' : 's'} · {observedCount} observed ·
            Routes: {routeIds.join(', ') || '—'}
          </p>
        </div>
      </div>

      <div className="stats-summary">
        <div className="stat-card">
          <div className="stat-value">{trips.length}</div>
          <div className="stat-label">Trips in chain</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{observedCount}</div>
          <div className="stat-label">Trips observed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{firstStart}</div>
          <div className="stat-label">First scheduled start</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{lastEnd}</div>
          <div className="stat-label">Last scheduled end</div>
        </div>
      </div>

      <div className="chart-container">
        <h2>Timeline</h2>
        <p className="drilldown-anchor">
          Trips are shown in scheduled order. Origin / destination chips show
          the deviation against the WMATA −2 / +7 min on-time window. A
          <span
            style={{
              display: 'inline-block',
              padding: '0 0.4rem',
              margin: '0 0.25rem',
              border: '1px solid #64748b',
              borderRadius: '3px',
              fontSize: '0.75rem',
            }}
          >
            swap
          </span>
          badge between cards means the dispatcher changed buses. A
          <span
            style={{
              display: 'inline-block',
              padding: '0 0.4rem',
              margin: '0 0.25rem',
              border: '1px solid #dc2626',
              color: '#dc2626',
              borderRadius: '3px',
              fontSize: '0.75rem',
            }}
          >
            carry
          </span>
          arrow means the leader's lateness propagated to the next trip
          without a vehicle change — a cascade signal.
        </p>

        {trips.length === 0 ? (
          <p style={{ color: '#64748b' }}>
            No trips found for this block.
          </p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {annotated.map((t, i) => (
              <div key={t.trip_id}>
                {i > 0 && (t.swapFromPrev || t.carryFromPrev) && (
                  <div
                    style={{
                      display: 'flex',
                      gap: '0.4rem',
                      alignItems: 'center',
                      paddingLeft: '0.75rem',
                      margin: '0.25rem 0',
                    }}
                  >
                    <span style={{ color: '#64748b' }}>↓</span>
                    {t.swapFromPrev && (
                      <span
                        style={{
                          padding: '0.15rem 0.5rem',
                          border: '1px solid #64748b',
                          color: '#475569',
                          borderRadius: '3px',
                          fontSize: '0.75rem',
                        }}
                        title="Vehicle changed between these trips — dispatcher swap"
                      >
                        swap
                      </span>
                    )}
                    {t.carryFromPrev && (
                      <span
                        style={{
                          padding: '0.15rem 0.5rem',
                          border: '1px solid #dc2626',
                          color: '#dc2626',
                          borderRadius: '3px',
                          fontSize: '0.75rem',
                        }}
                        title="Previous trip ended ≥5 min late and this trip started ≥5 min late with no vehicle change — cascade carry"
                      >
                        carry
                      </span>
                    )}
                  </div>
                )}

                <div
                  className="stat-card block-timeline-card"
                  style={{
                    textAlign: 'left',
                    borderLeft: `6px solid ${devColor(
                      t.destination_deviation_seconds ?? t.origin_deviation_seconds,
                    )}`,
                    padding: '0.75rem 1rem',
                    display: 'grid',
                    gridTemplateColumns: '1fr auto',
                    gap: '0.5rem',
                    cursor: t.run_id ? 'pointer' : 'default',
                  }}
                  onClick={() => {
                    if (t.run_id) navigate(`/runs/${t.run_id}`)
                  }}
                  title={t.run_id ? 'View per-run deviation chart' : ''}
                >
                  <div>
                    <div style={{ fontWeight: 600 }}>
                      Route {t.route_id} ·{' '}
                      {t.direction_id === 0 ? 'Outbound' : 'Inbound'}
                      {t.trip_headsign && (
                        <span
                          style={{
                            fontWeight: 400,
                            color: '#475569',
                            marginLeft: '0.5rem',
                          }}
                        >
                          — {t.trip_headsign}
                        </span>
                      )}
                    </div>
                    <div
                      style={{
                        color: '#64748b',
                        fontSize: '0.85rem',
                        marginTop: '0.15rem',
                      }}
                    >
                      Sched {formatTimeOnly(t.scheduled_start)} →{' '}
                      {formatTimeOnly(t.scheduled_end)} · Trip {t.trip_id}
                    </div>
                    <div style={{ marginTop: '0.4rem', display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
                      <span style={{ fontSize: '0.85rem' }}>
                        <span style={{ color: '#64748b' }}>Origin:</span>{' '}
                        <span
                          style={{
                            color: devColor(t.origin_deviation_seconds),
                            fontWeight: 500,
                          }}
                        >
                          {formatDeviation(t.origin_deviation_seconds)}
                        </span>
                      </span>
                      <span style={{ fontSize: '0.85rem' }}>
                        <span style={{ color: '#64748b' }}>Destination:</span>{' '}
                        <span
                          style={{
                            color: devColor(t.destination_deviation_seconds),
                            fontWeight: 500,
                          }}
                        >
                          {formatDeviation(t.destination_deviation_seconds)}
                        </span>
                      </span>
                      <span style={{ fontSize: '0.85rem', color: '#64748b' }}>
                        Status: {t.trip_status.replace('_', ' ')}
                      </span>
                    </div>
                  </div>
                  <div
                    style={{
                      textAlign: 'right',
                      color: '#475569',
                      fontSize: '0.8rem',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {t.observed_vehicle_id ? (
                      <span
                        style={{
                          padding: '0.15rem 0.5rem',
                          background: '#e2e8f0',
                          borderRadius: '999px',
                          fontWeight: 500,
                        }}
                        title="Observed vehicle_id (from runs)"
                      >
                        Bus {t.observed_vehicle_id}
                      </span>
                    ) : (
                      <span style={{ color: '#94a3b8' }}>no vehicle</span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </main>
  )
}

export default BlockTimeline
