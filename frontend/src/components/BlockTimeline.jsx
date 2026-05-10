import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'

// WMATA on-time band: -2 min early to +7 min late. Mirrors `src/otp_constants.py`.
const ON_TIME_LOWER_SEC = -120
const ON_TIME_UPPER_SEC = 420

// Cap the deviation bar visual scale so a single 30-min slip doesn't squash
// every other row to a hairline. Tuned to make the "moderate cascade" case
// (5–10 min) the visual bulk of the bar; outliers clip but are still labeled.
const BAR_SCALE_SEC = 900

function todayEasternIso() {
  /**
   * Return today's date in Eastern (America/New_York) as YYYY-MM-DD.
   *
   * Mirrors `src/timezones.py::eastern_today` — service-date semantics are
   * an Eastern question even though the underlying browser clock is local.
   */
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
  return fmt.format(new Date())
}

function formatDeviationSec(sec) {
  /**
   * Render an integer deviation_sec with sign and "late"/"early" suffix.
   *
   * Returns "—" for null so the cells don't visually collapse. Mirrors
   * the semantics used in RecentRuns and RunDetail.
   */
  if (sec == null) return '—'
  if (sec === 0) return '0s'
  const sign = sec > 0 ? '+' : ''
  const minutes = Math.round(sec / 60)
  if (Math.abs(sec) >= 60) {
    return `${sign}${sec}s (${sign}${minutes}m)`
  }
  return `${sign}${sec}s`
}

function bucketColor(bucket) {
  /**
   * Map an OTP bucket label to a hex color for the deviation bar swatch.
   *
   * Greens / oranges / reds align with the existing scorecard palette.
   */
  switch (bucket) {
    case 'on_time':
      return '#16a34a'
    case 'late':
      return '#dc2626'
    case 'early':
      return '#0ea5e9'
    default:
      return '#94a3b8'
  }
}

function DeviationBar({ devSec, bucket }) {
  /**
   * Render a horizontal bar visualizing one deviation_sec value.
   *
   * The bar is anchored at zero and extends right (late) or left (early),
   * scaled so BAR_SCALE_SEC fills half the track. Bars beyond the scale
   * clip — the numeric label still tells the truth.
   *
   * Renders nothing when devSec is null (null → no observation).
   */
  if (devSec == null) {
    return <div className="block-bar-track" aria-hidden="true" />
  }
  const color = bucketColor(bucket)
  const pct = Math.min(100, (Math.abs(devSec) / BAR_SCALE_SEC) * 50)
  const left = devSec >= 0 ? 50 : 50 - pct
  return (
    <div className="block-bar-track">
      <div className="block-bar-zero" />
      <div
        className="block-bar-otp-band"
        style={{
          left: `${50 + (ON_TIME_LOWER_SEC / BAR_SCALE_SEC) * 50}%`,
          width: `${((ON_TIME_UPPER_SEC - ON_TIME_LOWER_SEC) / BAR_SCALE_SEC) * 50}%`,
        }}
      />
      <div
        className="block-bar-fill"
        style={{
          left: `${left}%`,
          width: `${pct}%`,
          backgroundColor: color,
        }}
      />
    </div>
  )
}

function BlockTimeline() {
  /**
   * Block-level cascade timeline view (NOTES-45).
   *
   * Renders the chained trips on one block_id for a service_date as a
   * vertical timeline. Each row shows scheduled vs observed start/end
   * plus origin/destination deviation bars side-by-side, so the reader
   * can see lateness propagating from one trip's destination into the
   * next trip's origin (cascade) versus per-trip independent variance.
   *
   * Service date is read from the `?service_date=` query parameter and
   * defaults to today (Eastern). The date input updates the URL so the
   * view is bookmarkable.
   */
  const { blockId } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  const initialDate = searchParams.get('service_date') || todayEasternIso()
  const [serviceDate, setServiceDate] = useState(initialDate)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`/api/blocks/${blockId}/timeline?service_date=${serviceDate}`)
      .then((res) => {
        if (!res.ok) {
          return res.json().then(
            (body) => Promise.reject(body?.detail || `HTTP ${res.status}`),
            () => Promise.reject(`HTTP ${res.status}`),
          )
        }
        return res.json()
      })
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(typeof err === 'string' ? err : err?.message || String(err))
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [blockId, serviceDate])

  const handleDateChange = (e) => {
    const next = e.target.value
    setServiceDate(next)
    const nextParams = new URLSearchParams(searchParams)
    nextParams.set('service_date', next)
    setSearchParams(nextParams, { replace: true })
  }

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate(-1)} className="back-btn">
          ← Back
        </button>
        <div className="route-title">
          <h1>Block {blockId}</h1>
          <p style={{ color: '#64748b', marginTop: '0.25rem' }}>
            Vehicle's chained trips for one service day. Cascade lateness shows
            up as one trip's destination delay carrying into the next trip's
            origin.
          </p>
        </div>
        <div className="block-date-picker">
          <label htmlFor="block-service-date">Service date</label>
          <input
            id="block-service-date"
            type="date"
            value={serviceDate}
            onChange={handleDateChange}
          />
        </div>
      </div>

      {loading && (
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading block timeline...</p>
        </div>
      )}

      {error && !loading && (
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading block timeline:</strong> {error}
          </div>
        </div>
      )}

      {!loading && !error && data && (
        <>
          <div className="stats-summary">
            <div className="stat-card">
              <div className="stat-value">{data.summary?.n_trips ?? 0}</div>
              <div className="stat-label">Trips on block</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">{data.summary?.n_observed ?? 0}</div>
              <div className="stat-label">Observed</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {data.summary?.n_late_destination ?? 0}
              </div>
              <div className="stat-label">Late destination</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {formatDeviationSec(data.summary?.max_destination_dev_sec)}
              </div>
              <div className="stat-label">Worst destination dev</div>
            </div>
          </div>

          <div className="chart-container">
            <h2>Block timeline</h2>
            <p className="drilldown-anchor">
              Routes served: {(data.route_ids || []).join(', ') || '—'}.
              Bars show origin and destination deviation against the WMATA
              −2/+7 on-time band (faint green strip). Cascade signal: each
              trip's <em>origin</em> bar matches the prior trip's{' '}
              <em>destination</em> bar.
            </p>

            {(!data.trips || data.trips.length === 0) && (
              <p className="drilldown-empty">
                No trips found for this block on {serviceDate}.
              </p>
            )}

            {data.trips?.length > 0 && (
              <div className="block-timeline">
                {data.trips.map((trip, idx) => (
                  <div
                    key={trip.trip_id}
                    className="block-timeline-row"
                    onClick={() =>
                      trip.run_id != null && navigate(`/runs/${trip.run_id}`)
                    }
                    role={trip.run_id != null ? 'button' : undefined}
                    tabIndex={trip.run_id != null ? 0 : undefined}
                    title={
                      trip.run_id != null
                        ? 'Open per-stop deviation chart for this run'
                        : 'No run observed for this trip on this date'
                    }
                  >
                    <div className="block-timeline-marker">
                      <div className="block-timeline-marker-dot" />
                      {idx < data.trips.length - 1 && (
                        <div className="block-timeline-marker-line" />
                      )}
                    </div>
                    <div className="block-timeline-trip">
                      <div className="block-timeline-trip-header">
                        <strong>
                          Route {trip.route_id}
                          {trip.headsign ? ` — ${trip.headsign}` : ''}
                        </strong>
                        <span style={{ color: '#64748b', marginLeft: '0.5rem' }}>
                          Trip {trip.trip_id} · Dir{' '}
                          {trip.direction_id === 0 ? 'Out' : 'In'}
                        </span>
                      </div>
                      <div className="block-timeline-trip-times">
                        Scheduled {trip.scheduled_start ?? '—'} →{' '}
                        {trip.scheduled_end ?? '—'} · Observed{' '}
                        {trip.observed_start ?? '—'} →{' '}
                        {trip.observed_end ?? '—'}
                      </div>
                      <div className="block-timeline-bars">
                        <div className="block-timeline-bar-row">
                          <div className="block-timeline-bar-label">Origin</div>
                          <DeviationBar
                            devSec={trip.origin_dev_sec}
                            bucket={
                              trip.origin_dev_sec == null
                                ? null
                                : trip.origin_dev_sec < ON_TIME_LOWER_SEC
                                  ? 'early'
                                  : trip.origin_dev_sec > ON_TIME_UPPER_SEC
                                    ? 'late'
                                    : 'on_time'
                            }
                          />
                          <div className="block-timeline-bar-value">
                            {formatDeviationSec(trip.origin_dev_sec)}
                          </div>
                        </div>
                        <div className="block-timeline-bar-row">
                          <div className="block-timeline-bar-label">
                            Destination
                          </div>
                          <DeviationBar
                            devSec={trip.destination_dev_sec}
                            bucket={trip.otp_bucket}
                          />
                          <div className="block-timeline-bar-value">
                            {formatDeviationSec(trip.destination_dev_sec)}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </main>
  )
}

export default BlockTimeline
