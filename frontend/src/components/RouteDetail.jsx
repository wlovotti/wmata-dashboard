import { useState, useEffect, useMemo, useRef } from 'react'
import useMultiFetch from '../hooks/useMultiFetch'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import RouteMap from './RouteMap'
import PeriodDrilldown from './PeriodDrilldown'
import RecentRuns from './RecentRuns'
import BlockList from './BlockList'
import RouteTrend, {
  DeltaIndicator,
  TargetIndicator,
} from './RouteTrend'
import { computeWindowDelta } from '../utils/computeWindowDelta'
import StopDiagnostic from './StopDiagnostic'
import RouteDiagnosisPanel from './RouteDiagnosisPanel'
import { badgeColor, FREQUENCY_CLASS_LABELS } from '../frequencyClass'
import { getMoversFloor } from '../moversFloor'
import useUrlState from '../hooks/useUrlState'
import useWindowDays, { appendWindowParam } from '../hooks/useWindowDays'
import useAgency, { AGENCY_LABELS, DEFAULT_AGENCY } from '../hooks/useAgency'
import { apiUrl } from '../utils/apiUrl'
import './RouteDetail.css'

// Day-type / time-period filter options (NOTES-41). Keys must match the API's
// accepted values (src/time_periods.py: VALID_DAY_TYPES / VALID_PERIOD_KEYS).
const DAY_TYPE_OPTIONS = [
  { key: 'all', label: 'All days' },
  { key: 'weekday', label: 'Weekday' },
  { key: 'saturday', label: 'Saturday' },
  { key: 'sunday', label: 'Sunday' },
]
const PERIOD_OPTIONS = [
  { key: 'all', label: 'All hours' },
  { key: 'am_peak', label: 'AM Peak (6-10am)' },
  { key: 'midday', label: 'Midday (10am-3pm)' },
  { key: 'pm_peak', label: 'PM Peak (3-7pm)' },
  { key: 'evening', label: 'Evening (7-10pm)' },
  { key: 'late', label: 'Late (10pm-6am)' },
]

function _labelFor(options, key) {
  return options.find((o) => o.key === key)?.label || key
}

function RouteDetail() {
  const { routeId } = useParams()
  const navigate = useNavigate()
  const [routeData, setRouteData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Filter state (NOTES-41), moved to URL state (NOTES-140) so a filtered
  // view is linkable and survives back/refresh. Refetch the scorecard +
  // trend whenever either changes. Defaults to no filter so a link without
  // these params renders the unfiltered view exactly as before.
  const [dayType, setDayType] = useUrlState('day_type', 'all')
  const [period, setPeriod] = useUrlState('period', 'all')
  // Raw searchParams/setSearchParams (same idiom as SegmentDiagnostic.jsx)
  // for the "Clear filter" button below, which must delete both `day_type`
  // and `period` in one atomic update — two separate useUrlState setters
  // fired synchronously would race (see useUrlState.js's docstring).
  const [searchParams, setSearchParams] = useSearchParams()

  // Time-window picker (NOTES-140): `?days=` drives every fetch below whose
  // endpoint accepts `days`. Read-only here — WindowPicker in the app shell
  // owns writes.
  const [days] = useWindowDays()

  // Agency switch (NOTES-143): `?agency=` drives every fetch below via
  // `apiUrl`. Read-only here — AgencyToggle in the app shell owns writes.
  const [agency] = useAgency()

  // Rider-experience OTP toggle (NOTES-143, backend in PR #241). RouteDetail
  // is the only page this applies to — the scorecard and system pages stay
  // on the official WMATA -2/+7 window regardless. Omitted from the URL at
  // the default ("official") so existing links/fixtures/Playwright specs
  // are unaffected.
  const [otpWindowParam, setOtpWindow] = useUrlState('otp_window', 'official')
  // Validate against the API's accepted values (mirrors useAgency /
  // useWindowDays — PR #242 review finding 8): a hand-edited or malformed
  // `?otp_window=` (e.g. `Rider`, `banana`) falls back to `official`
  // rather than sending an unrecognized value to the API and then
  // comparing against it with no explanation.
  const otpWindow = otpWindowParam === 'rider' ? 'rider' : 'official'
  const isRiderWindow = otpWindow === 'rider'

  // Per-route targets and the frequent-route designation aren't available
  // for a non-wmata agency (NOTES-143) — the backend still returns
  // system-default targets (never null) and `is_frequent: false` rather
  // than erroring, so this is an explicit UI choice (hide the target
  // indicators, keep OTP as the headline) rather than a value check.
  const showTargets = agency === DEFAULT_AGENCY

  // Recent runs vs Blocks tab (NOTES-45). 'runs' is the default; the user
  // switches to 'blocks' to see the per-vehicle chained-trip view that
  // surfaces cascade lateness.
  const [trailingTab, setTrailingTab] = useState('runs')

  const clearFilters = () => {
    const next = new URLSearchParams(searchParams)
    next.delete('day_type')
    next.delete('period')
    setSearchParams(next, { replace: true })
  }

  // Tracks the (routeId, agency) this effect last ran for (PR #242 round-2
  // review finding 4). Only an actual IDENTITY change — a different route
  // or a different agency's database — is a genuine cold navigation that
  // should show the full-page spinner; a filter change (dayType, period,
  // otpWindow) re-fetches the SAME route/agency and should swap data in
  // place like any other stale-while-revalidate update, not flash a
  // spinner over content that's still perfectly valid to look at while
  // the re-slice loads.
  const routeIdentityRef = useRef({ routeId, agency })

  useEffect(() => {
    // Reset `error` at the top of every run (PR #242 review finding 1) —
    // previously `error` was only ever set inside the `.catch`, never
    // cleared, so one 404 (e.g. switching to an agency whose DB doesn't
    // have this route_id) latched the error banner permanently:
    // `if (error || !routeData)` short-circuited every subsequent
    // successful fetch. `cancelled` guards against a slow response for a
    // superseded (routeId/agency/...) combination landing after a newer
    // request already committed its result.
    let cancelled = false
    setError(null)
    const identityChanged =
      routeIdentityRef.current.routeId !== routeId || routeIdentityRef.current.agency !== agency
    routeIdentityRef.current = { routeId, agency }
    if (identityChanged) {
      setLoading(true)
    }
    // `agency` passed explicitly (not left to apiUrl's window.location.search
    // fallback) so this fetch is correct under MemoryRouter in tests too,
    // not just under the real BrowserRouter in production — matches the
    // explicit-agency pattern the trend fetches below already use.
    const params = { agency }
    if (dayType !== 'all') params.day_type = dayType
    if (period !== 'all') params.period = period
    // Deliberately NOT wired to the time-window picker's `days` (PR #239
    // review finding B). This endpoint's `days` scopes the
    // excess-trip-time freshest-day lookup (`_excess_trip_time_fields` in
    // api/aggregations.py, one query per day over `range(days+1)`) — it's
    // a metric-freshness knob, not a display window, and can present a
    // value up to N days stale. Sending the picker's value here would
    // silently change what "current" excess-trip-time means whenever the
    // user picks a wider window. Left at the endpoint's own default (7).
    if (otpWindow === 'rider') params.otp_window = otpWindow
    const url = apiUrl(`/api/routes/${routeId}`, params)
    fetch(url)
      .then(res => res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`))
      .then(data => {
        if (cancelled) return
        setRouteData(data)
        setLoading(false)
      })
      .catch(err => {
        if (cancelled) return
        setError(err.message || err)
        setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [routeId, dayType, period, agency, otpWindow])

  // Trend data is fetched here (rather than inside RouteTrend) so the same
  // `days`-window series (NOTES-140 — previously a hardcoded 30) can drive
  // both the sparkline block and the per-KPI-card 7-vs-prior-7-day deltas
  // above. Three fetches: OTP and excess_trip_time come from
  // route_metrics_daily, service_delivered is computed live per service
  // date (NOTES-37 / endpoint extension). NOTES-43 added the
  // excess_trip_time trend.
  //
  // Build filter querystring fragment shared across the three trend fetches.
  // Period is honored only for the OTP trend (the others are trip-level
  // / daily aggregates that don't decompose by hour); pass it on every
  // call anyway — the API silently ignores it for non-otp metrics.
  const trendUrls = useMemo(() => {
    const filterParams = {}
    if (dayType !== 'all') filterParams.day_type = dayType
    if (period !== 'all') filterParams.period = period
    // otp_window (NOTES-143/144) only applies to metric=otp; the API
    // silently ignores it for the other two metrics, so it's only worth
    // sending on that one call.
    const otpParams = { ...filterParams, metric: 'otp', days, agency }
    if (otpWindow === 'rider') otpParams.otp_window = otpWindow
    return [
      apiUrl(`/api/routes/${routeId}/trend`, otpParams),
      apiUrl(`/api/routes/${routeId}/trend`, {
        ...filterParams,
        metric: 'service_delivered',
        days,
        agency,
      }),
      apiUrl(`/api/routes/${routeId}/trend`, {
        ...filterParams,
        metric: 'excess_trip_time',
        days,
        agency,
      }),
    ]
  }, [routeId, dayType, period, days, agency, otpWindow])

  const {
    data: trendData,
    loading: trendLoading,
    error: trendError,
    revalidateError: trendRevalidateError,
  } = useMultiFetch(trendUrls)

  // Unpack the parallel responses into named variables for the downstream
  // useMemo calls. trendData is null until the first successful fetch.
  const otpTrend = trendData?.[0] ?? null
  const sdTrend = trendData?.[1] ?? null
  const excessTrend = trendData?.[2] ?? null


  // Memoized {date, value} series + deltas. Service-delivered is stored as a
  // 0..1 ratio in the payload but rendered as percentage points on the card,
  // so multiply by 100 here once.
  //
  // The trend endpoint now emits one row per service date in the window
  // with `value: null` for days with no data (so the API caller can
  // distinguish "no observations" from a real zero). Drop those nulls
  // here so the sparkline only plots real points and `computeWindowDelta`
  // sees only valid data — its <3-valid-days suppression rule then
  // actually kicks in for thin-data routes.
  const otpSeries = useMemo(
    () =>
      (otpTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value: row.otp_percentage,
        }))
        .filter((row) => row.value != null),
    [otpTrend],
  )
  const sdSeries = useMemo(
    () =>
      (sdTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value:
            row.service_delivered_ratio != null
              ? row.service_delivered_ratio * 100
              : null,
        }))
        .filter((row) => row.value != null),
    [sdTrend],
  )
  // Excess trip time: % of trips with actual end-to-end duration above 110%
  // of scheduled. Already a percentage in the payload; just pass through.
  const excessSeries = useMemo(
    () =>
      (excessTrend?.trend_data || [])
        .map((row) => ({
          date: row.date,
          value: row.excess_trip_time_pct,
        }))
        .filter((row) => row.value != null),
    [excessTrend],
  )
  const otpDelta = useMemo(() => computeWindowDelta(otpSeries), [otpSeries])
  const sdDelta = useMemo(() => computeWindowDelta(sdSeries), [sdSeries])
  const excessDelta = useMemo(
    () => computeWindowDelta(excessSeries),
    [excessSeries],
  )

  if (loading) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(appendWindowParam('/', days, agency))} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading route details...</p>
        </div>
      </main>
    )
  }

  if (error || !routeData) {
    return (
      <main>
        <div className="route-detail-header">
          <button onClick={() => navigate(appendWindowParam('/', days, agency))} className="back-btn">
            ← Back to All Routes
          </button>
        </div>
        <div className="error-banner">
          <div className="error-icon">⚠️</div>
          <div className="error-content">
            <strong>Error loading route data:</strong> {error || 'Route not found'}
            <div className="error-actions">
              <button onClick={() => navigate(appendWindowParam('/', days, agency))} className="retry-btn">
                Back to Routes
              </button>
            </div>
          </div>
        </div>
      </main>
    )
  }

  const hasMetrics = routeData.otp_all_pct != null
    || routeData.service_delivered_ratio != null
    || routeData.ewt_seconds != null
    || routeData.bunching_rate != null
    || routeData.excess_trip_time_pct != null

  // The `deltas` block is always computed from the official OTP window
  // regardless of `otp_window` (NOTES-144) — the response echoes which
  // window it used for `deltas` as `deltas_otp_window`, and which window
  // it actually computed the live OTP fields with as `otp_window`.
  // Compare those two server-echoed fields to each other (PR #242 review
  // finding 8), not the server's `deltas_otp_window` against this
  // component's own local `otpWindow` state — the local value can race
  // ahead of `routeData` (the URL updates synchronously; the fetch
  // hasn't landed yet), and comparing two fields off the SAME response is
  // what actually answers "is the arrow on this render comparable to the
  // value on this render," rather than hardcoding "hide when rider"
  // (which would silently go stale if the backend's official-only delta
  // behavior ever changes). Hoisted above the stats-summary IIFE (not
  // declared inside it) so the rider-mode note below can also gate on it.
  const otpDeltaMismatch = routeData.deltas_otp_window !== routeData.otp_window

  // Subline for the excess-trip-time card: "median trip ran X min,
  // schedule Y min" so a GM can see whether the running-over-110% rate
  // reflects 30% over schedule on a long route or 1% over on a short
  // one. Both come from the freshest daily row inside the 7-day window
  // (NOTES-43, _excess_trip_time_fields in api/aggregations.py).
  const excessActualMin =
    routeData.excess_trip_time_median_actual_sec != null
      ? Math.round(routeData.excess_trip_time_median_actual_sec / 60)
      : null
  const excessSchedMin =
    routeData.excess_trip_time_median_scheduled_sec != null
      ? Math.round(routeData.excess_trip_time_median_scheduled_sec / 60)
      : null
  const excessOverSchedPct =
    routeData.excess_trip_time_median_actual_sec != null &&
    routeData.excess_trip_time_median_scheduled_sec != null &&
    routeData.excess_trip_time_median_scheduled_sec > 0
      ? Math.round(
          ((routeData.excess_trip_time_median_actual_sec -
            routeData.excess_trip_time_median_scheduled_sec) /
            routeData.excess_trip_time_median_scheduled_sec) *
            100,
        )
      : null

  // Active-filter chip (NOTES-41) — only shown when at least one filter is
  // non-default. Keeps the unfiltered view chrome-free.
  const filterActive = dayType !== 'all' || period !== 'all'
  const filterChipText = filterActive
    ? `Filter: ${[
        dayType !== 'all' ? _labelFor(DAY_TYPE_OPTIONS, dayType) : null,
        period !== 'all' ? _labelFor(PERIOD_OPTIONS, period) : null,
      ]
        .filter(Boolean)
        .join(' / ')}`
    : null

  return (
    <main>
      <div className="route-detail-header">
        <button onClick={() => navigate(appendWindowParam('/', days, agency))} className="back-btn">
          ← Back to All Routes
        </button>
        <div className="route-title">
          <h1>
            <span
              className="route-badge-large"
              style={{ backgroundColor: badgeColor(routeData.frequency_class, hasMetrics) }}
              title={FREQUENCY_CLASS_LABELS[routeData.frequency_class] || ''}
            >
              {routeData.route_name}
            </span>
            {routeData.route_long_name}
            {filterChipText && (
              <span
                className="filter-chip"
                title="Active KPI filter — clear to see all data"
              >
                {filterChipText}
              </span>
            )}
          </h1>
        </div>
      </div>

      <div className="route-filter-bar">
        <label className="route-filter-label">
          <span className="opacity-80">Day:</span>
          <select
            value={dayType}
            onChange={(e) => setDayType(e.target.value)}
            aria-label="Day-type filter"
          >
            {DAY_TYPE_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label className="route-filter-label">
          <span className="opacity-80">Time:</span>
          <select
            value={period}
            onChange={(e) => setPeriod(e.target.value)}
            aria-label="Time-period filter"
          >
            {PERIOD_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        {filterActive && (
          <button
            type="button"
            onClick={clearFilters}
            className="clear-filter-btn"
          >
            Clear filter
          </button>
        )}
        {/* Rider-experience OTP toggle (NOTES-143). RouteDetail-only — the
            scorecard and system pages stay on the official WMATA -2/+7
            window regardless of this control. */}
        <label
          className="route-filter-label route-filter-label-push"
          title="Switch the OTP headline to the stricter rider-experience window (-1/+3 min)"
        >
          <input
            type="checkbox"
            checked={isRiderWindow}
            onChange={(e) => setOtpWindow(e.target.checked ? 'rider' : 'official')}
          />
          <span className="opacity-80">Rider-experience OTP</span>
        </label>
      </div>

      {(() => {
        // NOTES-56: WMATA-designated frequent-service routes get EWT
        // as the headline KPI; standard routes keep OTP. The two cards
        // are factored out as variables so the only swap is their
        // order in the grid — every other card on the row is unchanged.
        // Falls back to OTP-first if the API didn't return is_frequent
        // (older snapshots, or the config-load failed open).
        //
        // Server-side period-over-period deltas (NOTES-38). KPI cards
        // consume the `deltas` block from `/api/routes/{id}` so RouteList
        // and RouteDetail show the same numbers. `renderServerDelta` returns
        // null when `valid=false` (thin data) — no misleading arrow shown.
        // The trend block above keeps its own client-side deltas because
        // they pair with the sparkline render (different code path, same window).
        // `metric` selects the per-metric magnitude floor (../moversFloor,
        // PR #216) passed as `flatThreshold` — without it, DeltaIndicator's
        // own 0.5 default is two orders of magnitude too tight for the
        // 0..1-fraction service_delivered/bunching deltas, rendering every
        // real change flat.
        const serverDeltas = routeData.deltas || {}
        const renderServerDelta = (metric, block, unitFormat, lowerIsBetter = false) => {
          if (!block || !block.valid || block.value == null) return null
          return (
            <DeltaIndicator
              delta={block.value}
              format={unitFormat}
              flatThreshold={getMoversFloor(metric)}
              lowerIsBetter={lowerIsBetter}
              title={`Last 7 days vs prior 7 days (${block.current_n}/${block.prior_n} valid days)`}
            />
          )
        }
        const otpCard = (
          <div className="stat-card" key="otp">
            <div className="stat-value">
              {routeData.otp_all_pct != null
                ? `${Math.round(routeData.otp_all_pct)}%`
                : 'N/A'}
            </div>
            <div className="stat-label">
              {isRiderWindow ? 'Rider-experience OTP (−1/+3 min)' : 'On-Time Performance'}
              <div>
                {!otpDeltaMismatch &&
                  renderServerDelta('otp', serverDeltas.otp, (d) => `${d.toFixed(1)} pp`)}
              </div>
              <div>
                <TargetIndicator
                  value={routeData.otp_all_pct}
                  // The configured OTP target is calibrated against the
                  // official window — comparing a rider-window value
                  // against it would read "below target" purely because
                  // the window changed, not because performance did (PR
                  // #242 review finding 3). Suppressed in rider mode
                  // regardless of `showTargets`.
                  target={showTargets && !isRiderWindow ? routeData.targets?.otp : null}
                  higherIsBetter
                  format={(t) => `${t.toFixed(0)}%`}
                />
              </div>
            </div>
          </div>
        )
        const ewtCard = (
          <div className="stat-card" key="ewt">
            <div className="stat-value">
              {routeData.ewt_seconds != null
                ? `${Math.round(routeData.ewt_seconds)}`
                : 'N/A'}
              {routeData.ewt_seconds != null && <span className="text-2xl"> sec</span>}
              {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
                <span
                  className="data-thin-badge"
                  title={`Only ${Math.round(routeData.ewt_coverage_ratio * 100)}% of scheduled headways were observed`}
                >
                  Thin
                </span>
              )}
            </div>
            <div className="stat-label">
              Excess Wait Time
              <div>{renderServerDelta('ewt', serverDeltas.ewt, (d) => `${Math.round(d)}s`, true)}</div>
              {routeData.is_frequent && (
                <div>
                  <span
                    className="headline-kpi-tag"
                    title="EWT is the rider-relevant headline for WMATA frequent-service routes (config/frequent_routes.yaml)"
                  >
                    Frequent service
                  </span>
                </div>
              )}
              <div>
                <TargetIndicator
                  value={routeData.ewt_seconds}
                  target={showTargets ? routeData.targets?.ewt : null}
                  higherIsBetter={false}
                  format={(t) => `${(t / 60).toFixed(1)} min`}
                />
              </div>
            </div>
            {routeData.ewt_seconds == null && (
              <div className="stat-footnote">
                (frequent service only)
              </div>
            )}
            {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
              <div className="data-thin-note">
                Trip-update coverage {Math.round(routeData.ewt_coverage_ratio * 100)}% — metric unreliable
              </div>
            )}
          </div>
        )
        const headlineCards = routeData.is_frequent === true
          ? [ewtCard, otpCard]
          : [otpCard, ewtCard]
        return (
          <div className="stats-summary">
            {headlineCards}
            <div className="stat-card">
              <div className="stat-value">
                {routeData.service_delivered_ratio != null
                  ? `${Math.round(routeData.service_delivered_ratio * 100)}%`
                  : 'N/A'}
              </div>
              <div className="stat-label">
                Service Delivered
                {/* SD ratio is 0..1; the server delta is also 0..1 so scale to pp. */}
                <div>
                  {renderServerDelta(
                    'service_delivered',
                    serverDeltas.service_delivered,
                    (d) => `${(d * 100).toFixed(1)} pp`,
                  )}
                </div>
                <div>
                  <TargetIndicator
                    value={
                      routeData.service_delivered_ratio != null
                        ? routeData.service_delivered_ratio * 100
                        : null
                    }
                    target={
                      showTargets && routeData.targets?.service_delivered != null
                        ? routeData.targets.service_delivered * 100
                        : null
                    }
                    higherIsBetter
                    format={(t) => `${t.toFixed(0)}%`}
                  />
                </div>
              </div>
              {routeData.service_delivered_scheduled != null && (
                <div className="stat-footnote">
                  ({routeData.service_delivered_delivered} of {routeData.service_delivered_scheduled} trips)
                </div>
              )}
            </div>
            <div className="stat-card">
              <div className="stat-value text-2xl">
                {routeData.otp_origin_pct != null
                  ? `${Math.round(routeData.otp_origin_pct)}% / ${Math.round(routeData.otp_destination_pct ?? 0)}%`
                  : 'N/A'}
              </div>
              <div className="stat-label">OTP Origin / Destination</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {routeData.bunching_rate != null
                  ? `${(routeData.bunching_rate * 100).toFixed(1)}%`
                  : 'N/A'}
                {routeData.ewt_coverage_ratio != null && routeData.ewt_coverage_ratio < 0.5 && (
                  <span
                    className="data-thin-badge"
                    title={`Only ${Math.round(routeData.ewt_coverage_ratio * 100)}% of scheduled headways were observed`}
                  >
                    Thin
                  </span>
                )}
              </div>
              <div className="stat-label">
                Bunching Rate
                <div>
                  {renderServerDelta(
                    'bunching',
                    serverDeltas.bunching,
                    (d) => `${(d * 100).toFixed(1)} pp`,
                    true,
                  )}
                </div>
                <div>
                  <TargetIndicator
                    value={
                      routeData.bunching_rate != null
                        ? routeData.bunching_rate * 100
                        : null
                    }
                    target={
                      showTargets && routeData.targets?.bunching != null
                        ? routeData.targets.bunching * 100
                        : null
                    }
                    higherIsBetter={false}
                    format={(t) => `${t.toFixed(1)}%`}
                  />
                </div>
              </div>
              {routeData.bunching_total_headways != null && routeData.bunching_total_headways > 0 && (
                <div className="stat-footnote">
                  ({routeData.bunching_count} of {routeData.bunching_total_headways} pairs)
                </div>
              )}
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {routeData.excess_trip_time_pct != null
                  ? `${Math.round(routeData.excess_trip_time_pct)}%`
                  : 'N/A'}
              </div>
              <div className="stat-label">
                % of Trips Running Long
                <div>
                  {renderServerDelta(
                    'excess_trip_time_pct',
                    serverDeltas.excess_trip_time_pct,
                    (d) => `${d.toFixed(1)} pp`,
                    true,
                  )}
                </div>
              </div>
              {excessActualMin != null && excessSchedMin != null && (
                <div className="stat-footnote">
                  median trip {excessActualMin} min, schedule {excessSchedMin} min
                  {excessOverSchedPct != null && ` (${excessOverSchedPct >= 0 ? '+' : ''}${excessOverSchedPct}%)`}
                </div>
              )}
              {routeData.excess_trip_time_pct == null && (
                <div className="stat-footnote">
                  (no qualifying trips)
                </div>
              )}
            </div>
          </div>
        )
      })()}

      {/* Gated on `otpDeltaMismatch`, not `isRiderWindow` alone (PR #242
          review finding 7) — this note specifically explains the hidden
          delta arrow, so it should only render when the arrow is actually
          hidden. */}
      {otpDeltaMismatch && (
        <p className="rider-otp-note">
          The Routes scorecard and system-wide pages still report the official
          WMATA on-time window (−2/+7 min) — this route&apos;s OTP delta arrow
          above is hidden because it isn&apos;t comparable to the
          rider-experience value.
        </p>
      )}

      {!showTargets && (
        <p className="agency-targets-note">
          Frequent-route designation and per-route targets aren&apos;t
          configured for {AGENCY_LABELS[agency] || agency} yet — the headline
          KPI order and target indicators above are WMATA-only.
        </p>
      )}

      {trendRevalidateError && (
        <p className="stale-data-note">
          Showing cached trend data — last refresh failed. Retrying in the background.
        </p>
      )}

      {hasMetrics && (
        <RouteTrend
          otpSeries={otpSeries}
          sdSeries={sdSeries}
          excessSeries={excessSeries}
          otpDelta={otpDelta}
          sdDelta={sdDelta}
          excessDelta={excessDelta}
          otpTarget={showTargets && !isRiderWindow ? routeData.targets?.otp ?? null : null}
          sdTarget={showTargets ? routeData.targets?.service_delivered ?? null : null}
          otpCurrent={routeData.otp_all_pct ?? null}
          sdCurrent={
            routeData.service_delivered_ratio != null
              ? routeData.service_delivered_ratio * 100
              : null
          }
          loading={trendLoading}
          error={trendError}
          days={days}
        />
      )}

      {hasMetrics && (
        <StopDiagnostic
          routeId={routeId}
          dayType={dayType}
          period={period}
          otpWindow={otpWindow}
        />
      )}

      {hasMetrics && (
        <PeriodDrilldown routeId={routeId} dayType={dayType} period={period} />
      )}

      {hasMetrics && (
        <RouteDiagnosisPanel routeId={routeId} period={period} />
      )}

      <div className="route-tab-row">
        <button
          type="button"
          onClick={() => setTrailingTab('runs')}
          className={trailingTab === 'runs' ? 'route-tab-active' : 'route-tab'}
        >
          Recent runs
        </button>
        <button
          type="button"
          onClick={() => setTrailingTab('blocks')}
          className={trailingTab === 'blocks' ? 'route-tab-active' : 'route-tab'}
        >
          Blocks
        </button>
      </div>

      {trailingTab === 'runs' ? (
        <RecentRuns routeId={routeId} />
      ) : (
        <BlockList routeId={routeId} />
      )}

      <div className="chart-container">
        <h2>Route Map</h2>
        <RouteMap routeId={routeId} />
      </div>

      {!hasMetrics && (
        <div className="no-data-message">
          <div className="no-data-icon">📊</div>
          <h2>No Performance Data Available</h2>
          <p>This route does not have enough data to calculate performance metrics for the latest service date.</p>
        </div>
      )}

      <div className="detail-info">
        <h3>Route Information</h3>
        <div className="info-grid">
          <div className="info-item">
            <span className="info-label">Route ID:</span>
            <span className="info-value">{routeData.route_id}</span>
          </div>
        </div>
      </div>
    </main>
  )
}

export default RouteDetail
