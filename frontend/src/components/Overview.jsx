import { useMemo, useRef, useEffect } from 'react'
import useMultiFetch from '../hooks/useMultiFetch'
import useUrlState from '../hooks/useUrlState'
import useWindowDays, { appendWindowParam } from '../hooks/useWindowDays'
import useAgency from '../hooks/useAgency'
import { apiUrl } from '../utils/apiUrl'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { formatContribMetricValue } from '../utils/formatters'
import { latestNonNull } from '../utils/heroSummary'
import OverviewHero from './OverviewHero'
import MoversPanel from './MoversPanel'
import SystemMap from './SystemMap'
import SystemTrend from './SystemTrend'
import SystemWeeklyNarrativeLede from './SystemWeeklyNarrativeLede'

// Metric options for the "Biggest drags" table. Same 4-entry list as
// RouteList — kept inline per the existing convention.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const CONTRIB_TOP_N = 5

/**
 * Overview landing page, rebuilt as an editorial stack (NOTES-84):
 *
 *   0. SystemWeeklyNarrativeLede — cached LLM weekly recap (PR #219),
 *                      translating the metrics below into rider
 *                      consequences. Renders nothing until a narrative has
 *                      been generated offline, so it's invisible today.
 *   1. OverviewHero  — big-number verdict + compare teaser (absorbs the
 *                      retired HealthPulse banner)
 *   2. Fold          — SystemMap + MoversPanel side by side ("getting
 *                      worse" and "where" promoted to the top fold)
 *   3. SystemTrend   — 7-day-smoothed trend cards, daily points ghosted
 *   4. Biggest drags — the contributors table, demoted to the bottom
 *
 * One page-level fetch each for /api/routes and the trend fan-out, passed
 * down as props; the off-target panel moved to the Targets page.
 *
 * All three fetches route through `useMultiFetch`, which gives them
 * stale-while-revalidate caching for free (NOTES-122): returning to
 * Overview from a route page renders the last-known scorecard/trend/
 * contributors data instantly while a background fetch refreshes it,
 * instead of remounting cold into a wall of spinners.
 */
function Overview() {
  const navigate = useNavigate()
  const [contribMetric, setContribMetric] = useUrlState('metric', 'otp')
  // Time-window picker (NOTES-140): `?days=` drives every fetch below that
  // its endpoint accepts. `/api/routes/contributors` and `/api/system/trend`
  // both take `days`; `/api/routes` does too (previously called with none
  // at all here, silently taking the API's own 7-day default) — see PR #239
  // for the resulting delta-suppression interaction with computeSystemDelta
  // at the 7-day setting.
  const [days] = useWindowDays()
  // Agency switch (NOTES-143): `?agency=` drives every fetch below via
  // `apiUrl`, which reads it straight off the URL — included here only so
  // the URL arrays below (which must be memoized per `useMultiFetch`'s
  // contract) recompute on an agency switch instead of replaying the
  // previous agency's cached URLs.
  const [agency] = useAgency()

  // Memoized (PR #218 finding 4) so the array reference is stable across
  // renders that don't change `days`/`agency` — a fresh literal here would
  // still work (useMultiFetch keys its effect on a JSON.stringify of the
  // URLs, not reference identity), but the hook's docstring documents
  // memoization as the caller contract.
  const scorecardUrls = useMemo(() => [apiUrl('/api/routes', { days, agency })], [days, agency])
  const {
    data: scorecardResults,
    loading: scorecardLoading,
    revalidateError: scorecardRevalidateError,
  } = useMultiFetch(scorecardUrls)
  // Hero and movers degrade gracefully while this is null (loading, or a
  // fetch failure — the raw-fetch predecessor of this effect silently
  // ignored errors the same way).
  const scorecard = scorecardResults ? scorecardResults[0] : null

  // One page-level fan-out for the four trend payloads; SystemTrend and the
  // hero both read from this single fetch (props down — NOTES-84 data flow).
  const trendUrls = useMemo(
    () => [
      apiUrl('/api/system/trend', { metric: 'otp', days, agency }),
      apiUrl('/api/system/trend', { metric: 'service_delivered', days, agency }),
      apiUrl('/api/system/trend', { metric: 'ewt', days, agency }),
      apiUrl('/api/system/trend', { metric: 'bunching', days, agency }),
    ],
    [days, agency],
  )
  const {
    data: rawSystemTrendData,
    loading: trendLoading,
    error: trendError,
    revalidateError: trendRevalidateError,
  } = useMultiFetch(trendUrls, ([otp, sd, ewt, bun]) => ({
    otp,
    service_delivered: sd,
    ewt,
    bunching: bun,
  }))
  const systemTrendData = rawSystemTrendData ?? null

  // Memoized (PR #218 finding 4) so the array reference is stable across
  // renders that don't change `contribMetric`/`days`/`agency` — a fresh
  // literal here would still work (useMultiFetch keys its effect on a
  // JSON.stringify of the URLs, not reference identity), but the hook's
  // docstring documents memoization as the caller contract, and this is
  // one of the call sites whose URL genuinely depends on state.
  const contribUrls = useMemo(
    () => [apiUrl('/api/routes/contributors', { metric: contribMetric, days, agency })],
    [contribMetric, days, agency],
  )
  const {
    data: contribResults,
    loading: contribLoading,
    error: contribError,
    revalidateError: contribRevalidateError,
  } = useMultiFetch(contribUrls)
  const contribData = contribResults ? contribResults[0] : null

  // Hero's own fixed-30-day OTP fetch (PR #239 review finding C) — this
  // can't just reuse `systemTrendData.otp` from the picker-driven fan-out;
  // see the comment on `trendUrls` above for why. Memoized on `agency`
  // alone (days is fixed at 30) so an agency switch still recomputes the
  // URL instead of replaying the previous agency's cached array.
  const heroOtpTrendUrls = useMemo(
    () => [apiUrl('/api/system/trend', { metric: 'otp', days: 30, agency })],
    [agency],
  )
  const {
    data: heroOtpResults,
    loading: heroOtpLoading,
    revalidateError: heroOtpRevalidateError,
  } = useMultiFetch(heroOtpTrendUrls)
  const heroOtpTrend = heroOtpResults ? heroOtpResults[0] : null

  // Agency-switch guard (PR #242 review finding 2). `useMultiFetch`'s
  // `data` only updates once a fetch resolves — a genuine cross-agency
  // cache miss (sfmta isn't pre-warmed the way `_warm_scorecard_cache_sync`
  // covers wmata) otherwise leaves the PREVIOUS agency's scorecard/trend
  // numbers rendering under the NEW agency's header for however long the
  // cold fetch takes. `RouteList` already guards the equivalent case via
  // its own `_cachedAgency !== agency` check; this is the same idea for
  // Overview's `useMultiFetch`-backed fetches.
  //
  // Each of the three fetch groups tracks the URL(s) it last actually
  // finished loading (settled), in a ref seeded to the INITIAL url set so
  // a normal cold mount isn't treated as "stale." Comparing the ref to the
  // CURRENT url set is a synchronous render-time check — unlike gating on
  // the `loading` booleans directly, this can't race against the one-tick
  // lag between a url-array changing and `useMultiFetch`'s own effect
  // noticing and flipping `loading` true for it: the ref/url comparison
  // itself changes in the very same render `scorecardUrls`/etc. recompute,
  // so there's no window where a switch goes undetected. While any group
  // is stale, its corresponding `display*` value below is null — the same
  // "not loaded yet" state these components already render correctly
  // (including the hero's PR #239 "verdict unavailable" copy, which is
  // itself null-safe), so this reuses that existing path instead of adding
  // a new one. A cache hit (revisiting a url set already fetched this
  // session) settles on the very next render, so it never visibly gates.
  const scorecardUrlRef = useRef(scorecardUrls[0])
  useEffect(() => {
    if (!scorecardLoading) scorecardUrlRef.current = scorecardUrls[0]
  }, [scorecardLoading, scorecardUrls])
  const scorecardStale = scorecardUrlRef.current !== scorecardUrls[0]

  const trendUrlKeyRef = useRef(JSON.stringify(trendUrls))
  useEffect(() => {
    if (!trendLoading) trendUrlKeyRef.current = JSON.stringify(trendUrls)
  }, [trendLoading, trendUrls])
  const trendStale = trendUrlKeyRef.current !== JSON.stringify(trendUrls)

  const heroOtpUrlRef = useRef(heroOtpTrendUrls[0])
  useEffect(() => {
    if (!heroOtpLoading) heroOtpUrlRef.current = heroOtpTrendUrls[0]
  }, [heroOtpLoading, heroOtpTrendUrls])
  const heroOtpStale = heroOtpUrlRef.current !== heroOtpTrendUrls[0]

  const displayScorecard = scorecardStale ? null : scorecard
  const displaySystemTrendData = trendStale ? null : systemTrendData
  const displayHeroOtpTrend = heroOtpStale ? null : heroOtpTrend

  // Background-revalidate failure on any of the page's cached fetches
  // (NOTES-122 review finding 1): none of these ever blank the page — the
  // stale cached data keeps rendering — but a downed API otherwise leaves
  // that stale data on screen indefinitely with `error === null` and no
  // signal anywhere that it stopped refreshing. This can only be non-null
  // after at least one successful cache hit + failed revalidate, so it
  // never renders on a cold load.
  const staleData =
    scorecardRevalidateError || trendRevalidateError || contribRevalidateError || heroOtpRevalidateError

  // The 4-entry worst-of-four input for the hero — same construction the
  // retired HealthPulse used (percent-scaled fractions, trend targets).
  // Reads `displaySystemTrendData` (null while an agency switch is
  // cold-loading — PR #242 review finding 2), not the raw `systemTrendData`
  // that also feeds `SystemTrend` below (which already gates its own
  // rendering on `trendLoading`, so it doesn't need the same treatment).
  const systemMetrics = [
    {
      key: 'otp',
      label: 'OTP',
      higherIsBetter: true,
      current: latestNonNull(displaySystemTrendData?.otp?.trend_data, 'otp_percentage'),
      target: displaySystemTrendData?.otp?.target_value ?? null,
    },
    {
      key: 'service_delivered',
      label: 'Service Delivered',
      higherIsBetter: true,
      current: (() => {
        const v = latestNonNull(
          displaySystemTrendData?.service_delivered?.trend_data,
          'service_delivered_ratio',
        )
        return v != null ? v * 100 : null
      })(),
      target:
        displaySystemTrendData?.service_delivered?.target_value != null
          ? displaySystemTrendData.service_delivered.target_value * 100
          : null,
    },
    {
      key: 'ewt',
      label: 'EWT',
      higherIsBetter: false,
      current: latestNonNull(displaySystemTrendData?.ewt?.trend_data, 'ewt_seconds'),
      target: displaySystemTrendData?.ewt?.target_value ?? null,
    },
    {
      key: 'bunching',
      label: 'Bunching',
      higherIsBetter: false,
      current: (() => {
        const v = latestNonNull(displaySystemTrendData?.bunching?.trend_data, 'bunching_rate')
        return v != null ? v * 100 : null
      })(),
      target:
        displaySystemTrendData?.bunching?.target_value != null
          ? displaySystemTrendData.bunching.target_value * 100
          : null,
    },
  ]

  // Daily OTP series for the hero's week-over-week math — from the fixed
  // 30-day fetch above, not the picker-driven `systemTrendData`, so the
  // verdict is stable regardless of the selected window (PR #239 review
  // finding C).
  const otpSeries = (displayHeroOtpTrend?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.otp_percentage,
    data_quality: row.data_quality,
  }))

  const visibleContributors = (contribData?.contributors ?? []).slice(0, CONTRIB_TOP_N)

  return (
    <main>
      <SystemWeeklyNarrativeLede />
      {staleData && (
        <p
          className="stale-data-note"
          style={{ color: 'var(--color-muted)', fontSize: '0.85rem', marginBottom: '0.75rem' }}
        >
          Showing cached data — last refresh failed. Retrying in the background.
        </p>
      )}
      <OverviewHero
        systemMetrics={systemMetrics}
        scorecardRoutes={displayScorecard?.routes ?? null}
        otpSeries={otpSeries}
      />

      {/* "Where is it going badly" fold: system map + movers side by side.
          `displayScorecard` (not `scorecard`) so an agency switch shows the
          same "not loaded yet" state as a cold first mount instead of the
          previous agency's routes (PR #242 review finding 2). */}
      <div className="overview-fold overview-fold-with-map">
        <SystemMap scorecardRoutes={displayScorecard?.routes ?? null} />
        <MoversPanel routes={displayScorecard?.routes ?? null} />
      </div>

      <SystemTrend
        trendData={systemTrendData}
        loading={trendLoading}
        error={trendError}
        days={days}
      />

      <div className="table-container">
        <h2>Biggest drags</h2>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Top {CONTRIB_TOP_N} routes ranked by their contribution to system
          underperformance — the routes whose attention would move the
          system the most.
        </p>
        {contribData?.days_included != null && contribData.days_included < contribData.days && (
          <p style={{ color: 'var(--color-muted)', fontSize: '0.85rem', marginBottom: '0.75rem' }}>
            Based on {contribData.days_included} of {contribData.days} days —{' '}
            {contribData.days - contribData.days_included} excluded for partial data collection.
          </p>
        )}
        <div className="filters" style={{ marginBottom: '0.75rem' }}>
          <div>
            <label htmlFor="overview-contrib-metric" style={{ marginRight: '0.5rem' }}>
              Metric:
            </label>
            <select
              id="overview-contrib-metric"
              value={contribMetric}
              onChange={(e) => setContribMetric(e.target.value)}
            >
              {CONTRIB_METRICS.map((m) => (
                <option key={m.key} value={m.key}>
                  {m.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {contribError && (
          <p style={{ color: 'var(--color-muted)', padding: '0 1.5rem 1rem' }}>
            Unable to load contributors: {contribError}
          </p>
        )}

        {contribLoading ? (
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading contributors...</p>
          </div>
        ) : contribData == null ? null : visibleContributors.length === 0 ? (
          <p style={{ padding: '0 1.5rem 1rem' }}>
            No routes have enough data to score contribution for this metric yet.
          </p>
        ) : (
          <table className="routes-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Route</th>
                <th>Name</th>
                <th>Route value</th>
                <th title={`Per-route target if configured, otherwise system ${days}-day baseline`}>
                  Reference
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleContributors.map((c, idx) => (
                <tr
                  key={c.route_id}
                  onClick={() => navigate(appendWindowParam(`/route/${c.route_id}`, days, agency))}
                  style={{ cursor: 'pointer' }}
                >
                  <td>{idx + 1}</td>
                  <td className="route-id">
                    <span
                      className="route-badge"
                      style={{ backgroundColor: badgeColor(null, true) }}
                    >
                      {c.route_short_name || c.route_id}
                    </span>
                  </td>
                  <td className="route-name">{c.route_long_name || 'N/A'}</td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.route_value)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.reference_value ?? c.baseline_value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ padding: '1rem 1.5rem 1.5rem' }}>
          <Link to={appendWindowParam('/routes', days, agency)} className="see-all-link">
            See all routes →
          </Link>
        </div>
      </div>
    </main>
  )
}

export default Overview
