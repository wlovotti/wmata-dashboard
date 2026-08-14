import { countRoutesBelowTarget, worstMetric } from '../utils/heroSummary'
import { computeWindowDelta } from '../utils/computeWindowDelta'
import { formatContribMetricValue } from '../utils/formatters'
import CompareStrip from './CompareStrip'

// Deltas inside ±HERO_FLAT_PP read as "steady" — mirrors DeltaIndicator's
// 0.5 flat threshold so the hero never contradicts the trend cards below.
const HERO_FLAT_PP = 0.5

/**
 * Build the hero's plain-language week-over-week clause from a
 * computeWindowDelta result. Exposed for the component's tests via render
 * assertions only — not exported.
 */
function weekClause(delta) {
  if (delta == null) return null
  if (Math.abs(delta.delta) <= HERO_FLAT_PP) return 'steady vs last week'
  const direction = delta.delta > 0 ? 'up' : 'down'
  return `${direction} ${Math.abs(delta.delta).toFixed(1)} pts`
}

/**
 * The Overview's big-number verdict (NOTES-84) — replaces the HealthPulse
 * banner. Headline: 7-day mean OTP with a plain-language week-over-week
 * clause (strictly week-over-week — the pre-2026-05-25 window is
 * contaminated, so no longer-horizon framing). Subline: routes below
 * target. When the worst-of-four metric is not OTP, one extra sentence
 * names it. Tint reuses the HealthPulse thresholds (worst normalized gap:
 * <=0 green, <=0.1 yellow, >0.1 red) via the same gap math in heroSummary.
 *
 * @param {object} props
 * @param {Array<{key: string, label: string, higherIsBetter: boolean, current: number|null, target: number|null}>} props.systemMetrics
 *   the 4-entry array Overview builds from the trend payloads.
 * @param {Array<object>|null} props.scorecardRoutes - `routes` array from
 *   /api/routes, or null while loading.
 * @param {Array<{date: string, value: number|null, data_quality?: string}>} props.otpSeries
 *   daily OTP percent rows.
 * @returns {JSX.Element}
 */
function OverviewHero({ systemMetrics, scorecardRoutes, otpSeries }) {
  const cleanOtp = (otpSeries || []).filter((r) => r.data_quality !== 'partial')
  const weekDelta = computeWindowDelta(cleanOtp)
  const worst = worstMetric(systemMetrics || [])
  const { below, evaluated } = countRoutesBelowTarget(scorecardRoutes)

  let tint = 'overview-hero-green'
  if (worst == null) tint = 'overview-hero-neutral'
  else if (worst.gap > 0.1) tint = 'overview-hero-red'
  else if (worst.gap > 0) tint = 'overview-hero-yellow'

  return (
    <div className={`overview-hero ${tint}`} role="status">
      {weekDelta == null ? (
        <p className="overview-hero-headline">
          System verdict unavailable — not enough history yet this week.
        </p>
      ) : (
        <p className="overview-hero-headline">
          <span className="overview-hero-number">
            {Math.round(weekDelta.recentMean)}% on time this week
          </span>
          <span className="overview-hero-delta"> — {weekClause(weekDelta)}</span>
        </p>
      )}
      {evaluated > 0 && (
        <p className="overview-hero-subline">
          {below} of {evaluated} routes below target
        </p>
      )}
      {worst != null && worst.key !== 'otp' && worst.gap > 0 && (
        <p className="overview-hero-subline">
          {worst.label} is the sore spot:{' '}
          {formatContribMetricValue(
            worst.key,
            // formatters expect fractions for the x100-scaled metrics —
            // systemMetrics carries them pre-scaled to percent, so undo it.
            worst.key === 'service_delivered' || worst.key === 'bunching'
              ? worst.current / 100
              : worst.current,
          )}{' '}
          vs target{' '}
          {formatContribMetricValue(
            worst.key,
            worst.key === 'service_delivered' || worst.key === 'bunching'
              ? worst.target / 100
              : worst.target,
          )}
        </p>
      )}
      <CompareStrip />
    </div>
  )
}

export default OverviewHero
