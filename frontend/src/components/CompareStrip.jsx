import { Link } from 'react-router-dom'
import { formatMetricValue, formatDelta } from '../utils/agencyComparison'
import useMultiFetch from '../hooks/useMultiFetch'

// Stable module-level array so useMultiFetch's memoization contract holds
// and the URL set never changes across renders.
const COMPARE_STRIP_URLS = ['/api/agency-comparison']

/**
 * One-row WMATA-vs-Muni OTP teaser inside the Overview hero (NOTES-84),
 * linking to the full /compare page. Deliberately never load-bearing:
 * fetch failure, missing endpoint, or a single-agency payload all render
 * nothing rather than an error — the hero must not degrade because the
 * sidecar DB is unreachable.
 *
 * Disclosure fix (final-review wave): the strip's numbers are whole-
 * matched-window means (fixed start, currently ~17 days and growing —
 * see `/api/agency-comparison`'s `window_start`), not the hero's 7-day
 * figure directly above it. Rather than truncate the window to match (the
 * comparison needs the longer window for statistical stability — see
 * AgencyComparison.jsx), the strip discloses its window inline via a
 * trailing "since {window_start}" label so the two numbers are never read
 * as directly comparable time spans.
 *
 * Routed through `useMultiFetch` (PR #218 finding 2) instead of a raw
 * fetch: previously this was the one Overview-hero fetch left uncached, so
 * the strip popped in on every return visit even though every other
 * Overview fetch already had stale-while-revalidate caching (NOTES-122).
 */
function CompareStrip() {
  const { data } = useMultiFetch(COMPARE_STRIP_URLS, ([json]) => ({
    agencies: json?.agencies ?? null,
    windowStart: json?.window_start ?? null,
  }))
  const agencies = data?.agencies ?? null
  const windowStart = data?.windowStart ?? null

  if (!Array.isArray(agencies) || agencies.length < 2) return null

  return (
    <p className="compare-strip">
      {agencies.map((agency) => {
        const otp = agency.metrics?.otp
        const delta = otp ? formatDelta('otp', otp.wow_delta) : null
        return (
          <span key={agency.agency} className="compare-strip-agency">
            {agency.display_name} {formatMetricValue('otp', otp?.window_mean)}
            {delta && (
              <span className={`compare-strip-delta compare-strip-delta-${delta.tint}`}>
                {' '}
                {delta.text}
              </span>
            )}
          </span>
        )
      })}
      <Link to="/compare" className="compare-strip-link">
        Full comparison →
      </Link>
      {windowStart && <span className="compare-strip-window">· since {windowStart}</span>}
    </p>
  )
}

export default CompareStrip
