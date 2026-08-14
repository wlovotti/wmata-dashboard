import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { formatMetricValue, formatDelta } from '../utils/agencyComparison'

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
 */
function CompareStrip() {
  const [agencies, setAgencies] = useState(null)
  const [windowStart, setWindowStart] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/agency-comparison')
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((json) => {
        if (!cancelled) {
          setAgencies(json?.agencies ?? null)
          setWindowStart(json?.window_start ?? null)
        }
      })
      .catch(() => {
        // Teaser only — swallow and render nothing.
      })
    return () => {
      cancelled = true
    }
  }, [])

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
