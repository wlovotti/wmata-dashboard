import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { formatMetricValue, formatDelta } from '../utils/agencyComparison'

/**
 * One-row WMATA-vs-Muni OTP teaser inside the Overview hero (NOTES-84),
 * linking to the full /compare page. Deliberately never load-bearing:
 * fetch failure, missing endpoint, or a single-agency payload all render
 * nothing rather than an error — the hero must not degrade because the
 * sidecar DB is unreachable.
 */
function CompareStrip() {
  const [agencies, setAgencies] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/agency-comparison')
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((json) => {
        if (!cancelled) setAgencies(json?.agencies ?? null)
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
    </p>
  )
}

export default CompareStrip
