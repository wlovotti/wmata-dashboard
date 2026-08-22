import { useEffect, useState } from 'react'

/**
 * Overview lede — cached LLM narrative summarizing the most recent week of
 * system-wide performance (system weekly narrative, PR #219).
 *
 * Mirrors the read-only-serve half of the route diagnosis narrative pattern
 * (PR #141; see RouteDiagnosisPanel.jsx's NarrativeSection): fetches
 * `GET /api/system/weekly-narrative`, which reads a cache table written
 * offline by `scripts/generate_system_weekly_narrative.py` — Claude is
 * never called when a viewer loads this page.
 *
 * Renders nothing — no loading skeleton, no "not generated yet" placeholder
 * — during the initial fetch, on a 404 (nothing cached), and on any fetch
 * error. This is deliberate: the lede should never cause layout shift or
 * add visual noise to the Overview page in the common case where no
 * narrative has been generated yet (a fresh install, dev environment, or
 * CI's Playwright baseline). Once a narrative exists, it renders as the
 * page's editorial lede, above the hero.
 *
 * @returns {JSX.Element|null}
 */
function SystemWeeklyNarrativeLede() {
  const [data, setData] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/system/weekly-narrative')
      .then((res) => (res.ok ? res.json() : null))
      .then((json) => {
        if (!cancelled) setData(json)
      })
      .catch(() => {
        // Fail quiet — a downed API just means no lede this render, same
        // as the 404/not-generated-yet case. The rest of the Overview page
        // has its own error handling for its own fetches.
        if (!cancelled) setData(null)
      })
    return () => {
      cancelled = true
    }
  }, [])

  if (!data) return null

  return (
    <section className="system-weekly-lede" aria-label="Weekly system recap">
      {data.is_stale && (
        <p className="system-weekly-lede-stale">
          This week's recap may be out of date — the underlying metrics have
          changed since it was written. Regenerate with{' '}
          <code>scripts/generate_system_weekly_narrative.py</code>.
        </p>
      )}
      <p className="system-weekly-lede-text">{data.narrative}</p>
      <p className="system-weekly-lede-meta">
        Week ending {data.as_of_date} · AI-generated summary, not live-computed ·{' '}
        {data.model_id}
      </p>
    </section>
  )
}

export default SystemWeeklyNarrativeLede
