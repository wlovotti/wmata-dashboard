/**
 * Shared "not available for this agency" card (NOTES-143). Used by pages
 * / panels whose data is WMATA-only (GTFS-Plus timepoints, a WMATA-keyed
 * config file) rather than merely thin or unpopulated for another agency
 * — the distinction the design calls out explicitly: never render a WMATA
 * table under a non-WMATA header, or silently reuse WMATA's classification
 * for a same-numbered route on another agency.
 *
 * @param {{ agencyLabel: string, reason: string }} props
 *   - agencyLabel: display name of the current agency (e.g. "Muni").
 *   - reason: one-line explanation of why this is WMATA-only.
 * @returns {JSX.Element}
 */
function AgencyUnavailable({ agencyLabel, reason }) {
  return (
    <p
      className="agency-unavailable-note"
      style={{
        color: 'var(--color-muted)',
        fontSize: '0.875rem',
        padding: '0.75rem 1rem',
        // App.css has no surface/border tokens yet (NOTES-85 owns minting
        // that set) — reusing the existing `--color-neutral` token for
        // both roles (round-2 review finding 6) rather than hardcoding an
        // unrelated hex: a 15%-opacity tint of it (rgba — `--color-neutral`
        // is `#94a3b8` = `rgb(148, 163, 184)`) for the background, the
        // full color for the border.
        background: 'rgba(148, 163, 184, 0.15)',
        border: '1px solid var(--color-neutral)',
        borderRadius: '6px',
      }}
    >
      Not available for {agencyLabel}. {reason}
    </p>
  )
}

export default AgencyUnavailable
