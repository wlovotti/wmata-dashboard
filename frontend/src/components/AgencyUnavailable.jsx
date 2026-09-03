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
        color: '#64748b',
        fontSize: '0.875rem',
        padding: '0.75rem 1rem',
        background: '#f1f5f9',
        border: '1px solid #e2e8f0',
        borderRadius: '6px',
      }}
    >
      Not available for {agencyLabel}. {reason}
    </p>
  )
}

export default AgencyUnavailable
