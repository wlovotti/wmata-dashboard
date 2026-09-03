import useAgency from '../hooks/useAgency'

const AGENCY_OPTIONS = [
  { key: 'wmata', label: 'WMATA' },
  { key: 'sfmta', label: 'Muni' },
]

/**
 * App-shell agency switch (NOTES-143). A WMATA/Muni segmented toggle that
 * reads/writes the shared `?agency=` URL param via `useAgency`. Rendered in
 * the header next to `WindowPicker`, outside the routed subtree, so
 * switching agencies never remounts the current page — every page reads
 * the same URL param itself and refetches when it changes. Hidden on
 * `/compare` (see `App.jsx`'s `showAgencyToggle`), which is deliberately
 * agency-independent.
 *
 * @returns {JSX.Element}
 */
function AgencyToggle() {
  const [agency, setAgency] = useAgency()

  return (
    <div className="agency-toggle" role="group" aria-label="Agency">
      {AGENCY_OPTIONS.map((option) => (
        <button
          key={option.key}
          type="button"
          className={option.key === agency ? 'agency-toggle-btn active' : 'agency-toggle-btn'}
          aria-pressed={option.key === agency}
          onClick={() => setAgency(option.key)}
          title={`Show ${option.label} data`}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}

export default AgencyToggle
