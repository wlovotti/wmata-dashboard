import { Link } from 'react-router-dom'

// The four tools demoted from the top-level nav by the NOTES-84 collapse.
// Existing URLs are preserved verbatim — this page is an index, not a move.
const TOOLS = [
  {
    to: '/blocks',
    title: 'Blocks',
    description: 'Live block activity — which scheduled vehicle assignments are on the road right now.',
  },
  {
    to: '/targets',
    title: 'Targets',
    description: 'Route-level performance targets, and which routes are missing them.',
  },
  {
    to: '/schedule-audit',
    title: 'Schedule audit',
    description: 'Where the printed schedule itself is the problem — infeasible scheduled run times.',
  },
  {
    to: '/segments',
    title: 'Segments',
    description: 'Cross-route corridor segments — where routes slow down on shared streets.',
  },
]

/**
 * `/diagnostics` landing page (NOTES-84 nav collapse). One card per
 * deep-dive tool that used to hold a top-level nav slot. Each card links to
 * the tool's unchanged URL and answers "what question does this page
 * answer?" in one line, so the index earns its click instead of being a
 * bare list of nouns.
 */
function DiagnosticsIndex() {
  return (
    <main>
      <div className="chart-container">
        <h2>Diagnostics</h2>
        <p className="drilldown-anchor">
          Deep-dive tools behind the Overview and Routes pages.
        </p>
        <div className="diagnostics-grid">
          {TOOLS.map((tool) => (
            <Link key={tool.to} to={tool.to} className="diagnostics-card">
              <h3>{tool.title}</h3>
              <p>{tool.description}</p>
            </Link>
          ))}
        </div>
      </div>
    </main>
  )
}

export default DiagnosticsIndex
