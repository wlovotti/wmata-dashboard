# NOTES-61. Hold-down policy / dispatching candidates page

**Severity: low.**

Ranked timepoint-leakage table — which WMATA timepoints would benefit
most from enforced hold-downs (AVL alerting on early departures, or
operator-policy reminders). The operational complement to the
schedule-audit page's schedule-revision lever: zero capital, only
policy.

For each timepoint on each route, per period:
- **Leakage rate** — % of buses departing > 60s ahead of scheduled
  departure
- **Estimated downstream EWT impact** — expected reduction in
  next-bus headway variance at the first 2-3 downstream stops if
  early departures were eliminated
- **Affected daily trips** — count

Ranked descending by estimated EWT-savings × trip volume. Per-route
drill-down shows the full distribution at the timepoint.

Use cases:
- "Which timepoints should AVL flag early departures most aggressively?"
- "Where would headway-based dispatching (vs schedule adherence) most
  improve rider experience?" — the leakiest timepoints on frequent
  routes are also the best candidates for policy change.

## Dependencies

route_diagnostic_profile foundation (PR #107). "Frequent route"
filtering in the ranking (headway-based dispatching is the right
intervention specifically for frequent routes) uses the
WMATA-designated list in `config/frequent_routes.yaml`, loaded via
`src/frequent_routes.py`.
