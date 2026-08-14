# NOTES-85. Frontend design-system pass

**Severity: low (polish — but the generic internal-tool look is a stated
user dissatisfaction).**
**Effort: medium-high (touches every component; no behavior change).**

The frontend has no design language: one hand-rolled 1,287-line
`App.css` plus ~200 inline `style={{}}` blocks scattered across
components (RouteDiagnosisPanel alone has 54). Every panel made its own
micro-decisions on color, spacing, and type, which is why the UI reads
as generic and slightly inconsistent. Recharts and leaflet are already
in the dependency tree — the gap is deliberate tokens, not libraries.

Work: define CSS custom-property tokens (color roles, spacing scale,
type scale), one chart idiom (axis/grid/tooltip conventions applied to
every recharts instance), and migrate components off inline styles.
Decide deliberately whether to stay hand-rolled or adopt a utility/
component layer — that choice is the user's.

**Not subagent-suitable.** Aesthetic decisions need the user in the
loop, and the pass invalidates all Playwright visual baselines on both
platforms (regen is user-run). The mechanical migration *after* the
tokens are agreed could be subagent work, but not the design itself.

## Dependencies

After the Overview editorial redesign (PR #209/#<PR4>) — restyling
panels the redesign is about to rearrange is wasted work, and the two
would conflict on the same files (no stacked PRs).
