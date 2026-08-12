# NOTES-84. Overview editorial redesign

**Severity: medium (product value — the core "how is the network doing /
what's getting worse" question is answered only implicitly today).**
**Effort: high (multi-PR; spans IA, new map surface, and baselined pages).**

The 2026-06-10 product review found the Overview has the right
ingredients (HealthPulse, 30-day trends, contributors panel, What
changed) but renders them as a thin banner, four noisy daily-granularity
sparklines, and three equal-weight tables — nothing is a headline, and
the user must do the analyst's synthesis themselves. Rebuild the
Overview as an *editorial* page:

- **A big-number verdict** with plain-language framing ("75% on time
  this week, down 2 pts"), not a one-line banner.
- **A system map** (leaflet + `/api/routes/{id}/shapes` already exist)
  with routes colored by performance — the most direct answer to
  "where is it going badly."
- **A "movers" panel** ranking worsening routes using the existing
  `deltas` block (PR #125) — promote "getting worse" to the top fold.
- **Trend smoothing** — 7-day rolling line with daily points ghosted,
  replacing the raw daily squiggles.
- **Nav collapse** — Overview / Routes / Blocks / Targets / Schedule
  audit / Segments (`frontend/src/App.jsx`) is tool-shaped; collapse to
  roughly Overview / Routes / Diagnostics with the rest as drill-downs.

Constraint: trend framing must stay inside the post-cutover-clean window
(pre-2026-05-25 partial-day aggregates are contaminated; collection
starts 2026-05-02), so "getting worse" means weeks-over-weeks for now.

**Not subagent-suitable.** This is design work — it needs an interactive
brainstorming/design session with the user (layout, what gets demoted,
visual tone), and it invalidates the Overview/RouteList visual baselines
(regen is user-run). A subagent dispatched cold will produce another
accretion, which is the problem being fixed.

## Dependencies

None hard (the SSH tunnel shipped 2026-06-13 and dev runs on a local
socket, so the site is viewable during iteration). Sequence before
NOTES-85 (don't restyle panels that are about to be rearranged); both
touch the same files, so don't stack PRs. The agency comparison page
(PR #198) now exists; fold its placement into this redesign rather
than designing around it.
