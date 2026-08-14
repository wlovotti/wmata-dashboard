# NOTES-124. Agency comparison page: reformat as a comparison table

**Severity: low-medium** *(the page exists to compare, and the current
layout fights that — user feedback from 2026-08-13 manual testing:
"should really be in a table or some other format where it is easy to
compare and contrast the values across the agencies")*.
**Effort: low** *(single component rewrite over an unchanged payload;
`/compare` has no Playwright baseline, so no regen cost)*.

`AgencyComparison.jsx` renders a column of `MetricTile`s per agency
(`agency-column` / `agency-metric-grid`), so comparing WMATA vs Muni
on any one metric means visually jumping between columns of big-number
tiles. Invert the layout: one row per metric (OTP, service delivered,
scheduled wait, EWT, bunching, daytime service level), one column per
agency, with each cell carrying the value + week-over-week delta pill
+ partial-day disclosure the tiles carry today. The existing
`routes-table` styling is the obvious base; `METRIC_ORDER` /
`METRIC_LABELS` / formatters in `utils/agencyComparison.js` already
support this shape. Keep the window framing line and the comparability
caveats list exactly as they are — the caveats-in-body decision from
PR #198 stands.

The CompareStrip on the Overview hero (PR #210) is unaffected — it
reads the same endpoint but renders its own one-line summary.

Acceptance: each metric's values for all agencies sit on one visual
row; no information the tiles showed is lost (value, delta, partial
disclosure, service-level context line).

## Dependencies

None. Sequence freely; if NOTES-85 (design-system pass) lands first it
restyles the table, but the reformat shouldn't wait for it.
