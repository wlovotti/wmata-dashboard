# NOTES-121. Movers-panel magnitude floor for week-over-week deltas

**Severity: low.**
**Effort: low-medium.**

`MoversPanel` (the Overview editorial redesign, PR #209) ranks by
`|delta|` with only a zero-delta exclusion and a 3-valid-movers floor.
Sub-noise deltas (e.g. −0.1 pp OTP) still rank under "Getting worse"
while their own `DeltaIndicator` arrow renders flat (0.5 flat
threshold), and can satisfy the 3-mover floor — a route can appear in
a "worsening" table while its own delta badge visually says "no
change."

Work: research per-metric noise thresholds before asserting them
(project convention: scan agencies + TRB literature for delta
significance), then apply a magnitude floor consistent with the flat
band. Note `DeltaIndicator`'s `flatThreshold=0.5` is unit-inconsistent
across metrics — 0.5pp OTP vs 0.5s EWT vs 0.5 on 0–1 bunching
fractions — so the floor needs per-metric units, not one constant
reused across all four.

**Acceptance:** no row whose delta renders as "flat" appears in either
movers direction.

## Dependencies

None. Follow-up to the Overview editorial redesign (PR #209); the
noise-floor gap was called out but deliberately not handled there
(`MoversPanel.jsx` comment).
