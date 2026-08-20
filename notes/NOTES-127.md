# NOTES-127. Cross-surface flatThreshold inconsistency for 0..1-scale server deltas

**Severity: low.**
**Effort: low.**

`RouteList.jsx:94` and `RouteDetail.jsx:359` each define a local
`renderServerDelta` helper that wraps `DeltaIndicator` but never passes
a `flatThreshold` prop, so every call falls back to `DeltaIndicator`'s
own default of `0.5`. For `service_delivered` and `bunching`, the
server delta wire value is a 0..1 fraction (not percentage points), so
a real, meaningful change — e.g. a 2 pp swing, which arrives as
`0.02` — sits two orders of magnitude below that 0.5 default and
renders as a flat gray arrow essentially every time, regardless of
actual magnitude.

This is the same unit mismatch NOTES-121 fixed inside `MoversPanel.jsx`
(see `MOVERS_FLAT_FLOOR` there: `service_delivered`/`bunching` floors
are expressed as `0.005` fraction, i.e. 0.5 pp, specifically because
the wire units are fractions, not pp). `MoversPanel` now passes a
per-metric `flatThreshold` derived from that floor; `RouteList` and
`RouteDetail` do not, and NOTES-121 (the only tracker of this specific
unit inconsistency) is deleted by the PR that fixes `MoversPanel`
(PR #215) — this item exists so the remaining two call sites aren't
forgotten.

**Observable symptom:** the same route/metric can show a flat gray →
arrow in `RouteList`'s or `RouteDetail`'s service-delivered/bunching
column while the Overview movers panel shows a red ▼ or green ▲ for
the identical week-over-week delta — same data, two different-looking
answers to "did this change," because the two surfaces use different
effective flat thresholds for the same 0..1-scale metric.

**Suggested fix:** hoist the per-metric floor map out of
`MoversPanel.jsx` (`MOVERS_FLAT_FLOOR` / `getMoversFloor`) into a
shared module (e.g. `frontend/src/moversFloor.js` or similar), and
have both `RouteList.jsx`'s and `RouteDetail.jsx`'s
`renderServerDelta` helpers accept and pass a `flatThreshold` argument
sourced from that shared map, keyed by the same metric key used to
call each helper (`otp`, `service_delivered`, `ewt`, `bunching`).
`MoversPanel.jsx` would then import from the shared module instead of
keeping its own copy.

## Dependencies

Follows the movers-panel magnitude floor work, PR #215 (which fixes
`MoversPanel.jsx` only and deletes NOTES-121, the tracker for this
same unit-inconsistency class of bug). Not blocked by anything else.
