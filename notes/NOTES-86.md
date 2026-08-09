# NOTES-86. System-level weekly narrative

**Severity: low.**
**Effort: medium (the pattern already exists end-to-end for routes).**

Every Overview surface speaks in metric acronyms (OTP, EWT, bunching)
with no translation into consequences — "EWT 73s" doesn't drive
anything home; "riders on frequent routes waited about a minute longer
than scheduled, 12% worse than two weeks ago" does. The machinery for
this already exists: the route-diagnosis narrative (PR #141) generates
LLM summaries offline via CLI, caches them in
`route_diagnosis_narrative`, and serves them read-only — Claude is never
called at request time. Extend that exact pattern to one system-level
weekly narrative ("what happened on the network this week") sourced
from `system_metrics_daily` + the contributors/deltas data, cached in a
sibling table, rendered as the Overview's lede.

**Subagent note:** the code (CLI extension, cache table, endpoint,
panel) is subagent-suitable. The narrative *generation run* (live LLM
call) and the editorial tone review of the output are user-run — the
subagent should ship the machinery with a documented generation command,
not invoke it.

## Dependencies

Independent, but the rendered placement should land after (or inside)
the NOTES-84 Overview redesign so the lede has a home; coordinate to
avoid same-file PR stacking on `Overview.jsx`.
