# NOTES-87. Small honesty fixes in the frontend chrome

**Severity: low (trust erosion, individually trivial).**
**Effort: low.**

Three small dishonesties surfaced in the 2026-06-10 product review:

1. The header subtitle says "Real-time transit performance metrics"
   (`frontend/src/App.jsx`) but the dashboard is daily-batch — say what
   it is ("Daily bus network performance" or similar).
2. The Refresh button is a bare `window.location.reload()` — either
   refetch data in place or drop the button.
3. The Off-target panel renders empty unless `config/route_targets.yaml`
   has hand-edited overrides — the empty state should explain that (it
   partially does) or the panel should hide until targets exist.

**Subagent note:** the code is subagent-suitable, but item 1 changes the
header on every baselined page, invalidating all Playwright baselines on
both platforms — the regen step is user-run; document it in the PR body
rather than running it. Consider bundling with another
baseline-invalidating PR (NOTES-84/85) to amortize the regen.
