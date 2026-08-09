# NOTES-99. Agency comparison page (the north star)

**Severity: high (product — this is the project's stated goal, and
nothing on screen shows it today).**
**Effort: medium (one new page + one or two endpoints over existing
rollup tables; deliberately plain visually).**

One route-agnostic page showing WMATA vs SFMTA (Muni) side by side on
3–4 headline KPIs — OTP, EWT (frequent routes), bunching rate, and
service delivered — computed identically per agency over the matched
window (SFMTA collection began 2026-07-22; treat 2026-07-23 as the
first full matched day). Two columns, big numbers, week-over-week
deltas where the window allows.

Scope decisions (2026-08-09):
- **Audience v1 is the user only** — no deploy, no auth, no latency
  work (NOTES-88/50 stay parked).
- **Deliberately plain.** Ship ugly-but-honest; visual polish belongs
  to NOTES-84/85 later. Do not wait for the design system.
- **Honest comparability footnotes per metric**: frequent-route
  designation differs per agency (WMATA has an official list in
  `config/frequent_routes.yaml`; Muni starts with the data-driven
  cell-hour gate in `src/ewt.py`), the OTP window (−2/+7) is applied
  identically but agencies publish different official windows, and the
  511.org duplicate-stop_sequence artifact (PR #180) is worth a caveat
  line. A comparison that hides its caveats is worse than none.
- Matched-window framing in the page header ("since 2026-07-23"), not
  buried in a tooltip.

Acceptance: the page loads locally showing both agencies' KPIs for the
matched window, with the caveat footnotes rendered.

## Dependencies

The SFMTA derivation rollout (PR #189) threaded `--agency` through the
pipeline chain and fixed the schedule-anchor/service-date timezone bugs
it would otherwise have hit; SFMTA metrics still need the backfill run
(runbook in that PR's body) before rollup tables have real rows to
show. Placement/polish coordinates with NOTES-84 later — do not block
on it.
