# NOTES-143. Agency switch in the dashboard UI

**Severity: low-medium (north-star gap — the only Muni surface today is the static /compare table).**
**Effort: medium-high (touches the app shell, the fetch layer, and every page).**

Wave 2 of the 2026-09 UX program. Depends on the `agency` API query
param (NOTES-139, PR #TBD) and the URL-state mechanism from the window
picker (NOTES-140, PR #TBD).

Work:
- Agency toggle (WMATA / SFMTA) in the app shell, persisted in the URL
  alongside `days`; nav links and in-table route links carry it.
- Thread `agency` through the fetch layer (`useMultiFetch` /
  `fetchCache`) so every page request passes it; cache keys must include
  it.
- Title/header copy becomes agency-aware ("WMATA Performance Dashboard"
  is hard-coded in App.jsx).
- Handle WMATA-only concepts gracefully for SFMTA: the frequent-route
  list (config/frequent_routes.yaml → EWT-vs-OTP headline), per-route
  targets config, timepoints, and possibly shapes. "Not available for
  this agency" states, not crashes or silently-wrong headlines.
- Fold in the rider-experience OTP toggle UI (NOTES-144 backend) on
  RouteDetail, so the two changes share one Playwright baseline regen.

Changes Overview / RouteList / RouteDetail → Playwright baselines must
be regenerated on both platforms by the user at PR time.

## Dependencies

NOTES-139 (agency API param), NOTES-140 (window picker + URL state),
NOTES-144 (rider OTP backend) — all must be merged first.
