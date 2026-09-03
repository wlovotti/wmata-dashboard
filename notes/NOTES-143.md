# NOTES-143. Agency switch in the dashboard UI

**Severity: low-medium (north-star gap — the only Muni surface today is the static /compare table).**
**Effort: medium-high (touches the app shell, the fetch layer, and every page).**

Wave 2 of the 2026-09 UX program. Depends on the `agency` API query
param (NOTES-139, PR #236) and the window picker + URL state PR (wave 1,
in flight).

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
  targets config, timepoints, and possibly shapes. These aren't merely
  *unavailable* for SFMTA -- both files are keyed by WMATA route_id, and
  route_ids overlap across agencies (SFMTA has its own "1", "9", "14",
  "90", ...), so applying them unfiltered would silently classify SFMTA
  routes using WMATA's data for a same-numbered WMATA route (e.g. SFMTA's
  route "1" picking up WMATA route 1's frequent-service flag and target
  numbers). NOTES-139
  (PR #236) stops this for `/api/routes` and `/api/routes/{route_id}`
  as a stopgap by forcing `is_frequent=False` / `targets=None` for any
  non-wmata agency; this item should replace that stopgap with a real
  per-agency config (or an explicit "not available for this agency"
  UI state) rather than just removing the stub. "Not available for
  this agency" states, not crashes or silently-wrong headlines.
- Fold in the rider-experience OTP toggle UI (backend landed in the
  rider-OTP backend, PR #241) on RouteDetail, so the
  two changes share one Playwright baseline regen. The backend added
  `otp_window=official|rider` on `/api/routes/{id}/trend` (metric=otp)
  and `/api/routes/{id}/stops`.

Changes Overview / RouteList / RouteDetail → Playwright baselines must
be regenerated on both platforms by the user at PR time.

## Dependencies

The agency API param (NOTES-139, PR #236), the window picker + URL
state PR (wave 1, in flight), and the rider-OTP backend (PR
#241) — all must be merged first.
