# NOTES-20. Tighter rider-experience OTP

Backend shipped as the rider-OTP backend (PR #241):
`RIDER_OTP_EARLY_SEC` / `RIDER_OTP_LATE_SEC` (`src/otp_constants.py`) and
an `otp_window=official|rider` query param on
`/api/routes/{id}/trend` (metric=otp) and `/api/routes/{id}/stops`. UI
toggle ships with NOTES-143.

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. Remaining scope beyond the backend
above: the route-level `/api/routes/{id}` header KPI and
`/api/routes/{id}/period-drilldown` were left on the official window
only — the header's OTP comes from the shared cross-route live-metrics
cache that also backs the `/api/routes` scorecard and bunching's
official-window computation, and the drilldown endpoint doesn't compute
OTP at all (EWT/bunching only) — so threading a second window through
either was judged out of scope for a backend-only pass. Revisit if the
UI toggle (NOTES-143) needs the header card to respect it too.
