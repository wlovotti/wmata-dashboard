# NOTES-20. Tighter rider-experience OTP

Backend shipped in PR #241: `RIDER_OTP_EARLY_SEC` / `RIDER_OTP_LATE_SEC`
(`src/otp_constants.py`) and an `otp_window=official|rider` query param
on every route-level endpoint that computes OTP live from `stop_events`
— `/api/routes/{id}` (detail header KPI, which also drives the letter
grade), `/api/routes/{id}/trend` (metric=otp), and
`/api/routes/{id}/stops`. UI toggle ships with NOTES-143.

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. The `/api/routes/{id}` period-over-
period delta arrow and `/api/routes/{id}/period-drilldown` stay on the
official window by design: deltas are sourced from the precomputed
`route_metrics_daily_overlay` (not the live per-request path
`otp_window` controls), and drilldown doesn't compute OTP at all
(EWT/bunching only). The `/api/routes` scorecard, `system_metrics_daily`
system rollups, the daily batch, `src/bunching.py`, and the
`/api/agency-comparison` compare page all remain official-window only —
by design, for comparability across routes/agencies/history, not as
unfinished scope.
