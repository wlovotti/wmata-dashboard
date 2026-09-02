# NOTES-144. Rider-experience OTP window (backend)

**Severity: low.**
**Effort: low-medium.**

Wave 2 of the 2026-09 UX program; supersedes the backend half of
NOTES-20. Add a second constant pair to `src/otp_constants.py`
(rider-experience window, e.g. -60 s / +180 s) and an
`otp_window=official|rider` query param on the route-level endpoints
that compute OTP at request time from stop_events (`/api/routes/{id}`,
`/api/routes/{id}/trend`, `/api/routes/{id}/period-drilldown` — verify
the exact set). Default `official` keeps today's -2/+7 numbers for
comparability with WMATA's published scorecard.

Out of scope: system rollups (`system_metrics_daily`) and the agency
comparison page read precomputed daily tables, so the rider window is
route-level request-time only in this pass. The UI toggle ships with
NOTES-143.

## Dependencies

None on the backend side. Must not run concurrently with a PR that
edits the OTP paths of `api/aggregations.py`.
