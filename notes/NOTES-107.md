# NOTES-107. `route_service_profile` has the same calendar_dates blind spot NOTES-106 fixed for EWT/bunching — SFMTA weekday rows are entirely missing

**Severity: medium** *(SFMTA-only; `route_service_profile` — frequency classification, `mean_headway_min`, `scheduled_trips` — is silently empty for every SFMTA route on weekdays; weekends are fine)*.
**Effort: low** *(same fix shape as NOTES-106, smaller surface — one function, no multi-date/vectorized twin to keep in sync)*.

Found while fixing NOTES-106 (calendar_dates resolution for EWT/bunching's
scheduled-headway pool). `src/service_profile.py:_service_ids_for_day_type`
has the identical bug `src/ewt.py:_scheduled_headways_by_cell_hour` had
before NOTES-106's fix: it filters `Calendar.<field> == 1` directly and
never consults `calendar_dates`. Confirmed in data —
`sfmta_dashboard.route_service_profile` has 1,010 rows each for
`day_type='saturday'` and `day_type='sunday'`, and **zero** for
`day_type='weekday'`:

```
 day_type | count
----------+-------
 sunday   |  1010
 saturday |  1010
```

This is a separate code path from NOTES-106 (`service_profile.py` vs.
`ewt.py`/`bunching.py`) and a separate table (`route_service_profile` vs.
the runtime EWT/bunching pool computed straight from `stop_times`), so it
wasn't fixed by the NOTES-106 PR — `_resolve_service_ids_for_day_type` in
`src/ewt.py` is the converged resolver for the EWT/bunching path only.
`service_profile.py` needs its own call to the same resolution logic
(ideally by importing/sharing `_resolve_service_ids_for_day_type` rather
than re-deriving a third copy — check whether it's cleanly reusable given
`service_profile.py`'s different query shape, or whether the shared
resolver should move to a common module both files import from).

Blast radius: `compute_route_service_profile` populates
`route_service_profile.mean_headway_min` / `scheduled_trips` /
`is_frequent`, consumed by `compute_route_frequency_classes` (frequency
classification: high/medium/low/limited) and any UI/API surface that
reads route-level frequency for SFMTA. Does NOT affect the NOTES-106 fix
itself — EWT's cell-hour frequency gate (`get_cell_hour_gate_sec`) is
config-driven (`config/frequent_routes.yaml`, WMATA-only) and independent
of `route_service_profile`.

Acceptance: `sfmta_dashboard.route_service_profile` gets non-zero
`day_type='weekday'` rows after a GTFS-reload-triggered
`compute_route_service_profile` re-run; WMATA output unchanged on a
regression date.

## Dependencies

None (unblocked). Not required for NOTES-99's EWT/bunching KPIs (those
route through NOTES-106's fix, not this table) — but any future "frequent
route" classification surfaced for SFMTA on the comparison page needs
this fixed first.
