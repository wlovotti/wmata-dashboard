# NOTES — punch-list index

Forward-looking punch list. This file is the index; each open item's
body lives in `notes/NOTES-N.md`. Completed items are deleted in the
same PR that closes them (item file + index line) — git log and PR
descriptions are the durable history; there is no changelog here.
Item numbers (`NOTES-N`) are stable forever; new items take the next
unused number — verify against history, not just this file:

```bash
git log --all -p -- NOTES.md notes/ | grep -oE 'NOTES-[0-9]+' | sort -t- -k2 -n -u | tail -1
```

Punch-list edits ride on substantive PRs; standalone reconciliation
PRs are churn. Because a closing PR touches only its own item file
plus one index line here, cycles closing *different* items can run in
parallel worktrees — same-file conflicts arise only when two items'
cross-references overlap (check before dispatching).

## North star

An easily digestible display of bus performance metrics comparing
agencies — WMATA vs SFMTA (Muni) first: a few headline KPIs computed
identically per agency, shown side by side with honest comparability
caveats. Audience for now: personal. Every open item below either
serves this or is explicitly parked.

## Ops & reliability (parked pending NOTES-95)

- [NOTES-95](notes/NOTES-95.md) Stateless-collector rewrite (Path 2a, second half) — sev medium / eff high — needs its own spec/plan cycle
- [NOTES-94](notes/NOTES-94.md) VP-path dead-man coverage — sev low / eff low — subsumed by NOTES-95 if it lands first
- [NOTES-81](notes/NOTES-81.md) Phantom vehicle-reported timestamps — sev low / eff low — fold guard into NOTES-95 if it starts first
- [NOTES-82](notes/NOTES-82.md) Redundant vehicle_positions indexes — sev low / eff low — unblocked
- [NOTES-102](notes/NOTES-102.md) Backfill June recovery-window trip_update truncation — sev low / eff low — unblocked; needs S3 pull for 6/14–16 + snapshot-12 GTFS pin
- [NOTES-104](notes/NOTES-104.md) Replay-aware data-completeness signal — sev low-medium / eff medium — unblocked; replayed dates stamped 'partial' regardless of threshold, accepted for now (PR #189); the agency comparison page (PR #198) now annotates this via its caveats list; 2026-08-11 addendum: laptop VP-only numerator caps ~33%, so ALL laptop SFMTA dates flag partial
- [NOTES-109](notes/NOTES-109.md) Full per-date scheduled-pool resolution for EWT/bunching (declined third option from NOTES-106) — sev low-medium / eff medium — unblocked; modal-per-day_type (PR #191) is the interim
- [NOTES-112](notes/NOTES-112.md) Proximity fallback matcher emits +20–24h false matches (~3.5/day SFMTA, mostly rail/cable routes) — sev low / eff low-medium — unblocked
- [NOTES-113](notes/NOTES-113.md) Top-up SFMTA 8/9–8/10 (partial derive wrote `runs` rows, blocking auto-revisit) — sev low / eff low — blocked on next archive rsync

## WMATA depth & UX

The dashboard today is observational and route-anchored; these items
push toward an ops-manager view (trends, Pareto, drill-downs) and a
designed UI. The comparison sprint shipped with PR #198.

- [NOTES-84](notes/NOTES-84.md) Overview editorial redesign — sev medium / eff high — needs interactive design session
- [NOTES-85](notes/NOTES-85.md) Frontend design-system pass — sev low / eff medium-high — after NOTES-84
- [NOTES-86](notes/NOTES-86.md) System-level weekly narrative — sev low / eff medium — coordinate placement with NOTES-84
- [NOTES-87](notes/NOTES-87.md) Small honesty fixes in frontend chrome — sev low / eff low — baseline regen user-run
- [NOTES-83](notes/NOTES-83.md) Blank RouteDetail visual baselines — sev medium / eff low — regen user-run
- [NOTES-61](notes/NOTES-61.md) Hold-down policy / dispatching candidates page — sev low — unblocked
- [NOTES-20](notes/NOTES-20.md) Tighter rider-experience OTP — sev low — deferred

## Deferred / trigger-based

- [NOTES-88](notes/NOTES-88.md) `/api/routes` N+1 latency cliff over the tunnel — sev medium / eff medium — blocks public deploy only
- [NOTES-49](notes/NOTES-49.md) Cloud migration phase 2 — managed Postgres — trigger-based; partly mooted by NOTES-95
- [NOTES-50](notes/NOTES-50.md) Cloud migration phase 3 — deploy API + frontend — trigger: audience beyond personal
