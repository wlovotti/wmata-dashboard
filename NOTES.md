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

<!-- TODO(user): wordsmith the north-star paragraph — it steers item
     selection in /notes-cycle, so it should say what YOU mean. -->

## Comparison sprint (active — work top to bottom)

- [NOTES-96](notes/NOTES-96.md) Replay archive support for multi-agency pipelines — sev medium / eff medium — unblocked
- [NOTES-100](notes/NOTES-100.md) SFMTA derivation end-to-end (Plan 2) — sev high / eff medium-high — blocked by NOTES-96
- [NOTES-99](notes/NOTES-99.md) Agency comparison page (the north star) — sev high / eff medium — blocked by NOTES-100

## Ops floor (not frozen — prevents a known recurring failure)

- [NOTES-89](notes/NOTES-89.md) GTFS reload automation on the VM — sev high / eff medium — unblocked

## Ops & reliability (parked pending NOTES-95)

- [NOTES-95](notes/NOTES-95.md) Stateless-collector rewrite (Path 2a, second half) — sev medium / eff high — needs its own spec/plan cycle
- [NOTES-94](notes/NOTES-94.md) VP-path dead-man coverage — sev low / eff low — subsumed by NOTES-95 if it lands first
- [NOTES-81](notes/NOTES-81.md) Phantom vehicle-reported timestamps — sev low / eff low — fold guard into NOTES-95 if it starts first
- [NOTES-82](notes/NOTES-82.md) Redundant vehicle_positions indexes — sev low / eff low — unblocked

## WMATA depth & UX (parked during the sprint)

The dashboard today is observational and route-anchored; these items
push toward an ops-manager view (trends, Pareto, drill-downs) and a
designed UI. Resume after the comparison sprint ships.

- [NOTES-84](notes/NOTES-84.md) Overview editorial redesign — sev medium / eff high — needs interactive design session
- [NOTES-85](notes/NOTES-85.md) Frontend design-system pass — sev low / eff medium-high — after NOTES-84
- [NOTES-86](notes/NOTES-86.md) System-level weekly narrative — sev low / eff medium — coordinate placement with NOTES-84
- [NOTES-87](notes/NOTES-87.md) Small honesty fixes in frontend chrome — sev low / eff low — baseline regen user-run
- [NOTES-83](notes/NOTES-83.md) Blank RouteDetail visual baselines — sev medium / eff low — regen user-run
- [NOTES-61](notes/NOTES-61.md) Hold-down policy / dispatching candidates page — sev low — unblocked
- [NOTES-20](notes/NOTES-20.md) Tighter rider-experience OTP — sev low — deferred

## Deferred / trigger-based

- [NOTES-101](notes/NOTES-101.md) Parallel notes-cycle driver (batch mode) — sev low / eff medium — after PR #186 merges
- [NOTES-88](notes/NOTES-88.md) `/api/routes` N+1 latency cliff over the tunnel — sev medium / eff medium — blocks public deploy only
- [NOTES-49](notes/NOTES-49.md) Cloud migration phase 2 — managed Postgres — trigger-based; partly mooted by NOTES-95
- [NOTES-50](notes/NOTES-50.md) Cloud migration phase 3 — deploy API + frontend — trigger: audience beyond personal
