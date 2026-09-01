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

## Ops & reliability

- [NOTES-131](notes/NOTES-131.md) Full local test suite reads the production database — sev low / eff medium — unblocked
- [NOTES-132](notes/NOTES-132.md) Dead-man ping can false-positive on an empty-but-healthy feed — sev low / eff low-medium — unblocked
- [NOTES-133](notes/NOTES-133.md) Stateless-collector hardening follow-ups (deferred review minors) — sev low / eff low each — unblocked; good notes-batch candidate
- [NOTES-102](notes/NOTES-102.md) Backfill June recovery-window trip_update truncation — sev low / eff low — unblocked; needs S3 pull for 6/14–16 + snapshot-12 GTFS pin
- [NOTES-104](notes/NOTES-104.md) Replay-aware data-completeness signal — sev low-medium / eff medium — unblocked; replayed dates stamped 'partial' regardless of threshold, accepted for now (PR #189); the agency comparison page (PR #198) now annotates this via its caveats list; 2026-08-11 addendum: laptop VP-only numerator caps ~33%, so ALL laptop SFMTA dates flag partial
- [NOTES-109](notes/NOTES-109.md) Full per-date scheduled-pool resolution for EWT/bunching (declined third option from NOTES-106) — sev low-medium / eff medium — unblocked; modal-per-day_type (PR #191) is the interim
- [NOTES-112](notes/NOTES-112.md) Proximity fallback matcher emits +20–24h false matches (~3.5/day SFMTA, mostly rail/cable routes) — sev low / eff low-medium — unblocked
- [NOTES-118](notes/NOTES-118.md) Investigate scheduled-span data quality in runs (A90 67% zero-span, EXP/LCL junk route_ids) — sev low-medium / eff medium — unblocked; investigation first, guard is one possible outcome
- [NOTES-130](notes/NOTES-130.md) migrate_all.py doesn't strip its own argv before invoking sibling migration scripts — sev low / eff low — unblocked; two destructive scripts now share the `--yes` flag name

## WMATA depth & UX

The dashboard today is observational and route-anchored; these items
push toward an ops-manager view (trends, Pareto, drill-downs) and a
designed UI. The comparison sprint shipped with PR #198.

- [NOTES-85](notes/NOTES-85.md) Frontend design-system pass — sev low / eff medium-high — after the Overview editorial redesign (PR #209/#210)
- [NOTES-61](notes/NOTES-61.md) Hold-down policy / dispatching candidates page — sev low — unblocked
- [NOTES-20](notes/NOTES-20.md) Tighter rider-experience OTP — sev low — deferred
- [NOTES-126](notes/NOTES-126.md) Add an observed-departure timestamp to stop_events for a principled slip-origin exclusion — sev low / eff medium-high — unblocked; deferred from the segment-slip origin guard (PR #213)

## Deferred / trigger-based

- [NOTES-88](notes/NOTES-88.md) `/api/routes` N+1 latency cliff over the tunnel — sev medium / eff medium — blocks public deploy only
- [NOTES-50](notes/NOTES-50.md) Cloud migration phase 3 — deploy API + frontend + hosted DB — trigger: audience beyond personal; absorbed the retired phase-2 managed-Postgres item 2026-08-12
