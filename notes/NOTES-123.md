# NOTES-123. Contributors ranking averages partial-collection days into raw means (EWT shows 10x-inflated values)

**Severity: low-medium** *(actively misleading numbers on the "Biggest
drags" table — D20 showed EWT 1921s where the honest 7-day figure is
~594s)*. **Effort: low-medium** *(filter partial dates in the
contributors per-date loop; plus a small audit of sibling windows)*.

Surfaced 2026-08-13 during manual testing of the editorial Overview
(PR #210): D20 ranked on the 30-day EWT "Biggest drags" cut at
`route_value` 1920.58s. Root cause: 2026-07-17 was a collection-outage
day (coverage 1.7%, `data_quality='partial'`, 9 of ~215 D20 trips
observed), and with so few observed trips the EWT formula
(`Σh²/2Σh`, `src/ewt.py`) squares multi-hour observation gaps into
four-digit "waits" — that one day's ~1551s system EWT dominates the
30-day mean. (2026-07-11 was a second thin day: 39 trips.)

Every other consumer of the daily series respects the partial flag:
the trend endpoint emits `data_quality` and the Sparkline excludes
partial days from the line, `computeWindowDelta`/hero means filter
them, the deltas block counts valid days, and the agency-comparison
page discloses partial days. `get_route_contributors`
(`api/aggregations.py`) does not — it averages all dates in the
window raw.

Work items:
1. Exclude `data_quality='partial'` dates from the contributors
   per-route window means (same day-set the trend endpoint flags), and
   surface `days_included` so thin windows are visible.
2. Audit the other window aggregations that consume per-date values
   (`get_all_routes_scorecard` window means, route trend deltas) and
   document which already filter partial days — fix any that don't.
3. Acceptance: on the current data window, D20's 30-day EWT
   contributors value drops from ~1921s to the same order as its
   clean-day mean (~600s), and a test pins a partial day being
   excluded from a contributors mean.

## Dependencies

None hard. Related: the replay-aware completeness signal (NOTES-104)
defines when days get flagged partial; this item is about consumers
respecting the flag, not about the flagging itself.
