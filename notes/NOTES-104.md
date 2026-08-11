# NOTES-104. Replay-aware data-completeness signal

**Severity: low-medium** *(no data-correctness bug — `otp_percentage`,
`service_delivered_ratio`, `ewt_seconds`, and `bunching_rate` are all
numerically correct (the last two as of the agency-local hour bucketing
fix, PR #190). The problem is that the `data_quality` badge is
misleadingly conservative for an entire class of dates, which
undermines trust in an otherwise-honest signal.)*
**Effort: medium** *(needs a second completeness code path keyed on
how a date's data arrived — live collection vs. replay — plus deciding
what "trip_update_state poll density" should mean quantitatively.)*

The NOTES-100 completeness-guard fix (`agency_coverage_threshold`,
`src/data_completeness.py`) made the 80%-style threshold cadence-aware
so a healthy SFMTA day under live collection isn't permanently
penalized for SFMTA's coarser polling cadence. It does **not** fix a
separate, deeper gap: the guard's actual signal is the union of
`collector_heartbeats.ts` and `vehicle_positions.timestamp` minute-
buckets — and `pipelines/replay_archive_to_state.py` (the tool that
populates any date backfilled from the JSONL archive, including the
SFMTA sidecar's entire 2026-07-22-forward comparison window on first
derivation) writes **only** `trip_update_state`. It never touches
`collector_heartbeats` or `vehicle_positions`.

Effect: a purely-replayed date has a near-empty completeness numerator
regardless of how healthy the underlying collection actually was — no
threshold value fixes an empty numerator. Every SFMTA date derived via
the runbook in the NOTES-100 PR (#189) will be stamped
`data_quality='partial'` in `system_metrics_daily` and
`route_metrics_daily_overlay`, even on days where the sidecar collector
had zero downtime.

**Decision (2026-08-09, in the NOTES-100 PR's review thread):** accept
`data_quality='partial'` as the label for replayed-only dates for now
— it's not wrong, just conservative (it correctly says "we can't verify
ingest health from heartbeats/positions for this date," which happens
to be true for every replayed date, healthy or not). Do not block or
reinterpret it silently. **NOTES-99 (the comparison page) should
annotate this explicitly** — e.g. a footnote on the matched-window
header noting that early SFMTA dates read from replay and carry a
conservative partial-day label — rather than hide or suppress partial
days for the SFMTA side only, which would quietly break the "identical
definitions per agency" comparability promise the whole comparison
sprint is built on.

To close:

1. Add a second completeness signal (or a per-date "how did this data
   arrive" flag) driven by `trip_update_state` poll density instead of
   `collector_heartbeats`/`vehicle_positions` for dates known to have
   come from replay. `trip_update_state` doesn't retain raw per-poll
   history (it's collapsed to final-state per NOTES-72's fold-then-
   upsert design), so this likely needs either: (a) a coarser proxy —
   e.g. presence of `final_snapshot_ts` values spread across the
   expected day span rather than clustered, or (b) reading poll density
   from the JSONL archive directly at replay time and persisting a
   summary (new column or side table) rather than trying to reconstruct
   it from `trip_update_state` after the fact.
2. Decide the quantitative threshold/shape for "replay looks healthy"
   — this is a new judgment call, not a port of the existing minute-
   coverage math, since replay's available signal (poll density) isn't
   the same kind of measurement as live heartbeat coverage.
3. Once a real signal exists, re-run `upsert_system_metrics_daily` /
   `upsert_route_metrics_overlay` for the already-replayed SFMTA window
   so `data_quality` reflects it instead of a blanket 'partial'.
4. Land the NOTES-99 footnote regardless of whether 1-3 ship first —
   it's cheap and is the honest-comparability floor either way.

Scope note: this affects any FUTURE agency onboarded primarily via
archive replay too, not just SFMTA — worth keeping generic rather than
SFMTA-specific when implemented.

**Addendum (2026-08-11, NOTES-105/110 repave verification):** the gap
is wider than the empty-numerator replay case. The repave re-derived
7/22–8/8 with **full `vehicle_positions` present** (wholesale-pulled
from the VM), and every date still stamped `partial` at coverage
0.28–0.33 — exactly the ceiling VP-at-180s can reach when it is the
*only* numerator signal, because `collector_heartbeats` are written by
the live collector on the VM and never reach the laptop DB. So on the
laptop (the system of record), no SFMTA date can clear the threshold
even with perfect collection: the cadence-aware threshold from
NOTES-100 evidently sits above the ~33% VP-only ceiling. Closing this
item should therefore handle three numerator regimes, not two: live
(heartbeats+VP), replayed (neither), and rsync'd-to-laptop (VP only).
