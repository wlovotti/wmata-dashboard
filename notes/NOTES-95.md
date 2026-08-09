# NOTES-95. Stateless-collector rewrite (Path 2a, second half)

**Severity: medium (the current interim topology works but carries
manual chores and a full Postgres on a 2 GB box that no longer needs
one).**
**Effort: high (its own brainstorm → spec → plan cycle; do not start as
a side-task).**

Complete the Path 2a architecture decided 2026-07-13 and half-executed
2026-07-18 (see `docs/POSTMORTEM_2026-07.md`, "Architecture decision").
Target: the VM polls WMATA → writes raw zstd JSONL for both feeds →
uploads to S3 on a short cadence → pings a healthcheck. No Postgres, no
timers beyond the upload loop, smallest Lightsail tier. The laptop pulls
from S3 (not rsync-over-ssh) and derives on demand.

Four items are explicitly deferred TO this rewrite and land with it —
they are its acceptance criteria, not separate work:

1. **S3-upload-staleness alarm** (successor to the nightly-batch
   alerting item NOTES-91, closed as superseded — its collector
   dead-man ping shipped in PR #173, and Path 2a removed the unattended
   batch it would have paged on): the dead-man signal becomes "newest
   object in the raw prefix is older than N minutes," replacing the
   DB-commit ping. Covers both feeds by construction, which also
   subsumes NOTES-94 (VP-path coverage) if the rewrite lands first.
2. **Cutover job inventory** (postmortem lesson #3): the rewrite spec
   must enumerate every recurring job on both machines — what runs
   where, how each is verified — and the cutover follows that checklist.
   This incident's RC2 and both disk fills were cutover-gap failures.
3. **Instance downsize** (lesson #6): once Postgres leaves the box,
   drop to the smallest tier; the 2 GB instance and its swap/OOM
   mitigations stop mattering.
4. **Upload replaces retention chores** (lesson #5): the hourly S3
   upload obsoletes the VM's 14-day JSONL buffer pruning, the
   `wmata-archive-positions` timer, the weekly `pg_dump` backup (no DB
   to back up), and `bin/pull-and-derive.sh`'s rsync+tunnel path
   (pull from S3 instead).

Also fold in: laptop-side GTFS reload cadence (NOTES-89's home after
the VM DB retires — the weekly reload targets the laptop DB, likely as
a step in the pull-and-derive flow) and the collector VP-timestamp
sanity guard (NOTES-81) if the collector is being rewritten anyway.

Interim state until this lands (documented in DEPLOYMENT.md's 2026-07-18
banner): VM = collector + backup + VP-archive timers with hc-ping
dead-man; laptop = system of record with 30-day JSONL working set;
S3 `raw-jsonl-archive/` = permanent raw store, appended by manual
laptop-side sync.

## Dependencies

Needs its own spec/plan cycle — not dispatchable from the punch list.
Parked during the comparison sprint.
