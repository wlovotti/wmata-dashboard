# NOTES-132. Dead-man ping can false-positive on an empty-but-healthy feed

**Severity: low (a false page during a genuinely quiet window, not a
missed real outage).**
**Effort: low-medium (needs a "no vehicles to report" distinct from "feed
fetch failed" signal, or a looser owl-hours grace).**

Documented at cutover in `docs/DEPLOYMENT.md` §13.5: `PingGate`
(`src/stateless_poller.py`) only pings healthchecks.io when both the "tu"
and "vp" streams have shipped an S3 object within `freshness_sec`. An
owl-hours window with zero active vehicles produces no new rows for
either feed, so the writer never rotates a file, the upload cycle has
nothing to ship, and the gate correctly stays silent by its own logic —
but from the healthchecks.io side that silence is indistinguishable from
a real outage, so once the grace period elapses it pages even though the
collector process is up and polling normally.

Work: give the collector a way to distinguish "polled successfully, feed
had zero vehicles" from "poll failed" — e.g. ship a tiny heartbeat marker
file (or ping directly) on a successful zero-row poll, separate from the
"data shipped" signal `PingGate` currently keys on. Needs care not to
reintroduce the failure mode the ping-after-commit design was built to
avoid (a "the process is alive" heartbeat that fires even when the
actual fetch is silently broken).

## Dependencies

None — self-contained to the stateless collector (`src/stateless_poller.py`,
`scripts/stateless_collector.py`). Related to but distinct from
NOTES-133's hardening list (that item is deferred-review code-quality
minors; this one is a design gap in the alerting signal itself).
