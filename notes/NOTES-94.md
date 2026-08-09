# NOTES-94. Dead-man coverage for the vehicle-positions path

**Severity: low (the TU-path ping shipped in PR #173 covers whole-process
death; this covers VP-only failure).**
**Effort: low.**

`ping_healthcheck` fires from `_save_trip_updates` only. The VP fetch loop
(60 s cadence vs TU's 30 s) can fail independently — API-key issues on the
`BusPositions` endpoint, a VP-specific parse error — while TU keeps
committing and the check stays green, silently starving proximity-source
derivation (origin OTP, segment slips; see the vehicle-positions-necessity
note). Add a second healthchecks.io check pinged from
`_save_vehicle_positions` via a `COLLECTOR_VP_HEALTHCHECK_URL` env var,
same fire-on-commit-success pattern.

## Dependencies

Subsumed by NOTES-95's S3-upload-staleness alarm if the rewrite lands
first — check before starting.
