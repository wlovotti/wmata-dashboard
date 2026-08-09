# NOTES-89. GTFS reload automation on the VM

**Severity: high (without it, schedule staleness recurs at every WMATA
service change — the 2026-06/07 incident's second root cause; feed
S1000249 expired 2026-06-20 and nothing noticed).**
**Effort: medium (systemd service + timer + runbook; the reload script
itself already exists and ran weekly on the laptop for months).**

The weekly GTFS refresh (`scripts/reload_gtfs_complete.py`) ran as a laptop
launchd job (`gtfs-reload`, Sundays 08:00 UTC) and was durably disabled
2026-06-13 during laptop retirement — but the cloud cutover never created
a VM equivalent, so the last reload anywhere was 2026-05-31. When WMATA's
service change took effect ~2026-06-21, only 32% of live GTFS-RT trip_ids
matched the stale `trips` table and proximity stop_events collapsed from
~110k/day to a few hundred.

Work: add `deployment/systemd/wmata-gtfs-reload.service` + `.timer`
(weekly, Sunday 08:00 UTC, zone-pinned like the retention timers), running
the reload as the `wmata` user with the venv interpreter (not `uv run` —
same fix as the other units). Deploy section must cite `docs/DEPLOY.md` §2
(cp units + daemon-reload); a `git pull` alone does not install timers.
Verify a manual first run end-to-end before enabling the timer.

Note: if the NOTES-95 rewrite lands, the reload's home moves to the
laptop (likely a step in the pull-and-derive flow) — but do not wait
for NOTES-95; this failure mode recurs at every WMATA service change.

## Dependencies

Independent. The one ops item explicitly NOT frozen during the
comparison sprint.
