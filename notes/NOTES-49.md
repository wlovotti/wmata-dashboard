# NOTES-49. Cloud migration phase 2 — managed Postgres + backups

**Severity: low (only when the VM-hosted DB outgrows hand-maintenance).**

Trigger: any of (a) the Phase-1 VM (closed NOTES-48, live 2026-06-05)
has been stable for ≥30 days and we want to stop hand-maintaining
Postgres, (b) DB grows past ~150 GB and a larger VM becomes more
expensive than managed, (c) site goes semi-public and a single
accidental `DROP TABLE` becomes unrecoverable. Until one of those, the
VM-hosted Postgres is fine. Revisit when multi-developer, public
launch, or an automated migration cadence makes the on-demand
`bin/refresh-dev-db.sh` restore insufficient.

Note: the NOTES-95 stateless-collector rewrite may moot this item —
its target architecture removes Postgres from the VM entirely (laptop
DB + S3 raw store). Re-evaluate after NOTES-95 lands.

Candidate providers, cheapest → most robust: Neon (serverless, cold
starts on idle), Supabase (~$25/mo), DigitalOcean Managed Postgres
(~$15/mo, PITR), AWS RDS (most flexible, most expensive at this scale).

Concrete steps: pick provider → provision + update `.env` on VM and
laptop → `pg_dump`/`pg_restore`, run in parallel one week with the
collector double-writing via a small adapter, compare row counts daily
→ cut over → decommission VM Postgres. Managed providers handle PITR;
until then the weekly `pg_dump | aws s3 cp` stands.

## Dependencies

Trigger-based; likely superseded in part by NOTES-95.
