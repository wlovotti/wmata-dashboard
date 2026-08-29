# NOTES-50. Cloud migration phase 3 — deploy API + frontend

**Severity: low (only if/when the dashboard goes semi-public).**

Trigger: someone other than the user wants to view the dashboard
without a screenshare. Until then, running the API + Vite frontend on
the laptop pointed at the local DB is fine and keeps iteration speed
high. (Audience decision 2026-08-09: personal for now — this item
stays parked.)

Seams: the **API** config seam is **done** — `api/config.py` reads
env-driven CORS (`CORS_ALLOW_ORIGINS`, dev defaults to `["*"]`), so the
API is deploy-ready. The **frontend** `VITE_API_URL` seam is **deferred
to this item** (still hardcoded for local dev). The recommended deploy
**co-locates API + DB in-region**, which resolves NOTES-88 (the
`/api/routes` N+1 only bites over a high-latency link).

Concrete steps:
1. **Database** — the laptop Postgres is the system of record; a
   public deploy needs a hosted copy co-located with the API. (This
   absorbs the retired phase-2 managed-Postgres item, closed as
   superseded 2026-08-12 once the VM-hosted-DB premise it was written
   against ended — laptop is the DB of record and the stateless-collector
   rewrite removes Postgres from the collector box entirely.) Candidate providers,
   cheapest → most robust: Neon (serverless, cold starts on idle),
   Supabase (~$25/mo), DigitalOcean Managed Postgres (~$15/mo, PITR),
   AWS RDS (most flexible, most expensive at this scale). Seed via
   `pg_dump`/`pg_restore` from the laptop; decide a refresh cadence
   (a push after `bin/pull-and-derive.sh` is the natural hook).
2. **API** (`api/main.py`, FastAPI): Fly.io / Render / Railway or a
   VM; ship as a container; wire `DATABASE_URL`.
3. **Frontend** (`frontend/`, Vite static build): Cloudflare Pages /
   Vercel / Netlify; set `VITE_API_URL`.
4. **Domain + TLS** — provider-issued.
5. **Auth** — decide before launch; HTTP basic auth is probably enough.
6. **Monitoring** — wire an uptime monitor to the health endpoint.
7. **CORS** — tighten `allow_origins` to the deployed frontend domain.

Out of scope: scaling beyond a single API instance, real auth
(SSO/OAuth), CDN configuration.

## Dependencies

Trigger-based (audience beyond personal). Fix NOTES-88 before or with
this item.
