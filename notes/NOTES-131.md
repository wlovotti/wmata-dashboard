# NOTES-131. Full local test suite reads the production database

**Severity: low (CI is the real full-suite gate and runs against its own
throwaway Postgres; this only bites a human running the suite locally).**
**Effort: medium (needs a real test-env DATABASE_URL story, not just a
warning comment).**

Surfaced during the stateless-collector cutover PR (closing NOTES-95,
NOTES-94, and NOTES-81): a bare `uv run pytest` locally reads
`DATABASE_URL` from `.env`, which
points at the laptop's system-of-record `wmata_dashboard` — the same DB
the API and pipelines read/write for real. Only `uv run pytest -m smoke`
(fast, DB-light) and `bin/test-with-pg` (spins up its own Postgres,
mirrors CI) are safe to run locally without risk of a non-smoke test
touching production data or getting confusingly non-deterministic
results depending on what's actually in the DB that day. There is
currently no automatic guard against a plain `uv run pytest` in the
laptop's default shell — the working agreement is just "don't," enforced
by convention and by controller review, not tooling.

Work: give the test suite (or `.env` loading, or a conftest fixture) a
way to refuse or redirect a non-smoke, non-`pg_session` test run when
`DATABASE_URL` points at `wmata_dashboard` — e.g. a conftest check that
fails fast with a clear message unless `PYTEST_ALLOW_PROD_DB=1` or
similar is set, or a separate `.env.test` that test runs load instead of
`.env` by default.

## Dependencies

None — self-contained test-infrastructure fix. Independent of any other
open item.
