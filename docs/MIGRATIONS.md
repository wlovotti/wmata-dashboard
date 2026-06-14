# Migration Safety Ritual

Schema and data migrations now run against the production Lightsail VM, which
holds the **only copy of WMATA history since 2026-05-02**. The WMATA real-time
feed has no replay window — a destructive or table-locking migration that goes
wrong is potentially unrecoverable. This document captures the standing ritual
for any data-plane schema/data migration on the VM.

CI's `check_schema_drift.py` validates migration SQL against the models, but it
runs against an *empty* schema on a fresh CI Postgres. It cannot catch "this
`ALTER` takes ACCESS EXCLUSIVE and locks a multi-GB table for 20 minutes" or
"this backfill OOMs the 2 GB instance." The four steps below fill that gap.

See also `docs/DEPLOY.md` for the normal code-deploy runbook (pull → restart →
smoke check).

---

## Step 1 — Take a backup and confirm it landed

Before touching the production database, trigger a fresh backup and verify the
output file is non-empty and readable. Do **not** rely on the weekly
`wmata-backup.timer` having run recently enough.

```bash
# On the VM — trigger an immediate pg_dump to a timestamped file
ssh wmata@52.54.130.186

BACKUP_FILE="/home/wmata/backups/wmata_dashboard_pre_migration_$(date +%Y%m%dT%H%M%S).dump"
pg_dump -Fc -d wmata_dashboard -f "$BACKUP_FILE"

# Confirm the dump landed and is plausibly sized (should be several GB)
ls -lh "$BACKUP_FILE"
pg_restore --list "$BACKUP_FILE" | tail -5   # prints the table-of-contents tail
```

The backup is only useful if you have also tested restoring it at least once
(see NOTES-48 remaining item 1 — S3 off-box backups). An untested backup is
not a safety net. Until S3 transfer is wired in, keep the dump on the VM's
block disk and note the path.

---

## Step 2 — Test against a restored copy of prod data

CI runs the migration against an *empty* schema. That is not enough. Before
running on production, replay the migration against a real data copy to surface:

- Lock duration under prod row counts (multi-GB tables can hold ACCESS
  EXCLUSIVE for minutes; even a "safe" `ADD COLUMN` takes an exclusive lock)
- OOM risk from backfill loops on the 2 GB VM
- Unexpected query behaviour specific to the current data distribution

**Preferred approach — test DB on the VM itself:**

```bash
# On the VM, create a separate test database from the latest backup
createdb wmata_test
pg_restore --no-owner -d wmata_test "$BACKUP_FILE"

# Point the migration script at the test DB
DATABASE_URL=postgresql:///wmata_test uv run python scripts/migrate_<name>.py

# Confirm outcome, check timings, drop test DB
dropdb wmata_test
```

**Alternative — local scratch DB:**

Rehearse against a throwaway copy without disturbing your dev DB:

    bin/refresh-dev-db.sh --scratch          # schema-only migrations
    bin/refresh-dev-db.sh --scratch --full   # if the migration touches the pipeline / raw-feed tables

Apply the migration to `wmata_dashboard_scratch`, then verify with
`scripts/check_schema_drift.py` and a pipeline smoke run before touching the VM.
Local dev is now PostgreSQL 16 (matching prod), so a prod snapshot restores
cleanly — the former 14↔16 restore footgun is retired.

---

## Step 3 — Wrap the migration in an explicit transaction

Every migration script that modifies schema or data must be wrapped in a single
database transaction so that a failure mid-way rolls back to the pre-migration
state rather than leaving the schema in an intermediate state.

`scripts/migrate_*.py` already use `engine.begin()` (a context manager that
commits on exit and rolls back on exception), which is the correct pattern.
Verify your migration follows this pattern:

```python
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE ..."))
    conn.execute(text("UPDATE ..."))
    # Both statements commit together, or roll back together on exception.
```

**Do not** call `conn.commit()` manually inside the `with engine.begin()` block
— `engine.begin()` owns the transaction boundary. If you use a raw psycopg2
connection instead of SQLAlchemy, wrap with `conn.autocommit = False` and an
explicit `conn.rollback()` in the except branch.

For destructive migrations (DROP TABLE, DELETE, TRUNCATE), wrap in an
explicit transaction and print the row counts or object names before
committing, so the operator can Ctrl-C if the plan looks wrong:

```python
with engine.begin() as conn:
    # Inspect what will be affected
    count = conn.execute(text("SELECT COUNT(*) FROM target_table")).scalar()
    print(f"Will delete {count} rows from target_table — Ctrl-C within 5s to abort.")
    time.sleep(5)
    conn.execute(text("DELETE FROM target_table WHERE ..."))
```

---

## Step 4 — `--dry-run` convention

Migration scripts that perform destructive or backfill operations **should**
accept a `--dry-run` flag that prints the SQL and affected row counts without
committing. This lets an operator audit the plan before committing to
production.

The `migrate_drop_phase_f.py` script demonstrates the pattern using `--yes`
as the commit gate (without it, the script prints the plan and exits). The
equivalent `--dry-run` form is:

```python
import argparse
import sys

def main() -> None:
    """Run the migration, or print a dry-run plan."""
    parser = argparse.ArgumentParser(description="Migrate <description>.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL and affected row counts without committing.",
    )
    args = parser.parse_args()

    load_dotenv()
    engine = get_engine()

    with engine.connect() as conn:
        # Inspect / count before acting
        affected = conn.execute(
            text("SELECT COUNT(*) FROM target_table WHERE condition")
        ).scalar()
        print(f"Plan: ALTER TABLE target_table ... ({affected} rows affected)")

        if args.dry_run:
            print("DRY-RUN: no changes committed.")
            sys.exit(0)

    # Real execution path — uses engine.begin() for a committed transaction
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE target_table ..."))
    print("Migration complete.")
```

**Convention summary:**

| Flag | Behaviour |
|---|---|
| *(no flag)* | Execute and commit |
| `--dry-run` | Print plan + row counts; exit without committing |

Retrofitting every existing migrate script is *not* required. The convention
applies to **new** scripts and to any existing script that is being modified
for a significant schema/data change on production.

---

## Checklist summary

Before running any migration on the production VM:

- [ ] **1. Backup taken and verified** — `pg_dump -Fc` output is non-empty,
  `pg_restore --list` prints the table of contents. Path recorded.
- [ ] **2. Tested on a restored prod-data copy** — not just the empty CI
  schema. Lock duration and OOM risk were observable. Test DB dropped
  afterward.
- [ ] **3. Transaction-wrapped** — uses `engine.begin()` or equivalent. A
  failure mid-migration rolls back, not leaves the schema half-applied.
- [ ] **4. `--dry-run` plan reviewed** (if the script supports it) — row
  counts and SQL look correct before committing.
- [ ] **5. Collector paused if needed** — `VACUUM FULL`, `ACCESS EXCLUSIVE`
  locks, or large backfills should be run with the collector stopped:
  `sudo systemctl stop wmata-collector.service` on the VM before running,
  `sudo systemctl start wmata-collector.service` after.

---

## References

- Normal code deploy: `docs/DEPLOY.md`
- Full cloud ops runbook: `docs/DEPLOYMENT.md`
- Existing migration scripts: `scripts/migrate_*.py`
- Schema-drift CI check: `scripts/check_schema_drift.py`
- VM: AWS Lightsail us-east-1, static IP `52.54.130.186`. SSH as `wmata`.
