# AWS Lightsail Operator Runbook

WMATA Dashboard — cloud migration Phase 1 (NOTES-48, closed 2026-06-13).

**Authoritative design spec:** `docs/superpowers/specs/2026-05-28-cloud-migration-phase1-design.md`
This runbook is a field-operator summary derived from the spec. The spec is the
canonical record of *why* each decision was made — read §3 before deviating
from anything here.

**Status:** live cutover completed 2026-06-05; Phase 1 closed 2026-06-13.
This document covers the deployed topology; the cutover steps that produced it
are in the spec §5 runbook.

> **✅ Topology (current, since the stateless-collector cutover — PR C):**
> a stateless nano box (`wmata-poller`) is the only running collector: two
> processes, `collector@wmata` and `collector@sfmta`, each poll their
> agency's GTFS-RT, write rotating zstd JSONL, and upload to S3 on a
> 15-minute cadence — no database anywhere on that box. §13 below is the
> live runbook for it. **The laptop's local PostgreSQL 16
> (`wmata_dashboard`) is the system of record**; freshness is
> `bin/pull-and-derive.sh`, which processes one service date at a time
> (§12.3): fetch just that date's files from the four raw prefixes under
> `s3://wmata-dashboard-backups/raw-jsonl-archive/`, load VP
> (`pipelines/load_vp_archive.py`), replay TU, derive, then delete the
> local raw files no later date needs once each is verified in S3 — no
> SSH tunnel, no VM access, no manual S3 sync step required.
> `s3://wmata-dashboard-backups/raw-jsonl-archive/` is the permanent raw
> store, written directly by the collector rather than synced up after
> the fact; the laptop holds only a couple of days of raw files at a time.
> Before deriving, the script reloads each agency's static GTFS if stale
> (`scripts/run_gtfs_reload.py --max-age-days 7`), and every derived date
> passes `run_daily_batch.py`'s per-date trip_id match-rate guard, which
> catches a service change landing while the age gate still calls the
> snapshot "fresh" — see §12.1 for the gate/guard detail and the recovery
> procedure.
>
> §1–§11 below document the outgoing `wmata-data` Lightsail VM (Phase 1)
> that the nano box replaces. That VM is scheduled for decommission (O3,
> tracked alongside PR C) but isn't gone yet as of this writing — its
> systemd units keep running unattended until the decommission actually
> happens, so those sections stay as an accurate historical/operational
> reference for as long as the box exists. Once O3 completes, §1–§11 (and
> the `deployment/systemd/wmata-*` unit files they reference, several of
> which this PR already removed from the repo since they have no future
> redeploy) can be deleted outright. §12 is laptop-side `launchd` jobs —
> orthogonal to the VM and to O3 — and is updated in place below to
> reflect this PR's retirement of the `gtfs-reload` job.

---

## 1. Target topology (summary)

| Decision | Choice |
|---|---|
| Provider | AWS Lightsail |
| Plan | $12/mo — 2 GB RAM, 60 GB SSD (OS only), 3 TB bundled transfer |
| Region | us-east-1 (N. Virginia) — lowest latency to WMATA API |
| OS | Ubuntu LTS (latest LTS at provision time) |
| PostgreSQL | **16** (prod + CI; local dev runs 14 — a 14→16 `pg_restore` is routine, 16→14 is not supported) |
| PGDATA | Attached Lightsail block-storage disk, starts ~50 GB, ~$5/mo |
| Object storage | AWS S3 private bucket (weekly `pg_dump` + parquet archives) |
| Firewall | SSH (22) only — ideally restricted to your IP; Postgres never publicly reachable |
| DB access from laptop | Dev uses a local PostgreSQL 16 copy via `bin/refresh-dev-db.sh` (see "Local development data" below). Ad-hoc prod `psql` + `--from-vm` refresh against the still-running old VM use a manual SSH tunnel (local **5433** → VM 5432) — the `bin/db-tunnel.sh` wrapper was retired with the tunnel-based ingest path |

See spec §3 for the full rationale. See spec §3.5 for the three-tier retention
model that bounds the DB to a ~105 GB plateau.

---

## 2. Provisioning (Phase 0 — do this now, in parallel with Phase F)

For step-by-step console navigation, defer to official AWS Lightsail docs:
<https://docs.aws.amazon.com/lightsail/>

**High-level checklist:**

1. Create the Lightsail instance ($12 plan, us-east-1, Ubuntu LTS).
2. Attach a ~50 GB block-storage disk to the instance.
3. Create a private S3 bucket for backups and parquet archives.
4. Restrict the Lightsail firewall to allow SSH (22) only. Optionally restrict
   to your home IP.

### 2.1 SSH key setup

```bash
# Generate a dedicated key if you don't have one
ssh-keygen -t ed25519 -C "wmata-lightsail"

# Upload the public key during Lightsail instance creation (or via the
# Lightsail console → Account → SSH keys).
```

### 2.2 Initial server hardening

```bash
# SSH in as the default Lightsail user (always 'ubuntu')
ssh -i ~/.ssh/id_ed25519 ubuntu@<INSTANCE_IP>

# Create application user (no sudo, no interactive SSH — service account only)
adduser wmata
rsync --archive --chown=wmata:wmata ~/.ssh /home/wmata

# Disable password login
sudo sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo systemctl reload sshd

# Verify firewall allows only SSH (Lightsail's built-in firewall handles ingress;
# optionally add ufw as a second layer for defense-in-depth)
```

**Access model:** humans always SSH as `ubuntu` (the Lightsail sudo account) and
assume the service identity via `sudo -u wmata <cmd>` for service-owned
operations. `wmata` has no interactive SSH login (no `authorized_keys`), no
usable password (locked with `passwd -l wmata`), and no sudo rights. The systemd
units run as `User=wmata`, which is unaffected by these restrictions — systemd
activates the unit directly without going through SSH or PAM login. This leaves a
full audit trail in the sudo log for any human-initiated service operations.

If you are migrating an existing VM that was provisioned with `wmata` in the
`sudo` group and an SSH key, harden it like this:

```bash
# Disable wmata's interactive SSH (rename, don't delete — easier to re-enable if needed)
sudo mv /home/wmata/.ssh/authorized_keys /home/wmata/.ssh/authorized_keys.disabled

# Lock the wmata password (passwd -S wmata should show 'L' after this)
sudo passwd -l wmata

# Remove wmata from the sudo group
sudo deluser wmata sudo

# Verify: 'L' in the second field, no 'sudo' in groups
sudo passwd -S wmata
groups wmata
```

### 2.3 System packages

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl build-essential
```

### 2.4 Install uv (as wmata user)

```bash
su - wmata
curl -LsSf https://astral.sh/uv/install.sh | sh
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
uv --version
exit
```

### 2.5 PostgreSQL 16 — install and move PGDATA to attached disk

```bash
# Install PostgreSQL 16 (matches the production VM + CI; PG14 is EOL Nov 2026)
sudo apt install -y postgresql-16 postgresql-client-16

# Stop Postgres before moving PGDATA
sudo systemctl stop postgresql

# Mount the attached block disk (adjust /dev/xvdf to the device shown in
# 'lsblk' — Lightsail typically presents the first attached disk as /dev/xvdf
# or /dev/nvme1n1 depending on instance type)
sudo mkfs.ext4 /dev/xvdf
sudo mkdir -p /mnt/pgdata
sudo mount /dev/xvdf /mnt/pgdata

# Add to /etc/fstab for persistence across reboots
echo '/dev/xvdf /mnt/pgdata ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab

# Move PGDATA
sudo mv /var/lib/postgresql/16/main /mnt/pgdata/
sudo ln -s /mnt/pgdata/main /var/lib/postgresql/16/main
sudo chown -h postgres:postgres /var/lib/postgresql/16/main

# Update postgresql.conf if data_directory is set explicitly
sudo -u postgres psql -c "SHOW data_directory;"   # verify

sudo systemctl start postgresql
sudo systemctl status postgresql
```

### 2.6 Configure pg_hba.conf for tunnel-only access

Postgres should only accept connections from localhost (the SSH tunnel delivers
all client connections through localhost). Edit
`/etc/postgresql/16/main/pg_hba.conf` and ensure only `127.0.0.1/32` (IPv4)
and `::1/128` (IPv6) local entries exist — remove any `0.0.0.0/0` or public
entries. Reload after any change:

```bash
sudo -u postgres psql -c "SELECT pg_reload_conf();"
```

### 2.7 Create database and application user

```bash
sudo -u postgres psql <<'SQL'
CREATE DATABASE wmata_dashboard;
CREATE USER wmata WITH ENCRYPTED PASSWORD 'choose_a_strong_password';
GRANT ALL PRIVILEGES ON DATABASE wmata_dashboard TO wmata;
SQL
```

---

## 3. Data transfer (Phase 1 — after NOTES-72 Phase F drops)

Wait for the Phase F drops (`trip_update_snapshots`, `stop_events_v2`) to run
on the laptop. See `scripts/migrate_drop_phase_f.py` for the Phase F shrink
runbook. After the drops the transfer is ~28 GB instead of ~95 GB.

Verify Phase F is done:

```bash
# On laptop — these should return "does not exist" after Phase F:
psql -d wmata_dashboard -c "\dt trip_update_snapshots"
psql -d wmata_dashboard -c "\dt stop_events_v2"
```

---

## 4. Cutover (Phase 2 — short downtime, minutes)

Follows spec §5 Phase 2 closely:

```bash
# 1. Stop the laptop collector gracefully (SIGINT — PR #129 handler)
kill -INT $(cat logs/collector.pid)

# 2. Stream dump directly to VM (no intermediate file)
pg_dump -Fc -d wmata_dashboard | ssh ubuntu@<INSTANCE_IP> \
  'sudo -u wmata pg_restore -U wmata -d wmata_dashboard -Fc'
# Expect ~1–3 hours at typical consumer upload speeds for ~28 GB.

# 3. On the VM — verify row counts match laptop:
#    (run same queries on both sides and compare)
ssh ubuntu@<INSTANCE_IP> 'sudo -u wmata psql -U wmata -d wmata_dashboard \
  -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 20;"'

# 4. Move WMATA API key to VM .env
ssh ubuntu@<INSTANCE_IP> 'sudo -u wmata nano /home/wmata/wmata-dashboard/.env'
# Set: WMATA_API_KEY=<key>
#      DATABASE_URL=postgresql://wmata:<password>@localhost/wmata_dashboard

# 5. Start collector on VM under systemd
ssh ubuntu@<INSTANCE_IP> 'sudo systemctl enable --now wmata-collector.service'
ssh ubuntu@<INSTANCE_IP> 'journalctl -u wmata-collector -f'
# Verify vehicle_positions row count is climbing.

# 6. From laptop — open the SSH tunnel and point local API at VM
#    Use local port 5433, NOT 5432: the laptop's dev Postgres@14 keeps 5432
#    bound (CLAUDE.md keeps local on 14 for fast iteration), so forwarding
#    5432 would collide. (Historical note: `bin/db-tunnel.sh` used to wrap
#    this tunnel + re-add the SSH key from the keychain after a reboot; it
#    was retired along with the tunnel-based ingest path by the
#    stateless-collector cutover. Open the tunnel manually if you ever
#    need one against the still-running old VM before its O3 decommission:)
ssh -N -L 5433:localhost:5432 ubuntu@<INSTANCE_IP> &
# In .env on laptop:
#   DATABASE_URL=postgresql://wmata:<password>@localhost:5433/wmata_dashboard
# Then restart the local API:
uv run uvicorn api.main:app --reload
```

### Local development data

Dev runs against a local PostgreSQL 16 copy of the prod dataset, refreshed on
demand — never against the live VM DB.

    bin/refresh-dev-db.sh              # slim (~17 GiB): everything the API reads, no raw-feed tables
    bin/refresh-dev-db.sh --full       # add raw-feed tables so the pipeline can run (~31 GiB)
    bin/refresh-dev-db.sh --prune-gtfs # also drop stale GTFS history (~9 GiB)

It pulls the latest weekly dump from
`s3://wmata-dashboard-backups/wmata-db-backups/` (needs local AWS creds with
`s3:GetObject`+`ListBucket` on that prefix). An SSH tunnel to the
still-running old VM is ops-only — ad-hoc prod `psql` and
`bin/refresh-dev-db.sh --from-vm` — never the dev DB connection; open one
manually (`ssh -N -L 5433:localhost:5432 ubuntu@<INSTANCE_IP> &`), since the
`bin/db-tunnel.sh` wrapper was retired with the tunnel-based ingest path.

---

## 5. Harden (Phase 3)

### 5.1 Install all systemd units

```bash
# On VM, as root:
REPO=/home/wmata/wmata-dashboard
cp ${REPO}/deployment/systemd/wmata-collector.service     /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-metrics.service       /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-metrics.timer         /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-backup.service        /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-backup.timer          /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-archive-positions.service  /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-archive-positions.timer    /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-window-derived.service     /etc/systemd/system/
cp ${REPO}/deployment/systemd/wmata-window-derived.timer       /etc/systemd/system/

mkdir -p /var/log/wmata
chown wmata:wmata /var/log/wmata

systemctl daemon-reload

# Enable and start:
systemctl enable --now wmata-collector.service
systemctl enable --now wmata-metrics.timer
systemctl enable --now wmata-backup.timer
systemctl enable --now wmata-archive-positions.timer
systemctl enable --now wmata-window-derived.timer

# Verify timers:
systemctl list-timers --all | grep wmata
```

**Scheduled jobs and their times (Eastern — set TZ=America/New_York on the
server, or note that OnCalendar= runs in the server's local timezone):**

| Timer | Schedule | Job |
|---|---|---|
| wmata-metrics | daily 02:00 | `run_daily_batch.py` |
| wmata-archive-positions | daily 04:00 | tier-3 VP → S3 parquet (30-day window) |
| wmata-window-derived | daily 04:30 | tier-2 stop_events/runs 365-day window |
| wmata-backup | Sun 01:00 | weekly `pg_dump -Fc | xz` → local + optional S3 |

### 5.2 S3 off-box backups (AWS S3)

The weekly `wmata-backup` job (`deployment/scripts/backup_db.sh`) uploads each
`pg_dump` to S3 when `S3_BACKUP_BUCKET` is set, and skips the upload silently
otherwise (so the script stays locally runnable without AWS). **Validated
end-to-end against the live bucket on 2026-06-09** (2.0 GiB dump uploaded).

**Credentials: Lightsail has no instance roles — you must use an IAM *user*
access key.** Unlike EC2, a Lightsail *compute instance* cannot have an IAM role
or instance profile attached — the Lightsail IAM docs state plainly that
"Lightsail does not support service roles"
(<https://docs.aws.amazon.com/lightsail/latest/userguide/security_iam_service-with-iam.html>).
So the VM must carry a long-lived IAM user key. Create a dedicated,
least-privilege user rather than reusing an admin key, and keep admin
operations on a trusted workstation (the VM never holds account-wide creds).

1. **Create the bucket** in the VM's region (`us-east-1`, to avoid cross-region
   transfer cost). Block public access and enable versioning:
   ```bash
   aws s3api create-bucket --bucket <bucket> --region us-east-1   # us-east-1 takes NO LocationConstraint
   aws s3api put-public-access-block --bucket <bucket> \
     --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
   aws s3api put-bucket-versioning --bucket <bucket> \
     --versioning-configuration Status=Enabled
   ```
2. **Create the least-privilege IAM user** using the policy in
   `deployment/aws/s3-backup-policy.json` (it targets the deployed
   `wmata-dashboard-backups` bucket — edit the ARNs if you use a different
   name). It grants only `s3:ListBucket` on the bucket and
   `s3:PutObject`/`s3:GetObject` on the `wmata-db-backups/` and
   `wmata-vp-archive/` prefixes — deliberately **no `s3:DeleteObject`**, so a
   compromised VM cannot wipe its own backups; expiry is the lifecycle rule's
   job (step 4). `GetObject` is included because the tier-3 archive job
   (`archive_vehicle_positions.py`) does a `HeadObject` read-back to verify an
   upload before it deletes rows from Postgres.
   ```bash
   aws iam create-user --user-name wmata-vm-backup
   aws iam put-user-policy --user-name wmata-vm-backup \
     --policy-name wmata-s3-backup \
     --policy-document file://deployment/aws/s3-backup-policy.json
   aws iam create-access-key --user-name wmata-vm-backup   # copy the SecretAccessKey — shown ONCE
   ```
3. **On the VM:** install AWS CLI **v2** (the official bundle — *not*
   `apt install awscli`, which is the older v1 line), then put the key + bucket
   into the systemd `EnvironmentFile` (`/home/wmata/wmata-dashboard/.env`). Use
   the `EnvironmentFile`, not `~/.aws/credentials`: the `wmata-backup` unit runs
   with `ProtectHome=read-only`, and env vars are honored by both the CLI and
   boto3 via the default credential chain regardless.
   ```bash
   sudo apt install -y unzip
   curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
   cd /tmp && unzip -q awscliv2.zip && sudo ./aws/install

   # Append creds (as the wmata user; the quoted heredoc protects $/backticks in the secret):
   tee -a /home/wmata/wmata-dashboard/.env >/dev/null <<'EOF'
   S3_BACKUP_BUCKET=<bucket>
   AWS_DEFAULT_REGION=us-east-1
   AWS_ACCESS_KEY_ID=<AKIA...>
   AWS_SECRET_ACCESS_KEY=<secret>
   EOF
   chmod 600 /home/wmata/wmata-dashboard/.env

   # Smoke-test (empty output + exit 0 = success):
   set -a; . /home/wmata/wmata-dashboard/.env; set +a
   aws s3 ls s3://$S3_BACKUP_BUCKET/
   ```
4. **Lifecycle rule** — bound storage growth with
   `deployment/aws/s3-lifecycle.json` (current dumps expire after 90 days;
   noncurrent versions after 30 — the latter is **required** on a versioned
   bucket or superseded versions accumulate forever and nothing is reclaimed):
   ```bash
   aws s3api put-bucket-lifecycle-configuration \
     --bucket <bucket> --lifecycle-configuration file://deployment/aws/s3-lifecycle.json
   ```
5. **Test the full path:** `sudo systemctl start --no-block wmata-backup.service`,
   then watch `ls -lh /home/wmata/backups/*.dump.xz` grow (the script logs
   nothing during the dump — monitor the artifact, not the log), and finally
   confirm the object landed: `aws s3 ls s3://<bucket>/wmata-db-backups/`.

### 5.3 Reclaim PR #152 pruned columns (post-cutover maintenance)

The `pg_dump | pg_restore` transfer carries five dead `vehicle_positions`
columns (`vehicle_label`, `bearing`, `trip_start_time`, `schedule_relationship`,
`occupancy_status`) because `pg_dump` is faithful. After ≥7 clean days on the
VM, in a quiet window (pause the collector first with SIGINT):

```bash
# On VM:
psql -U wmata -d wmata_dashboard <<'SQL'
ALTER TABLE vehicle_positions
  DROP COLUMN IF EXISTS vehicle_label,
  DROP COLUMN IF EXISTS bearing,
  DROP COLUMN IF EXISTS trip_start_time,
  DROP COLUMN IF EXISTS schedule_relationship,
  DROP COLUMN IF EXISTS occupancy_status;
SQL

# Then reclaim space (ACCESS EXCLUSIVE — collector must be stopped):
psql -U wmata -d wmata_dashboard -c \
  "VACUUM (FULL, ANALYZE) vehicle_positions;"

# Restart collector:
sudo systemctl start wmata-collector.service
```

### 5.4 Disk resize procedure (when PGDATA approaches capacity)

Lightsail block disks **cannot be resized in place.** To grow the PGDATA disk:

1. Stop Postgres: `sudo systemctl stop postgresql`
2. Snapshot the current block disk via the Lightsail console.
3. Create a new, larger disk from the snapshot.
4. Detach the old disk; attach the new disk to the same mount point.
5. Start Postgres: `sudo systemctl start postgresql`

Expect one or two such operations during the first year as the DB climbs
toward the ~105 GB plateau. See
<https://docs.aws.amazon.com/lightsail/latest/userguide/> for console steps.

### 5.5 Automatic disk snapshots

Daily automatic snapshots of the PGDATA block disk are a same-region,
fast-restore complement to the off-box S3 dumps (§5.2) — different failure
domain, so run both. **For block disks the auto-snapshot add-on is CLI/API-only;
the Lightsail console cannot enable it for disks** (it can for instances).
Lightsail retains the latest **7** daily auto-snapshots and auto-expires the
rest; `snapshotTimeOfDay` is **UTC and hour-granular** (`HH:00`). **Enabled
2026-06-09 at 08:00 UTC** (~04:00 ET — after the 02:00 ET metrics batch's heavy
writes, so the snapshot captures a consistent post-batch state).

```bash
# Find the PGDATA disk name:
aws lightsail get-disks --region us-east-1 \
  --query 'disks[].{name:name,gb:sizeInGb,attachedTo:attachedTo}' --output table

# Enable daily auto-snapshots at 08:00 UTC:
aws lightsail enable-add-on --region us-east-1 \
  --resource-name wmata-pgdata \
  --add-on-request 'addOnType=AutoSnapshot,autoSnapshotAddOnRequest={snapshotTimeOfDay=08:00}'

# Verify (status settles Enabling -> Enabled within ~1 min):
aws lightsail get-disk --region us-east-1 --disk-name wmata-pgdata --query 'disk.addOns'

# List snapshots once the first has fired:
aws lightsail get-auto-snapshots --region us-east-1 --resource-name wmata-pgdata
```

To restore, create a new disk from an auto-snapshot
(`aws lightsail create-disk-from-snapshot --use-latest-restorable-auto-snapshot`
or `--restore-date`), then follow §5.4 to swap it in.

### 5.6 Enable retention timers (tier-3 VP archival + tier-2 derived-table window)

> **Historical (VM era, superseded).** The VM and these timers are gone;
> `window_derived_tables.py` was removed and `archive_vehicle_positions.py`
> refuses to run. Laptop retention is §12.4.

Two nightly housekeeping jobs keep the DB from growing without bound. They were
authored on 2026-06-03 but held until (a) S3 was in place (item 1, done
2026-06-09) and (b) explicit user sign-off was given, because both jobs perform
destructive `DELETE`s. **Sign-off given 2026-06-10.** The deployment checklist
below was run on that date.

**What each job does:**

| Unit | Schedule | Action |
|---|---|---|
| `wmata-archive-positions` | daily 04:00 ET | Stages `vehicle_positions` rows older than 30 days to local zstd parquet, uploads to `s3://<bucket>/wmata-vp-archive/<date>.parquet`, verifies the upload, then DELETEs from Postgres. After all deletions, runs a regular (non-FULL) VACUUM. |
| `wmata-window-derived` | daily 04:30 ET | DELETEs `stop_events` + `runs` rows whose `service_date` is older than 365 days. |

**First-run expectations:**

- **Tier-3 (`wmata-archive-positions`):** the window is 30 days back from the
  *run date*, so the first-run backlog is everything from the start of
  collection (2026-05-02) up to that cutoff — it grows by a day for every day
  enablement waits. Verify the dry-run lists exactly that date range before
  enabling. **Actual first run (2026-06-10):** 10,030,782 rows across 16 UTC
  dates — the expected 2026-05-03 → 2026-05-12 backlog *plus six phantom dates
  from October 2025* (~2.26M rows with bogus vehicle-reported timestamps
  predating collection; tracked in a since-closed punch-list item). Run time ~1¾ h,
  dominated by the post-DELETE VACUUM; steady-state nightly runs handle one
  ~1M-row day and are far shorter. A day already partially past the cutoff is
  archived **whole** (the script expires whole UTC days), so rows can leave
  Postgres up to ~24 h before their 30-day birthday — harmless, and they're in
  S3 first.

- **Tier-2 (`wmata-window-derived`):** the 365-day window runs from the run date.
  Since data starts 2026-05-02 and a full year hasn't elapsed, runs are
  expected to delete **zero rows** until 2027. The script will print `DRY-RUN:
  would delete {'stop_events': 0, 'runs': 0}` (confirmed on the 2026-06-10
  first run).

**Deployment checklist:**

1. **Set `S3_ARCHIVE_BUCKET` in the EnvironmentFile.** The archive pipeline uses
   `S3_ARCHIVE_BUCKET` (distinct from `S3_BACKUP_BUCKET`); both can point at the
   same `wmata-dashboard-backups` bucket, as the IAM user `wmata-vm-backup`
   already grants the `wmata-vp-archive/` prefix. On the VM (as `ubuntu`):

   ```bash
   # Append to the wmata EnvironmentFile (the quoted heredoc protects $ in secrets):
   sudo -u wmata tee -a /home/wmata/wmata-dashboard/.env >/dev/null <<'EOF'
   S3_ARCHIVE_BUCKET=wmata-dashboard-backups
   EOF
   chmod 600 /home/wmata/wmata-dashboard/.env
   ```

2. **Install unit files per `docs/DEPLOY.md` §2** (copy to `/etc/systemd/system/`
   + `daemon-reload` + restart — `git pull` on the VM does **not** update installed
   units). In particular copy the four retention units:

   ```bash
   REPO=/home/wmata/wmata-dashboard
   sudo cp ${REPO}/deployment/systemd/wmata-archive-positions.service /etc/systemd/system/
   sudo cp ${REPO}/deployment/systemd/wmata-archive-positions.timer    /etc/systemd/system/
   sudo cp ${REPO}/deployment/systemd/wmata-window-derived.service     /etc/systemd/system/
   sudo cp ${REPO}/deployment/systemd/wmata-window-derived.timer       /etc/systemd/system/
   sudo systemctl daemon-reload
   ```

3. **Dry-run both scripts first** (as the `wmata` user via `sudo -u wmata`):

   ```bash
   # Tier-3: should list every vehicle_positions day older than 30 days
   sudo -u wmata sh -c 'cd /home/wmata/wmata-dashboard && \
     .venv/bin/python3 pipelines/archive_vehicle_positions.py --dry-run --retention-days 30'

   # Tier-2: should show zero rows (data started 2026-05-02, 365-day window not yet reached)
   sudo -u wmata sh -c 'cd /home/wmata/wmata-dashboard && \
     .venv/bin/python3 pipelines/window_derived_tables.py --dry-run --retention-days 365'
   ```

   **Expected tier-3 dry-run output:** lines like
   `DRY-RUN 2026-05-02: would archive NNN rows → s3://wmata-dashboard-backups/wmata-vp-archive/2026-05-02.parquet, then DELETE from vehicle_positions`
   for each expired UTC date (everything from the oldest data to the 30-day
   cutoff — sanity-check the range, and treat dates predating collection as a
   signal of bogus vehicle timestamps, not a script bug).
   If the bucket name is wrong or `S3_ARCHIVE_BUCKET` is unset, the dry-run still
   runs (it prints the plan without contacting S3). **Caveat:** the dry-run never
   touches S3, so it cannot catch credential/prefix problems — the IAM user only
   grants the `wmata-vp-archive/` prefix, which is why `KEY_PREFIX` in
   `pipelines/archive_vehicle_positions.py` must match it (PR #166).

   **Expected tier-2 dry-run output:** `DRY-RUN: would delete {'stop_events': 0, 'runs': 0}`

4. **Enable both timers:**

   ```bash
   sudo systemctl enable --now wmata-archive-positions.timer
   sudo systemctl enable --now wmata-window-derived.timer
   ```

5. **Verify:**

   ```bash
   systemctl list-timers --all | grep wmata
   ```

   Expected: `wmata-archive-positions.timer` and `wmata-window-derived.timer`
   listed with next-trigger times at 04:00 ET and 04:30 ET respectively.

---

## 6. Backup / restore drill

**Create a backup manually** (run as `ubuntu` on the VM):
```bash
sudo -u wmata /home/wmata/wmata-dashboard/deployment/scripts/backup_db.sh
```

**Restore from a local `.sql.xz` backup:**
```bash
xz -d < wmata_db_YYYYMMDD_HHMMSS.sql.xz | \
  pg_restore -U wmata -d wmata_dashboard --no-owner
```

**Restore from a `pg_dump -Fc` (custom-format) backup:**
```bash
pg_restore -U wmata -d wmata_dashboard -Fc wmata_db_YYYYMMDD.dump
```

Run this drill at least once on a scratch DB before you need it.

---

## 7. Verification checklist (post-cutover)

- [ ] `SELECT COUNT(*) FROM vehicle_positions` matches laptop within minutes
      of collection start
- [ ] `journalctl -u wmata-collector -f` shows rows being written every
      30–60 s
- [ ] Local API (`uvicorn`) serves correctly through the SSH tunnel
- [ ] One full nightly batch (`run_daily_batch.py`) completes on the VM
- [ ] `uv run python scripts/collector_status.py` shows healthy coverage

---

## 8. Rollback

The laptop DB stays intact and authoritative until the VM is proven. See spec
§7. To revert: SIGINT the VM collector; restart the laptop collector:

```bash
nohup env PYTHONUNBUFFERED=1 \
  uv run python scripts/continuous_combined_collector.py \
  >> logs/collector.log 2>&1 &
```

The local laptop DB is kept read-only as a fallback for ≥7 days post-cutover.

---

## 9. Day-to-day operations

All commands below are run as `ubuntu` on the VM (`ssh ubuntu@52.54.130.186`).
Use `sudo -u wmata <cmd>` whenever operating on wmata-owned files or processes.
See `docs/DEPLOY.md` for the full deploy runbook.

**Check all service statuses:**
```bash
sudo systemctl status wmata-collector
sudo systemctl list-timers --all | grep wmata
sudo journalctl -u wmata-collector --since "1 hour ago"
```

**Restart collector (e.g., after a code update):**
```bash
sudo -u wmata git -C /home/wmata/wmata-dashboard pull origin main
sudo -u wmata sh -c 'cd /home/wmata/wmata-dashboard && uv sync --extra postgres'
sudo systemctl restart wmata-collector
```

**After any `.env` change (credentials especially): restart BOTH collector
units** — `wmata-collector` AND `sfmta-collector`. They read the same
`EnvironmentFile` at process start and cache it for the life of the process,
so a unit that isn't restarted keeps the old values indefinitely. This bit
for real on 2026-08-08: a DB password change restarted only
`wmata-collector`, and the SFMTA sidecar spent ~27 h failing every upsert
with `password authentication failed` while its feed fetches and JSONL
archive kept running (no trip-update data lost — replayable from archive —
but SFMTA vehicle positions for the gap were not archived and are gone).
The dead-man ping (§11.4) is what surfaced it.

```bash
sudo systemctl restart wmata-collector sfmta-collector
```

**Manually trigger a job:**
```bash
sudo systemctl start wmata-metrics.service       # nightly batch
sudo systemctl start wmata-backup.service        # manual backup
sudo systemctl start wmata-archive-positions.service
sudo systemctl start wmata-window-derived.service
```

**Check disk usage:**
```bash
df -h /mnt/pgdata          # block disk (PGDATA)
df -h /                    # bundled SSD (OS)
du -sh /mnt/pgdata/main    # Postgres data dir
```

---

## 10. References

- **Authoritative spec:** `docs/superpowers/specs/2026-05-28-cloud-migration-phase1-design.md`
- **Phase F shrink runbook:** `scripts/migrate_drop_phase_f.py`
- **AWS Lightsail docs:** <https://docs.aws.amazon.com/lightsail/>
- **Systemd units:** `deployment/systemd/`
- **Backup script:** `deployment/scripts/backup_db.sh`
- **Issue #255** (deploy API + frontend + hosted DB; Phase 1 closed 2026-06-13; the phase-2 managed-Postgres item was closed as superseded 2026-08-12 and folded into #255)

---

## 11. SFMTA (Muni) sidecar collector

A second, independent collector for SFMTA (Muni) GTFS-RT via 511.org — a
comparison-agency sidecar, not part of the WMATA data plane. It runs as its
own systemd unit against its **own database** (`sfmta_dashboard`), on the
same VM, sharing nothing with the WMATA collector except the box itself and
(optionally) the same `.env` file for its own separately-namespaced vars. See
`docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md` for the design
rationale and `scripts/sfmta_collector.py` for the entrypoint.

### 11.1 Create the database

```bash
sudo -u postgres createdb -O wmata sfmta_dashboard
```

Uses the existing `wmata` role — no new database user needed.

### 11.2 Add environment variables

Add these three to the VM's `/home/wmata/wmata-dashboard/.env` (see
`.env.example` for the annotated template):

```bash
SFMTA_API_KEY=<your 511.org token>
SFMTA_DATABASE_URL=postgresql://wmata:<password>@localhost:5432/sfmta_dashboard
SFMTA_COLLECTOR_HEALTHCHECK_URL=<healthchecks.io ping URL, once created — see 11.4>
```

**Never point `SFMTA_DATABASE_URL` at `wmata_dashboard`** — the two agencies'
data must stay in separate databases.

### 11.3 Install and start the unit

Follow **`docs/DEPLOY.md` §2** ("Deploy with systemd unit changes") for the
copy-to-`/etc/systemd/system/` + `daemon-reload` + enable/start sequence —
add `deployment/systemd/sfmta-collector.service` to the list of units copied.
(This document's own §2 is VM provisioning, not the unit-install step —
`docs/DEPLOY.md` §2 is the canonical reference for that, per NOTES-77.)

```bash
sudo systemctl enable --now sfmta-collector.service
```

### 11.4 Verify

```bash
# Tail recent logs for startup errors:
journalctl -u sfmta-collector -n 20

# Confirm heartbeats are landing in the sidecar DB:
sudo -u wmata psql -d sfmta_dashboard -c "SELECT max(ts) FROM collector_heartbeats"
```

Expect a `max(ts)` within the last ~2–3 minutes of "now" (heartbeats are
written only on trip-update ticks every 120 s, and the timestamp reflects
the feed-header snapshot, which can lag further).

### 11.5 Operator steps (manual, not part of any script)

- **Create the healthchecks.io check** (a separate check from WMATA's
  dead-man ping — one per agency) and set its ping URL as
  `SFMTA_COLLECTOR_HEALTHCHECK_URL` in the VM `.env`, then restart the unit
  to pick it up.
- **Send the 511.org rate-increase request** — the default token cap is
  60 req/rolling-hour; email
  `511sfbaydeveloperresources@googlegroups.com` to request a higher limit if
  the collector's budget gets tight. **Do not include the API key itself in
  that email.**

---

## 12. Laptop-side scheduled jobs (Path 2a)

Since Path 2a (2026-07-18, §0 banner above) the laptop's local
PostgreSQL 16 is the system of record, so jobs that would naturally be
VM systemd units are laptop-side. As of 2026-11-24 none is scheduled:
freshness is on demand via `bin/pull-and-derive.sh`, and the one
remaining `launchd` job below is kept but not deployed (#260). Plists live in `deployment/launchd/`
(consolidated there by issue #246, alongside `deployment/systemd/` for
the nano collector — `scripts/launchd/` is retired, see its `README.md`
for what moved where). This section is the pointer so the existence of
these jobs is discoverable from the topology doc, not just the
`deployment/` directory.

**Status column is load-bearing — check it, don't assume.** A plist
sitting in this repo, or even in `~/Library/LaunchAgents/`, proves
nothing by itself; only `launchctl bootstrap` (or the older
`launchctl load -w`) actually activates it. Verify live/actual state
with:

```sh
launchctl list | grep wmata
```

| Job | Schedule (when loaded) | Purpose |
|---|---|---|
| `com.wmata-dashboard.gtfs-reload` | ~~weekly, Sun 04:00 local~~ | **retired in the repo, but was still installed AND loaded on the laptop as of 2026-11** (stateless-collector cutover) — the weekly GTFS reload is now step 1 of `bin/pull-and-derive.sh` (`scripts/run_gtfs_reload.py --max-age-days 7`), gated on snapshot age rather than run on its own schedule. The stale job reloaded WMATA on 2026-09-13 ahead of the 9/12 derive (issue #258) and failed every run from 2026-10-25 (`psycopg2` missing — see the issue #258 PR). Unload it: `launchctl bootout gui/$UID/com.wmata-dashboard.gtfs-reload && rm ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist`. |
| `com.wmata-dashboard.daily-batch` | ~~daily, 03:00 local~~ | **retired** (issue #246) — was installed-but-never-loaded; `pipelines/run_daily_batch.py` is now one step inside `bin/pull-and-derive.sh`. |
| `com.wmata-dashboard.retain-trip-update-state` | ~~daily, 04:30 local~~ | **retired** (issue #246) — was installed-but-never-loaded; its `trip_update_state`-pruning job is superseded now `bin/pull-and-derive.sh` runs `cleanup_trip_update_state.py` (both agencies) on every run — see `scripts/launchd/README.md` for the detail. |
| `com.wmata-dashboard.pull-and-derive` | daily, 06:30 local | **not deployed; not the intended mode** (issue #260 closed as not planned, 2026-11-24 — freshness is on demand: run `bin/pull-and-derive.sh` by hand). Kept in the repo; installing it per "Install" below would run `bin/pull-and-derive-nightly.sh`, a thin wrapper around `bin/pull-and-derive.sh` (§0 banner), with a healthchecks.io dead-man ping on success/failure. |

Check GTFS freshness at any time:
`curl -s localhost:8000/api/gtfs/freshness` (requires the API running
locally) or `psql -d wmata_dashboard -Atc "SELECT max(created_at) FROM gtfs_snapshots;"`.

### 12.0 `com.wmata-dashboard.pull-and-derive` — install / verify / uninstall

> **Status (2026-11-24): not deployed, not the intended mode.** Issue
> #260 was closed as not planned; freshness is on demand via
> `bin/pull-and-derive.sh`. The wrapper and plist stay in the repo and
> the notes below remain accurate if it is ever installed.

**Why a nightly job at all, and why not before now (issue #246):**
before the replay manifest (PR #245), `bin/pull-and-derive.sh`'s replay
leg re-folded the whole `LOOKBACK_DAYS` window from scratch every run —
a multi-hour DB writer that would have collided with interactive dev use
if it ran unattended every night. The manifest makes a run cost only the
archive files not yet folded, so a nightly unattended run is now cheap
(steady-state ~13.5 min, verified 2026-09-13 — see
`project_notes145_replay_manifest.md`).

**API cache staleness (issue #246):** the API is run manually
(`uv run uvicorn api.main:app --reload`), so there's no process to
restart when fresh data lands mid-TTL. Instead, `api/aggregations.py`'s
live-metrics caches (`_live_metrics_cache` / `_window_metrics_cache`)
key each entry on `_current_data_version(db)` — a
`MAX(system_metrics_daily.computed_at)` read once per request — on top
of their existing TTL, so a cache entry from before the nightly run is
treated as a miss the moment new data lands, no restart needed. This
was chosen over a shorter TTL (still stale for up to the shorter
window, and a much-more-frequent recompute cost the rest of the day)
or a restart hook (nothing to hook, since the API isn't a managed
process).

**Fire time is 06:30 ET, not right after WMATA's day-roll (review
finding, PR #259):** the job must fire after BOTH agencies' owl service
has ended in their own timezone, plus S3 upload lag — `run_daily_batch.py`
only re-derives a service_date with zero `runs` rows, so firing too
early derives a date while trips are still in flight and permanently
loses them (a later run sees non-zero `runs` and never revisits it), and
the trip_update_state cleanup step then prunes their still-relevant
state. SFMTA's owl service runs to ~03:00 Pacific = 06:00 Eastern — later
than WMATA's cutoff — so it's the binding constraint, not WMATA's own
Eastern day-roll. (Correction, issue #261: current Muni GTFS schedules
owl trips of service day D to 30:53 = 06:53 PT on D+1, so 06:30 ET is
too early for SFMTA. The per-day refresh no longer depends on fire
time: it only derives a date by default once its raw window is
complete — §12.3.)

**Single-instance lock (review finding, PR #259; moved by issue #261):**
launchd's catch-up-on-wake behavior means a missed 06:30 fire can run
hours later, which could otherwise land in the middle of a manual
`bin/pull-and-derive.sh` run or overlap a second catch-up fire. Since
issue #261 **`bin/pull-and-derive.sh` itself** takes the lock — the
directory `logs/.pull-and-derive.lock.d` **in the main checkout** (a
`git worktree` resolves to it through its `.git` file, so all worktrees of
this clone exclude each other; a separate clone does not — don't refresh
from two clones), with a `holder.json` naming pid/host/start/command
(`src/run_lock.py`) — for every non-dry run, manual or nightly, and exits
75 naming the holder when another run has it. A dead holder's lock, or a
holder-less lock dir older than 60 s, is taken over atomically (a
`takeover` sub-dir first, then rename-aside), so two starters can't both
take it. The nightly wrapper no longer locks itself and treats 75 as
"skip, exit 0". `pipelines/refresh_by_day.py` run directly takes the same
lock.

**Install:**

```sh
mkdir -p logs   # launchd opens StandardOutPath before exec and fails
                # the spawn silently if this directory doesn't exist yet
cp deployment/launchd/com.wmata-dashboard.pull-and-derive.plist \
   ~/Library/LaunchAgents/com.wmata-dashboard.pull-and-derive.plist
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.wmata-dashboard.pull-and-derive.plist
```

(`launchctl load -w ~/Library/LaunchAgents/com.wmata-dashboard.pull-and-derive.plist`
on an older macOS still works if `bootstrap` isn't available.) `RunAtLoad`
is `false`, so installing doesn't trigger an immediate run — validate
end-to-end first with a manual invocation:

```sh
bin/pull-and-derive-nightly.sh
```

**Healthchecks.io check:** create one check (period ~24h / grace a few
hours — this job runs once a night, not every few minutes like the
collector's §13.5 checks), copy its ping URL into `.env` as
`PULL_AND_DERIVE_HEALTHCHECK_URL=`. The wrapper pings it (plain URL on
success, `<url>/fail` on failure) only when the var is set — unset is a
silent no-op with a log line, not an error, so the job still runs fine
before the check exists.

**Status / logs:**

```sh
launchctl list | grep wmata-dashboard
tail -f logs/pull-and-derive.log
```

**Uninstall:**

```sh
launchctl bootout gui/$UID/com.wmata-dashboard.pull-and-derive
rm ~/Library/LaunchAgents/com.wmata-dashboard.pull-and-derive.plist
```

**Updating:** after editing the plist in this repo, bootout, re-copy,
re-bootstrap (same three steps as Install, preceded by Uninstall's
`launchctl bootout`).

### 12.1 GTFS reload gate + trip_id match-rate guard — how it works, and recovery

> **Since issue #261** `bin/pull-and-derive.sh` no longer runs the
> separate canary (item 2 below): every date it derives goes through
> `run_daily_batch.py --date D`, whose per-date match-rate guard (the
> "Per-date GTFS snapshot" paragraph below) performs the same check on
> *every* date against that date's own resolved snapshot. A held date is
> reported as `held` by the per-day refresh, the run continues, and it
> exits 3. `scripts/gtfs_trip_match_canary.py` still works by hand, and
> `GTFS_CANARY_SKIP` is gone. The canary text below is kept as the
> description of that script.

`bin/pull-and-derive.sh` ran two checks per agency (WMATA and SFMTA)
before it derived:

1. **Age gate** (`scripts/run_gtfs_reload.py --max-age-days 7`) — reloads
   an agency's static GTFS if the newest loaded snapshot is more than 7
   days old. This is a staleness *proxy*: it bounds how old the loaded
   snapshot can get, but it can't see a service change land while the
   local copy is still "fresh" by that clock. Since the issue #258 PR a
   reload first archives the exact downloaded zip to
   `s3://wmata-dashboard-backups/gtfs-archive/<agency>/<UTC ts>_<feed_version>_<sha12>.zip`
   (an upload failure fails the reload with the DB untouched; the reload's
   AWS credentials therefore need `s3:GetObject` and `s3:PutObject` on
   `arn:aws:s3:::wmata-dashboard-backups/gtfs-archive/*` and `s3:ListBucket`
   on `arn:aws:s3:::wmata-dashboard-backups` with an `s3:prefix` condition
   of `gtfs-archive/*` — ListBucket is a bucket-level action, so granting
   it on an object ARN grants nothing), reusing an existing object with the same
   content hash instead of uploading a second copy, and skips creating a
   snapshot at all when the feed's content hash matches the current
   snapshot's and that snapshot is archived (`content_sha256=` and
   `archive=` in `gtfs_snapshots.notes`; a snapshot loaded with
   `--no-archive` is archived on the next unchanged run instead). A
   skipped reload doesn't advance the snapshot's age, so once the newest
   snapshot is > 7 days old the gate re-downloads (and re-hashes) the feed
   on every run until the agency publishes a new one — a cheap
   feed-change check.
2. **Post-replay trip_id match-rate canary**
   (`scripts/gtfs_trip_match_canary.py`) — after replay, recomputes the
   same feed-trip_id ∩ current-GTFS-trip_id intersection both derive
   paths (`pipelines/derive_stop_events.py`,
   `pipelines/derive_stop_events_from_state.py`) gate on, for each
   agency's latest agency-local service date. This is the check that
   actually catches a service change landing while the snapshot is still
   "fresh" by the age gate — the 2026-08-30 Muni fall service change did
   exactly that (SFMTA's snapshot was < 7 days old, so the age gate
   skipped a reload, but the feed's trip_ids had moved to a brand-new
   space; a collapsed intersection means derivation would otherwise
   silently write ~0 `stop_events` while exiting 0).

The canary exits 0 (ok, or a "nothing observed yet" skip), 1 (a real
match-rate collapse), or 2 (an operational error — the check itself
couldn't run: unset `<AGENCY>_DATABASE_URL`, an unknown `--agency`, a
transient DB failure). `bin/pull-and-derive.sh` replays and derives
*both* agencies (SFMTA pull-and-derive automation) — the replay loop
runs `pipelines/replay_archive_to_state.py` for WMATA and SFMTA every
lookback date (SFMTA's replay passes `--allow-empty`, since its S3
archive prefix only starts 2026-07-22 and a missing/empty day there is
expected, not an error — WMATA keeps the strict, no-`--allow-empty`
behavior), and derive calls `run_daily_batch.py` once with no
`--agency` flag (WMATA) and again with `--agency sfmta`. Failure
handling is per-agency throughout: a replay failure, a canary collapse
(exit 1), or a derive failure for one agency skips only *that*
agency's remaining steps (derive is skipped outright on a replay
failure or a collapse; a derive failure is just captured) — it never
blocks or aborts the other, healthy agency's replay/derive. An
operational error (exit 2) from the canary for either agency is
captured as a nonzero return code and reported in the run's summary,
and does not skip that agency's derive. Any of these failure modes,
for either agency, makes the overall script exit nonzero (with a
summary of what happened) even though everything unaffected still ran
— check the summary, don't assume a nonzero exit means nothing
derived. `GTFS_CANARY_SKIP=1 bin/pull-and-derive.sh` skips the canary
step entirely (loud "skipped by operator" line) — a manual escape
hatch for a legitimately-0% backfill date; use deliberately, not
routinely. (Single-agency canary collapses no longer need this flag to
protect the other agency, since a collapse now only skips its own
agency's derive.)

**Per-date GTFS snapshot (issue #258).** Every per-date pipeline that
`run_daily_batch.py` drives, and the canary above, use the snapshot
`src/gtfs_versioning.py:resolve_gtfs_snapshot_for_date` picks for *that*
service date — the newest snapshot that actually schedules service on
the date under the GTFS rule the pipelines use
(`src/gtfs_calendar.py:scheduled_service_ids_for_date`: weekday flag in
range, minus `calendar_dates` removals, plus additions); if none does, the
newest snapshot already loaded by the end of that date; if the date
predates every loaded snapshot, the oldest one. Each batch log carries a
`gtfs_snapshot:` line per date, and every per-date pipeline — including
bunching's scheduled-headway thresholds — reads that one snapshot.
This closes the *reverse* race to the 2026-08-30 one: a reload that lands
a feed effective "today" before "yesterday" was derived (WMATA 9/13
reload → 9/12 at 0/9,917 against snapshot 20) no longer collapses
yesterday — it is derived against the outgoing snapshot automatically.
`run_daily_batch.py` also runs the canary's match-rate check per target
date (catch-up dates included, which the canary itself never sees) and
skips a collapsed date loudly rather than writing a near-empty day whose
`runs` rows would read as a real 0% and block the zero-runs auto-revisit
(what happened to WMATA 9/12 in the 2026-09-29 catch-up). Held-back dates
are listed as `guard-skipped dates` in the batch summary, not counted as
pipeline failures, and stay excluded from aggregation even if a
neighbouring date's derive writes owl-corrected rows under them. A batch
whose only problem is held-back dates exits **3** (any pipeline failure
is still 1); `bin/pull-and-derive.sh` still runs SFMTA
`trip_update_state` cleanup on 3 — the held-back date's state is
rebuildable from the S3 raw archive (`replay_archive_to_state.py --date D
--force`), and gating cleanup on a date that re-fails every run would
freeze retention for the whole lookback window — but the overall run
still exits nonzero with a summary line.
A date that slipped through before this fix just needs its derive re-run;
no reload is involved (reloading again changes nothing — the date needs
the *previous* snapshot, which the resolver now selects).

**Recovery procedure**, once a real collapse is confirmed (agency,
date, and match rate are in the canary's error output) — i.e. the
date's *own* resolved schedule doesn't know the feed's trip_ids, the
2026-08-30 direction (feed changed, new GTFS not loaded yet):

1. Reload that agency's static GTFS:
   `uv run python scripts/reload_gtfs_complete.py --agency <agency>`.
2. An operator then deletes the affected date's `runs` rows for that
   agency (`DELETE FROM runs WHERE service_date = '<date>' AND
   agency = '<agency>'` — or the agency-scoped equivalent for a
   single-agency DB) so `run_daily_batch.py`'s
   `determine_target_dates` picks the date back up automatically;
   without this, a near-zero derive still writes `runs` rows, which
   blocks `run_daily_batch.py`'s auto-revisit — the shape that
   required the manual SFMTA 8/9–8/10 top-up (PR #227).
3. Re-derive: `bin/pull-and-derive.sh --agency <agency> --date D`
   (add `--force` if the date is already `done` in
   `logs/refresh_progress_<agency>.json` — that also forces the replay,
   and re-fetches the date's raw files from S3 if they were pruned).
   A plain rerun of `bin/pull-and-derive.sh` also retries a `held` date
   inside the 14-day lookback.

---

### 12.2 Replay manifest (`tu_archive_replayed_files`, PR #245)

The replay leg is manifest-idempotent: each `(archive file, target
service date)` pair is folded into `trip_update_state` once and
recorded in `tu_archive_replayed_files` (same transaction as the
upsert). A lookback date whose files are all manifested is a no-op, so
the 14-day `LOOKBACK_DAYS` costs only not-yet-folded files instead of
re-folding ~1.8B snapshot rows per run (the 2026-09-07 run took ~9 h
before this). The key includes the target date because the UTC-next-day
supplement file is also the next date's first primary file.

Three cases re-fold a date in full from its archive files:

- a not-yet-folded file whose open epoch precedes the newest snapshot
  already folded for the date (restart-overlap sibling that missed a
  sync, or a legacy epoch-less name) — the always-overwrite upsert can't
  fold it alone safely;
- a fully manifested date with no `trip_update_state` rows *and* no
  `runs` rows — its state was pruned by the 7-day retention before it
  was ever derived, and the 14-day catch-up would otherwise derive
  against empty state forever;
- `--force`, the operator escape hatch for a deliberate re-replay of a
  date that still has `runs` (a `runs`-DELETE-then-re-derive, or the
  dated `scripts/local_recovery_2026_07.sh` driver, lands in the
  pruned-state case above on its own and needs no flag).

**Prerequisite, per database.** `scripts/migrate_all.py` only reaches
the WMATA DB; without the SFMTA invocation every SFMTA replay date fails
with `relation "tu_archive_replayed_files" does not exist`, which skips
SFMTA derive and retention for the whole run:

```sh
uv run python scripts/migrate_create_tu_archive_replayed_files.py
uv run python scripts/migrate_create_tu_archive_replayed_files.py --agency sfmta
```

The first run after the migration re-folds the lookback window once to
seed the manifest. `bin/refresh-dev-db.sh` TOC-excludes the manifest
alongside `trip_update_state` so a scratch restore can still replay.

### 12.3 Day-at-a-time refresh + local raw-archive prune (issue #261)

The laptop can't hold the raw archive (WMATA TU alone is ~1.3 GB/day;
all four prefixes ~1.7 GB/day), so `bin/pull-and-derive.sh` runs
`pipelines/refresh_by_day.py`, which processes **one service date at a
time, oldest first, agencies interleaved**:

| Step | What | Files / command |
|---|---|---|
| fetch | only that date's raw files from S3 (size + ETag verified, `.part` then rename) | TU: UTC D, D+1 (replay's primary + supplement). VP: UTC D..D+2 (the derive's VP window, local midnight D → local midnight D+2, widened by the loader's 30-min timestamp guard) |
| load VP | per UTC day; a day whose rows were deleted is reloaded whole | `load_vp_archive.py --utc-date U ...` |
| replay | TU for D (SFMTA `--allow-empty`) | `replay_archive_to_state.py --date D` |
| derive | per-date chain, no housekeeping, held/failed dates excluded | `run_daily_batch.py --date D --skip-housekeeping --exclude-date …` |
| clean state | delete `trip_update_state` for service dates before `min(D, today − 7)`, then `VACUUM (ANALYZE)` | `cleanup_trip_update_state.py --before-date D --vacuum` |
| prune | local files dated before the next date's first needed UTC day, only if S3-identical + manifested | `src/raw_archive.py:prune_stream` |

The derive also gets `--exclude-date` for every date the progress file
has as held/failed, and `run_daily_batch` refuses to aggregate a
non-target date that has no `runs` of its own — otherwise date D's
owl-corrected rows written under a held D−1 would aggregate D−1 into a
near-empty day that reads as a real 0% (the #258 failure, across two
invocations).

**trip_update_state between dates.** Derive of D reads only
`service_date = D`; D's completeness stamp reads D's own state (the
trip-update gap leg) plus state polled inside local day D, which
includes D−1's owl trips (the coverage leg). So after D, state before D
(but never inside the normal 7-day window) is dead: the per-date cleanup
deletes it and VACUUMs on its own autocommit connection (VACUUM cannot
run in a transaction; plain VACUUM takes only SHARE UPDATE EXCLUSIVE),
so the next date's replay reuses that space and the table stays at its
current size instead of growing ~0.43 GB/day (WMATA) + ~0.28 GB/day
(SFMTA) for the whole backfill. Autovacuum would also fire (deleting a
day of a ~2-day table is far past the 20% scale factor) but on its own
schedule, possibly after the next replay already extended the file.
After the last date: WMATA housekeeping once (`run_daily_batch.py
--housekeeping-only`, ~1.5 h — the diagnostic refreshes plus WMATA's
7-day cleanup) and SFMTA's 7-day cleanup once; each is skipped if a date
of that agency failed this run (so the failed date's state survives for
the retry), and WMATA's with `--skip-housekeeping`.

**Completeness stamp (`data_quality`).** Each aggregate rollup stamps
the date `complete` or `partial` (`src/data_completeness.py:assess_data_quality`).
A date is `complete` only if (1) minute coverage — heartbeats ∪ VP ∪
trip-update snapshot times — reaches the cadence-aware threshold (0.80
WMATA, 0.533 SFMTA) AND (2) neither feed is silent for more than
`completeness.feed_gap_max_minutes` (90) inside the agency-local
`service_start_hour`–`service_end_hour` window (05–24), both set per
agency in `config/agencies/*.yaml` (issue #270: the coverage union alone
missed a 5.7 h Muni trip-update outage on 2026-10-21 and can never see a
single-feed outage at WMATA). The trip-update leg reads D's own
`trip_update_state` rows, the VP leg distinct `vehicle_positions.collected_at`.
A leg whose inputs are gone (state and raw VP are kept 7 days) is *not
assessed*: it never demotes a date and never upgrades one — a date that
already has a stamp keeps it and its stored coverage (logged as kept),
unless a leg that *is* assessable fails. That is also why the end-of-run
cleanup deleting the last processed date's state is harmless: when the
next run's derive re-aggregates that date as D−1, its trip-update leg is
not assessable (no rows with its `service_date`, whatever the next
date's replay left in its evening window), so its stamp and coverage are
kept rather than re-measured from VP alone (issue #272, SFMTA 9/29). The
upsert logs one `completeness <date>: …` disclosure line per date
(coverage, each feed's longest gap, the rule that made it partial, any
leg not assessed); nothing is stored beyond `data_quality` and
`coverage_pct`.

`pipelines/restamp_data_quality.py --agency X --start A --end B`
re-evaluates stamps after a rule change (dry-run by default; prints the
same disclosure per date). It writes only dates whose stamp flips, and
writes `coverage_pct` only when it was re-measured with every input
present, so it can never overwrite a real stored coverage with the ~0 a
pruned date measures; dates whose inputs are pruned are listed as kept.
Correcting the stamp of such a date needs an owner-run, backed-up
`UPDATE` of `data_quality` in `system_metrics_daily` and
`route_metrics_daily_overlay` (see PR for #270/#272 for the runbook);
afterwards a re-aggregation keeps the corrected stamp, because the date's
inputs are absent (keep rule) and its stop-level rows, once pruned, make
the aggregate-overwrite guard refuse the rollup.

**Which dates.** Default, per agency: the day after `max(runs.service_date)`
through the latest date whose **raw window is complete**, plus zero-`runs`
dates in the last `--lookback-days` (default 14), plus every `failed` /
`derived` / `deferred` date in the progress file (any age — a date can
fail after `aggregate_runs` wrote runs) and `held` dates inside the
lookback (older held dates are listed as "not retried"; rerun them with
`--date D` after a GTFS reload), never an `abandoned` date. Complete =
now ≥ agency-local midnight after D + 5 h (WMATA, last scheduled arrival
26:58 ET) / 9 h (SFMTA, owl service to 30:53 PT). S3 object timestamps
(2026-11-20) put rows in S3 at most ~16 min after their file opens
(15-min rotation + upload), which leaves ~1 h 45 min for late trips.
Later dates are printed and recorded as `deferred`, so a default run
always returns to them even if a later date was derived explicitly in
between. `--start A --end B` / `--date D`
select explicitly (an incomplete date then gets a loud warning). Dates
before 2026-06-14 are refused. `--dry-run` prints the plan and writes
nothing (no GTFS reload, no lock, no progress file).

**Resume / failure.** `logs/refresh_progress_<agency>.json` records each
date (`done`, `held`, `failed`, or `derived` = derive committed, prune
pending). It is a hint; the DB wins: a date counts as done only if the
file says `done` AND `runs` has rows for it, a `derived` date without
runs is re-run in full, and an unreadable file is moved aside with a
warning. A rerun therefore resumes at the first unfinished date, for an
explicit range too; `--force` re-does done dates and forces the replay.
A hard failure (fetch, VP load, replay, derive exit 1, or free disk below
`--min-free-gb`) **stops that agency** — the other continues — and keeps
the failed date's raw files; failures are almost always systemic, so
carrying on would fail and re-download N more times. A `held` date
(match-rate guard, exit 3) is reported and the run continues. A date
whose guard could not run (batch exit 4) is derived, listed as a warning,
and makes the run exit 1.

A retry of a previously `failed`/`held`/`abandoned` date replays with
`--force`. `aggregate_runs` depends only on the proximity derive, so a
date that failed in the trip-update derive already has runs; if a later
run's per-date cleanup (or housekeeping) deleted its state meanwhile, a
plain replay would see manifest + runs, not re-fold, and the retry would
derive from empty state.

**A date that fails every time.** Failed dates are re-selected first and
a failure stops the agency, so one permanently broken date would block
that agency's default runs forever (`--start` past it doesn't help — the
default run still returns to it). Once you have looked at it and decided
to give up, record it:

```sh
bin/pull-and-derive.sh --agency wmata --abandon-date 2026-10-07
```

It is refused for a date that already has runs or is `done` unless you
add `--abandon-derived` (abandoning a healthy date would stop a
neighbour's owl-corrected rows from ever being re-aggregated onto it).
It is then never re-selected by default (main range, holes or progress
rules), never aggregated as a side
effect of a neighbour's derive (like held/failed dates), and listed as
`abandoned` in every run's summary. `bin/pull-and-derive.sh --agency
wmata --date 2026-10-07` still processes it explicitly (re-folding), and
success marks it `done`.

**Single instance.** A non-dry run holds the main checkout's
`logs/.pull-and-derive.lock.d` (§12.0). A second run exits 75 naming the
holder. Arguments are validated first (`refresh_by_day --check-args`): a
typo exits 2 before the lock, the GTFS reload, or anything else runs.

**Disk budget for the 2026-09-29 → yesterday backfill (56 dates).**

- Raw files: peak ≈ 2 UTC days of TU + 3 of VP per agency ≈ 2.9 GB
  WMATA + 0.9 GB SFMTA (S3 listings: WMATA TU max 1.44 GB/day, SFMTA TU
  max 0.42 GB/day, VP 0.04/0.01 GB/day).
- `trip_update_state`: bounded by the per-date cleanup at its current
  on-disk size (WMATA 3.1 GB, SFMTA 2.2 GB — already allocated, so ~0
  new; it does not shrink, VACUUM FULL would).
- Permanent DB growth (stop_events, runs, vehicle_positions, rollups):
  reviewer-measured all-in growth was ~0.88 GB/day WMATA + ~0.48 GB/day
  SFMTA including state; minus state (~0.43 + ~0.28) that is ~0.65
  GB/day, **≈ 36 GB for 56 dates**, plus some WAL/temp headroom. Nothing
  in this PR removes it (retention is the next PR).
- So the backfill needs roughly **40 GB** free beyond today's usage.
  With 61 GB free now, run the archive prune first (+~50 GB) to finish
  with ~70 GB free; without it the run would approach the floor near the
  end.
- The 15 GB `--min-free-gb` floor is checked once per date before its
  fetch. It stops a backfill from *starting* a date with the disk nearly
  full; it does not bound what one date writes (~1.5 GB incl. raw), does
  not watch other disk consumers, and does not reserve space for
  Postgres WAL spikes or VACUUM.

**Pruning an existing local archive.** `scripts/prune_local_archive.py`
(dry run by default; `--apply` deletes) applies the same rule to the
whole local archive with "needed" = dated after the agency's latest
`runs.service_date` (an older hole/held date re-derived later simply
re-fetches from S3). It prints per-directory counts/bytes and every
refusal (missing from S3, size or checksum mismatch, not in the
manifest, unrecognized name). Directories that aren't one of the four
streams — `archive/trip_update_snapshots/` (the retired May 2026
`trip_update_snapshots` parquet export, no S3 copy) — are listed and
never touched; that directory and the 889 MB orphan
`raw_snapshots/2026-08-28.307981.1786210105.jsonl.zst` (not in S3, not in
the manifest) are left for the owner to upload or delete.

**VP reload idempotency.** `vehicle_positions` has no natural unique key,
so the manifest is what stops duplicates. `--utc-date U` compares the
rows with `collected_at` on U against the manifest's row counts for
files `U.*` (files are named by each row's `collected_at` UTC date, so
the two describe the same set): equal → load only new files; fewer
(rows deleted — by `pipelines/apply_retention.py`, §12.4, which deletes
whole `collected_at` UTC days and keeps their manifest rows, or a manual
delete) or `--force` → delete U's rows and manifest rows and
re-insert every `U.*` file in one transaction; more (VM-era days through
2026-08-22, written straight to the DB) → no-op when there is nothing
new to load, refusal when there is (or with `--force`). A reload also
refuses if any manifested `U.*` file is not local.

### 12.4 Local-DB retention tiers + GTFS snapshot de-dup (issue #265)

**Status:** tools only. Nothing prunes automatically and nothing has been
run against the real databases. Every delete path below is dry-run by
default; the owner runs them.

**Tiers** (`config/agencies/<agency>.yaml` → `retention:`, parsed by
`src/agency_config.py:RetentionPolicy`):

| Data | WMATA (deep) | SFMTA / peers (shallow) |
|---|---|---|
| `stop_events`, `runs` (`stop_level_days`) | 100 days | 35 days |
| `vehicle_positions`, `trip_update_state` (`raw_days`) | 7 days | 7 days |
| `system_metrics_daily`, `route_metrics_daily_overlay`, `route_headway_metrics` | forever | forever |

A missing `retention:` block means the shallow tier; values below
15 / 3 days are rejected (they must exceed the refresh's 14-day zero-runs
lookback; the VP window for date D reads UTC days D..D+2).

**Window anchor.** Both windows count back from the latest *fully derived*
service date — newest `system_metrics_daily` row stamped `complete` with
non-null OTP that still has `runs` — capped at agency-local yesterday,
never from today. While a backfill has only reached mid-October the
anchor is mid-October (the tool keeps more, never less); after weeks of
not running anything the window does not shrink. No derived date → no
deletes.

**What `pipelines/apply_retention.py --agency X` does** (dry run prints
rows / date ranges / estimated bytes per table and every kept date with
its reason; `--apply` re-checks each guard immediately before each date
or day, deletes in `--batch-size` batches committing each, and holds the
refresh run lock):

| Delete path | Unit | Guards (all must pass) |
|---|---|---|
| `runs`, then `stop_events` | service date D < anchor − (stop_level_days − 1) | D's `system_metrics_daily` is `complete` with non-null OTP and service-delivered, and D has `route_metrics_daily_overlay` and `route_headway_metrics` rows (`--release-date D` releases a date whose aggregates are not final, but never one without an observed `system_metrics_daily` row); D is not `failed`/`held`/`deferred`/`derived` in `logs/refresh_progress_<agency>.json`; D's VP window starts at or after the first manifested S3 JSONL VP file (2026-08-22 20:54 UTC), i.e. the automated per-day path can re-derive D — unless `--allow-unrederivable` (see "Where older data exists") |
| `vehicle_positions` | UTC day of `collected_at` before the first VP day the raw window needs | every row of the day came from manifested archive files (rows ≤ `vp_archive_loaded_files` count — VM-era days are kept unless `--include-unmanifested-vp`); no not-yet-derived date (no `system_metrics_daily` row, or an unfinished progress status) reads the day. Manifest rows are **kept**, so `load_vp_archive --utc-date U` sees "fewer rows than manifested" and reloads the whole day when `refresh_by_day --date D` needs it |
| `trip_update_state` | `service_date < local today − raw_days` | the existing `cleanup_trip_update_state.run_cleanup` (a date whose state goes before it is derived re-folds from S3 on replay) |

`runs` go before `stop_events` so an interrupted run never leaves a date
with `runs` but no `stop_events` (the API floor and the rollup guard key
on `runs`).

**Rolling-window tables need nothing.** `route_diagnostic_*`,
`cross_route_segment_rollup` and `corridor_slip_rollup` are rebuilt by the
housekeeping refresh from the last 30 days of `stop_events`, which both
tiers keep.

**Safety elsewhere in the pipeline.**
- `upsert_system_metrics_daily`, `upsert_route_metrics_overlay` and
  `compute_bunching` exit 1 without writing when a kept observed aggregate's
  *source* rows are absent for the date (`--force-overwrite-aggregates`
  overrides). Sources: OTP ← `proximity` stop_events/runs; EWT, bunching,
  destination OTP, `route_headway_metrics` ← `trip_update` stop_events;
  service-delivered ← `runs`. So a re-derive that rebuilds only trip-update
  rows (VP gone) cannot null a kept proximity OTP.
  **If it refuses inside a refresh** (`bin/pull-and-derive.sh --date D`),
  the derive fails, the date is recorded `failed`, that agency stops, and
  default runs keep re-selecting it. Choose one:
  - the raw inputs are genuinely gone and the kept aggregates are the best
    record of D (the usual case) → keep them and stop the retries:
    `bin/pull-and-derive.sh --agency X --abandon-date D`, adding
    `--abandon-derived` when D still has `runs` (the refusal itself means
    some rows were rebuilt);
  - the stop-level rows are complete again (e.g. you loaded D's VP from the
    parquet archive) but some *other* source is still absent, or you have
    checked that the kept values are wrong → overwrite deliberately, per
    rollup, outside the refresh:
    `uv run python -m pipelines.upsert_system_metrics_daily --agency X --date D --force-overwrite-aggregates`
    (same flag for `pipelines.upsert_route_metrics_overlay` and
    `pipelines.compute_bunching --all-routes`). Never use it just to make a
    refusal go away — it replaces kept values with whatever the current
    rows compute.
- `refresh_by_day` (and so `bin/pull-and-derive.sh`) refuses any date whose
  VP read window is neither inside the S3 JSONL VP archive nor still in the
  DB (`src/retention.py:dates_missing_vp`); `--allow-missing-vp` overrides.
  Re-deriving a date inside the archive with `--date D` reloads its raw
  data first and works normally. With an empty `vp_archive_loaded_files`
  (e.g. a dev copy restored without it) the archive start is unknown and
  nothing is refused (a warning is logged). The "still in the DB" test is
  any-row per UTC day, so a VM-era day left partly deleted by an
  interrupted `--include-unmanifested-vp` run counts as present: rerun
  retention to finish deleting it before re-deriving any date that reads it.
- The zero-runs "hole" scans (`refresh_by_day` default selection,
  `run_daily_batch` catch-up) never reach past `today − stop_level_days`,
  so a pruned date is re-derived only when named with `--date`.
- API: a date is *pruned* when its daily aggregates show observations but
  it has no `runs` (kept older islands — partial or not automatically
  re-derivable dates — are not pruned, so this is a set, not a cut-off).
  `/api/retention?agency=` reports the windows, `detail_retained_from`
  (every observed date from there on still has detail),
  `earliest_detail_date` and the pruned count. `/api/routes/{id}/trend`
  falls back to the daily overlay on pruned dates (official-window OTP,
  service-delivered; rows marked `"source": "daily_aggregate"`, otherwise
  null with `"detail_retained": false`) and is capped at 400 days.
  `/api/routes`, `/api/routes/{id}`, `/stops`, `/bunching-causes`,
  `/contributors` and each agency in `/api/agency-comparison` carry a
  `retention` block with `window_truncated` / `pruned_dates_in_window`
  (e.g. `?agency=sfmta&days=90` once SFMTA's 35-day tier is applied).
  Block views return 410 for a pruned date. (`/time-periods` is the
  deprecated stub that reads nothing; `/period-drilldown` reads one recent
  anchor date, never a pruned one.)

**Dates kept forever unless released.** A date whose aggregates are not
final keeps its stop-level rows. `apply_retention --agency X --apply
--release-date YYYY-MM-DD` (repeatable; the progress-file and
re-derivability guards still apply) releases one deliberately when an
observed daily record survives:

- releasable: a `partial` stamp (WMATA 5/29, 7/11, 7/12, 7/17; SFMTA 7/21),
  one NULL headline with other observed values (SFMTA 7/22 and 8/28 —
  OTP, EWT and bunching present, service-delivered NULL), or missing
  overlay / headway rows (WMATA 5/02–5/03, whose system row is fully
  observed);
- **never releasable**: no `system_metrics_daily` row at all (WMATA 5/24),
  or a row with no observed value (OTP, EWT, bunching all NULL and
  service-delivered NULL/0 — WMATA 5/07, checked read-only) — there the
  stop-level rows are the only record of the day. The dry run says `cannot be released` for these.

**Where older data exists (checked 2026-11-24, read-only).**

| Data | Known | Probable / unknown |
|---|---|---|
| WMATA trip-update JSONL | `s3://wmata-dashboard-backups/raw-jsonl-archive/` from 2026-05-03 to today (listed) | — |
| WMATA VP, 2026-05-03 … 2026-08-01 | daily parquet `s3://…/wmata-vp-archive/<UTC date>.parquet`, all 91 days present (listed); partitioned by `DATE(timestamp)`, loadable with `pipelines/load_vp_from_parquet.py --date U` (keeps original ids, `ON CONFLICT DO NOTHING`) | whether every parquet day equals the DB's rows for that day — not compared |
| WMATA VP 2026-08-02 … 2026-08-22 20:54 UTC; SFMTA VP 2026-07-22 … 2026-08-22 | in the DB (VM-era rows, not in the JSONL manifest) | probably in the VM's final pg_dumps `s3://…/final-dumps/wmata_db_20260831_140041.dump.xz` (2.31 GB) and `final-dumps/sfmta_db_final_20260831.dump` (0.47 GB) — contents not inspected (not downloaded) |
| WMATA / SFMTA `stop_events`, `runs` through ~8/29 | in the DB | probably also in the same final dumps — not inspected |
| VP from 2026-08-22 20:54 UTC on (both agencies) | JSONL in `raw-jsonl-archive/vp/` and `…/sfmta_vp/`, manifested; reloadable by `refresh_by_day` | — |

So "not re-derivable" in the tool means **not re-derivable by the automated
per-day path** (`refresh_by_day`): its VP step only knows the JSONL
archive, and its `DATA_FLOOR` refuses dates before 2026-06-14. A WMATA date
D ≤ 2026-07-30 (VP window D..D+2 inside the parquet days) can be re-derived
by hand, as `scripts/local_recovery_2026_07.sh` did for 6/11–6/12:

```bash
# TU files for UTC D and D+1 must be local first (aws s3 sync of
# raw-jsonl-archive/ with --include 'D*' --include 'D+1*' into archive/raw_snapshots)
for u in D D+1 D+2; do uv run python pipelines/load_vp_from_parquet.py --date $u; done
uv run python pipelines/replay_archive_to_state.py --agency wmata --date D --force
uv run python pipelines/run_daily_batch.py --agency wmata --date D --skip-housekeeping
```

The parquet files are partitioned by `DATE(timestamp)` (the vehicle's
fix time, UTC), while the JSONL files and the loader's day unit are by
`collected_at`; the derive reads a `timestamp` window from local midnight
D to local midnight D+2. So load every UTC day that window touches —
`src/raw_archive.py:vp_utc_dates(D, tz)`, i.e. D, D+1 and D+2 for WMATA —
not only D's own file.

That path is not tested end to end by this PR (the per-date GTFS snapshot
resolution and the TU replay are; the parquet loader has its own test),
and the reloaded VP rows count as "unmanifested" for VP retention.
Teaching the automated guard about the parquet archive would need
`refresh_by_day` to fetch and load parquet days below the JSONL start and
to drop or relax `DATA_FLOOR`; until then the guard stays conservative.
For WMATA 7/31–8/22 and all pre-8/22 SFMTA dates the only known copies of
VP are the DB and (probably) the final dumps.

**Backups that expire.** The bucket has a lifecycle rule
`expire-db-backups` on `wmata-db-backups/` (expire after 90 days,
noncurrent versions 30 days later; `aws s3api
get-bucket-lifecycle-configuration --bucket wmata-dashboard-backups`).
`head-object` shows `wmata-db-backups/wmata_db_20260830_050213.dump.xz`
expiring **2026-11-29 00:00 UTC**, and
`wmata-db-backups/wmata_db_20260831_140041.dump.xz` and
`wmata-db-backups/sfmta_db_final_20260831.dump` expiring **2026-11-30 00:00
UTC**. The two 8/31 dumps also exist under `final-dumps/` (no rule
matches it; `head-object` shows no expiration); the 8/30 dump has no other
copy. The other rules cover `raw-jsonl-archive/` (Glacier IR after 90 days,
no expiry) and `travel-log-db-backups/`. Put any safety copy this runbook
makes under **`s3://wmata-dashboard-backups/retention-exports/`** (no rule
matches it), never under `wmata-db-backups/`.

**Read-only measurements, 2026-11-24** (backfill running; WMATA and
SFMTA derived through 2026-09-29; stop_events rows estimated from per-date
`runs` counts × the table's stop_events/runs ratio, bytes from
`pg_total_relation_size / reltuples`; "after the backfill" rows for dates
not yet derived are projected at the measured per-day rate):

| | anchor 2026-09-29 (today) | anchor 2026-11-23 (after the backfill) |
|---|---|---|
| SFMTA stop-level, deletable by default | 3 dates (8/23–8/25), ~1.0M stop_events, ~0.4 GB | ~58 dates (8/23–10/19): 37 measured (8/23–9/29) ~13.0M ~5.5 GB, plus ~21 projected (9/30–10/19) → ~20M, ~8.6 GB |
| SFMTA stop-level, needs `--allow-unrederivable` (VP only in DB / final dump) | 31 dates (7/23–8/22), ~10.5M, ~4.5 GB | same |
| SFMTA kept (aggregates partial / null service-delivered) | 7/21 partial, 7/22 null SD | + 8/28 null SD |
| WMATA stop-level, deletable by default | none | none (everything before 8/16 predates the JSONL VP archive) |
| WMATA stop-level, needs `--allow-unrederivable` | 46 dates (5/04–6/21), ~25.3M, ~9.7 GB — all have parquet VP | 98 dates (5/04–8/15), ~59.9M, ~22.9 GB — those ≤ 7/30 have parquet VP, 7/31–8/15 do not |
| WMATA kept (aggregates missing / partial / null) | 5/02–5/03 (no overlay), 5/07 (null), 5/24 (no row), 5/29 (partial) | + 7/11, 7/12, 7/17 (partial) |
| VP manifested days older than the raw window | WMATA ~29.6M rows ~6.8 GB, SFMTA ~8.3M ~1.6 GB | WMATA ~39.1M ~9.0 GB, SFMTA ~10.7M ~2.1 GB |
| VP VM-era rows (kept unless `--include-unmanifested-vp`) | WMATA ~44.4M ~10.2 GB (6/11–8/22), SFMTA ~5.7M ~1.1 GB (7/22–8/22) | same |

**GTFS snapshot de-dup** (`scripts/dedup_gtfs_snapshots.py`, Postgres
only): groups snapshots by a content fingerprint of all six versioned
tables (refusing to run if `CONTENT_COLUMNS` no longer matches the live
schema), keeps one per distinct content (`is_current` > has an `archive=`
token > newest load — the newest member is what
`resolve_gtfs_snapshot_for_date` already picks among identical covering
snapshots), refuses a group if excluding its duplicates would change the
resolved content of any date from the first `system_metrics_daily` date
through the last date any calendar mentions, proves row-for-row equality
(`EXCEPT ALL` both ways; `trips`/`stop_times` in trip_id ranges) right
before each delete, repoints `corridors.gtfs_snapshot_id` and
`route_service_profile.snapshot_id`, deletes the duplicate's calendar
first (a calendar-less snapshot is never resolved), tags it
`dedup_into=N` (a rerun resumes), then its rows in batches and its
`gtfs_snapshots` row. `--metadata-only` (feed_version + stored counts,
instant, never used by `--apply`) today shows WMATA 16 snapshots → 4
groups, 12 "duplicates", ~18.3 GB; SFMTA 5 → 2, 3 duplicates, ~1.3 GB.
That grouping is wrong for WMATA 11/12 (calendar `end_date` 20260613 vs
20260620 — checked on the small `calendar` table), so the content run
should keep 4, 5, 11, 12, 19 and 22 and delete 6, 8, 9, 10, 15–18, 20, 21
(~16 GB) if their large tables match. Snapshot 5 has no `calendar` /
`calendar_dates` rows at all (a partial load), so it is distinct content,
never resolved, and kept — removing it is a separate decision.
The `export` zips are a **CSV safety copy of the six versioned tables**,
not a loadable GTFS feed: shapes, agency, feed_info and the GTFS-Plus
timepoint files are not versioned in the DB and are absent, and
`reload_gtfs_complete.py` cannot read them. Restoring means inserting the
CSV rows back under a snapshot id.

#### Runbook (owner-run, in this order)

Steps marked **(no backfill)** read or rewrite large tables: run them only
when `logs/.pull-and-derive.lock.d` does not exist in the main checkout.
The retention dry run and the de-dup fingerprint / prove / export refuse
while another process holds that lock (`--allow-during-refresh`
overrides the read-only ones); `--apply` takes the lock itself.

1. **Dry runs (no backfill)**:

   ```bash
   uv run python -m pipelines.apply_retention --agency sfmta
   uv run python -m pipelines.apply_retention --agency wmata
   uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --metadata-only
   uv run python scripts/dedup_gtfs_snapshots.py --agency sfmta --metadata-only
   ```

2. **Backups** (`docs/MIGRATIONS.md` step 1, adapted to the laptop; ~97 GB
   free on 2026-11-24). The aggregates are what retention promises to keep
   forever, so dump them every time (small) and keep a copy off the laptop:

   ```bash
   mkdir -p ~/backups
   ts=$(date +%Y%m%dT%H%M%S)
   for db in wmata_dashboard sfmta_dashboard; do
     f=~/backups/${db}_aggregates_${ts}.dump
     pg_dump -Fc -d $db -t system_metrics_daily -t route_metrics_daily_overlay \
       -t route_headway_metrics -t gtfs_snapshots -t vp_archive_loaded_files \
       -t tu_archive_replayed_files -f "$f"
     pg_restore --list "$f" | tail -3
     aws s3 cp "$f" s3://wmata-dashboard-backups/retention-exports/
   done
   ```

3. **Before any override run** (`--allow-unrederivable`,
   `--include-unmanifested-vp`): find out what the final dumps hold, and
   copy out what the override will delete.

   a. Table list of each final dump (streams the object; nothing is
      written to disk; `pg_restore -l` only needs the table of contents
      at the start of a custom-format dump, so the transfer should end
      early with a harmless broken-pipe message — if the dump turns out
      not to be custom format, `pg_restore` says so and you need
      `… | xz -dc | grep -m20 -E '^(CREATE TABLE|COPY) public\.'` instead):

      ```bash
      aws s3 cp s3://wmata-dashboard-backups/final-dumps/wmata_db_20260831_140041.dump.xz - \
        | xz -dc | pg_restore -l | grep -E 'TABLE DATA public (stop_events|runs|vehicle_positions) '
      aws s3 cp s3://wmata-dashboard-backups/final-dumps/sfmta_db_final_20260831.dump - \
        | pg_restore -l | grep -E 'TABLE DATA public (stop_events|runs|vehicle_positions) '
      ```

      (If `sfmta_db_final_20260831.dump` turns out to be compressed too,
      add `xz -dc |` after `aws s3 cp`.) Which dates a dump holds can only
      be seen by restoring the table into a scratch DB
      (`createdb scratch_final && pg_restore -d scratch_final -t vehicle_positions …`),
      ~tens of GB of disk.

   b. Copy out the rows the override deletes **(no backfill)**. C = the
      dry run's "stop-level keeps >=" date; V = the first VP day the dry
      run keeps (`VP UTC days >=`):

      ```bash
      DB=wmata_dashboard; A=wmata; C=2026-08-16; V=2026-11-17   # from the dry run
      psql -d $DB -c "\copy (SELECT * FROM stop_events WHERE service_date < '$C') TO PROGRAM 'zstd -T0 > ~/backups/${A}_stop_events_before_$C.csv.zst' CSV HEADER"
      psql -d $DB -c "\copy (SELECT * FROM runs WHERE service_date < '$C') TO PROGRAM 'zstd -T0 > ~/backups/${A}_runs_before_$C.csv.zst' CSV HEADER"
      # only for --include-unmanifested-vp (WMATA ~44M rows, ~10 GB in the DB; days 6/11–8/22).
      # First check that NO day on/after the JSONL archive start has rows the manifest can't
      # account for — the copy below only covers collected_at < 2026-08-23 (read-only, no backfill):
      #   psql -d $DB -c "WITH m AS (SELECT left(filename,10)::date AS d, sum(row_count) AS n
      #       FROM vp_archive_loaded_files GROUP BY 1)
      #     SELECT d, n, (SELECT count(*) FROM vehicle_positions v
      #       WHERE v.collected_at >= m.d AND v.collected_at < m.d + 1) AS rows
      #     FROM m WHERE d > '2026-08-22' AND (SELECT count(*) FROM vehicle_positions v
      #       WHERE v.collected_at >= m.d AND v.collected_at < m.d + 1) > n ORDER BY d"
      # If it returns any row, do not use --include-unmanifested-vp until that is explained.
      # (The tool also never deletes unmanifested rows on/after the archive's first day, and
      # the dry run lists any such day — including days with no manifest row at all — as
      # "never deleted".)
      psql -d $DB -c "\copy (SELECT * FROM vehicle_positions WHERE collected_at < '2026-08-23') TO PROGRAM 'zstd -T0 > ~/backups/${A}_vp_vm_era.csv.zst' CSV HEADER"
      ```

      The VP copy writes several GB locally and reads ~44M rows; the
      recommended path is **not** to delete the VM-era VP at all (it is
      ~10 GB WMATA / ~1 GB SFMTA, and for WMATA 8/02–8/22 and SFMTA the DB
      may be the only verified copy). Use the flag only if the space is
      really needed.

   c. Upload and **verify before deleting anything**:

      ```bash
      for f in ~/backups/${A}_*_before_$C.csv.zst ~/backups/${A}_vp_vm_era.csv.zst; do
        [ -e "$f" ] || continue   # the VP copy exists only if you made it
        aws s3 cp "$f" s3://wmata-dashboard-backups/retention-exports/
        echo "$f local=$(stat -f%z "$f") s3=$(aws s3api head-object --bucket wmata-dashboard-backups \
          --key retention-exports/$(basename "$f") --query ContentLength --output text)"
        echo "  rows in file: $(zstd -dc "$f" | tail -n +2 | wc -l)"
      done
      psql -d $DB -Atc "SELECT (SELECT count(*) FROM stop_events WHERE service_date < '$C'), (SELECT count(*) FROM runs WHERE service_date < '$C')"
      # with the VP copy:
      psql -d $DB -Atc "SELECT count(*) FROM vehicle_positions WHERE collected_at < '2026-08-23'"
      ```

      Sizes must match, and each file's row count must equal the matching
      live count printed by the `psql` lines (and the dry run's totals for the
      dates it marks deletable plus the dates it keeps — the copy is of
      everything below C). Rows containing newlines in a CSV field would
      make `wc -l` overcount; none of these tables has free-text columns
      that do.

4. **SFMTA retention** (smallest first): `uv run python -m pipelines.apply_retention --agency sfmta --apply --vacuum`.
5. **WMATA retention, default guards**: `uv run python -m pipelines.apply_retention --agency wmata --apply --vacuum`
   (today: VP manifested days + `trip_update_state` only).
6. **Optional, only after step 3**: rerun 4/5 with `--allow-unrederivable`
   (~23 GB WMATA stop-level after the backfill) and, only if really
   needed, `--include-unmanifested-vp`.
7. **GTFS de-dup (no backfill)**:

   ```bash
   uv run python scripts/dedup_gtfs_snapshots.py --agency wmata            # content fingerprints (reads all stop_times)
   uv run python scripts/dedup_gtfs_snapshots.py --agency wmata export --out-dir ~/gtfs-db-export
   aws s3 cp ~/gtfs-db-export/ s3://wmata-dashboard-backups/retention-exports/gtfs-db-export/wmata/ --recursive --exclude '*' --include '*.zip'
   aws s3 ls s3://wmata-dashboard-backups/retention-exports/gtfs-db-export/wmata/   # sizes == ls -l ~/gtfs-db-export
   uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --prove    # exact comparison only
   uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --apply
   ```

   then the same for `--agency sfmta`.
8. **Space.** `--vacuum` / the de-dup's suggested `VACUUM (ANALYZE)` are
   plain VACUUMs: no exclusive lock, the freed pages become reusable by
   later inserts, but the files do not shrink. That is enough for
   `stop_events`, `runs`, `vehicle_positions` and `trip_update_state`,
   which refill their free space daily. Use `VACUUM FULL` only when the
   disk itself is needed back and the table will not refill soon — mainly
   `stop_times` after the de-dup (~24 GB → ~6–8 GB live), or `stop_events`
   after a large `--allow-unrederivable` cut. `VACUUM FULL` rewrites the
   table: it holds **ACCESS EXCLUSIVE** for the whole rewrite (the API,
   the refresh and every read of that table block), and needs free disk
   roughly equal to the table's *live* size plus its indexes while it
   runs. Check first, then run **(no backfill, API stopped)**:

   ```bash
   psql -d wmata_dashboard -c "SELECT pg_size_pretty(pg_total_relation_size('stop_times'))"
   df -h ~
   psql -d wmata_dashboard -c "VACUUM (FULL, ANALYZE, VERBOSE) stop_times"
   ```

9. **Opt in to retention after every refresh** (only once 4–7 look
   right): set `retention.apply_after_refresh: true` (a YAML boolean,
   unquoted — anything else is rejected) in the agency YAML, or pass
   `bin/pull-and-derive.sh --apply-retention`. It runs
   `apply_retention --apply --vacuum` after the end-of-run housekeeping
   for each agency that processed dates without a failure — default
   guards only (never the override flags). Note: a date older than the
   stop-level window re-derived with `--date D` in the same run is pruned
   again by that retention step (its aggregates are what is kept); leave
   the hook off for such runs if you want the detail to stay.
10. **Remove the stale LaunchAgents** (never loaded; their jobs are gone or
    superseded):

    ```bash
    launchctl bootout gui/$UID/com.wmata-dashboard.daily-batch 2>/dev/null
    launchctl bootout gui/$UID/com.wmata-dashboard.retain-trip-update-state 2>/dev/null
    rm ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist \
       ~/Library/LaunchAgents/com.wmata-dashboard.retain-trip-update-state.plist
    ```

**Retired by this PR:** `pipelines/window_derived_tables.py` (flat
365-day today-anchored delete whose `--dry-run` ran the DELETE) and
`pipelines/retain_trip_update_state.py` (unreferenced; superseded by
`cleanup_trip_update_state`). `pipelines/archive_vehicle_positions.py`
stays only because `load_vp_from_parquet.py` imports its constants; its
CLI now refuses to run (it would delete VP by `DATE(timestamp)` and
duplicate an archive S3 already holds).

## 13. Stateless collector (nano) — live topology

**Status:** this section documents the stateless-collector rewrite, shipped
across three PRs (#222 VM side, #223 VP loader + manifest + reload gate,
and the cutover PR that retired the tunnel path —
`docs/superpowers/specs/2026-08-22-stateless-collector-design.md` is the
authoritative design). The ≥1-week parallel-run verification against the
old `wmata-data` VM (spec decision 3) ran 2026-08-22→28 and returned a GO
verdict — see the cutover PR body for the full O1/O2 numbers. This is now
the sole live collector path; §1–§11 above describe the outgoing VM,
pending its O3 decommission.

`scripts/stateless_collector.py --agency {wmata,sfmta}` (Tasks 1–4) is a
DB-free poll → zstd JSONL → S3 loop — no Postgres, no timers beyond the
loop itself. One process per agency, run under the templated unit
`deployment/systemd/collector@.service` as `collector@wmata.service` /
`collector@sfmta.service`.

### 13.1 Provision the instance

Same high-level checklist as §2, sized down:

1. Create a fresh Lightsail `nano_3_0` instance ($5/mo, 512 MB RAM,
   20 GB SSD, us-east-1, Ubuntu LTS) — **do not** reuse or resize the
   existing `wmata-data` VM; Lightsail cannot downsize an instance in
   place, and the ≥1-week parallel-run verification (§13 status note
   above — completed 2026-08-22→28, GO) depended on the old VM staying
   untouched for its duration.
2. Restrict the Lightsail firewall to SSH (22) only.
3. Follow §2.1–§2.4 for SSH key setup, the `wmata` service account
   (no interactive login, no sudo), system packages, and `uv`.
4. No PostgreSQL install — this box never runs a database.

### 13.2 Deploy the repo

```bash
su - wmata
git clone <repo-url> /home/wmata/wmata-dashboard
cd /home/wmata/wmata-dashboard
uv sync   # boto3 + zstandard are core deps — no --extra needed here
mkdir -p archive logs
exit
```

### 13.3 Per-agency environment files: `.env.wmata` / `.env.sfmta`

Unlike the legacy collector's single shared `.env` (§9's "restart both
units" lesson), the templated unit reads
`EnvironmentFile=/home/wmata/wmata-dashboard/.env.%i` — a **separate**
file per agency instance, so rotating one agency's credentials can't
silently leave the other running on stale ones.

`.env.wmata`:
```bash
WMATA_API_KEY=<key>
COLLECTOR_HEALTHCHECK_URL=<healthchecks.io ping URL — see 13.5>
AWS_ACCESS_KEY_ID=<AKIA...>
AWS_SECRET_ACCESS_KEY=<secret>
AWS_DEFAULT_REGION=us-east-1
```

`.env.sfmta`:
```bash
SFMTA_API_KEY=<511.org token>
SFMTA_COLLECTOR_HEALTHCHECK_URL=<healthchecks.io ping URL — see 13.5>
AWS_ACCESS_KEY_ID=<AKIA...>
AWS_SECRET_ACCESS_KEY=<secret>
AWS_DEFAULT_REGION=us-east-1
```

Both files can share the same AWS key. Create a dedicated, least-privilege
IAM user for this box (don't reuse `wmata-vm-backup` — this key lives on a
less-hardened, internet-facing collector box) scoped to `PutObject` on the
`raw-jsonl-archive/*` prefixes, extending
`deployment/aws/s3-backup-policy.json`'s pattern (that file currently
grants `wmata-vm-backup` `PutObject`/`GetObject` on `wmata-db-backups/*`
and `wmata-vp-archive/*` — add an analogous statement scoped to
`raw-jsonl-archive/*`, which covers the existing WMATA-root and `sfmta/`
TU prefixes plus the new `vp/` and `sfmta_vp/` VP prefixes from the
spec's §2 S3 layout).

```bash
chmod 600 /home/wmata/wmata-dashboard/.env.wmata /home/wmata/wmata-dashboard/.env.sfmta
```

### 13.4 Install the unit

Follow **`docs/DEPLOY.md` §2** ("Deploy with systemd unit changes") for
the canonical copy-to-`/etc/systemd/system/` + `daemon-reload` +
enable/start sequence — units are user-installed; `git pull` on the box
does **not** update an installed unit, so skipping this step silently
no-ops (the same NOTES-77 lesson as §11.3's SFMTA note).

```bash
REPO=/home/wmata/wmata-dashboard
sudo cp ${REPO}/deployment/systemd/collector@.service /etc/systemd/system/

sudo mkdir -p /var/log/wmata
sudo chown wmata:wmata /var/log/wmata

sudo systemctl daemon-reload
sudo systemctl enable --now collector@wmata collector@sfmta
```

Verify:
```bash
systemctl status collector@wmata collector@sfmta
journalctl -u collector@wmata -f
journalctl -u collector@sfmta -f
```

During the ≥1-week parallel-run overlap with the old VM (2026-08-22→28,
see the status note at the top of this section for the O1/O2 result),
both instances' memory RSS on this box was recorded periodically — the
nano's 512 MB is shared between them (observed steady-state: 74 MB
`collector@wmata`, 68 MB `collector@sfmta`):

```bash
systemctl status collector@wmata collector@sfmta   # ActiveState + Memory
# or, for a fuller per-process breakdown:
smem -tk -P collector
```

If the two instances' combined RSS crowds the nano's 512 MB budget,
swap to a `micro_3_0` instance ($7/mo) before decommissioning the old
VM — don't let a resource squeeze force an early cutover.

### 13.5 healthchecks.io checks

Create **two new checks** (one per agency — independent of any existing
WMATA collector check tied to the old VM in §1–§11), each configured with
**period 5 min / grace 45 min**:

- **Period 5 min** because `PingGate` (`src/stateless_poller.py`)
  rate-limits pings to once per 300 s once both of that agency's feeds
  are shipping fresh data (`min_gap_sec=300`) — matching the check's
  period to the actual ping cadence avoids a check that reads as
  chronically "just barely" green.
- **Grace 45 min** bounds worst-case detection at ≈50 min for a total
  collector outage (a ping was due right when the outage started, plus
  the 45-min grace) and ≈80 min for a single-feed wedge (`PingGate`'s
  20-minute freshness window — `freshness_sec=1200` — must also elapse
  before the still-healthy feed stops counting as "both fresh" and pings
  actually stop, on top of the same grace).
- The 45-min grace also exists so **deploys don't page**: after a
  `collector@` start or restart, the first ping only arrives once the
  first 15-minute rotation completes and its file finishes uploading —
  in practice ~15–20 min, not immediately. A shorter grace would
  false-page on every routine restart.
- **The dead-man cannot distinguish "collector wedged" from "S3
  unreachable for >5 min"** — both silence the ping identically. This is
  intended, not a gap: the alert means "raw data is not reaching S3,"
  and either underlying cause needs the same first response (SSH in,
  `journalctl -u collector@<agency>`, then check S3
  reachability/credentials/bucket).
- **An empty-but-healthy feed window can page** — e.g. an owl-hours
  window with zero active vehicles produces no new rows, so the writer
  never rotates a file and the upload cycle has nothing to ship, which
  means no ping and, once the grace elapses, a page even though the
  collector is working correctly. Accepted for now; tracked as
  [issue #248](https://github.com/wlovotti/wmata-dashboard/issues/248).

Set each check's ping URL into `COLLECTOR_HEALTHCHECK_URL` /
`SFMTA_COLLECTOR_HEALTHCHECK_URL` in the matching `.env.<agency>` file,
then `sudo systemctl restart collector@wmata collector@sfmta` to pick it
up — env files are read once at process start, the same restart-required
caveat as §9.

---

**Last Updated:** 2026-08-28
**Deployment Cost:** ~$5/mo (the `nano_3_0` collector box; S3 negligible at
this scale) once the old `wmata-data` VM (§1–§11, ~$17/mo — $12 instance +
~$5 block disk) is decommissioned (O3); both run in parallel, ~$22/mo
combined, until then.
