# AWS Lightsail Operator Runbook

WMATA Dashboard — cloud migration Phase 1 (NOTES-48).

**Authoritative design spec:** `docs/superpowers/specs/2026-05-28-cloud-migration-phase1-design.md`
This runbook is a field-operator summary derived from the spec. The spec is the
canonical record of *why* each decision was made — read §3 before deviating
from anything here.

**Status:** infrastructure deployed once live migration completes (NOTES-48 is
open). This document covers the target topology; the live cutover steps are in
the spec §5 runbook.

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
| DB access from laptop | SSH tunnel only: `ssh -L 5432:localhost:5432 <vm>` |

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
ssh -N -L 5432:localhost:5432 ubuntu@<INSTANCE_IP> &
# In .env on laptop:
#   DATABASE_URL=postgresql://wmata:<password>@localhost/wmata_dashboard
# Then restart the local API:
uv run uvicorn api.main:app --reload
```

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
- **NOTES-48** (open until live cutover), **NOTES-49**, **NOTES-50** (later phases)

---

**Last Updated:** 2026-06-10
**Deployment Cost:** ~$17/mo ($12 instance + ~$5 block disk; S3 negligible at this scale)
