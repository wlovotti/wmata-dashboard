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
| PostgreSQL | **14** (must match local 14.x — `src/database.py` / `pyproject.toml`) |
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
# SSH in as the default Lightsail user (usually 'ubuntu')
ssh -i ~/.ssh/id_ed25519 ubuntu@<INSTANCE_IP>

# Create application user
adduser wmata
usermod -aG sudo wmata
rsync --archive --chown=wmata:wmata ~/.ssh /home/wmata

# Disable password login
sudo sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo systemctl reload sshd

# Verify firewall allows only SSH (Lightsail's built-in firewall handles ingress;
# optionally add ufw as a second layer for defense-in-depth)
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

### 2.5 PostgreSQL 14 — install and move PGDATA to attached disk

```bash
# Install PostgreSQL 14
sudo apt install -y postgresql-14 postgresql-client-14

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
sudo mv /var/lib/postgresql/14/main /mnt/pgdata/
sudo ln -s /mnt/pgdata/main /var/lib/postgresql/14/main
sudo chown -h postgres:postgres /var/lib/postgresql/14/main

# Update postgresql.conf if data_directory is set explicitly
sudo -u postgres psql -c "SHOW data_directory;"   # verify

sudo systemctl start postgresql
sudo systemctl status postgresql
```

### 2.6 Configure pg_hba.conf for tunnel-only access

Postgres should only accept connections from localhost (the SSH tunnel delivers
all client connections through localhost). Edit
`/etc/postgresql/14/main/pg_hba.conf` and ensure only `127.0.0.1/32` (IPv4)
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
pg_dump -Fc -d wmata_dashboard | ssh wmata@<INSTANCE_IP> \
  'pg_restore -U wmata -d wmata_dashboard -Fc'
# Expect ~1–3 hours at typical consumer upload speeds for ~28 GB.

# 3. On the VM — verify row counts match laptop:
#    (run same queries on both sides and compare)
ssh wmata@<INSTANCE_IP> 'psql -U wmata -d wmata_dashboard \
  -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 20;"'

# 4. Move WMATA API key to VM .env
ssh wmata@<INSTANCE_IP> 'nano /home/wmata/wmata-dashboard/.env'
# Set: WMATA_API_KEY=<key>
#      DATABASE_URL=postgresql://wmata:<password>@localhost/wmata_dashboard

# 5. Start collector on VM under systemd
ssh wmata@<INSTANCE_IP> 'sudo systemctl enable --now wmata-collector.service'
ssh wmata@<INSTANCE_IP> 'journalctl -u wmata-collector -f'
# Verify vehicle_positions row count is climbing.

# 6. From laptop — open the SSH tunnel and point local API at VM
ssh -N -L 5432:localhost:5432 wmata@<INSTANCE_IP> &
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

### 5.2 S3 backup configuration

Set `S3_BACKUP_BUCKET` in `/home/wmata/wmata-dashboard/.env`:

```bash
S3_BACKUP_BUCKET=your-s3-bucket-name
```

The backup script (`deployment/scripts/backup_db.sh`) uploads to S3 only when
this variable is set. The `aws` CLI must be installed and credentials
configured (e.g., via an IAM role attached to the Lightsail instance, or an
IAM user access key in `~/.aws/credentials` as the `wmata` user).

```bash
# Install AWS CLI
sudo apt install -y awscli

# Test S3 access
aws s3 ls s3://your-s3-bucket-name/
```

Add an S3 lifecycle rule to expire old dumps (recommended: expire objects
with prefix `wmata-db-backups/` after 30–90 days).

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

---

## 6. Backup / restore drill

**Create a backup manually:**
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

**Check all service statuses:**
```bash
systemctl status wmata-collector
systemctl list-timers --all | grep wmata
journalctl -u wmata-collector --since "1 hour ago"
```

**Restart collector (e.g., after a code update):**
```bash
git -C /home/wmata/wmata-dashboard pull origin main
uv -C /home/wmata/wmata-dashboard sync --extra postgres
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

**Last Updated:** 2026-06-03
**Deployment Cost:** ~$17/mo ($12 instance + ~$5 block disk; S3 negligible at this scale)
