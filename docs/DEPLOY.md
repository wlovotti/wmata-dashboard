# VM Deploy Runbook

Code deployment to the production Lightsail VM (static IP `52.54.130.186`,
AWS Lightsail us-east-1). SSH key: `~/.ssh/id_ed25519`. After a laptop reboot run
`ssh-add --apple-use-keychain ~/.ssh/id_ed25519` before connecting.

**Access model — always SSH as `ubuntu`, use `sudo -u wmata` for service ops.**
`wmata` is a service account (owns the repo, collector, and data) that has no
interactive SSH login and no sudo rights — it cannot be SSH'd into directly.
Humans log in as `ubuntu` (the Lightsail sudo account) and assume the service
identity for individual operations:

```bash
# Example: run collector_status as wmata
ssh ubuntu@52.54.130.186 \
  'sudo -u wmata sh -c "cd /home/wmata/wmata-dashboard && .venv/bin/python scripts/collector_status.py"'
```

All deploy commands below run as `ubuntu` with `sudo -u wmata` where needed.
This matches `docs/DEPLOYMENT.md` §2.2.

Repo path on VM: `/home/wmata/wmata-dashboard`

---

## 0. Before you deploy — know the live SHA

```bash
ssh ubuntu@52.54.130.186 \
  'sudo -u wmata git -C /home/wmata/wmata-dashboard rev-parse HEAD'
```

Record this SHA. If the deploy goes wrong, this is what you roll back to.

---

## 1. Normal deploy (code change only — no systemd unit changes)

All commands run **on the VM** unless noted.

```bash
ssh ubuntu@52.54.130.186

# Confirm the tree is clean (there should be no local edits on the VM)
sudo -u wmata git -C /home/wmata/wmata-dashboard status

# Pull from main
sudo -u wmata git -C /home/wmata/wmata-dashboard pull origin main

# Sync Python dependencies (in case pyproject.toml changed)
sudo -u wmata sh -c 'cd /home/wmata/wmata-dashboard && uv sync --extra postgres'

# Restart the collector to pick up the new code.
# The collector is the only always-running service.
sudo systemctl restart wmata-collector.service

# Verify the restart succeeded and the process is healthy
sudo systemctl status wmata-collector.service
```

Then run the post-deploy smoke check (see §4).

---

## 2. Deploy with systemd unit changes

When a `.service` or `.timer` file under `deployment/systemd/` has changed,
you must copy the updated unit file(s) to `/etc/systemd/system/` **and**
reload systemd before restarting. See `docs/DEPLOYMENT.md` §2 for the
canonical §2 copy + daemon-reload + restart sequence.

```bash
ssh ubuntu@52.54.130.186

sudo -u wmata git -C /home/wmata/wmata-dashboard pull origin main
sudo -u wmata sh -c 'cd /home/wmata/wmata-dashboard && uv sync --extra postgres'

# Copy whichever unit files changed:
REPO=/home/wmata/wmata-dashboard
sudo cp ${REPO}/deployment/systemd/wmata-collector.service    /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-metrics.service      /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-metrics.timer        /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-backup.service       /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-backup.timer         /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-archive-positions.service  /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-archive-positions.timer    /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-window-derived.service     /etc/systemd/system/
sudo cp ${REPO}/deployment/systemd/wmata-window-derived.timer       /etc/systemd/system/

# Always reload after copying unit files
sudo systemctl daemon-reload

# Restart the services/timers whose units changed.
# For the collector (the critical always-running service):
sudo systemctl restart wmata-collector.service

# For timers (they fire on a schedule; reload is usually sufficient):
# sudo systemctl restart wmata-metrics.timer
# sudo systemctl restart wmata-backup.timer
# sudo systemctl restart wmata-archive-positions.timer
# sudo systemctl restart wmata-window-derived.timer

sudo systemctl status wmata-collector.service
```

**Unit names and their roles:**

| Unit | Type | Schedule / behavior |
|---|---|---|
| `wmata-collector.service` | persistent service | `Restart=always`; always running |
| `wmata-metrics.service` | oneshot | driven by timer |
| `wmata-metrics.timer` | timer | daily 02:00 ET — nightly batch |
| `wmata-backup.service` | oneshot | driven by timer |
| `wmata-backup.timer` | timer | Sun 01:00 ET — weekly pg_dump |
| `wmata-archive-positions.service` | oneshot | driven by timer |
| `wmata-archive-positions.timer` | timer | daily 04:00 ET — tier-3 VP archive |
| `wmata-window-derived.service` | oneshot | driven by timer |
| `wmata-window-derived.timer` | timer | daily 04:30 ET — stop_events/runs retention |

---

## 3. Roll back

If the deploy breaks the collector, revert to the previous SHA immediately to
minimise the data gap.

```bash
ssh ubuntu@52.54.130.186

# Stop the broken collector first (SIGINT for a clean shutdown)
sudo systemctl stop wmata-collector.service

# Check out the last known-good SHA (recorded in §0)
sudo -u wmata git -C /home/wmata/wmata-dashboard checkout <prev-sha>

# If the bad deploy included unit file changes, also restore the old units:
sudo cp /home/wmata/wmata-dashboard/deployment/systemd/wmata-collector.service /etc/systemd/system/
sudo systemctl daemon-reload

# Restart on the old code
sudo systemctl start wmata-collector.service
sudo systemctl status wmata-collector.service
```

Verify with the smoke check (§4), then open an issue to diagnose the breakage
before re-attempting the deploy.

---

## 4. Post-deploy smoke check

Run this after every deploy (or rollback):

```bash
# From the laptop (no tunnel needed for status):
ssh ubuntu@52.54.130.186 \
  'sudo -u wmata sh -c "cd /home/wmata/wmata-dashboard && .venv/bin/python scripts/collector_status.py"'
```

Expected healthy output: process alive, sleep-state normal, recent
`vehicle_positions` rows arriving, no log-level ERROR lines.

---

## 5. Check current live SHA at any time

```bash
ssh ubuntu@52.54.130.186 \
  'sudo -u wmata git -C /home/wmata/wmata-dashboard rev-parse HEAD'
```

---

## References

- **Systemd units:** `deployment/systemd/`
- **Full cloud ops runbook:** `docs/DEPLOYMENT.md`
- **Collector health check:** `scripts/collector_status.py`
- **Schema/data migration ritual:** `docs/MIGRATIONS.md`
- **VM:** AWS Lightsail us-east-1, static IP `52.54.130.186`. SSH as `ubuntu`
  (the sudo account); service ops use `sudo -u wmata`. SSH key `~/.ssh/id_ed25519`.
