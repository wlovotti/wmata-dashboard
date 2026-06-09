# VM Deploy Runbook

Code deployment to the production Lightsail VM (static IP `52.54.130.186`,
AWS Lightsail us-east-1). SSH key: `~/.ssh/id_ed25519`. After a laptop reboot run
`ssh-add --apple-use-keychain ~/.ssh/id_ed25519` before connecting.

**Two users — use `wmata`, not `ubuntu`.** `ubuntu` is the Lightsail image's
default sudo account, used only for first-time provisioning. The app, the repo,
and the collector are owned by a dedicated `wmata` system user (SSH keys were
copied to it during provisioning, so `ssh wmata@52.54.130.186` works directly).
**Every command below runs as `wmata`** — running `git pull` as `ubuntu` would
fail against the `wmata`-owned repo. This matches `docs/DEPLOYMENT.md` §9.

Repo path on VM: `/home/wmata/wmata-dashboard`

---

## 0. Before you deploy — know the live SHA

```bash
ssh wmata@52.54.130.186 \
  'git -C /home/wmata/wmata-dashboard rev-parse HEAD'
```

Record this SHA. If the deploy goes wrong, this is what you roll back to.

---

## 1. Normal deploy (code change only — no systemd unit changes)

All commands run **on the VM** unless noted.

```bash
ssh wmata@52.54.130.186
cd /home/wmata/wmata-dashboard

# Confirm the tree is clean (there should be no local edits on the VM)
git status

# Pull from main
git pull origin main

# Sync Python dependencies (in case pyproject.toml changed)
uv sync --extra postgres

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
reload systemd before restarting.

```bash
ssh wmata@52.54.130.186
cd /home/wmata/wmata-dashboard

git pull origin main
uv sync --extra postgres

# Copy whichever unit files changed:
sudo cp deployment/systemd/wmata-collector.service    /etc/systemd/system/
sudo cp deployment/systemd/wmata-metrics.service      /etc/systemd/system/
sudo cp deployment/systemd/wmata-metrics.timer        /etc/systemd/system/
sudo cp deployment/systemd/wmata-backup.service       /etc/systemd/system/
sudo cp deployment/systemd/wmata-backup.timer         /etc/systemd/system/
sudo cp deployment/systemd/wmata-archive-positions.service  /etc/systemd/system/
sudo cp deployment/systemd/wmata-archive-positions.timer    /etc/systemd/system/
sudo cp deployment/systemd/wmata-window-derived.service     /etc/systemd/system/
sudo cp deployment/systemd/wmata-window-derived.timer       /etc/systemd/system/

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
ssh wmata@52.54.130.186
cd /home/wmata/wmata-dashboard

# Stop the broken collector first (SIGINT for a clean shutdown)
sudo systemctl stop wmata-collector.service

# Check out the last known-good SHA (recorded in §0)
git checkout <prev-sha>

# If the bad deploy included unit file changes, also restore the old units:
sudo cp deployment/systemd/wmata-collector.service /etc/systemd/system/
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
# On the VM:
cd /home/wmata/wmata-dashboard
uv run python scripts/collector_status.py
```

Expected healthy output: process alive, sleep-state normal, recent
`vehicle_positions` rows arriving, no log-level ERROR lines.

If running from the laptop (tunnel not needed for status):

```bash
ssh wmata@52.54.130.186 \
  'cd /home/wmata/wmata-dashboard && uv run python scripts/collector_status.py'
```

---

## 5. Check current live SHA at any time

```bash
ssh wmata@52.54.130.186 \
  'git -C /home/wmata/wmata-dashboard rev-parse HEAD'
```

---

## References

- **Systemd units:** `deployment/systemd/`
- **Full cloud ops runbook:** `docs/DEPLOYMENT.md`
- **Collector health check:** `scripts/collector_status.py`
- **VM:** AWS Lightsail us-east-1, static IP `52.54.130.186`. Provisioned via
  the `ubuntu` sudo account; app/repo/collector owned by the `wmata` user — all
  deploy work SSHes in as `wmata`. SSH key `~/.ssh/id_ed25519`.
