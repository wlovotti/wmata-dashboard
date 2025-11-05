# DigitalOcean Deployment Guide

Complete step-by-step guide for deploying the WMATA Dashboard to DigitalOcean in production.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Phase 1: DigitalOcean Setup](#phase-1-digitalocean-setup)
- [Phase 2: Server Configuration](#phase-2-server-configuration)
- [Phase 3: Database Setup](#phase-3-database-setup)
- [Phase 4: Application Deployment](#phase-4-application-deployment)
- [Phase 5: Service Configuration](#phase-5-service-configuration)
- [Phase 6: Monitoring & Backups](#phase-6-monitoring--backups)
- [Maintenance](#maintenance)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

- WMATA API Key (get from https://developer.wmata.com/)
- SSH key pair for server access
- Email account for alerts (Gmail recommended)

---

## Phase 1: DigitalOcean Setup

### 1.1 Create Account and Claim Free Credit

1. Sign up at https://www.digitalocean.com
2. Verify email address
3. Add payment method (required for $200 credit)
4. Credit should be automatically applied (valid for 60 days)

### 1.2 Create SSH Key

On your local machine:
```bash
# Generate SSH key if you don't have one
ssh-keygen -t ed25519 -C "your_email@example.com"

# Copy public key to clipboard (macOS)
pbcopy < ~/.ssh/id_ed25519.pub

# Or display it to copy manually
cat ~/.ssh/id_ed25519.pub
```

In DigitalOcean:
1. Go to Settings → Security → SSH Keys
2. Click "Add SSH Key"
3. Paste your public key
4. Name it (e.g., "macbook-pro")

### 1.3 Create Droplet

1. Click "Create" → "Droplets"
2. **Choose Region**: New York 3 (NYC3) - closest to DC
3. **Choose Image**: Ubuntu 24.04 LTS x64
4. **Choose Size**: Basic → Regular → $4/month
   - 1 vCPU
   - 512 MB RAM
   - 10 GB SSD
   - 500 GB transfer
5. **Authentication**: Select your SSH key
6. **Hostname**: `wmata-dashboard`
7. **Tags**: `production`, `wmata`
8. **Backups**: Optional ($1/month extra)
9. Click "Create Droplet"

### 1.4 Note Your Droplet's IP Address

Once created, note the public IPv4 address (e.g., `206.189.123.45`)

---

## Phase 2: Server Configuration

### 2.1 Initial Server Access

```bash
# SSH into your droplet (replace with your IP)
ssh root@206.189.123.45
```

### 2.2 Create Non-Root User

```bash
# Create wmata user
adduser wmata

# Add to sudo group
usermod -aG sudo wmata

# Copy SSH keys to new user
rsync --archive --chown=wmata:wmata ~/.ssh /home/wmata
```

### 2.3 Configure Firewall

```bash
# Allow SSH
uv allow OpenSSH

# Enable firewall
ufw enable

# Check status
ufw status
```

### 2.4 Update System Packages

```bash
# Update package list
apt update

# Upgrade existing packages
apt upgrade -y

# Install essential packages
apt install -y git curl build-essential
```

### 2.5 Install Python 3.11+

```bash
# Install Python and pip
apt install -y python3.11 python3.11-venv python3-pip

# Verify installation
python3.11 --version
```

### 2.6 Install uv Package Manager

```bash
# Install uv for the wmata user
su - wmata
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add to PATH (add to ~/.bashrc for persistence)
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Verify installation
uv --version
exit  # Return to root
```

---

## Phase 3: Database Setup

### 3.1 Install PostgreSQL

```bash
# Install PostgreSQL 14
apt install -y postgresql postgresql-contrib

# Verify installation
systemctl status postgresql
```

### 3.2 Create Database and User

```bash
# Switch to postgres user
sudo -u postgres psql

# In PostgreSQL shell:
CREATE DATABASE wmata_dashboard;
CREATE USER wmata WITH ENCRYPTED PASSWORD 'your_secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE wmata_dashboard TO wmata;
\q
```

### 3.3 Configure PostgreSQL for Local Access

PostgreSQL is already configured for localhost connections. No changes needed.

### 3.4 Test Database Connection

```bash
# Test as wmata user
su - wmata
psql -U wmata -d wmata_dashboard -h localhost
# Enter password when prompted
# Type \q to exit
exit
```

---

## Phase 4: Application Deployment

### 4.1 Clone Repository

```bash
# Switch to wmata user
su - wmata
cd ~

# Clone the repository
git clone https://github.com/yourusername/wmata-dashboard.git
cd wmata-dashboard
```

### 4.2 Configure Environment Variables

```bash
# Copy example env file
cp .env.example .env

# Edit with your credentials
nano .env
```

Update the following values:
```bash
WMATA_API_KEY=your_actual_api_key_here
DATABASE_URL=postgresql://wmata:your_db_password@localhost/wmata_dashboard
```

Save and exit (Ctrl+X, then Y, then Enter)

**Important**: Secure the .env file:
```bash
chmod 600 .env
```

### 4.3 Install Dependencies

```bash
# Install with PostgreSQL support
uv sync --extra postgres --extra dev

# Verify installation
uv run python -c "import fastapi; import sqlalchemy; print('Dependencies OK')"
```

### 4.4 Initialize Database

```bash
# This will:
# - Create all database tables
# - Download and load GTFS static data
# Takes 5-10 minutes

uv run python scripts/init_database.py
```

### 4.5 Test Data Collection

```bash
# Collect a few cycles of test data
uv run python scripts/collect_sample_data.py all 5

# Verify data was collected
uv run python -c "
from src.database import get_session
from src.models import VehiclePosition
db = get_session()
count = db.query(VehiclePosition).count()
print(f'Vehicle positions collected: {count}')
db.close()
"
```

---

## Phase 5: Service Configuration

### 5.1 Install Systemd Service Files

```bash
# Still as wmata user, copy service files to system directory
exit  # Return to root

# Copy service files
cp /home/wmata/wmata-dashboard/deployment/systemd/wmata-collector.service /etc/systemd/system/
cp /home/wmata/wmata-dashboard/deployment/systemd/wmata-metrics.service /etc/systemd/system/
cp /home/wmata/wmata-dashboard/deployment/systemd/wmata-metrics.timer /etc/systemd/system/

# Reload systemd to recognize new services
systemctl daemon-reload
```

### 5.2 Create Log Directory

```bash
# Create log directory
mkdir -p /var/log/wmata
chown wmata:wmata /var/log/wmata
```

### 5.3 Enable and Start Collector Service

```bash
# Enable service to start on boot
systemctl enable wmata-collector.service

# Start the collector
systemctl start wmata-collector.service

# Check status
systemctl status wmata-collector.service

# View logs
journalctl -u wmata-collector.service -f
# Press Ctrl+C to exit log view
```

### 5.4 Enable and Start Metrics Timer

```bash
# Enable timer to run daily
systemctl enable wmata-metrics.timer

# Start the timer
systemctl start wmata-metrics.timer

# Check timer status
systemctl list-timers --all | grep wmata

# Manually trigger metrics computation (optional)
systemctl start wmata-metrics.service

# Check metrics service status
systemctl status wmata-metrics.service
```

---

## Phase 6: Monitoring & Backups

### 6.1 Set Up Database Backups

```bash
# Make backup script executable
chmod +x /home/wmata/wmata-dashboard/deployment/scripts/backup_db.sh

# Create backups directory
mkdir -p /home/wmata/backups
chown wmata:wmata /home/wmata/backups

# Test backup script
su - wmata
~/wmata-dashboard/deployment/scripts/backup_db.sh
exit

# Add to cron (run daily at 3am)
crontab -e -u wmata
```

Add this line:
```
0 3 * * * /home/wmata/wmata-dashboard/deployment/scripts/backup_db.sh >> /var/log/wmata/backup.log 2>&1
```

### 6.2 Set Up External Monitoring (UptimeRobot)

1. Sign up at https://uptimerobot.com (free tier)
2. Click "Add New Monitor"
3. **Monitor Type**: HTTP(s)
4. **Friendly Name**: WMATA Dashboard Health
5. **URL**: `http://YOUR_DROPLET_IP:8000/health` (will add when API is running)
6. **Monitoring Interval**: 5 minutes
7. Click "Create Monitor"

Note: For now, you can monitor the collector by checking systemd status. API monitoring will be added when frontend is public.

### 6.3 Configure Email Alerts (Optional)

To receive email alerts when services fail:

```bash
# Install sendmail
apt install -y sendmail

# Create systemd drop-in directory
mkdir -p /etc/systemd/system/wmata-collector.service.d

# Create override file for email alerts
cat > /etc/systemd/system/wmata-collector.service.d/email-alert.conf << 'EOF'
[Unit]
OnFailure=email-alert@%n.service

[Service]
# Email on failure
EOF

# Reload systemd
systemctl daemon-reload
```

Create email alert service template:
```bash
cat > /etc/systemd/system/email-alert@.service << 'EOF'
[Unit]
Description=Send email alert for %i

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo "Service %i failed at $(date)" | sendmail your_email@example.com'
EOF
```

---

## Maintenance

### Daily Operations

**Check Service Status**:
```bash
# Check collector
systemctl status wmata-collector

# Check recent logs
journalctl -u wmata-collector --since "1 hour ago"

# Check data collection
su - wmata
cd wmata-dashboard
uv run python -c "
from datetime import datetime, timedelta
from src.database import get_session
from src.models import VehiclePosition
db = get_session()
one_hour_ago = datetime.utcnow() - timedelta(hours=1)
count = db.query(VehiclePosition).filter(VehiclePosition.timestamp >= one_hour_ago).count()
print(f'Vehicle positions in last hour: {count}')
db.close()
"
```

**Restart Services**:
```bash
# Restart collector (e.g., after code update)
systemctl restart wmata-collector

# Manually run metrics computation
systemctl start wmata-metrics
```

### Weekly Tasks

**Update Code**:
```bash
su - wmata
cd wmata-dashboard

# Pull latest changes
git pull origin main

# Install any new dependencies
uv sync --extra postgres

# Restart services
exit
systemctl restart wmata-collector
```

**Check Disk Usage**:
```bash
# Check overall disk usage
df -h

# Check database size
du -sh /var/lib/postgresql/

# Check backup size
du -sh /home/wmata/backups/
```

### Monthly Tasks

**Update System Packages**:
```bash
apt update
apt upgrade -y
```

**Review Backups**:
```bash
ls -lh /home/wmata/backups/
```

**Update GTFS Static Data** (if routes/schedules have changed):
```bash
su - wmata
cd wmata-dashboard
uv run python scripts/reload_gtfs_complete.py
exit
systemctl restart wmata-collector
```

---

## Troubleshooting

### Collector Service Won't Start

**Check logs**:
```bash
journalctl -u wmata-collector -n 50
```

**Common issues**:
- Missing WMATA_API_KEY in .env
- Database connection failed (check DATABASE_URL)
- Permission issues (check file ownership)

**Fix**:
```bash
# Verify .env file exists and has correct values
su - wmata
cat ~/wmata-dashboard/.env

# Test database connection
uv run python -c "from src.database import get_session; db = get_session(); print('DB OK'); db.close()"

# Check file permissions
ls -la ~/wmata-dashboard/.env
```

### No Data Being Collected

**Check API key validity**:
```bash
su - wmata
cd wmata-dashboard
uv run python -c "
from src.wmata_collector import WMATADataCollector
import os
from dotenv import load_dotenv
load_dotenv()
collector = WMATADataCollector(os.getenv('WMATA_API_KEY'))
vehicles = collector.get_realtime_vehicle_positions()
print(f'Fetched {len(vehicles)} vehicles')
"
```

### Database Connection Errors

**Check PostgreSQL is running**:
```bash
systemctl status postgresql
```

**Test manual connection**:
```bash
psql -U wmata -d wmata_dashboard -h localhost
```

**Reset database password** (if forgotten):
```bash
sudo -u postgres psql
ALTER USER wmata WITH PASSWORD 'new_password';
\q

# Update .env file with new password
```

### Out of Disk Space

**Find large files**:
```bash
du -sh /* | sort -h
```

**Clean up old backups manually**:
```bash
# Keep only last 7 days
find /home/wmata/backups/ -name "wmata_db_*.sql.gz" -mtime +7 -delete
```

**Clean up old logs**:
```bash
journalctl --vacuum-time=7d
```

### High Memory Usage

**Check processes**:
```bash
htop  # or: top
```

**Restart collector if needed**:
```bash
systemctl restart wmata-collector
```

---

## Next Steps: When Frontend is Ready

When you're ready to make the frontend public:

1. **Open firewall ports**:
```bash
ufw allow 80/tcp
ufw allow 443/tcp
```

2. **Install nginx**:
```bash
apt install -y nginx
```

3. **Get domain name** and point to droplet IP

4. **Set up SSL with Let's Encrypt**:
```bash
apt install -y certbot python3-certbot-nginx
certbot --nginx -d your-domain.com
```

5. **Configure nginx reverse proxy** (example config to create later)

6. **Create systemd service for API** (when serving frontend)

---

## Support

- **WMATA API Docs**: https://developer.wmata.com/docs/services/
- **DigitalOcean Docs**: https://docs.digitalocean.com/
- **Project Issues**: https://github.com/yourusername/wmata-dashboard/issues

---

**Last Updated**: 2025-11-04
**Deployment Cost**: $4/month (after $200 credit expires)
