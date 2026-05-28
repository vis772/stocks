#!/bin/bash
# setup_cron.sh — One-time setup for update_status.sh cron job.
# Run this once on EC2 after deploying the repo.
# Sets up git credentials, makes scripts executable, registers the cron entry.

set -euo pipefail

REPO_DIR="/home/ubuntu/axiom"
UPDATE_SCRIPT="$REPO_DIR/update_status.sh"
LOG_FILE="/tmp/update_status.log"
BRANCH="clean-combined-version"

echo ""
echo "=================================================="
echo " Axiom Terminal — Status Update Cron Setup"
echo "=================================================="
echo ""

# ── Validate repo dir ──────────────────────────────────────────────────────────
if [ ! -d "$REPO_DIR" ]; then
    echo "ERROR: Repo directory not found: $REPO_DIR"
    echo "       Run this script from the EC2 instance after cloning."
    exit 1
fi

if [ ! -f "$UPDATE_SCRIPT" ]; then
    echo "ERROR: update_status.sh not found at $UPDATE_SCRIPT"
    echo "       Pull the latest code first: cd $REPO_DIR && git pull"
    exit 1
fi

# ── Make scripts executable ────────────────────────────────────────────────────
chmod +x "$UPDATE_SCRIPT"
chmod +x "$REPO_DIR/setup_cron.sh"
echo "✅ Scripts made executable"

# ── Git identity ───────────────────────────────────────────────────────────────
GIT_EMAIL=$(git -C "$REPO_DIR" config --global user.email 2>/dev/null || true)
if [ -z "$GIT_EMAIL" ]; then
    echo ""
    echo "Git identity not configured. Setting up now..."
    git config --global user.email "axiom@ec2.local"
    git config --global user.name "Axiom Terminal"
    echo "✅ Git identity set (axiom@ec2.local)"
else
    echo "✅ Git identity already configured: $GIT_EMAIL"
fi

# ── Git credentials for non-interactive push ──────────────────────────────────
REMOTE_URL=$(git -C "$REPO_DIR" remote get-url origin 2>/dev/null || echo "")
echo ""
echo "Remote URL: $REMOTE_URL"

if echo "$REMOTE_URL" | grep -q "https://"; then
    echo ""
    echo "── GitHub Credentials ──────────────────────────────────────"
    echo "   HTTPS remote detected. A Personal Access Token (PAT) is"
    echo "   required for non-interactive git push from cron."
    echo ""
    echo "   Create a PAT at: https://github.com/settings/tokens"
    echo "   Required scope: repo (or just 'Contents: Read and Write')"
    echo ""
    read -p "   GitHub username: " GH_USER
    read -s -p "   GitHub PAT: " GH_TOKEN
    echo ""

    # Store credentials
    git config --global credential.helper store
    printf "https://%s:%s@github.com\n" "$GH_USER" "$GH_TOKEN" > ~/.git-credentials
    chmod 600 ~/.git-credentials
    echo "✅ Git credentials stored in ~/.git-credentials (chmod 600)"

    # Verify by doing a dry-run ls-remote
    echo "   Verifying credentials..."
    if git -C "$REPO_DIR" ls-remote origin HEAD > /dev/null 2>&1; then
        echo "✅ GitHub credentials verified — push will work"
    else
        echo "⚠️  WARNING: Could not verify credentials. Check your PAT and try again."
    fi

elif echo "$REMOTE_URL" | grep -q "git@"; then
    echo "✅ SSH remote — no credential setup needed"
else
    echo "⚠️  WARNING: Unrecognised remote format. Push may fail."
fi

# ── Cron entry ─────────────────────────────────────────────────────────────────
# 10 AM ET = 14:00 UTC during EDT (Mar–Nov) = 15:00 UTC during EST (Nov–Mar)
# This entry uses 14:00 UTC (EDT / summer). Update to 15:00 in November.
echo ""
echo "── Cron Registration ───────────────────────────────────────"
CRON_ENTRY="0 14 * * 1-5 /bin/bash $UPDATE_SCRIPT >> $LOG_FILE 2>&1"

EXISTING_CRON=$(crontab -l 2>/dev/null || true)

if echo "$EXISTING_CRON" | grep -qF "$UPDATE_SCRIPT"; then
    echo "ℹ️  Cron entry already exists — not duplicating."
    echo ""
    echo "   Existing entry:"
    echo "$EXISTING_CRON" | grep "$UPDATE_SCRIPT"
else
    # Append new entry
    (
        echo "$EXISTING_CRON"
        echo ""
        echo "# Axiom Terminal — instance status update (10 AM EDT / 14:00 UTC, weekdays)"
        echo "$CRON_ENTRY"
    ) | crontab -
    echo "✅ Cron entry registered:"
    echo "   $CRON_ENTRY"
fi

# ── Show full crontab ──────────────────────────────────────────────────────────
echo ""
echo "── Current crontab ─────────────────────────────────────────"
crontab -l
echo ""

# ── Log file ───────────────────────────────────────────────────────────────────
touch "$LOG_FILE"
chmod 664 "$LOG_FILE"
echo "✅ Log file ready: $LOG_FILE"

# ── Optional: run now ──────────────────────────────────────────────────────────
echo ""
read -p "Run update_status.sh now to generate the first report? [Y/n] " RUN_NOW
RUN_NOW="${RUN_NOW:-Y}"

if [[ "$RUN_NOW" =~ ^[Yy]$ ]]; then
    echo ""
    echo "── Running update_status.sh ────────────────────────────────"
    bash "$UPDATE_SCRIPT"
    echo ""
    echo "── INSTANCE_STATUS.md preview ──────────────────────────────"
    cat "$REPO_DIR/INSTANCE_STATUS.md"
fi

echo ""
echo "=================================================="
echo " Setup complete."
echo ""
echo " Schedule: weekdays at 10 AM EDT (14:00 UTC)"
echo "           ⚠️  Change to 15:00 UTC in November (EST)"
echo " Log:      $LOG_FILE"
echo " Status:   $REPO_DIR/INSTANCE_STATUS.md"
echo " Branch:   $BRANCH"
echo "=================================================="
echo ""
