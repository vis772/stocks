#!/usr/bin/env bash
# =============================================================================
# Axiom Terminal — EC2 bootstrap script
# Target: Ubuntu 24.04 LTS, t3.micro (or any EC2 instance)
# Run as root or with sudo:  sudo bash setup_ec2.sh
# =============================================================================
set -euo pipefail

REPO_URL="https://github.com/vis772/stocks.git"
APP_DIR="/home/ubuntu/axiom"
APP_USER="ubuntu"

echo "================================================================"
echo "  Axiom Terminal — EC2 Setup"
echo "================================================================"

# ── 1. System update ──────────────────────────────────────────────────────────
echo "[1/6] Updating system packages..."
apt-get update -y
apt-get upgrade -y

# ── 2. Install dependencies ───────────────────────────────────────────────────
echo "[2/6] Installing Docker, Compose plugin, git, curl, ufw..."
apt-get install -y --no-install-recommends \
    docker.io \
    docker-compose-plugin \
    git \
    curl \
    ufw

# ── 3. Enable Docker and add ubuntu to the docker group ──────────────────────
echo "[3/6] Enabling Docker service and adding '$APP_USER' to docker group..."
systemctl enable docker
systemctl start docker
usermod -aG docker "$APP_USER"

# ── 4. Configure UFW firewall ─────────────────────────────────────────────────
echo "[4/6] Configuring UFW firewall rules..."
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp    comment "SSH"
ufw allow 80/tcp    comment "HTTP"
ufw allow 443/tcp   comment "HTTPS"
ufw allow 8501/tcp  comment "Streamlit dashboard"
ufw --force enable
ufw status verbose

# ── 5. Clone the repository ───────────────────────────────────────────────────
echo "[5/6] Cloning Axiom Terminal repository..."
if [ -d "$APP_DIR/.git" ]; then
    echo "  Repo already exists at $APP_DIR — pulling latest changes..."
    git -C "$APP_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$APP_DIR"
    chown -R "$APP_USER:$APP_USER" "$APP_DIR"
fi

# ── 6. Done — print next steps ────────────────────────────────────────────────
echo "[6/6] Setup complete."
echo ""
echo "================================================================"
echo "  NEXT STEPS"
echo "================================================================"
echo ""
echo "  1. Copy your .env file to the server:"
echo "       scp .env ubuntu@<your-ec2-ip>:$APP_DIR/.env"
echo ""
echo "  2. (Optional) Install the systemd service for auto-start on reboot:"
echo "       sudo cp $APP_DIR/scripts/axiom.service /etc/systemd/system/"
echo "       sudo systemctl daemon-reload"
echo "       sudo systemctl enable axiom"
echo "       sudo systemctl start axiom"
echo ""
echo "  3. Or start manually (you may need to log out and back in first"
echo "     for docker group membership to take effect):"
echo "       cd $APP_DIR"
echo "       docker compose up -d"
echo ""
echo "  4. Check logs:"
echo "       docker compose -f $APP_DIR/docker-compose.yml logs -f"
echo ""
echo "  Note: Make this script executable before running:"
echo "        chmod +x $APP_DIR/scripts/setup_ec2.sh"
echo "================================================================"
