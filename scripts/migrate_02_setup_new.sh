#!/usr/bin/env bash
# =============================================================================
# Axiom Terminal — Migration Step 2: Setup NEW g4dn.xlarge (us-east-1)
# Run on the NEW instance as root:  sudo bash migrate_02_setup_new.sh
#
# Before running:
#   1. Transfer your axiom_migration_*.tar.gz to this instance (see step 1 output)
#   2. Run: sudo bash ~/axiom/scripts/migrate_02_setup_new.sh ~/axiom_migration_*.tar.gz
#
# What this does:
#   - System update + dependencies
#   - NVIDIA driver + CUDA 12.x (T4 GPU on g4dn.xlarge)
#   - nvidia-container-toolkit (GPU passthrough to Docker)
#   - Docker + Docker Compose plugin
#   - Ollama + 4 models (qwen2.5:1.5b, phi4-mini, gemma3:1b, smollm2)
#   - Clone Axiom Terminal repo (clean-combined-version)
#   - Restore .env + Docker volumes + PostgreSQL (if local)
#   - Install Python deps
#   - Install systemd service (axiom.service)
#   - Start containers
# =============================================================================
set -euo pipefail

ARCHIVE="${1:-}"
REPO_URL="https://github.com/vis772/stocks.git"
REPO_BRANCH="clean-combined-version"
APP_DIR="/home/ubuntu/axiom"
APP_USER="ubuntu"
NVIDIA_DRIVER_VERSION="550"   # T4 supports up to CUDA 12.x; driver 550 is stable on Ubuntu 24.04

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠  $*"; }
die()  { echo "[$(date '+%H:%M:%S')] ✗ FATAL: $*" >&2; exit 1; }
section() {
    echo ""
    echo "[$(date '+%H:%M:%S')] ════════════════════════════════════════"
    echo "[$(date '+%H:%M:%S')]  $*"
    echo "[$(date '+%H:%M:%S')] ════════════════════════════════════════"
}

# ── Pre-flight ────────────────────────────────────────────────────────────────
[ "$(id -u)" -eq 0 ] || die "Must run as root: sudo bash $0 $*"

if [ -z "$ARCHIVE" ]; then
    die "Usage: sudo bash $0 <path-to-axiom_migration_*.tar.gz>"
fi
[ -f "$ARCHIVE" ] || die "Archive not found: $ARCHIVE"

log "================================================================"
log "  Axiom Terminal — New Instance Setup (g4dn.xlarge)"
log "  Archive   : $ARCHIVE"
log "  App dir   : $APP_DIR"
log "  Repo      : $REPO_URL ($REPO_BRANCH)"
log "================================================================"

# ── Extract archive ───────────────────────────────────────────────────────────
section "Extracting migration archive"
EXTRACT_DIR="/tmp/axiom_restore_$$"
mkdir -p "$EXTRACT_DIR"
tar xzf "$ARCHIVE" -C "$EXTRACT_DIR" --strip-components=1
ok "Archive extracted to $EXTRACT_DIR"

# Sanity-check .env is present
[ -f "$EXTRACT_DIR/.env" ] || die "No .env found in archive — backup may be corrupt"

# ── 1. System update ──────────────────────────────────────────────────────────
section "1/9 — System update"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get upgrade -y
apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    wget \
    git \
    ufw \
    ca-certificates \
    gnupg \
    lsb-release \
    linux-headers-$(uname -r) \
    python3 \
    python3-pip \
    python3-venv \
    postgresql-client \
    jq \
    htop \
    tmux
ok "System packages installed"

# ── 2. NVIDIA drivers + CUDA ──────────────────────────────────────────────────
section "2/9 — NVIDIA driver ${NVIDIA_DRIVER_VERSION} + CUDA 12 (T4 GPU)"

if nvidia-smi &>/dev/null; then
    ok "NVIDIA driver already installed — $(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)"
else
    log "Installing NVIDIA driver ${NVIDIA_DRIVER_VERSION}..."
    apt-get install -y --no-install-recommends \
        nvidia-driver-${NVIDIA_DRIVER_VERSION} \
        nvidia-utils-${NVIDIA_DRIVER_VERSION}

    # CUDA keyring from NVIDIA
    CUDA_KEYRING_DEB="cuda-keyring_1.1-1_all.deb"
    wget -q "https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/${CUDA_KEYRING_DEB}" \
        -O "/tmp/${CUDA_KEYRING_DEB}"
    dpkg -i "/tmp/${CUDA_KEYRING_DEB}"
    apt-get update -y
    apt-get install -y --no-install-recommends cuda-toolkit-12-4

    ok "NVIDIA driver + CUDA 12.4 installed"
    warn "NOTE: A reboot is required before nvidia-smi will work."
    warn "      The script will continue — verify GPU after reboot with: nvidia-smi"
fi

# ── 3. nvidia-container-toolkit (GPU → Docker passthrough) ────────────────────
section "3/9 — nvidia-container-toolkit"

if ! dpkg -l nvidia-container-toolkit &>/dev/null 2>&1; then
    curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
        | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

    curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
        | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
        | tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

    apt-get update -y
    apt-get install -y nvidia-container-toolkit
    ok "nvidia-container-toolkit installed"
else
    ok "nvidia-container-toolkit already installed"
fi

# ── 4. Docker + Compose ───────────────────────────────────────────────────────
section "4/9 — Docker + Compose plugin"

if ! command -v docker &>/dev/null; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        | tee /etc/apt/sources.list.d/docker.list

    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    ok "Docker installed"
else
    ok "Docker already installed — $(docker --version)"
fi

systemctl enable docker
systemctl start docker
usermod -aG docker "$APP_USER"

# Configure Docker to use NVIDIA runtime
nvidia-ctk runtime configure --runtime=docker
systemctl restart docker
ok "Docker NVIDIA runtime configured"

# ── 5. Ollama ─────────────────────────────────────────────────────────────────
section "5/9 — Ollama + 4 models"

if ! command -v ollama &>/dev/null; then
    log "Installing Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
    ok "Ollama installed"
else
    ok "Ollama already installed — $(ollama --version 2>/dev/null || echo 'unknown version')"
fi

# Ensure Ollama service is running
systemctl enable ollama 2>/dev/null || true
systemctl start ollama  2>/dev/null || true
sleep 3  # give it a moment to be ready

log "Pulling models (this may take several minutes)..."

MODELS=("qwen2.5:1.5b" "phi4-mini" "gemma3:1b" "smollm2")
for model in "${MODELS[@]}"; do
    log "  Pulling $model..."
    ollama pull "$model" || die "Failed to pull model: $model"
    ok "  $model ready"
done

log "Installed models:"
ollama list

# ── 6. Clone repository ───────────────────────────────────────────────────────
section "6/9 — Axiom Terminal repository"

if [ -d "$APP_DIR/.git" ]; then
    log "Repo already exists — pulling latest..."
    git -C "$APP_DIR" fetch origin
    git -C "$APP_DIR" reset --hard "origin/$REPO_BRANCH"
    ok "Repo updated to latest $REPO_BRANCH"
else
    git clone -b "$REPO_BRANCH" "$REPO_URL" "$APP_DIR"
    ok "Repo cloned to $APP_DIR"
fi
chown -R "$APP_USER:$APP_USER" "$APP_DIR"

# ── 7. Restore .env + volumes + DB ───────────────────────────────────────────
section "7/9 — Restore .env, volumes, database"

# .env
cp "$EXTRACT_DIR/.env" "$APP_DIR/.env"
chown "$APP_USER:$APP_USER" "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"
ok ".env restored"

# Docker named volume
if [ -f "$EXTRACT_DIR/axiom-data-volume.tar.gz" ]; then
    log "Restoring axiom-data Docker volume..."
    docker volume create axiom-data 2>/dev/null || true
    docker run --rm \
        -v axiom-data:/target \
        -v "$EXTRACT_DIR":/backup:ro \
        alpine \
        sh -c "cd /target && tar xzf /backup/axiom-data-volume.tar.gz"
    ok "axiom-data volume restored"
else
    warn "No volume backup found — axiom-data will start empty"
fi

# PostgreSQL (only if local dump exists)
DB_STATUS=$(cat "$EXTRACT_DIR/db_status.txt" 2>/dev/null || echo "NOT_SET")

if [ "$DB_STATUS" = "LOCAL" ]; then
    log "Restoring local PostgreSQL database..."
    DB_URL=$(grep -E "^DATABASE_URL=" "$APP_DIR/.env" | cut -d'=' -f2- | tr -d '"' | tr -d "'")

    if ! command -v psql &>/dev/null; then
        apt-get install -y postgresql postgresql-client
        systemctl enable postgresql
        systemctl start postgresql
    fi

    psql "$DB_URL" < "$EXTRACT_DIR/axiom_db.sql" \
        || die "PostgreSQL restore failed"
    ok "PostgreSQL database restored"

elif [ "$DB_STATUS" = "REMOTE" ]; then
    DB_HOST=$(sed -n '2p' "$EXTRACT_DIR/db_status.txt" 2>/dev/null || echo "unknown")
    ok "Remote database ($DB_HOST) — no restore needed, .env already has the correct URL"

else
    warn "Database status unknown ($DB_STATUS) — check .env DATABASE_URL manually"
fi

# ── 8. Python dependencies ────────────────────────────────────────────────────
section "8/9 — Python dependencies"

if [ -f "$APP_DIR/requirements.txt" ]; then
    log "Installing Python packages from requirements.txt..."
    pip3 install --break-system-packages -r "$APP_DIR/requirements.txt" \
        || warn "pip install had errors — check output above"
    ok "Python dependencies installed"
else
    warn "No requirements.txt found — skipping"
fi

# ── 9. systemd service + firewall ─────────────────────────────────────────────
section "9/9 — systemd service, firewall, start containers"

# UFW firewall
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp   comment "SSH"
ufw allow 80/tcp   comment "HTTP"
ufw allow 443/tcp  comment "HTTPS"
ufw allow 8501/tcp comment "Streamlit"
ufw allow 11434/tcp comment "Ollama API"
ufw --force enable
ok "UFW firewall configured"

# systemd service
cp "$APP_DIR/scripts/axiom.service" /etc/systemd/system/axiom.service
systemctl daemon-reload
systemctl enable axiom
ok "axiom.service installed and enabled"

# Start containers
log "Starting Docker containers..."
cd "$APP_DIR"
sudo -u "$APP_USER" docker compose up -d --build \
    || die "docker compose up failed — check logs with: docker compose logs"
ok "Containers started"

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$EXTRACT_DIR"

# ── Done ──────────────────────────────────────────────────────────────────────
log ""
log "================================================================"
log "  SETUP COMPLETE"
log "================================================================"
log ""
log "  ⚠  REBOOT REQUIRED for NVIDIA drivers to fully activate."
log "     After reboot, run the verification script:"
log ""
log "     sudo reboot"
log "     # wait for instance to come back, then:"
log "     bash $APP_DIR/scripts/migrate_03_verify.sh"
log ""
log "  Manual checks:"
log "     nvidia-smi                          # GPU recognized"
log "     docker compose -f $APP_DIR/docker-compose.yml ps  # containers up"
log "     ollama list                         # models present"
log "     docker logs axiom-scanner --tail=20 # scanner running"
log ""
log "================================================================"
