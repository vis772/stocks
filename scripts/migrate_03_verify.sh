#!/usr/bin/env bash
# =============================================================================
# Axiom Terminal — Migration Step 3: Verification Checklist
# Run on the NEW g4dn.xlarge AFTER reboot and setup:
#
#   bash ~/axiom/scripts/migrate_03_verify.sh
#
# Prints a pass/fail checklist. Exit code 0 = all checks passed.
# Do NOT terminate the old instance until this exits 0.
# =============================================================================
set -uo pipefail

APP_DIR="/home/ubuntu/axiom"

# ── Helpers ───────────────────────────────────────────────────────────────────
PASS=0
FAIL=0
WARN=0
RESULTS=()

pass() { PASS=$((PASS+1)); RESULTS+=("  ✅  $*"); }
fail() { FAIL=$((FAIL+1)); RESULTS+=("  ❌  $*"); }
warn() { WARN=$((WARN+1)); RESULTS+=("  ⚠️   $*"); }

run_check() {
    local label="$1"; shift
    if "$@" &>/dev/null 2>&1; then
        pass "$label"
    else
        fail "$label"
    fi
}

echo "================================================================"
echo "  Axiom Terminal — Migration Verification"
echo "  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "  Host: $(hostname) / $(curl -sf --max-time 3 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo 'IP unavailable')"
echo "================================================================"
echo ""
echo "Running checks..."
echo ""

# ── 1. GPU ────────────────────────────────────────────────────────────────────
echo "[ GPU ]"

if nvidia-smi &>/dev/null; then
    GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)
    DRIVER=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)
    pass "nvidia-smi — $GPU_NAME (driver $DRIVER)"
else
    fail "nvidia-smi not available — reboot may be needed, or driver install failed"
fi

if command -v nvcc &>/dev/null; then
    CUDA_VER=$(nvcc --version 2>/dev/null | grep "release" | awk '{print $5}' | tr -d ',')
    pass "CUDA toolkit — $CUDA_VER"
else
    warn "CUDA toolkit (nvcc) not in PATH — Ollama bundles its own, so this may be fine"
fi

run_check "nvidia-container-toolkit installed" dpkg -l nvidia-container-toolkit

echo ""

# ── 2. Docker ─────────────────────────────────────────────────────────────────
echo "[ Docker ]"

if command -v docker &>/dev/null; then
    DOCKER_VER=$(docker --version 2>/dev/null)
    pass "Docker — $DOCKER_VER"
else
    fail "Docker not found"
fi

if docker compose version &>/dev/null; then
    COMPOSE_VER=$(docker compose version --short 2>/dev/null)
    pass "Docker Compose plugin — $COMPOSE_VER"
else
    fail "Docker Compose plugin not found"
fi

if systemctl is-active --quiet docker; then
    pass "Docker daemon — active"
else
    fail "Docker daemon — not running"
fi

echo ""

# ── 3. Axiom containers ───────────────────────────────────────────────────────
echo "[ Axiom containers ]"

check_container() {
    local name="$1"
    local status running
    status=$(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null || echo "missing")
    running=$(docker inspect --format='{{.State.Running}}' "$name" 2>/dev/null || echo "false")
    if [ "$running" = "true" ]; then
        UPTIME=$(docker inspect --format='{{.State.StartedAt}}' "$name" 2>/dev/null || echo "unknown")
        pass "$name — running (started $UPTIME)"
    else
        fail "$name — status=$status (not running)"
    fi
}

check_container "axiom-scanner"
check_container "axiom-telegram-bot"

# Check scanner is producing logs (not silently crashed)
if docker logs axiom-scanner --tail=5 2>&1 | grep -qE "SCAN #|Scanning|scanner_loop"; then
    pass "axiom-scanner — producing scan logs"
else
    warn "axiom-scanner — no scan activity in last 5 log lines (may be normal outside market hours)"
fi

echo ""

# ── 4. systemd service ────────────────────────────────────────────────────────
echo "[ systemd ]"

if systemctl is-enabled --quiet axiom 2>/dev/null; then
    pass "axiom.service — enabled (will auto-start on reboot)"
else
    fail "axiom.service — not enabled"
fi

echo ""

# ── 5. Ollama ─────────────────────────────────────────────────────────────────
echo "[ Ollama ]"

if command -v ollama &>/dev/null; then
    pass "Ollama binary — $(ollama --version 2>/dev/null || echo 'installed')"
else
    fail "Ollama binary — not found"
fi

if systemctl is-active --quiet ollama 2>/dev/null; then
    pass "Ollama service — active"
elif curl -sf http://localhost:11434/api/tags &>/dev/null; then
    pass "Ollama API — responding on :11434"
else
    fail "Ollama service — not running"
fi

REQUIRED_MODELS=("qwen2.5:1.5b" "phi4-mini" "gemma3:1b" "smollm2")
INSTALLED_MODELS=$(ollama list 2>/dev/null | awk 'NR>1 {print $1}' || true)

for model in "${REQUIRED_MODELS[@]}"; do
    BASE=$(echo "$model" | cut -d: -f1)
    if echo "$INSTALLED_MODELS" | grep -q "^${BASE}"; then
        pass "Model $model — present"
    else
        fail "Model $model — NOT found (run: ollama pull $model)"
    fi
done

echo ""

# ── 6. Repository ─────────────────────────────────────────────────────────────
echo "[ Repository ]"

if [ -d "$APP_DIR/.git" ]; then
    BRANCH=$(git -C "$APP_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    COMMIT=$(git -C "$APP_DIR" log --oneline -1 2>/dev/null || echo "unknown")
    if [ "$BRANCH" = "clean-combined-version" ]; then
        pass "Repo — on branch $BRANCH ($COMMIT)"
    else
        warn "Repo — on branch '$BRANCH' (expected clean-combined-version)"
    fi
else
    fail "Repo — $APP_DIR is not a git repository"
fi

if [ -f "$APP_DIR/.env" ]; then
    ENV_LINES=$(wc -l < "$APP_DIR/.env")
    pass ".env file — present ($ENV_LINES lines)"
else
    fail ".env file — MISSING at $APP_DIR/.env"
fi

echo ""

# ── 7. Database connectivity ──────────────────────────────────────────────────
echo "[ Database ]"

DB_URL=$(grep -E "^DATABASE_URL=" "$APP_DIR/.env" 2>/dev/null | cut -d'=' -f2- | tr -d '"' | tr -d "'") || true

if [ -z "$DB_URL" ]; then
    warn "DATABASE_URL not set in .env"
else
    DB_HOST=$(echo "$DB_URL" | sed -E 's|.*@([^:/]+).*|\1|' 2>/dev/null || echo "unknown")

    if echo "$DB_URL" | grep -qE "localhost|127\.0\.0\.1"; then
        # Local PostgreSQL
        if systemctl is-active --quiet postgresql 2>/dev/null; then
            pass "PostgreSQL service — active (local)"
        else
            fail "PostgreSQL service — not running (local)"
        fi

        if psql "$DB_URL" -c "SELECT 1" &>/dev/null 2>&1; then
            TABLE_COUNT=$(psql "$DB_URL" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null | tr -d ' ' || echo "?")
            pass "PostgreSQL connection — OK ($TABLE_COUNT tables in public schema)"
        else
            fail "PostgreSQL connection — failed (check DATABASE_URL and pg_hba.conf)"
        fi

    else
        # Remote (Supabase / Railway)
        if psql "$DB_URL" -c "SELECT 1" &>/dev/null 2>&1; then
            TABLE_COUNT=$(psql "$DB_URL" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null | tr -d ' ' || echo "?")
            pass "Remote DB ($DB_HOST) — reachable ($TABLE_COUNT tables)"
        else
            fail "Remote DB ($DB_HOST) — connection failed (check DATABASE_URL, firewall, credentials)"
        fi
    fi
fi

echo ""

# ── 8. GitHub Actions auto-deploy ─────────────────────────────────────────────
echo "[ Auto-deploy ]"

if curl -sf --max-time 5 https://github.com &>/dev/null; then
    pass "GitHub reachable"
else
    warn "GitHub not reachable — auto-deploy will fail until network is confirmed"
fi

warn "GitHub Actions EC2_HOST secret still points to OLD instance IP"
warn "  → Update it in: https://github.com/vis772/stocks/settings/secrets/actions"
warn "  → Set EC2_HOST = $(curl -sf --max-time 3 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo '<new-instance-public-ip>')"

echo ""

# ── 9. Spot-check scanner output ─────────────────────────────────────────────
echo "[ Scanner output (last 10 log lines) ]"
docker logs axiom-scanner --tail=10 2>&1 | sed 's/^/    /' || true
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  RESULTS"
echo "================================================================"
for r in "${RESULTS[@]}"; do echo "$r"; done
echo ""
echo "  Passed : $PASS"
echo "  Warned : $WARN"
echo "  Failed : $FAIL"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo "  ✅  ALL CHECKS PASSED — safe to terminate the old instance"
    echo ""
    echo "  Before terminating old instance, also:"
    echo "    1. Update GitHub Actions secret EC2_HOST to this instance's IP"
    echo "    2. Update any DNS records / Elastic IP pointing at the old instance"
    echo "    3. Confirm Telegram bot is receiving messages"
    echo "    4. Run a test scan from the Telegram bot"
    echo ""
    exit 0
else
    echo "  ❌  $FAIL CHECK(S) FAILED — do NOT terminate the old instance yet"
    echo "      Fix the failures above, then re-run this script."
    echo ""
    exit 1
fi
