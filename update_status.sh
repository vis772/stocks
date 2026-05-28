#!/bin/bash
# update_status.sh — Captures EC2 instance status and pushes INSTANCE_STATUS.md to GitHub.
# Runs weekdays at 10 AM ET via cron (configured by setup_cron.sh).
# Errors are logged to /tmp/update_status.log — script never exits silently.

REPO_DIR="/home/ubuntu/axiom"
LOG_FILE="/tmp/update_status.log"
STATUS_FILE="$REPO_DIR/INSTANCE_STATUS.md"
ENV_FILE="$REPO_DIR/.env"
VENV_PYTHON="/home/ubuntu/venv/bin/python3"
BRANCH="clean-combined-version"
TB='```'  # triple backtick for markdown code blocks

NOW_ET=$(TZ='America/New_York' date '+%Y-%m-%d %H:%M:%S ET')
NOW_UTC=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
DATE_ONLY=$(TZ='America/New_York' date '+%Y-%m-%d')

# ── Logging ────────────────────────────────────────────────────────────────────
log() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG_FILE"; }
log "================================================================"
log "update_status.sh started"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ -f "$ENV_FILE" ]; then
    set -a; source "$ENV_FILE"; set +a
    log ".env loaded"
else
    log "WARNING: .env not found at $ENV_FILE"
fi

# ── EC2 Metadata (IMDSv2) ──────────────────────────────────────────────────────
TOKEN=$(curl -s --max-time 3 -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)

if [ -n "$TOKEN" ]; then
    _meta() { curl -s --max-time 3 -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null || echo "unknown"; }
    INSTANCE_TYPE=$(_meta "instance-type")
    INSTANCE_ID=$(_meta "instance-id")
    REGION=$(_meta "placement/region")
    AZ=$(_meta "placement/availability-zone")
else
    INSTANCE_TYPE="unknown (IMDS unavailable)"
    INSTANCE_ID="unknown"
    REGION="unknown"
    AZ="unknown"
fi
log "EC2: $INSTANCE_TYPE ($INSTANCE_ID) in $REGION"

# ── Ollama ─────────────────────────────────────────────────────────────────────
OLLAMA_VERSION=$(ollama --version 2>/dev/null || echo "⚠️ WARNING: ollama binary not found")
OLLAMA_MODELS=$(ollama list 2>/dev/null || echo "⚠️ WARNING: ollama not running or no models")
log "Ollama: $OLLAMA_VERSION"

# ── Service Status Helper ──────────────────────────────────────────────────────
svc_status() {
    local name="$1"
    local state
    state=$(systemctl is-active "$name" 2>/dev/null || echo "unknown")
    case "$state" in
        active)   echo "✅ active" ;;
        inactive) echo "⚠️ WARNING: inactive" ;;
        failed)   echo "🔴 FAILED" ;;
        *)        echo "⚠️ WARNING: $state" ;;
    esac
}

docker_status() {
    local filter="$1"
    local status
    status=$(docker ps --filter "name=$filter" --format "{{.Status}}" 2>/dev/null || true)
    if [ -n "$status" ]; then
        echo "✅ $status"
    else
        echo "⚠️ WARNING: container not running"
    fi
}

PE_STATUS=$(svc_status "axiom-pe")
SCANNER_STATUS=$(docker_status "axiom-scanner")
BOT_STATUS=$(docker_status "axiom-telegram")
POSTGRES_STATUS="remote (Supabase)"
log "Services: PE=$PE_STATUS | Scanner=$SCANNER_STATUS"

# ── System Resources ───────────────────────────────────────────────────────────
DISK_USAGE=$(df -h / | awk 'NR==2 {printf "**%s** used of %s (%s full)", $3, $2, $5}')
MEM_USAGE=$(free -h | awk '/^Mem:/ {printf "**%s** used of %s", $3, $2}')
SWAP_USAGE=$(free -h | awk '/^Swap:/ {printf "%s used of %s", $3, $2}' || echo "N/A")
PYTHON_VER=$("$VENV_PYTHON" --version 2>/dev/null || python3 --version 2>/dev/null || echo "not found")
UPTIME_STR=$(uptime -p 2>/dev/null || uptime)
LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}' | xargs || echo "N/A")

# GPU info if available (g4dn.xlarge)
GPU_INFO=$(nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu \
    --format=csv,noheader,nounits 2>/dev/null || echo "not available (CPU mode)")

# ── Failed Systemd Units ───────────────────────────────────────────────────────
FAILED_RAW=$(systemctl --failed --no-legend 2>/dev/null | head -20 || true)
if [ -z "$FAILED_RAW" ]; then
    FAILED_SECTION="✅ No failed units."
else
    log "WARNING: failed systemd units detected"
    FAILED_SECTION="⚠️ **WARNING — failed units detected:**

${TB}
$FAILED_RAW
${TB}"
fi

# ── Database Queries ───────────────────────────────────────────────────────────
log "Running DB queries..."
DB_RESULT=$("$VENV_PYTHON" - << 'PYEOF' 2>/dev/null
import os, sys
try:
    from dotenv import load_dotenv
    load_dotenv('/home/ubuntu/axiom/.env')
    import psycopg2
    url = os.environ.get('DATABASE_URL', '').replace('postgres://', 'postgresql://', 1)
    if not url:
        print("NO_DB_URL|NO_DB_URL|0|0|0")
        sys.exit(0)
    conn = psycopg2.connect(url, sslmode='require', connect_timeout=10)
    cur = conn.cursor()

    cur.execute("SELECT MAX(created_at) FROM signal_log")
    last_signal = cur.fetchone()[0]
    last_signal = str(last_signal) if last_signal else "no signals yet"

    cur.execute("SELECT MAX(created_at) FROM prediction_engine_signals")
    last_pe = cur.fetchone()[0]
    last_pe = str(last_pe) if last_pe else "no picks yet"

    cur.execute("SELECT COUNT(*) FROM signal_log WHERE created_at >= CURRENT_DATE")
    signals_today = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM prediction_engine_signals WHERE date = CURRENT_DATE")
    pe_today = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM stock_universe WHERE active = TRUE")
    universe_count = cur.fetchone()[0]

    conn.close()
    print(f"{last_signal}|{last_pe}|{signals_today}|{pe_today}|{universe_count}")
except Exception as e:
    print(f"DB_ERROR: {e}|DB_ERROR|0|0|0")
PYEOF
)

if [[ "$DB_RESULT" == DB_ERROR* ]] || [ -z "$DB_RESULT" ]; then
    log "WARNING: DB query failed — $DB_RESULT"
    LAST_SIGNAL="⚠️ WARNING: DB query failed"
    LAST_PE_PICK="⚠️ WARNING: DB query failed"
    SIGNALS_TODAY="?"
    PE_TODAY="?"
    UNIVERSE_COUNT="?"
elif [[ "$DB_RESULT" == "NO_DB_URL"* ]]; then
    log "WARNING: DATABASE_URL not set"
    LAST_SIGNAL="⚠️ WARNING: DATABASE_URL not configured"
    LAST_PE_PICK="⚠️ WARNING: DATABASE_URL not configured"
    SIGNALS_TODAY="?"
    PE_TODAY="?"
    UNIVERSE_COUNT="?"
else
    IFS='|' read -r LAST_SIGNAL LAST_PE_PICK SIGNALS_TODAY PE_TODAY UNIVERSE_COUNT <<< "$DB_RESULT"
    log "DB OK — signals today: $SIGNALS_TODAY | PE picks today: $PE_TODAY | universe: $UNIVERSE_COUNT"
fi

# ── Write INSTANCE_STATUS.md ───────────────────────────────────────────────────
cat > "$STATUS_FILE" << MDEOF
# Axiom Terminal — Instance Status

> **Last updated:** $NOW_ET ($NOW_UTC)
> Auto-generated by \`update_status.sh\` — do not edit manually.

---

## 🖥️ EC2 Instance

| Field | Value |
|-------|-------|
| Instance ID | \`$INSTANCE_ID\` |
| Instance Type | \`$INSTANCE_TYPE\` |
| Region | \`$REGION\` |
| Availability Zone | \`$AZ\` |
| Uptime | $UPTIME_STR |
| Load Average | $LOAD_AVG |

---

## 🤖 Ollama

**Version:** \`$OLLAMA_VERSION\`

**Installed Models:**
${TB}
$OLLAMA_MODELS
${TB}

**GPU:**
${TB}
$GPU_INFO
${TB}

---

## 🔧 Services

| Service | Status |
|---------|--------|
| Axiom Scanner (Docker) | $SCANNER_STATUS |
| Axiom Telegram Bot (Docker) | $BOT_STATUS |
| Prediction Engine (systemd) | $PE_STATUS |
| PostgreSQL | $POSTGRES_STATUS |

---

## 📊 Database

| Metric | Value |
|--------|-------|
| Active tickers in universe | $UNIVERSE_COUNT |
| Last scanner signal | \`$LAST_SIGNAL\` |
| Scanner signals today | **$SIGNALS_TODAY** |
| Last PE conviction pick | \`$LAST_PE_PICK\` |
| PE picks today | **$PE_TODAY** |

---

## 💾 System Resources

| Resource | Usage |
|----------|-------|
| Disk (/) | $DISK_USAGE |
| Memory | $MEM_USAGE |
| Swap | $SWAP_USAGE |
| Python (venv) | \`$PYTHON_VER\` |

---

## 🚨 Failed Systemd Units

$FAILED_SECTION

---

*Runs weekdays at 10 AM ET via cron. Log: \`/tmp/update_status.log\`*
MDEOF

log "INSTANCE_STATUS.md written to $STATUS_FILE"

# ── Git Push ───────────────────────────────────────────────────────────────────
cd "$REPO_DIR"

if ! git diff --quiet INSTANCE_STATUS.md 2>/dev/null || ! git ls-files --error-unmatch INSTANCE_STATUS.md &>/dev/null; then
    git add INSTANCE_STATUS.md >> "$LOG_FILE" 2>&1
    git commit -m "auto: update instance status $DATE_ONLY" >> "$LOG_FILE" 2>&1
    if git push origin "$BRANCH" >> "$LOG_FILE" 2>&1; then
        log "Pushed INSTANCE_STATUS.md to $BRANCH"
    else
        log "WARNING: git push failed — check credentials (run setup_cron.sh again)"
    fi
else
    log "No changes in INSTANCE_STATUS.md — skipping commit"
fi

log "update_status.sh complete"
log "================================================================"
