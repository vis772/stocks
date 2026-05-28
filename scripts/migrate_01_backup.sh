#!/usr/bin/env bash
# =============================================================================
# Axiom Terminal — Migration Step 1: Backup OLD instance
# Run on the OLD t3.micro (us-east-2) as ubuntu user
#
#   bash ~/axiom/scripts/migrate_01_backup.sh
#
# Produces: ~/axiom_migration_YYYYMMDD_HHMMSS.tar.gz
# Transfer it to the new instance, then run migrate_02_setup_new.sh there.
# =============================================================================
set -euo pipefail

APP_DIR="/home/ubuntu/axiom"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/tmp/axiom_backup_${TIMESTAMP}"
ARCHIVE="$HOME/axiom_migration_${TIMESTAMP}.tar.gz"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠  $*"; }
die()  { echo "[$(date '+%H:%M:%S')] ✗ FATAL: $*" >&2; exit 1; }

log "================================================================"
log "  Axiom Terminal — Backup (OLD instance)"
log "  Timestamp : $TIMESTAMP"
log "  App dir   : $APP_DIR"
log "================================================================"

# ── Pre-flight ────────────────────────────────────────────────────────────────
[ -d "$APP_DIR" ] || die "$APP_DIR not found — are you on the right instance?"
[ -f "$APP_DIR/.env" ] || die ".env not found at $APP_DIR/.env"

mkdir -p "$BACKUP_DIR"

# ── Step 1: Save .env ─────────────────────────────────────────────────────────
log "[1/5] Backing up .env..."
cp "$APP_DIR/.env" "$BACKUP_DIR/.env"
ok ".env copied"

# ── Step 2: PostgreSQL — local or remote? ─────────────────────────────────────
log "[2/5] Checking database..."

DB_URL=$(grep -E "^DATABASE_URL=" "$APP_DIR/.env" | cut -d'=' -f2- | tr -d '"' | tr -d "'") || true

if [ -z "$DB_URL" ]; then
    warn "DATABASE_URL not set in .env — skipping DB backup"
    echo "NOT_SET" > "$BACKUP_DIR/db_status.txt"

elif echo "$DB_URL" | grep -qE "localhost|127\.0\.0\.1|@postgres:|@db:|@localhost"; then
    log "  Local PostgreSQL detected — dumping..."
    pg_dump "$DB_URL" > "$BACKUP_DIR/axiom_db.sql" \
        || die "pg_dump failed. Is PostgreSQL running? Check: sudo systemctl status postgresql"
    echo "LOCAL" > "$BACKUP_DIR/db_status.txt"
    ok "Local DB dumped to axiom_db.sql ($(wc -l < "$BACKUP_DIR/axiom_db.sql") lines)"

else
    # Remote (Supabase / Railway / RDS) — record the URL but don't dump
    warn "Remote database URL detected (Supabase/Railway/RDS)."
    warn "  The database lives outside this EC2 instance and does NOT need migration."
    warn "  Your .env already contains the correct DATABASE_URL — it will work on the new instance."
    echo "REMOTE" > "$BACKUP_DIR/db_status.txt"
    # Mask credentials in the log but record the host for reference
    DB_HOST=$(echo "$DB_URL" | sed -E 's|.*@([^:/]+).*|\1|')
    echo "$DB_HOST" >> "$BACKUP_DIR/db_status.txt"
    ok "Remote DB host recorded: $DB_HOST (no dump needed)"
fi

# ── Step 3: Docker named volume (axiom-data) ──────────────────────────────────
log "[3/5] Backing up Docker named volume 'axiom-data'..."

if docker volume inspect axiom-data &>/dev/null; then
    docker run --rm \
        -v axiom-data:/source:ro \
        -v "$BACKUP_DIR":/backup \
        alpine \
        tar czf /backup/axiom-data-volume.tar.gz -C /source . \
        || die "Docker volume backup failed"
    ok "axiom-data volume backed up ($(du -sh "$BACKUP_DIR/axiom-data-volume.tar.gz" | cut -f1))"
else
    warn "Docker volume 'axiom-data' not found — skipping (may be empty)"
    echo "VOLUME_NOT_FOUND" > "$BACKUP_DIR/volume_status.txt"
fi

# ── Step 4: Capture running container state ────────────────────────────────────
log "[4/5] Recording container state..."
docker ps -a > "$BACKUP_DIR/docker_ps_before_migration.txt" 2>&1 || true
docker images  > "$BACKUP_DIR/docker_images_before_migration.txt" 2>&1 || true
ok "Container state recorded"

# ── Step 5: Create archive ─────────────────────────────────────────────────────
log "[5/5] Creating archive..."
tar czf "$ARCHIVE" -C /tmp "axiom_backup_${TIMESTAMP}"
rm -rf "$BACKUP_DIR"
ARCHIVE_SIZE=$(du -sh "$ARCHIVE" | cut -f1)
ok "Archive created: $ARCHIVE ($ARCHIVE_SIZE)"

# ── Done ──────────────────────────────────────────────────────────────────────
log ""
log "================================================================"
log "  BACKUP COMPLETE"
log "================================================================"
log ""
log "  Archive : $ARCHIVE"
log "  Size    : $ARCHIVE_SIZE"
log ""
log "  Next: copy this file to the new instance and run migrate_02_setup_new.sh"
log ""
log "  Transfer command (run this from your LOCAL machine):"
log "    scp -i <your-key.pem> ubuntu@<OLD-IP>:$ARCHIVE ."
log "    scp -i <your-key.pem> axiom_migration_${TIMESTAMP}.tar.gz ubuntu@<NEW-IP>:~/"
log ""
log "  Or copy directly between instances (if they're in the same VPC):"
log "    scp $ARCHIVE ubuntu@<NEW-IP>:~/"
log ""
log "================================================================"
