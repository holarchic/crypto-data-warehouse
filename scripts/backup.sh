#!/usr/bin/env bash
# Backup candle_collector DB via pg_dump in the db container.
# Creates timestamped custom-format dump in backups/ directory.
# Sends webhook alert on failure. Cleans up dumps older than 7 days.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-${PROJECT_DIR}/backups}"
mkdir -p "$BACKUP_DIR"

# -- Load webhook URL for failure alerts --------------------------------------

ENV_FILE="$PROJECT_DIR/.env"
WEBHOOK_URL=""
if [ -f "$ENV_FILE" ]; then
    WEBHOOK_URL=$(grep -E '^WEBHOOK_URL=' "$ENV_FILE" | cut -d= -f2- | tr -d '"' || true)
fi

send_alert() {
    local message="$1"
    local hostname
    hostname=$(hostname)
    echo "==> ALERT: $message" >&2
    if [ -n "$WEBHOOK_URL" ]; then
        curl -sf -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"🚨 candle-collector backup ($hostname): $message\"}" \
            > /dev/null 2>&1 || true
    fi
}

# Alert on any failure
trap 'send_alert "Backup failed — check /var/log/candle-collector-backup.log"' ERR

# -- Pre-flight: disk space check ---------------------------------------------

avail_kb=$(df "$BACKUP_DIR" --output=avail 2>/dev/null | tail -1 | tr -d ' ' || df "$BACKUP_DIR" | tail -1 | awk '{print $4}')
avail_gb=$((avail_kb / 1048576))
if [ "$avail_gb" -lt 2 ]; then
    send_alert "Disk space too low for backup: ${avail_gb}GB available (need 2GB)"
    exit 1
fi

# -- Backup -------------------------------------------------------------------

TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
FILENAME="candle_collector-${TIMESTAMP}.dump"
FILEPATH="${BACKUP_DIR}/${FILENAME}"

echo "==> Backing up candle_collector to ${FILEPATH}"

cd "$PROJECT_DIR"
docker compose exec -T db pg_dump \
    -U candles \
    --format=custom \
    --compress=6 \
    candle_collector > "$FILEPATH"

# Verify dump is readable
echo "==> Verifying backup..."
pg_restore --list "$FILEPATH" > /dev/null 2>&1 || {
    # pg_restore may not be on host — try in container
    docker compose exec -T db pg_restore --list /dev/stdin < "$FILEPATH" > /dev/null
}

SIZE=$(du -h "$FILEPATH" | cut -f1)
echo "==> Backup complete: ${FILEPATH} (${SIZE})"

# -- Retention: delete dumps older than 7 days --------------------------------

DELETED=$(find "$BACKUP_DIR" -name "*.dump" -mtime +7 -delete -print | wc -l)
if [ "$DELETED" -gt 0 ]; then
    echo "==> Cleaned up ${DELETED} old backup(s)"
fi
