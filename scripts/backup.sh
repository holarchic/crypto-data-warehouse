#!/usr/bin/env bash
# Backup candle_collector DB via pg_dump in the db container.
# Creates timestamped custom-format dump in backups/ directory.
# Sends webhook alert on failure. Cleans up dumps older than 7 days.
# A dump is written under a .partial name and takes its .dump name only after it reads back
# whole, so every *.dump in the directory is complete. One run at a time.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-${PROJECT_DIR}/backups}"
DC=(docker compose -f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker-compose.prod.yml")
# mkdir is atomic on every platform the script runs on, unlike flock (absent on macOS).
LOCK_DIR="${BACKUP_LOCK_DIR:-/tmp/candle-collector-backup.lock}"
mkdir -p "$BACKUP_DIR"

# -- Failure alerts -------------------------------------------------------------

# shellcheck source=scripts/webhook-lib.sh
. "$SCRIPT_DIR/webhook-lib.sh"
WEBHOOK_URL=$(webhook_url "$PROJECT_DIR")

send_alert() {
    local message="$1"
    echo "==> ALERT: $message" >&2
    webhook_post "$WEBHOOK_URL" "🚨 candle-collector backup ($(hostname)): $message" \
        || echo "==> Page not delivered" >&2
}

# Alert on any failure
trap 'send_alert "Backup failed — check /var/log/candle-collector-backup.log"' ERR

# -- Pre-flight: disk space check ---------------------------------------------

# POSIX output (-P, 1K blocks): GNU df's --output isn't on macOS.
avail_kb=$(df -Pk "$BACKUP_DIR" | awk 'NR == 2 {print $4}')
avail_gb=$((avail_kb / 1048576))
if [ "$avail_gb" -lt 2 ]; then
    send_alert "Disk space too low for backup: ${avail_gb}GB available (need 2GB)"
    exit 1
fi

# -- Backup -------------------------------------------------------------------

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    send_alert "Another backup holds ${LOCK_DIR}; if none is running, remove it"
    exit 1
fi
PARTIAL=""
cleanup() {
    [ -z "$PARTIAL" ] || rm -f "$PARTIAL"
    rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
# The name carries the TimescaleDB version, which a restore must match and the dump itself
# does not record (scripts/restore.sh reads it from here).
TSDB_VERSION=$("${DC[@]}" exec -T db psql -X -U candles -d candle_collector -Atc \
    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
FILENAME="candle_collector-${TIMESTAMP}-timescaledb-${TSDB_VERSION}.dump"
FILEPATH="${BACKUP_DIR}/${FILENAME}"
PARTIAL="${FILEPATH}.partial"

echo "==> Backing up candle_collector to ${FILEPATH}"

"${DC[@]}" exec -T db pg_dump \
    -U candles \
    --format=custom \
    --compress=6 \
    candle_collector > "$PARTIAL"

# Read the whole archive, not just its table of contents, before older dumps are pruned.
echo "==> Verifying backup..."
"${DC[@]}" exec -T db pg_restore --file=/dev/null < "$PARTIAL"
mv "$PARTIAL" "$FILEPATH"
PARTIAL=""

SIZE=$(du -h "$FILEPATH" | cut -f1)
echo "==> Backup complete: ${FILEPATH} (${SIZE})"

# -- Retention: delete dumps older than 7 days --------------------------------

DELETED=$(find "$BACKUP_DIR" -name "*.dump" -mtime +7 -delete -print | wc -l)
if [ "$DELETED" -gt 0 ]; then
    echo "==> Cleaned up ${DELETED} old backup(s)"
fi
