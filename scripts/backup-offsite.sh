#!/usr/bin/env bash
# Nightly offsite backup: streams pg_dump from the db container into a restic repository in an
# S3-compatible bucket, applies retention, checks the repository, then records success.
#
# Credentials live outside the checkout, in RESTIC_ENV_FILE (deploy/README.md § Offsite backups):
#   RESTIC_REPOSITORY, RESTIC_PASSWORD_FILE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# Without that file the script exits 0: offsite backup is opt-in.
# Sends a webhook alert on failure; deploy/host-monitor.sh pages when BACKUP_OK_MARKER goes stale.
set -euo pipefail

# cron's PATH is /usr/bin:/bin, and deploy/install-restic.sh installs to /usr/local/bin.
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESTIC_ENV_FILE="${RESTIC_ENV_FILE:-/etc/candle-collector/restic.env}"
BACKUP_OK_MARKER="${BACKUP_OK_MARKER:-/var/lib/candle-collector/backup-ok}"
LOCK_FILE="/run/lock/candle-collector-backup.lock"
# Fixed rather than the hostname, so retention groups survive a host rebuild.
SNAPSHOT_HOST="candle-collector"
DC=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

log() { echo "==> $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

if [ ! -f "$RESTIC_ENV_FILE" ]; then
    log "Offsite backup not configured: ${RESTIC_ENV_FILE} absent"
    exit 0
fi

# -- Load webhook URL for failure alerts --------------------------------------

ENV_FILE="$PROJECT_DIR/.env"
WEBHOOK_URL="${WEBHOOK_URL:-}"
if [ -z "$WEBHOOK_URL" ] && [ -f "$ENV_FILE" ]; then
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
            -d "{\"text\":\"🚨 candle-collector offsite backup ($hostname): $message\"}" \
            > /dev/null 2>&1 || true
    fi
}

fail() {
    send_alert "$1"
    exit 1
}

trap 'send_alert "Offsite backup failed — check /var/log/candle-collector-backup.log"' ERR

# The bucket credentials exist only in restic's process, never in this shell.
restic_run() {
    # shellcheck source=/dev/null
    ( set -a; . "$RESTIC_ENV_FILE"; set +a; exec restic "$@" )
}

# -- One run at a time ----------------------------------------------------------
# A run still going when the next starts needs a look, not a second dump on top of it.

exec 9<>"$LOCK_FILE"
if ! flock -n 9; then
    fail "Previous offsite backup still running (started $(cat "$LOCK_FILE"))"
fi
date -u +%Y-%m-%dT%H:%M:%SZ > "$LOCK_FILE"

cd "$PROJECT_DIR"
log "Offsite backup starting"

# Only stale locks: one left by an interrupted run clears, a live drill elsewhere keeps its own.
restic_run unlock

TSDB_VERSION=$("${DC[@]}" exec -T db psql -X -U candles -d candle_collector -Atc \
    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
PG_MAJOR=$("${DC[@]}" exec -T db psql -X -U candles -d candle_collector -Atc \
    "SELECT current_setting('server_version_num')::int / 10000")
[ -n "$TSDB_VERSION" ] || fail "TimescaleDB extension not found in candle_collector"
log "TimescaleDB ${TSDB_VERSION}, PostgreSQL ${PG_MAJOR}"

# -- Backup -------------------------------------------------------------------
# Uncompressed, so restic's chunking dedups the unchanged (already-compressed) chunks night to
# night; restic compresses what it stores. The tags name the image scripts/restore-drill.sh
# restores into. restic hands its environment to the dump command, so `env -u` drops the
# bucket credentials before compose, which reads its own AWS_* from the project .env.

log "Dumping candle_collector into the repository"
restic_run backup --host "$SNAPSHOT_HOST" \
    --tag "timescaledb-${TSDB_VERSION}" --tag "pg${PG_MAJOR}" \
    --stdin-filename candle_collector.dump --stdin-from-command -- \
    env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
    "${DC[@]}" exec -T db pg_dump -U candles --format=custom --compress=0 candle_collector

log "Applying retention"
restic_run forget --host "$SNAPSHOT_HOST" --keep-daily 7 --keep-weekly 5 --keep-monthly 12 --prune

log "Checking the repository"
restic_run check

mkdir -p "$(dirname "$BACKUP_OK_MARKER")"
date -u +%Y-%m-%dT%H:%M:%SZ > "$BACKUP_OK_MARKER"
log "Offsite backup complete"
