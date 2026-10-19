#!/usr/bin/env bash
# Nightly offsite backup: streams pg_dump from the db container into a restic repository in a
# bucket, applies retention, checks the repository (on Sundays reading back a quarter of its
# data), then records success.
#
# Credentials live outside the checkout, in RESTIC_ENV_FILE (deploy/README.md § Offsite backups):
# RESTIC_REPOSITORY and RESTIC_PASSWORD_FILE, plus the bucket key as rclone's RCLONE_CONFIG_B2_*
# for B2, or AWS_* for an S3 store. Without that file the script exits 0: offsite backup is opt-in.
# Sends a webhook alert on failure; deploy/host-monitor.sh pages when BACKUP_OK_MARKER goes stale.
set -euo pipefail

# cron's PATH is /usr/bin:/bin, and deploy/install-restic.sh installs to /usr/local/bin.
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=scripts/offsite-lib.sh
. "$SCRIPT_DIR/offsite-lib.sh"
LOCK_FILE="/run/lock/candle-collector-backup.lock"
LOG_HINT="check /var/log/candle-collector-backup.log"
DC=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

log() { echo "==> $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

if [ ! -f "$RESTIC_ENV_FILE" ]; then
    log "Offsite backup not configured: ${RESTIC_ENV_FILE} absent"
    exit 0
fi

# -- Failure alerts -------------------------------------------------------------

# shellcheck source=scripts/webhook-lib.sh
. "$SCRIPT_DIR/webhook-lib.sh"
WEBHOOK_URL=$(webhook_url "$PROJECT_DIR")

send_alert() {
    local message="$1"
    echo "==> ALERT: $message" >&2
    webhook_post "$WEBHOOK_URL" "🚨 candle-collector offsite backup ($(hostname)): $message" \
        || echo "==> Page not delivered" >&2
}

fail() {
    send_alert "$1"
    exit 1
}

# Any other failing command. restic calls report their own failure through fail(): the ERR trap
# does not reach inside the restic_run function.
trap 'send_alert "Offsite backup failed; ${LOG_HINT}"' ERR

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
restic_run unlock || fail "restic unlock failed (exit $?); ${LOG_HINT}"

TSDB_VERSION=$("${DC[@]}" exec -T db psql -X -U candles -d candle_collector -Atc \
    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
PG_MAJOR=$("${DC[@]}" exec -T db psql -X -U candles -d candle_collector -Atc \
    "SELECT current_setting('server_version_num')::int / 10000")
[ -n "$TSDB_VERSION" ] || fail "TimescaleDB extension not found in candle_collector"
log "TimescaleDB ${TSDB_VERSION}, PostgreSQL ${PG_MAJOR}"

# -- Backup -------------------------------------------------------------------
# Uncompressed, so restic's chunking dedups the unchanged (already-compressed) chunks night to
# night; restic compresses what it stores. The tags name the image scripts/restore-drill.sh
# restores into. restic hands its environment to the dump command, so `env` drops every
# variable the env file sets: the bucket key never reaches compose, which reads the HL pull's
# AWS_* from the project .env.

STRIP=$(restic_strip_env_args)
read -r -a STRIP_ENV <<< "$STRIP"

log "Dumping candle_collector into the repository"
restic_run backup --host "$SNAPSHOT_HOST" \
    --tag "${SNAPSHOT_TAG_TIMESCALEDB}${TSDB_VERSION}" --tag "${SNAPSHOT_TAG_PG}${PG_MAJOR}" \
    --stdin-filename "$SNAPSHOT_FILE" --stdin-from-command -- \
    env "${STRIP_ENV[@]}" \
    "${DC[@]}" exec -T db pg_dump -U candles --format=custom --compress=0 candle_collector \
    || fail "restic backup failed (exit $?), no snapshot made; ${LOG_HINT}"

log "Applying retention"
restic_run forget --host "$SNAPSHOT_HOST" --keep-daily 7 --keep-weekly 5 --keep-monthly 12 --prune \
    || fail "restic forget/prune failed (exit $?); ${LOG_HINT}"

# Sundays also read back a quarter of the stored data, so a pack the bucket can no longer
# serve pages within weeks rather than at a restore. A later night's success refreshes the
# marker without rereading it: the page closes only when the named subset rereads clean.
SUBSET=$(read_back_subset "$(date -u +%u)" "$(date -u +%V)")
CHECK_ARGS=()
if [ -n "$SUBSET" ]; then
    CHECK_ARGS=(--read-data-subset "$SUBSET")
    log "Checking the repository and reading back packs ${SUBSET}"
else
    log "Checking the repository"
fi
restic_run check "${CHECK_ARGS[@]}" \
    || fail "restic check ${SUBSET:+--read-data-subset ${SUBSET} }failed (exit $?); ${LOG_HINT}"

mkdir -p "$(dirname "$BACKUP_OK_MARKER")"
date -u +%Y-%m-%dT%H:%M:%SZ > "$BACKUP_OK_MARKER"
log "Offsite backup complete"
