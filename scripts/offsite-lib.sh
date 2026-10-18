# shellcheck shell=bash
# shellcheck disable=SC2034  # the names below are read by the scripts that source this file
# The offsite backup's shared contract, sourced by scripts/backup-offsite.sh,
# scripts/restore-drill.sh and deploy/host-monitor.sh: where the credentials and the success
# marker live, how snapshots are named, and how restic gets the bucket credentials.

RESTIC_ENV_FILE="${RESTIC_ENV_FILE:-/etc/candle-collector/restic.env}"
BACKUP_OK_MARKER="${BACKUP_OK_MARKER:-/var/lib/candle-collector/backup-ok}"
# Fixed rather than the hostname, so retention groups survive a host rebuild.
SNAPSHOT_HOST="candle-collector"
SNAPSHOT_FILE="candle_collector.dump"

# Runs restic with the env file's variables, which exist only in restic's process, never in
# the caller's shell. Returns restic's status, and the caller decides how a failure is
# reported. Called from a `||` list errexit is off in here, hence the explicit exit.
restic_run() {
    # shellcheck source=/dev/null
    ( set -a; . "$RESTIC_ENV_FILE" || exit; set +a; exec restic "$@" )
}
