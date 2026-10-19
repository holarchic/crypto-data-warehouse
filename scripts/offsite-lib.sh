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
# Each snapshot carries one tag of each, <prefix><version>, naming the image to restore into.
SNAPSHOT_TAG_TIMESCALEDB="timescaledb-"
SNAPSHOT_TAG_PG="pg"

# Runs restic with the env file's variables, which exist only in restic's process and the
# rclone it starts, never in the caller's shell. Returns restic's status, and the caller decides
# how a failure is reported. Called from a `||` list errexit is off in here, hence the explicit
# exit.
#
# restic starts rclone with `--b2-hard-delete` by default. A bucket key without deleteFiles
# refuses that, and with the capability it would destroy the versions the bucket's lifecycle
# keeps for recovery. Without it rclone hides a removed file, as the S3 backend does. restic
# ignores the option for every other backend.
restic_run() {
    # shellcheck source=/dev/null
    ( set -a; . "$RESTIC_ENV_FILE" || exit; set +a
      exec restic -o rclone.args="serve restic --stdio" "$@" )
}

# Prints `-u NAME` for each variable the env file assigns, on one line, for `env` to drop from
# a command restic starts. Names only: the values never leave the file. The env file holds one
# NAME=value assignment per line.
restic_strip_env_args() {
    local names name
    names=$(sed -n -E 's/^[[:space:]]*(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)=.*/\2/p' \
        "$RESTIC_ENV_FILE") || return
    for name in $names; do printf -- '-u %s ' "$name"; done
    echo
}

# Prints the quarter of the repository's packs to read back for ISO weekday $1 (1-7) of ISO
# week $2, as restic's `--read-data-subset n/4`: Sundays only, the next quarter each week, so
# every pack is read within 4 weeks (5 where a year end restarts the count).
read_back_subset() {
    [ "$1" -eq 7 ] || return 0
    echo "$(( 10#$2 % 4 + 1 ))/4"
}
