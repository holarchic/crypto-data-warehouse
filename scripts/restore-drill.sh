#!/usr/bin/env bash
# Restore drill: restores the latest offsite snapshot (scripts/backup-offsite.sh) into a
# throwaway TimescaleDB container, proves the restored database is live, and prints its row
# counts (scripts/drill-counts.sql) for comparison with prod. Runs on any machine with Docker,
# restic, python3 and the backup's env file, and never touches prod. Its fresh-target steps are
# the disaster-recovery runbook's (deploy/README.md § Offsite backups).
#
# Usage: RESTIC_ENV_FILE=<env file> scripts/restore-drill.sh [--keep]
#   DRILL_DIR  base directory (default: the temp dir). The run downloads into a new
#              subdirectory of it, which needs the dump's size free.
#   --keep     leave the container and the download in place for inspection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/offsite-lib.sh
. "$SCRIPT_DIR/offsite-lib.sh"
CONTAINER="candle-collector-restore-drill"

KEEP=0
case "$#:${1:-}" in
    0:) ;;
    1:--keep) KEEP=1 ;;
    *) echo "Usage: $0 [--keep]" >&2; exit 2 ;;
esac

log() { echo "==> $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }
fail() { echo "==> FATAL: $*" >&2; exit 1; }

# What this run created, and so the only things its cleanup removes.
RUN_DIR=""
CONTAINER_ID=""

# A cleanup failure is reported, and fails a drill that otherwise passed: a leftover restore
# holds as much disk as the database.
cleanup() {
    local status=$? leftover=0
    if [ "$KEEP" -eq 1 ]; then
        if [ -n "$CONTAINER_ID" ]; then
            log "Kept container ${CONTAINER} (remove with: docker rm -fv ${CONTAINER})"
        fi
        if [ -n "$RUN_DIR" ]; then
            log "Kept ${RUN_DIR}"
        fi
        return
    fi
    if [ -n "$CONTAINER_ID" ]; then
        # -v: the image's PGDATA is an anonymous volume, as large as the restore.
        if ! docker rm -fv "$CONTAINER_ID" > /dev/null; then
            echo "==> WARNING: container ${CONTAINER_ID:0:12} and its volume remain: docker rm -fv ${CONTAINER_ID:0:12}" >&2
            leftover=1
        fi
    fi
    if [ -n "$RUN_DIR" ] && ! rm -rf "$RUN_DIR"; then
        echo "==> WARNING: ${RUN_DIR} remains" >&2
        leftover=1
    fi
    if [ "$status" -eq 0 ] && [ "$leftover" -eq 1 ]; then
        exit 1
    fi
}
trap cleanup EXIT

# Every statement goes in on stdin, so psql variables (-v) interpolate.
psql_drill() { docker exec -i "$CONTAINER_ID" psql -X -v ON_ERROR_STOP=1 -U candles "$@"; }

[ -f "$RESTIC_ENV_FILE" ] || fail "${RESTIC_ENV_FILE} not found"
BASE_DIR="${DRILL_DIR:-${TMPDIR:-/tmp}}"
BASE_DIR=$(cd "$BASE_DIR" && pwd) || fail "DRILL_DIR ${BASE_DIR} is not a usable directory"
RUN_DIR=$(mktemp -d "$BASE_DIR/restore-drill.XXXXXX")
DUMP="$RUN_DIR/$SNAPSHOT_FILE"

# -- Snapshot -----------------------------------------------------------------
# The newest snapshot, the image its tags pin, and the count window: UTC days 14 to 4 before it.

SNAPSHOT=$(restic_run snapshots --host "$SNAPSHOT_HOST" --json | python3 -c '
import json, re, sys
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

TIME = re.compile(
    r"(?P<clock>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(?P<fraction>\d+))?(?P<offset>Z|[+-]\d\d:\d\d)$"
)
VERSION = re.compile(r"\d+(\.\d+)*$")

class Taken(NamedTuple):
    """When a snapshot was taken; orders by instant, then by the sub-second nanoseconds."""
    instant: datetime
    nanoseconds: int

def taken(snap):
    # restic writes nanoseconds and a local offset; fromisoformat takes neither everywhere.
    m = TIME.match(snap["time"])
    if not m:
        sys.exit("snapshot " + snap["short_id"] + " has an unreadable time: " + snap["time"])
    offset = "+00:00" if m["offset"] == "Z" else m["offset"]
    instant = datetime.fromisoformat(m["clock"] + offset).astimezone(timezone.utc)
    nanoseconds = int((m["fraction"] or "0").ljust(9, "0")[:9])
    return Taken(instant=instant, nanoseconds=nanoseconds)

def tag(snap, prefix):
    values = [t[len(prefix):] for t in snap.get("tags") or [] if t.startswith(prefix)]
    if len(values) != 1 or not VERSION.match(values[0]):
        sys.exit("snapshot " + snap["short_id"] + " lacks one " + prefix + "<version> tag")
    return values[0]

snaps = json.load(sys.stdin)
if not snaps:
    sys.exit("no snapshots for this host")
latest = max(({"taken": taken(s), "snap": s} for s in snaps), key=lambda dated: dated["taken"])
snap, instant = latest["snap"], latest["taken"].instant
day = instant.date()
fields = {
    "snapshot_id": snap["id"],
    "snapshot_time": instant.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "timescaledb_version": tag(snap, "timescaledb-"),
    "pg_major": tag(snap, "pg"),
    "count_since": (day - timedelta(days=14)).isoformat(),
    "count_until": (day - timedelta(days=4)).isoformat(),
}
for key, value in fields.items():
    print(key + "=" + value)
')
SNAP_ID="" SNAP_TIME="" TSDB_VERSION="" PG_MAJOR="" SINCE="" UNTIL=""
while IFS='=' read -r key value; do
    case "$key" in
        snapshot_id) SNAP_ID="$value" ;;
        snapshot_time) SNAP_TIME="$value" ;;
        timescaledb_version) TSDB_VERSION="$value" ;;
        pg_major) PG_MAJOR="$value" ;;
        count_since) SINCE="$value" ;;
        count_until) UNTIL="$value" ;;
    esac
done <<< "$SNAPSHOT"
for field in "$SNAP_ID" "$SNAP_TIME" "$TSDB_VERSION" "$PG_MAJOR" "$SINCE" "$UNTIL"; do
    [ -n "$field" ] || fail "incomplete snapshot metadata: ${SNAPSHOT}"
done
IMAGE="timescale/timescaledb:${TSDB_VERSION}-pg${PG_MAJOR}"
log "Snapshot ${SNAP_ID:0:8} taken ${SNAP_TIME}: TimescaleDB ${TSDB_VERSION}, PostgreSQL ${PG_MAJOR}"

log "Downloading the dump to ${DUMP}"
restic_run dump "$SNAP_ID" "/$SNAPSHOT_FILE" > "$DUMP"

# -- Fresh target ---------------------------------------------------------------
# Mirrors prod: the one superuser `candles` owns every object and job, `ts_1m` exists for the
# restored tablespace attachments, and the database starts from template0 because the image
# installs TimescaleDB into template1.

log "Starting ${IMAGE}"
CONTAINER_ID=$(docker run -d --name "$CONTAINER" -e POSTGRES_USER=candles -e POSTGRES_PASSWORD=drill \
    -v "$RUN_DIR:/drill:ro" "$IMAGE") \
    || fail "could not start ${CONTAINER}; if a kept drill holds the name: docker rm -fv ${CONTAINER}"
# Over TCP: the image's init server listens on the socket only, then restarts.
for _ in $(seq 90); do
    if docker exec "$CONTAINER_ID" pg_isready -q -h 127.0.0.1 -U candles; then
        break
    fi
    sleep 2
done
docker exec "$CONTAINER_ID" pg_isready -q -h 127.0.0.1 -U candles || fail "${IMAGE} never became ready"

docker exec "$CONTAINER_ID" sh -c \
    'mkdir -p /var/lib/postgresql/ts_1m && chown postgres:postgres /var/lib/postgresql/ts_1m'
psql_drill -d candles <<'SQL'
CREATE TABLESPACE ts_1m LOCATION '/var/lib/postgresql/ts_1m';
CREATE DATABASE candle_collector TEMPLATE template0;
SQL
psql_drill -d candle_collector -v tsdb="$TSDB_VERSION" <<'SQL'
CREATE EXTENSION timescaledb VERSION :'tsdb';
SELECT timescaledb_pre_restore();
SQL
installed=$(psql_drill -d candle_collector -At <<'SQL'
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
SQL
)
[ "$installed" = "$TSDB_VERSION" ] || fail "TimescaleDB ${installed} installed; the snapshot needs ${TSDB_VERSION}"

# -- Restore --------------------------------------------------------------------
# Strict: any error fails the drill. Investigate it, then rerun on a fresh container.

log "Restoring"
docker exec "$CONTAINER_ID" pg_restore -U candles -d candle_collector --exit-on-error "/drill/$SNAPSHOT_FILE"
psql_drill -d candle_collector <<'SQL'
SELECT timescaledb_post_restore();
SQL
log "Restore complete"

# -- Live checks ----------------------------------------------------------------

log "Checking the restored database is live"
psql_drill -d candle_collector <<'SQL'
-- Every hypertable is owned by the role its jobs run as, and that role can log in.
DO $$
DECLARE mismatched text;
BEGIN
    SELECT string_agg(DISTINCT h.hypertable_name, ', ') INTO mismatched
    FROM timescaledb_information.hypertables h
    JOIN timescaledb_information.jobs j USING (hypertable_schema, hypertable_name)
    JOIN pg_roles r ON r.rolname = j.owner::text
    WHERE h.owner::text <> j.owner::text OR NOT r.rolcanlogin;
    IF mismatched IS NOT NULL THEN
        RAISE EXCEPTION 'job owner differs from hypertable owner, or cannot log in: %', mismatched;
    END IF;
END $$;

-- A restored policy runs under its retained owner.
SELECT job_id AS compression_job FROM timescaledb_information.jobs
    WHERE hypertable_name = 'funding_rates' AND proc_name = 'policy_compression' \gset
CALL run_job(:compression_job);

-- A new candles_1m chunk lands in ts_1m, through the restored attachment. The row goes a
-- day past every existing chunk, so its chunk is created here. Rolled back.
BEGIN;
CREATE TEMP TABLE drill_row ON COMMIT DROP AS
    SELECT * FROM candles_1m ORDER BY timestamp DESC LIMIT 1;
UPDATE drill_row SET "timestamp" = (
    SELECT max(range_end) FROM timescaledb_information.chunks WHERE hypertable_name = 'candles_1m'
) + interval '1 day';
CREATE TEMP TABLE drill_chunks_before ON COMMIT DROP AS
    SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'candles_1m';
INSERT INTO candles_1m SELECT * FROM drill_row;
DO $$
DECLARE chunk_tablespace text;
BEGIN
    SELECT c.chunk_tablespace INTO chunk_tablespace
    FROM timescaledb_information.chunks c, drill_row r
    WHERE c.hypertable_name = 'candles_1m'
      AND c.range_start <= r.timestamp AND c.range_end > r.timestamp
      AND c.chunk_name NOT IN (SELECT chunk_name FROM drill_chunks_before);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'the inserted candles_1m row did not create a new chunk';
    END IF;
    IF chunk_tablespace IS DISTINCT FROM 'ts_1m' THEN
        RAISE EXCEPTION 'new candles_1m chunk is in %, expected ts_1m',
            coalesce(chunk_tablespace, 'the default tablespace');
    END IF;
END $$;
ROLLBACK;
SQL
log "Live checks passed"

log "Row counts over [${SINCE}, ${UNTIL}); prod's before and after the dump must bracket them"
psql_drill -d candle_collector -At -F $'\t' -v since="$SINCE" -v until="$UNTIL" \
    < "$SCRIPT_DIR/drill-counts.sql"
