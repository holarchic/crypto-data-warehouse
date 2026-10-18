#!/usr/bin/env bash
# Restore drill: restores the latest offsite snapshot (scripts/backup-offsite.sh) into a
# throwaway TimescaleDB container, proves the restored database is live, and prints its row
# counts (scripts/drill-counts.sql) for comparison with prod. Runs on any machine with Docker,
# restic, python3 and the backup's env file, and never touches prod. Its fresh-target steps are
# the disaster-recovery runbook's (deploy/README.md § Offsite backups).
#
# Usage: RESTIC_ENV_FILE=<env file> scripts/restore-drill.sh [--keep]
#   DRILL_DIR  where the dump is downloaded (default: a new temp dir; needs the dump's size free)
#   --keep     leave the container and the dump in place for inspection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESTIC_ENV_FILE="${RESTIC_ENV_FILE:-/etc/candle-collector/restic.env}"
CONTAINER="candle-collector-restore-drill"
SNAPSHOT_HOST="candle-collector"
KEEP=0
if [ "${1:-}" = "--keep" ]; then
    KEEP=1
fi
MADE_DRILL_DIR=0
if [ -z "${DRILL_DIR:-}" ]; then
    DRILL_DIR=$(mktemp -d)
    MADE_DRILL_DIR=1
fi
DUMP="$DRILL_DIR/candle_collector.dump"

log() { echo "==> $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }
fail() { echo "==> FATAL: $*" >&2; exit 1; }

restic_run() {
    # shellcheck source=/dev/null
    ( set -a; . "$RESTIC_ENV_FILE"; set +a; exec restic "$@" )
}

# Every statement goes in on stdin, so psql variables (-v) interpolate.
psql_drill() { docker exec -i "$CONTAINER" psql -X -v ON_ERROR_STOP=1 -U candles "$@"; }

cleanup() {
    if [ "$KEEP" -eq 1 ]; then
        log "Kept container ${CONTAINER} and ${DUMP}"
        return
    fi
    docker rm -f "$CONTAINER" > /dev/null 2>&1 || true
    rm -f "$DUMP"
    if [ "$MADE_DRILL_DIR" -eq 1 ]; then
        rmdir "$DRILL_DIR" 2>/dev/null || true
    fi
}

[ -f "$RESTIC_ENV_FILE" ] || fail "${RESTIC_ENV_FILE} not found"
if docker container inspect "$CONTAINER" > /dev/null 2>&1; then
    fail "container ${CONTAINER} already exists (a kept drill?); remove it with: docker rm -f ${CONTAINER}"
fi
trap cleanup EXIT

# -- Snapshot -----------------------------------------------------------------
# The latest snapshot, the image its tags pin, and the count window: UTC days 14 to 4 before it.

SNAPSHOT=$(restic_run snapshots --host "$SNAPSHOT_HOST" --json | python3 -c '
import json, re, sys
from datetime import datetime, timedelta, timezone

snaps = json.load(sys.stdin)
if not snaps:
    sys.exit("no snapshots for this host")
snap = max(snaps, key=lambda s: s["time"])
tags = snap.get("tags") or []

def tag(prefix):
    values = [t[len(prefix):] for t in tags if t.startswith(prefix)]
    if len(values) != 1:
        sys.exit("snapshot " + snap["short_id"] + " lacks one " + prefix + "<version> tag")
    return values[0]

# restic writes nanoseconds and a local offset; fromisoformat takes neither everywhere.
m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)$", snap["time"])
offset = "+00:00" if m[2] == "Z" else m[2]
taken = datetime.fromisoformat(m[1] + offset).astimezone(timezone.utc)
day = taken.date()
print(snap["short_id"], taken.strftime("%Y-%m-%dT%H:%M:%SZ"), tag("timescaledb-"), tag("pg"),
      day - timedelta(days=14), day - timedelta(days=4))
')
read -r SNAP_ID SNAP_TIME TSDB_VERSION PG_MAJOR SINCE UNTIL <<< "$SNAPSHOT"
IMAGE="timescale/timescaledb:${TSDB_VERSION}-pg${PG_MAJOR}"
log "Snapshot ${SNAP_ID} taken ${SNAP_TIME}: TimescaleDB ${TSDB_VERSION}, PostgreSQL ${PG_MAJOR}"

log "Downloading the dump to ${DUMP}"
restic_run dump "$SNAP_ID" /candle_collector.dump > "$DUMP"

# -- Fresh target ---------------------------------------------------------------
# Mirrors prod: the one superuser `candles` owns every object and job, `ts_1m` exists for the
# restored tablespace attachments, and the database starts from template0 because the image
# installs TimescaleDB into template1.

log "Starting ${IMAGE}"
docker run -d --name "$CONTAINER" -e POSTGRES_USER=candles -e POSTGRES_PASSWORD=drill \
    -v "$DRILL_DIR:/drill:ro" "$IMAGE" > /dev/null
# Over TCP: the image's init server listens on the socket only, then restarts.
for _ in $(seq 90); do
    if docker exec "$CONTAINER" pg_isready -q -h 127.0.0.1 -U candles; then
        break
    fi
    sleep 2
done
docker exec "$CONTAINER" pg_isready -q -h 127.0.0.1 -U candles || fail "${IMAGE} never became ready"

docker exec "$CONTAINER" sh -c \
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
docker exec "$CONTAINER" pg_restore -U candles -d candle_collector --exit-on-error /drill/candle_collector.dump
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

-- A new candles_1m chunk lands in ts_1m, through the restored attachment. Rolled back.
BEGIN;
CREATE TEMP TABLE drill_row ON COMMIT DROP AS
    SELECT * FROM candles_1m ORDER BY timestamp DESC LIMIT 1;
UPDATE drill_row SET "timestamp" = "timestamp" + interval '60 days';
INSERT INTO candles_1m SELECT * FROM drill_row;
DO $$
DECLARE chunk_tablespace text;
BEGIN
    SELECT c.chunk_tablespace INTO chunk_tablespace
    FROM timescaledb_information.chunks c, drill_row r
    WHERE c.hypertable_name = 'candles_1m'
      AND c.range_start <= r.timestamp AND c.range_end > r.timestamp;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no candles_1m chunk holds the inserted row';
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
