#!/usr/bin/env bash
# Restore candle_collector from a pg_dump custom-format file into the local compose db, the way
# the disaster-recovery runbook does (deploy/README.md § Disaster recovery). TimescaleDB can't
# restore over an existing database, so the database is dropped and recreated from template0 at
# the dump's TimescaleDB version. The restore is strict (--exit-on-error), and migration 013 runs
# before the job scheduler restarts, so a dump taken before it brings no retention jobs back.
#
# Nothing changes until every check passes: no client session is connected (stop the
# collectors, api and monitor first; TimescaleDB's own workers don't count, and DROP DATABASE
# stops them), the dump's TimescaleDB version is available in the db image, and the archive
# reads end to end. A dump that places candles_1m on the ts_1m tablespace needs that tablespace
# in the db (migration 010).
#
# Usage: ./scripts/restore.sh backups/candle_collector-20260329-120000.dump   (from the project root)
#   TSDB_VERSION  the dump's TimescaleDB version (default: the one installed in candle_collector)
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <backup-file>"
    echo "Example: $0 backups/candle_collector-20260329-120000.dump"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKUP_FILE="$1"
CONTAINER_DUMP="/tmp/candle_collector-restore-$$.dump"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: backup file not found: ${BACKUP_FILE}"
    exit 1
fi

fail() { echo "==> FATAL: $*" >&2; exit 1; }
# Statements go in on stdin, so psql variables (-v) interpolate.
db_sql() { docker compose exec -T db psql -X -v ON_ERROR_STOP=1 -U candles "$@"; }
# A staged dump left behind is reported; the exit status stays the run's own.
cleanup() {
    if ! docker compose exec -T db rm -f "$CONTAINER_DUMP" > /dev/null 2>&1; then
        echo "==> WARNING: ${CONTAINER_DUMP} may remain in the db container" >&2
    fi
}

echo "==> WARNING: This drops the candle_collector database and recreates it from the dump."
echo "    Backup file: ${BACKUP_FILE}"
read -rp "    Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# -- Preflight: the database is untouched until all of this passes --------------------

sessions=$(db_sql -d postgres -At <<'SQL'
SELECT count(*) FROM pg_stat_activity
WHERE datname = 'candle_collector' AND backend_type = 'client backend' AND pid <> pg_backend_pid();
SQL
)
[ "$sessions" = "0" ] || fail "${sessions} client session(s) on candle_collector; stop the collectors, api and monitor first"

if [ -z "${TSDB_VERSION:-}" ]; then
    TSDB_VERSION=$(db_sql -d candle_collector -At 2>/dev/null <<'SQL'
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
SQL
) || fail "cannot read the installed TimescaleDB version; set TSDB_VERSION to the dump's version"
fi
available=$(db_sql -d postgres -At -v tsdb="$TSDB_VERSION" <<'SQL'
SELECT count(*) FROM pg_available_extension_versions WHERE name = 'timescaledb' AND version = :'tsdb';
SQL
)
[ "$available" = "1" ] || fail "TimescaleDB ${TSDB_VERSION} is not available in this db image; the restore needs the dump's version"

trap cleanup EXIT
echo "==> Copying the dump into the db container..."
docker compose cp "$BACKUP_FILE" "db:${CONTAINER_DUMP}"
echo "==> Checking the archive reads end to end..."
docker compose exec -T db pg_restore --file=/dev/null "$CONTAINER_DUMP" \
    || fail "${BACKUP_FILE} is not a readable pg_dump archive; candle_collector is untouched"

echo "==> Recreating candle_collector (TimescaleDB ${TSDB_VERSION})..."
db_sql -d postgres <<'SQL'
DROP DATABASE IF EXISTS candle_collector;
CREATE DATABASE candle_collector TEMPLATE template0;
SQL
db_sql -d candle_collector -v tsdb="$TSDB_VERSION" <<'SQL'
CREATE EXTENSION timescaledb VERSION :'tsdb';
SELECT timescaledb_pre_restore();
SQL

echo "==> Restoring from ${BACKUP_FILE} (strict: any error stops the restore)..."
docker compose exec -T db pg_restore -U candles -d candle_collector --exit-on-error "$CONTAINER_DUMP"

# A dump taken before migration 013 carries retention jobs. Clear them before post_restore
# restarts the job scheduler, or the next run drops old history.
echo "==> Removing restored retention policies (migration 013)..."
db_sql -d candle_collector < "$SCRIPT_DIR/../db/migrations/013-keep-all-history.sql" > /dev/null
retention_jobs=$(db_sql -d candle_collector -At <<'SQL'
SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention';
SQL
)
[ "$retention_jobs" = "0" ] || fail "${retention_jobs} retention job(s) remain; the scheduler stays stopped"

echo "==> Finalizing TimescaleDB restore..."
db_sql -d candle_collector <<'SQL'
SELECT timescaledb_post_restore();
SQL

echo "==> Row counts after restore:"
db_sql -d candle_collector <<'SQL'
SELECT 'candles' AS table_name, count(*) FROM candles
UNION ALL SELECT 'funding_rates', count(*) FROM funding_rates
UNION ALL SELECT 'open_interest', count(*) FROM open_interest
UNION ALL SELECT 'collection_status', count(*) FROM collection_status
ORDER BY 1;
SQL

echo "==> Restore complete."
