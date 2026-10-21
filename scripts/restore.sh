#!/usr/bin/env bash
# Restore the application database (the db container's POSTGRES_DB) from a pg_dump custom-format
# file into the local compose db, the way the disaster-recovery runbook does (deploy/README.md
# § Disaster recovery). TimescaleDB can't
# restore over an existing database, so the database is dropped and recreated from template0 at
# the dump's TimescaleDB version. The restore is strict (--exit-on-error), and migration 013 runs
# before the job scheduler restarts, so a dump taken before it brings no retention jobs back.
#
# Nothing changes until every check passes: no other restore is running; no client session is
# connected (stop the collectors, api and monitor first; TimescaleDB's own workers don't count,
# and DROP DATABASE stops them); the dump's TimescaleDB version is available in the db image;
# the archive reads end to end; and every tablespace it places tables on exists (prod's ts_1m
# comes from migration 010).
#
# Usage: ./scripts/restore.sh backups/candle_collector-<time>-timescaledb-<version>.dump
#   (from the project root). The dump's TimescaleDB version comes from that name, as
#   scripts/backup.sh writes it; for a dump named otherwise, set TSDB_VERSION.
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <backup-file>"
    echo "Example: $0 backups/candle_collector-20260329-120000-timescaledb-2.26.2.dump"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/ops-lib.sh
. "$SCRIPT_DIR/ops-lib.sh"
BACKUP_FILE="$1"
CONTAINER_DUMP="/tmp/candle_collector-restore-$$.dump"
# mkdir is atomic on every platform the script runs on, unlike flock (absent on macOS). A fixed
# path, not $TMPDIR, so every shell on the host contends for the same lock.
LOCK_DIR="/tmp/candle-collector-restore.lock"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: backup file not found: ${BACKUP_FILE}"
    exit 1
fi
BACKUP_FILE="$(cd "$(dirname "$BACKUP_FILE")" && pwd)/$(basename "$BACKUP_FILE")"

fail() { echo "==> FATAL: $*" >&2; exit 1; }
# Statements go in on stdin, so psql variables (-v) interpolate. The application database by
# default; `-d postgres` reaches the maintenance database.
db_sql() { "${DB_TOOL[@]}" psql -X -v ON_ERROR_STOP=1 "$@"; }
# A staged dump left behind is reported; the exit status stays the run's own.
cleanup() {
    if ! "${DC[@]}" exec -T db rm -f "$CONTAINER_DUMP" > /dev/null 2>&1; then
        echo "==> WARNING: ${CONTAINER_DUMP} may remain in the db container" >&2
    fi
    rmdir "$LOCK_DIR" 2>/dev/null || true
}

# The target is the db container's own POSTGRES_DB, read once: the prompt names the database
# that every statement below drops, recreates and restores.
DB=$(db_name) || fail "could not read the db container's POSTGRES_DB; is the db running?"
[ -n "$DB" ] || fail "the db container has no POSTGRES_DB"

echo "==> WARNING: This drops the ${DB} database and recreates it from the dump."
echo "    Backup file: ${BACKUP_FILE}"
read -rp "    Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# -- Preflight: the database is untouched until all of this passes --------------------

mkdir "$LOCK_DIR" 2>/dev/null || fail "another restore holds ${LOCK_DIR}; if none is running, remove it"
trap cleanup EXIT

sessions=$(db_sql -d postgres -At -v db="$DB" <<'SQL'
SELECT count(*) FROM pg_stat_activity
WHERE datname = :'db' AND backend_type = 'client backend' AND pid <> pg_backend_pid();
SQL
)
[ "$sessions" = "0" ] || fail "${sessions} client session(s) on ${DB}; stop the collectors, api and monitor first"

if [ -z "${TSDB_VERSION:-}" ]; then
    case "$(basename "$BACKUP_FILE")" in
        *-timescaledb-*.dump) TSDB_VERSION="$(basename "$BACKUP_FILE" .dump)"; TSDB_VERSION="${TSDB_VERSION##*-timescaledb-}" ;;
        *) fail "the dump's name carries no TimescaleDB version; set TSDB_VERSION to the version it was taken at" ;;
    esac
fi
available=$(db_sql -d postgres -At -v tsdb="$TSDB_VERSION" <<'SQL'
SELECT count(*) FROM pg_available_extension_versions WHERE name = 'timescaledb' AND version = :'tsdb';
SQL
)
[ "$available" = "1" ] || fail "TimescaleDB ${TSDB_VERSION} is not available in this db image; the restore needs the dump's version"

echo "==> Copying the dump into the db container..."
"${DC[@]}" cp "$BACKUP_FILE" "db:${CONTAINER_DUMP}"
echo "==> Checking the archive reads end to end, and the tablespaces it needs..."
# shellcheck disable=SC2016  # $1 expands inside the container
tablespaces=$("${DC[@]}" exec -T db sh -c \
    'set -o pipefail; pg_restore --file=- "$1" | sed -n "s/^SET default_tablespace = \(.*\);\$/\1/p" | sort -u' \
    _ "$CONTAINER_DUMP") \
    || fail "${BACKUP_FILE} is not a readable pg_dump archive; ${DB} is untouched"
# One identifier per line, as pg_dump prints it: '' is the default tablespace, and a name that
# needs quoting comes double-quoted with any inner quote doubled.
while IFS= read -r tablespace; do
    [ -z "$tablespace" ] || [ "$tablespace" = "''" ] && continue
    case "$tablespace" in
        \"*\") tablespace="${tablespace#\"}"; tablespace="${tablespace%\"}"; tablespace="${tablespace//\"\"/\"}" ;;
    esac
    found=$(db_sql -d postgres -At -v ts="$tablespace" <<'SQL'
SELECT count(*) FROM pg_tablespace WHERE spcname = :'ts';
SQL
)
    [ "$found" = "1" ] || fail "the dump places tables on tablespace ${tablespace}, which this db lacks (on prod: migration 010); ${DB} is untouched"
done <<< "$tablespaces"

echo "==> Recreating ${DB} (TimescaleDB ${TSDB_VERSION})..."
db_sql -d postgres -v db="$DB" <<'SQL'
DROP DATABASE IF EXISTS :"db";
CREATE DATABASE :"db" TEMPLATE template0;
SQL
db_sql -v tsdb="$TSDB_VERSION" <<'SQL'
CREATE EXTENSION timescaledb VERSION :'tsdb';
SELECT timescaledb_pre_restore();
SQL

echo "==> Restoring from ${BACKUP_FILE} (strict: any error stops the restore)..."
"${DB_TOOL[@]}" pg_restore --exit-on-error "$CONTAINER_DUMP"

# A dump taken before migration 013 carries retention jobs. Clear them before post_restore
# restarts the job scheduler, or the next run drops old history.
echo "==> Removing restored retention policies (migration 013)..."
db_sql < "$SCRIPT_DIR/../db/migrations/013-keep-all-history.sql" > /dev/null
retention_jobs=$(db_sql -At <<'SQL'
SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention';
SQL
)
[ "$retention_jobs" = "0" ] || fail "${retention_jobs} retention job(s) remain; the scheduler stays stopped"

echo "==> Finalizing TimescaleDB restore..."
db_sql <<'SQL'
SELECT timescaledb_post_restore();
SQL

echo "==> Row counts after restore:"
db_sql <<'SQL'
SELECT 'candles' AS table_name, count(*) FROM candles
UNION ALL SELECT 'funding_rates', count(*) FROM funding_rates
UNION ALL SELECT 'open_interest', count(*) FROM open_interest
UNION ALL SELECT 'collection_status', count(*) FROM collection_status
ORDER BY 1;
SQL

echo "==> Restore complete."
