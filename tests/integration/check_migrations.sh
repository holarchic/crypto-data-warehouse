#!/usr/bin/env bash
# The migration ledger against a real TimescaleDB: initdb with init.sql + ledger.sql, apply with real
# hypertable and compression DDL, rollback of a failing migration, and adoption of a database built
# without the ledger. Runs outside pytest, from the project root, with Docker:
#
#     ./tests/integration/check_migrations.sh
#
# Prints one line per check, exits non-zero on the first failure, and removes its containers.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE=timescale/timescaledb:2.26.2-pg16
WORK=$(mktemp -d)
CONTAINERS=()
TARGET=""

cleanup() {
    for c in "${CONTAINERS[@]}"; do docker rm -fv "$c" > /dev/null 2>&1 || true; done
    rm -rf "$WORK"
}
trap cleanup EXIT

die() { echo "FAIL $*" >&2; exit 1; }
ok() { echo "ok   $*"; }

migrations_psql() { docker exec -i "$TARGET" psql -X -U candles -d candle_collector "$@"; }
# shellcheck source=scripts/migrations-lib.sh
. "$ROOT/scripts/migrations-lib.sh"
REAL_MIGRATIONS="$MIGRATIONS_DIR"
REAL_LEDGER="$LEDGER_SQL"

q() { docker exec -i "$TARGET" psql -X -U candles -d candle_collector -At -v ON_ERROR_STOP=1 -c "$1"; }

# start_db NAME INITDB_FILE... : a fresh container whose initdb runs the given files, ready over TCP.
start_db() {
    local name="cc-migrate-check-$1" dir="$WORK/$1"
    shift
    mkdir -p "$dir"
    cp "$@" "$dir/"
    docker run -d --name "$name" -e POSTGRES_USER=candles -e POSTGRES_PASSWORD=check \
        -e POSTGRES_DB=candle_collector -v "$dir:/docker-entrypoint-initdb.d:ro" "$IMAGE" > /dev/null
    CONTAINERS+=("$name")
    for _ in $(seq 90); do
        if docker exec "$name" pg_isready -q -h 127.0.0.1 -U candles; then TARGET="$name"; return 0; fi
        sleep 2
    done
    die "$name never accepted TCP connections"
}

# A migrations dir: the real files plus the named scratch migrations.
scratch_dir() {
    local dir="$WORK/migrations-$1" m
    shift
    mkdir -p "$dir"
    cp "$REAL_MIGRATIONS"/*.sql "$dir/"
    for m in "$@"; do cp "$WORK/fixtures/$m" "$dir/"; done
    echo "$dir"
}

mkdir -p "$WORK/fixtures"
cat > "$WORK/fixtures/014-check.sql" <<'SQL'
CREATE TABLE check_014 (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION);
SELECT create_hypertable('check_014', 'ts', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE check_014 SET (timescaledb.compress, timescaledb.compress_orderby = 'ts DESC');
SELECT add_compression_policy('check_014', INTERVAL '7 days');
SQL
cat > "$WORK/fixtures/015-fails.sql" <<'SQL'
CREATE TABLE check_015 (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION);
SELECT create_hypertable('check_015', 'ts', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE check_015 SET (timescaledb.compress, timescaledb.compress_orderby = 'ts DESC');
SELECT add_compression_policy('check_015', INTERVAL '7 days');
SELECT 1 / 0;
SQL
cat > "$WORK/fixtures/016-after.sql" <<'SQL'
CREATE TABLE check_016 (x INTEGER);
SQL
cat > "$WORK/ledger-fails.sql" <<'SQL'
BEGIN;
CREATE TABLE public.schema_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL);
SELECT 1 / 0;
COMMIT;
SQL

baseline_failures() { _migrations_sql -At -F '|' < "$BASELINE_CHECK_SQL" | awk -F '|' '$2 != "t" {print $1}'; }
compression_job() { q "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = '$1';"; }

# --- fresh initdb: init.sql, then ledger.sql ------------------------------------------------------
start_db fresh "$ROOT/db/init.sql" "$ROOT/db/ledger.sql"
[ "$(migrations_state)" = "adopted" ] || die "fresh: state is not adopted"
expected=$(find "$REAL_MIGRATIONS" -name '[0-9][0-9][0-9]-*.sql' -exec basename {} \; | sort | tr '\n' ' ')
[ "$(q "SELECT string_agg(name, ' ' ORDER BY version) || ' ' FROM schema_migrations;")" = "$expected" ] \
    || die "fresh: ledger is not the baseline files"
[ -z "$(migrations_pending)" ] || die "fresh: something is pending"
ok "fresh initdb records the 12 baseline files and nothing is pending"
[ -z "$(baseline_failures)" ] || die "fresh: baseline check fails on init.sql: $(baseline_failures)"
ok "baseline-check.sql passes on the init.sql schema"

MIGRATIONS_DIR=$(scratch_dir one 014-check.sql)
migrations_apply > /dev/null
[ "$(q "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'check_014';")" = "1" ] \
    || die "014 hypertable missing"
[ "$(compression_job check_014)" = "1" ] || die "014 compression policy missing"
[ "$(q "SELECT name FROM schema_migrations WHERE version = '014';")" = "014-check.sql" ] || die "014 ledger row missing"
ok "014 (hypertable, compression, policy) applied with its ledger row"
[ "$(migrations_apply)" = "migrate: nothing pending" ] || die "second apply was not a no-op"
[ "$(q "SELECT count(*) FROM schema_migrations;")" = "13" ] || die "second apply changed the ledger"
ok "a second apply does nothing"

MIGRATIONS_DIR=$(scratch_dir three 014-check.sql 015-fails.sql 016-after.sql)
if migrations_apply > /dev/null 2>&1; then die "a failing 015 returned success"; fi
[ "$(q "SELECT to_regclass('public.check_015') IS NULL;")" = "t" ] || die "015's table survived"
[ "$(compression_job check_015)" = "0" ] || die "015's compression job survived"
[ "$(q "SELECT count(*) FROM schema_migrations WHERE version IN ('015', '016');")" = "0" ] || die "015/016 ledger rows exist"
[ "$(q "SELECT to_regclass('public.check_016') IS NULL;")" = "t" ] || die "016 ran after a failure"
ok "a failing 015 exits non-zero, rolls back its hypertable, policy and row, and 016 waits"

# --- pre-ledger: init.sql only ----------------------------------------------------------------------
MIGRATIONS_DIR=$(scratch_dir one 014-check.sql)
start_db preledger "$ROOT/db/init.sql"
[ "$(migrations_state)" = "pre-ledger" ] || die "pre-ledger: wrong state"
if migrations_apply > /dev/null 2>&1; then die "apply ran on a pre-ledger database"; fi
[ "$(q "SELECT to_regclass('public.check_014') IS NULL AND to_regclass('public.schema_migrations') IS NULL;")" = "t" ] \
    || die "apply changed a pre-ledger database"
ok "apply refuses a pre-ledger database and changes nothing"
LEDGER_SQL="$WORK/ledger-fails.sql"
if migrations_adopt > /dev/null 2>&1; then die "a failing ledger.sql returned success"; fi
[ "$(q "SELECT to_regclass('public.schema_migrations') IS NULL;")" = "t" ] || die "a failed adopt left a ledger"
ok "a failing ledger.sql exits non-zero and leaves no ledger"
LEDGER_SQL="$REAL_LEDGER"
migrations_adopt > /dev/null
[ "$(q "SELECT count(*) FROM schema_migrations;")" = "12" ] || die "adopt did not record the baseline"
ok "adopt records the baseline after a failed attempt"
if migrations_adopt > /dev/null 2>&1; then die "a second adopt returned success"; fi
ok "a second adopt refuses"
migrations_apply > /dev/null
[ "$(q "SELECT count(*) FROM schema_migrations WHERE version = '014';")" = "1" ] || die "apply after adopt did not run 014"
ok "apply after adopt runs 014"

# --- pre-ledger, incomplete: init.sql without discovery_volumes --------------------------------------
start_db incomplete "$ROOT/db/init.sql"
q "DROP TABLE discovery_volumes;" > /dev/null
if out=$(migrations_adopt 2>&1); then die "adopt accepted an incomplete baseline"; fi
[[ "$out" == *"table discovery_volumes"* ]] || die "adopt did not name the missing table: $out"
[ "$(q "SELECT to_regclass('public.schema_migrations') IS NULL;")" = "t" ] || die "a refused adopt left a ledger"
ok "adopt refuses an incomplete baseline, names the gap and records nothing"

echo "all migration checks passed"
