#!/usr/bin/env bash
# The migration ledger against a real TimescaleDB: initdb with init.sql + ledger.sql, apply with real
# hypertable and compression DDL, rollback of a failing migration, the one-transaction guard, and
# adoption of a database built without the ledger. Runs outside pytest, from the project root, with
# Docker:
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
    local c
    for c in "${CONTAINERS[@]}"; do
        docker rm -fv "$c" > /dev/null 2>&1 || echo "WARN could not remove container $c" >&2
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

die() { echo "FAIL $*" >&2; exit 1; }
ok() { echo "ok   $*"; }

migrations_psql() { docker exec -i "$TARGET" psql -X -U candles -d candle_collector "$@"; }
# shellcheck source=scripts/migrations-lib.sh
. "$ROOT/scripts/migrations-lib.sh"

# One value from the target, failing the check on a SQL error.
q() { docker exec -i "$TARGET" psql -X -U candles -d candle_collector -At -v ON_ERROR_STOP=1 "$@" || die "query failed: $*"; }
compression_jobs() {
    q -v table="$1" <<'SQL'
SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = :'table';
SQL
}

# start_db NAME INITDB_FILE... : a fresh container whose initdb runs the given files, ready over TCP.
start_db() {
    local name="cc-migrate-check-$1" dir="$WORK/init-$1"
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

# The baseline, from ledger.sql: its file names, one per line.
BASELINE=$(grep -oE "'[0-9]{3}-[a-z0-9-]+\.sql'" "$MIGRATIONS_DEFAULT_LEDGER" | tr -d "'")
[ "$(grep -c . <<< "$BASELINE")" -gt 0 ] || die "no baseline rows in ledger.sql"

# A migrations dir: the baseline files plus the named fixtures (900s, clear of any real migration).
mkdir -p "$WORK/fixtures"
scratch_dir() {
    local dir="$WORK/migrations-$1" m
    shift
    mkdir -p "$dir"
    while IFS= read -r m; do cp "$MIGRATIONS_DEFAULT_DIR/$m" "$dir/"; done <<< "$BASELINE"
    for m in "$@"; do cp "$WORK/fixtures/$m" "$dir/"; done
    echo "$dir"
}

hypertable_fixture() {
    cat <<SQL
CREATE TABLE $1 (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION);
SELECT create_hypertable('$1', 'ts', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE $1 SET (timescaledb.compress, timescaledb.compress_orderby = 'ts DESC');
SELECT add_compression_policy('$1', INTERVAL '7 days');
SQL
}
hypertable_fixture check_901 > "$WORK/fixtures/901-check.sql"
{ hypertable_fixture check_902; echo "SELECT 1 / 0;"; } > "$WORK/fixtures/902-fails.sql"
echo "CREATE TABLE check_903 (x INTEGER);" > "$WORK/fixtures/903-after.sql"
printf 'CREATE TABLE check_904 (x INTEGER);\nCOMMIT WORK;\n' > "$WORK/fixtures/904-commits.sql"
printf 'CREATE TABLE check_905a (x INTEGER);\nCOMMIT;\nCREATE TABLE check_905b (x INTEGER);\n' > "$WORK/fixtures/905-commits.sql"
cat > "$WORK/fixtures/906-do-block.sql" <<'SQL'
DO $$
BEGIN
    IF to_regclass('public.check_906') IS NULL THEN
        CREATE TABLE check_906 (x INTEGER);
    END IF;
END
$$;
SQL
cat > "$WORK/ledger-fails.sql" <<'SQL'
BEGIN;
CREATE TABLE public.schema_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL);
SELECT 1 / 0;
COMMIT;
SQL

# --- fresh initdb: init.sql, then ledger.sql ------------------------------------------------------
start_db fresh "$ROOT/db/init.sql" "$ROOT/db/ledger.sql"
state=$(migrations_state) || die "fresh: state query failed"
[ "$state" = "adopted" ] || die "fresh: state is $state"
[ "$(q -c "SELECT string_agg(name, E'\n' ORDER BY version) FROM schema_migrations;")" = "$BASELINE" ] \
    || die "fresh: ledger is not ledger.sql's baseline"
pending=$(migrations_pending "$(scratch_dir base)") || die "fresh: pending failed"
[ -z "$pending" ] || die "fresh: something is pending: $pending"
ok "fresh initdb records ledger.sql's baseline and nothing is pending"

failures=$(migrations_baseline_failures) || die "fresh: the baseline check failed to run"
[ -z "$failures" ] || die "fresh: baseline check fails on init.sql: $failures"
extra=$(sed '/^SELECT e.kind/,$d' "$MIGRATIONS_DEFAULT_CHECK" \
    | { cat; echo "SELECT count(*) FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected) x;"; } \
    | q) || die "fresh: catalog comparison failed"
[ "$extra" = "0" ] || die "fresh: $extra catalog rows of init.sql are missing from baseline-check.sql's expected"
ok "baseline-check.sql passes on init.sql, and its expected catalog is the whole of init.sql's"

dir=$(scratch_dir one 901-check.sql)
migrations_apply "$dir" > /dev/null || die "901 apply failed"
[ "$(q -c "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'check_901';")" = "1" ] \
    || die "901 hypertable missing"
[ "$(compression_jobs check_901)" = "1" ] || die "901 compression policy missing"
[ "$(q -c "SELECT name FROM schema_migrations WHERE version = '901';")" = "901-check.sql" ] || die "901 ledger row missing"
ok "901 (hypertable, compression, policy) applied with its ledger row"
[ "$(migrations_apply "$dir")" = "migrate: nothing pending" ] || die "second apply was not a no-op"
[ "$(q -c "SELECT count(*) FROM schema_migrations WHERE version >= '900';")" = "1" ] || die "second apply changed the ledger"
ok "a second apply does nothing"

dir=$(scratch_dir three 901-check.sql 902-fails.sql 903-after.sql)
if migrations_apply "$dir" > /dev/null 2>&1; then die "a failing 902 returned success"; fi
[ "$(q -c "SELECT to_regclass('public.check_902') IS NULL;")" = "t" ] || die "902's table survived"
[ "$(compression_jobs check_902)" = "0" ] || die "902's compression job survived"
[ "$(q -c "SELECT count(*) FROM schema_migrations WHERE version IN ('902', '903');")" = "0" ] || die "902/903 ledger rows exist"
[ "$(q -c "SELECT to_regclass('public.check_903') IS NULL;")" = "t" ] || die "903 ran after a failure"
ok "a failing 902 exits non-zero, rolls back its hypertable, policy and row, and 903 waits"

dir=$(scratch_dir commits 904-commits.sql)
if out=$(migrations_apply "$dir" 2>&1); then die "COMMIT WORK was accepted"; fi
[[ "$out" == *"904-commits.sql: holds transaction control"* ]] || die "COMMIT WORK refusal did not name the file: $out"
[ "$(q -c "SELECT to_regclass('public.check_904') IS NULL;")" = "t" ] || die "a refused file ran"
ok "a file holding COMMIT WORK is refused before anything runs"

dir=$(scratch_dir guard 905-commits.sql)
# Bypass the text check, as a form it missed would: the transaction guard alone must hold the ledger.
if (_migrations_read() { cat "$1"; }; migrations_apply "$dir") > /dev/null 2>&1; then die "a mid-file COMMIT returned success"; fi
[ "$(q -c "SELECT count(*) FROM schema_migrations WHERE version = '905';")" = "0" ] || die "a mid-file COMMIT recorded its ledger row"
ok "a COMMIT past the text check fails the apply and records no ledger row"

dir=$(scratch_dir do-block 906-do-block.sql)
migrations_apply "$dir" > /dev/null || die "a PL/pgSQL DO block was refused"
[ "$(q -c "SELECT count(*) FROM schema_migrations WHERE version = '906';")" = "1" ] || die "906 not recorded"
ok "a PL/pgSQL DO block applies"

# --- pre-ledger: init.sql only ----------------------------------------------------------------------
dir=$(scratch_dir one 901-check.sql)
start_db preledger "$ROOT/db/init.sql"
[ "$(migrations_state)" = "pre-ledger" ] || die "pre-ledger: wrong state"
if migrations_apply "$dir" > /dev/null 2>&1; then die "apply ran on a pre-ledger database"; fi
[ "$(q -c "SELECT to_regclass('public.check_901') IS NULL AND to_regclass('public.schema_migrations') IS NULL;")" = "t" ] \
    || die "apply changed a pre-ledger database"
ok "apply refuses a pre-ledger database and changes nothing"
if migrations_adopt "$MIGRATIONS_DEFAULT_CHECK" "$WORK/ledger-fails.sql" > /dev/null 2>&1; then
    die "a failing ledger.sql returned success"
fi
[ "$(q -c "SELECT to_regclass('public.schema_migrations') IS NULL;")" = "t" ] || die "a failed adopt left a ledger"
ok "a failing ledger.sql exits non-zero and leaves no ledger"
migrations_adopt > /dev/null || die "adopt failed"
[ "$(q -c "SELECT string_agg(name, E'\n' ORDER BY version) FROM schema_migrations;")" = "$BASELINE" ] \
    || die "adopt did not record the baseline"
ok "adopt records the baseline after a failed attempt"
if migrations_adopt > /dev/null 2>&1; then die "a second adopt returned success"; fi
ok "a second adopt refuses"
migrations_apply "$dir" > /dev/null || die "apply after adopt failed"
[ "$(q -c "SELECT count(*) FROM schema_migrations WHERE version = '901';")" = "1" ] || die "apply after adopt did not run 901"
ok "apply after adopt runs 901"

# --- pre-ledger, incomplete: properties gone while the objects' names remain ------------------------
for gap in "DROP TABLE discovery_volumes|column discovery_volumes.run_date: date not null" \
           "ALTER TABLE discovery_volumes DROP CONSTRAINT discovery_volumes_pkey|index discovery_volumes_pkey" \
           "SELECT remove_compression_policy('premium_index'); SELECT add_compression_policy('premium_index', INTERVAL '8 days')|compression policy premium_index: 7 days"; do
    start_db "incomplete-${#CONTAINERS[@]}" "$ROOT/db/init.sql"
    q -c "${gap%%|*}" > /dev/null
    if out=$(migrations_adopt 2>&1); then die "adopt accepted: ${gap%%|*}"; fi
    [[ "$out" == *"${gap##*|}"* ]] || die "adopt did not name the gap '${gap##*|}': $out"
    [ "$(q -c "SELECT to_regclass('public.schema_migrations') IS NULL;")" = "t" ] || die "a refused adopt left a ledger"
    ok "adopt refuses '${gap%%|*}', names it and records nothing"
done

echo "all migration checks passed"
