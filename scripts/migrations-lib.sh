# shellcheck shell=bash
# The migration ledger's operations, sourced by scripts/migrate.sh and tests/integration/check_migrations.sh.
#
# The caller defines `migrations_psql`, which runs psql against the target database with the given
# arguments and SQL on stdin. Each operation takes its files as arguments (defaulting to the repo's
# db/ files) and returns non-zero, with a message on stderr, rather than continue past a failure.
#
# A migration is db/migrations/NNN-<name>.sql: plain SQL, run by the server in one transaction with
# its ledger row. psql never interprets its text, and Postgres refuses transaction control inside it,
# so DDL that can't run in a transaction (CREATE TABLESPACE, CREATE INDEX CONCURRENTLY) is a runbook
# step, not a migration.

_MIGRATIONS_DB="$(cd "$(dirname "${BASH_SOURCE[0]}")/../db" && pwd)"
readonly MIGRATIONS_DEFAULT_DIR="$_MIGRATIONS_DB/migrations"
readonly MIGRATIONS_DEFAULT_LEDGER="$_MIGRATIONS_DB/ledger.sql"
readonly MIGRATIONS_DEFAULT_CHECK="$_MIGRATIONS_DB/baseline-check.sql"

_migrations_fail() { echo "migrate: $*" >&2; return 1; }

# Every query stops at its first SQL error, so psql's exit status is the query's outcome.
_migrations_sql() { migrations_psql -v ON_ERROR_STOP=1 "$@"; }

# A migration's text, read once and whole.
_migrations_read() {
    local f=$1
    [ -f "$f" ] && [ -r "$f" ] || _migrations_fail "$(basename "$f"): not a readable file" || return 1
    cat "$f" || _migrations_fail "$(basename "$f"): read failed"
}

# A dollar-quote tag that occurs nowhere in TEXT.
_migrations_tag() {
    local text=$1 tag
    for _ in 1 2 3; do
        tag="cc_$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
        [[ "$text" != *"\$$tag\$"* ]] && { echo "$tag"; return 0; }
    done
    _migrations_fail "no free dollar-quote tag"
}

# Each migration file's path, in version order, after checking every file in DIR.
migrations_check_files() {
    local dir=${1:-$MIGRATIONS_DEFAULT_DIR} f name version seen=" "
    for f in "$dir"/*; do
        name=$(basename "$f")
        [[ "$name" =~ ^([0-9]{3})-[a-z0-9-]+\.sql$ ]] || _migrations_fail "$name: not NNN-<name>.sql" || return 1
        version=${BASH_REMATCH[1]}
        [[ "$seen" != *" $version "* ]] || _migrations_fail "$name: version $version is used twice" || return 1
        seen+="$version "
        _migrations_read "$f" > /dev/null || return 1
    done
    printf '%s\n' "$dir"/*
}

# adopted (the ledger exists), pre-ledger (a schema without a ledger) or empty.
migrations_state() {
    _migrations_sql -At <<'SQL' || _migrations_fail "could not read the database state"
SELECT CASE WHEN to_regclass('public.schema_migrations') IS NOT NULL THEN 'adopted'
            WHEN to_regclass('public.candles') IS NOT NULL THEN 'pre-ledger'
            ELSE 'empty' END;
SQL
}

migrations_applied_count() {
    _migrations_sql -At <<'SQL' || _migrations_fail "could not read schema_migrations"
SELECT count(*) FROM public.schema_migrations;
SQL
}

# The files in DIR whose version the ledger lacks, in version order. Refuses an empty ledger and a
# pending file older than the newest applied one: both need a person, not an apply.
migrations_pending() {
    local files applied newest f version
    files=$(migrations_check_files "${1:-$MIGRATIONS_DEFAULT_DIR}") || return 1
    applied=$(_migrations_sql -At <<'SQL'
SELECT version FROM public.schema_migrations ORDER BY version;
SQL
) || _migrations_fail "could not read schema_migrations" || return 1
    [ -n "$applied" ] || _migrations_fail "schema_migrations is empty; the baseline was never recorded" || return 1
    newest=$(tail -n 1 <<< "$applied")
    while IFS= read -r f; do
        version=$(basename "$f" | cut -c1-3)
        grep -qx "$version" <<< "$applied" && continue
        [[ "$version" > "$newest" ]] || _migrations_fail "$(basename "$f") is older than applied $newest; apply it by hand or renumber it" || return 1
        echo "$f"
    done <<< "$files"
}

# Apply every pending migration in DIR in order, each in one transaction with its ledger row. The
# file's text reaches the server as a dollar-quoted literal that a DO block EXECUTEs, so psql
# interprets none of it (a `\q` is just text) and the server refuses any COMMIT, ROLLBACK, END or
# SAVEPOINT in it ("EXECUTE of transaction commands is not implemented"), rolling everything back.
migrations_apply() {
    local dir=${1:-$MIGRATIONS_DEFAULT_DIR} state pending f name sql outer inner
    state=$(migrations_state) || return 1
    [ "$state" = "adopted" ] || _migrations_fail "the database is $state; run migrate.sh --adopt (pre-ledger) or initialise it first" || return 1
    pending=$(migrations_pending "$dir") || return 1
    [ -n "$pending" ] || { echo "migrate: nothing pending"; return 0; }
    while IFS= read -r f; do
        name=$(basename "$f")
        sql=$(_migrations_read "$f") || return 1
        outer=$(_migrations_tag "$sql") && inner=$(_migrations_tag "$sql") || return 1
        echo "migrate: applying $name"
        _migrations_sql -1 -v version="${name:0:3}" -v name="$name" -f - <<SQL || _migrations_fail "$name failed and was rolled back; later migrations were not applied" || return 1
DO \$$outer\$ BEGIN EXECUTE \$$inner\$$sql
\$$inner\$; END \$$outer\$;
INSERT INTO public.schema_migrations (version, name) VALUES (:'version', :'name');
SQL
    done <<< "$pending"
}

# The baseline checks CHECK_SQL reports as failing, one per line (empty when all pass).
migrations_baseline_failures() {
    local out line
    out=$(_migrations_sql -At -F '|' < "${1:-$MIGRATIONS_DEFAULT_CHECK}") \
        || _migrations_fail "the baseline check did not run" || return 1
    [ -n "$out" ] || _migrations_fail "the baseline check returned no rows" || return 1
    while IFS= read -r line; do
        [ "${line##*|}" = "t" ] || echo "${line%|*}"
    done <<< "$out"
}

# Record the baseline (LEDGER_SQL) on a database built before the ledger, once CHECK_SQL passes.
migrations_adopt() {
    local check=${1:-$MIGRATIONS_DEFAULT_CHECK} ledger=${2:-$MIGRATIONS_DEFAULT_LEDGER} state failed
    state=$(migrations_state) || return 1
    [ "$state" = "pre-ledger" ] || _migrations_fail "the database is $state; only a pre-ledger database is adopted" || return 1
    failed=$(migrations_baseline_failures "$check") || _migrations_fail "nothing recorded" || return 1
    [ -z "$failed" ] || _migrations_fail "baseline effects missing, nothing recorded:
$failed" || return 1
    _migrations_sql < "$ledger" > /dev/null || _migrations_fail "recording the baseline failed and was rolled back" || return 1
    echo "migrate: baseline recorded"
}
