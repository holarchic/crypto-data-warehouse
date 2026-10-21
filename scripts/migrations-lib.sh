# shellcheck shell=bash
# The migration ledger's operations, sourced by scripts/migrate.sh and tests/integration/check_migrations.sh.
#
# The caller defines `migrations_psql`, which runs psql against the target database with the given
# arguments and SQL on stdin, and may set MIGRATIONS_DIR, LEDGER_SQL and BASELINE_CHECK_SQL. Every
# function returns non-zero, with a message on stderr, rather than continue past a failure.
#
# A migration is db/migrations/NNN-<name>.sql. It runs in one transaction with its ledger row, so
# it must not hold transaction control of its own, and DDL that can't run in a transaction
# (CREATE TABLESPACE, CREATE INDEX CONCURRENTLY) is a runbook step, not a migration.

_MIGRATIONS_DB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../db" && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$_MIGRATIONS_DB_DIR/migrations}"
LEDGER_SQL="${LEDGER_SQL:-$_MIGRATIONS_DB_DIR/ledger.sql}"
BASELINE_CHECK_SQL="${BASELINE_CHECK_SQL:-$_MIGRATIONS_DB_DIR/baseline-check.sql}"

_migrations_fail() { echo "migrate: $*" >&2; return 1; }

# Every lib query stops at its first SQL error, so psql's exit status is the query's outcome.
_migrations_sql() { migrations_psql -v ON_ERROR_STOP=1 "$@"; }

# Each migration file's path, in version order, after checking the whole directory.
migrations_check_files() {
    local f name version seen=" "
    for f in "$MIGRATIONS_DIR"/*; do
        name=$(basename "$f")
        [[ "$name" =~ ^([0-9]{3})-[a-z0-9-]+\.sql$ ]] || _migrations_fail "$name: not NNN-<name>.sql" || return 1
        version=${BASH_REMATCH[1]}
        [[ "$seen" != *" $version "* ]] || _migrations_fail "$name: version $version is used twice" || return 1
        seen+="$version "
        if grep -qiE '^[[:space:]]*(BEGIN|COMMIT|ROLLBACK|START[[:space:]]+TRANSACTION|SAVEPOINT)[[:space:]]*;' "$f"; then
            _migrations_fail "$name: holds its own transaction control; migrate.sh runs it in one transaction" || return 1
        fi
    done
    printf '%s\n' "$MIGRATIONS_DIR"/*
}

# adopted (the ledger exists), pre-ledger (a schema without a ledger) or empty.
migrations_state() {
    _migrations_sql -At <<'SQL' || _migrations_fail "could not read the database state"
SELECT CASE WHEN to_regclass('public.schema_migrations') IS NOT NULL THEN 'adopted'
            WHEN to_regclass('public.candles') IS NOT NULL THEN 'pre-ledger'
            ELSE 'empty' END;
SQL
}

# The migration files the ledger lacks, in version order. Refuses an empty ledger and a pending
# file older than the newest applied one: both need a person, not an apply.
migrations_pending() {
    local files applied newest f version
    files=$(migrations_check_files) || return 1
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

# Apply every pending migration in order, each in one transaction with its ledger row. Stops at the
# first failure.
migrations_apply() {
    local state pending f name
    state=$(migrations_state) || return 1
    [ "$state" = "adopted" ] || _migrations_fail "the database is $state; run migrate.sh --adopt (pre-ledger) or initialise it first" || return 1
    pending=$(migrations_pending) || return 1
    [ -n "$pending" ] || { echo "migrate: nothing pending"; return 0; }
    while IFS= read -r f; do
        name=$(basename "$f")
        echo "migrate: applying $name"
        { cat "$f"; printf '\nINSERT INTO public.schema_migrations (version, name) VALUES (:%s, :%s);\n' "'version'" "'name'"; } \
            | _migrations_sql -1 -v version="${name:0:3}" -v name="$name" -f - \
            || _migrations_fail "$name failed and was rolled back; later migrations were not applied" || return 1
    done <<< "$pending"
}

# Record the baseline on a database built before the ledger, once it has every baseline effect.
migrations_adopt() {
    local state failed
    state=$(migrations_state) || return 1
    [ "$state" = "pre-ledger" ] || _migrations_fail "the database is $state; only a pre-ledger database is adopted" || return 1
    failed=$(_migrations_sql -At -F '|' < "$BASELINE_CHECK_SQL" | awk -F '|' '$2 != "t" {print $1}'; exit "${PIPESTATUS[0]}") \
        || _migrations_fail "the baseline check did not run; nothing recorded" || return 1
    [ -z "$failed" ] || _migrations_fail "baseline effects missing, nothing recorded:
$failed" || return 1
    _migrations_sql < "$LEDGER_SQL" > /dev/null || _migrations_fail "recording the baseline failed and was rolled back" || return 1
    echo "migrate: baseline recorded"
}
