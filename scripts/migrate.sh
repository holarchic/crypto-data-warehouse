#!/usr/bin/env bash
# Apply the pending schema migrations to the compose db, recording each in schema_migrations.
#
#   ./scripts/migrate.sh             apply every pending migration, in order
#   ./scripts/migrate.sh --status    show the state, the applied count and what is pending
#   ./scripts/migrate.sh --adopt     record the baseline (001-013) on a database built before the
#                                    ledger, once db/baseline-check.sql passes
#
# It waits for the db to accept TCP connections first. The image's initdb server listens on the
# socket only, so a TCP answer means initdb, ledger.sql included, has finished; callers start the
# db, run this, then start the consumers. A migration still goes in when its ROLLOUT note says
# (before or after a rebuild): `--status` shows what a deploy has pending.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/ops-lib.sh
. "$SCRIPT_DIR/ops-lib.sh"
READY_TRIES="${MIGRATE_READY_TRIES:-30}"

migrations_psql() { "${DB_TOOL[@]}" psql -X "$@"; }
# shellcheck source=scripts/migrations-lib.sh
. "$SCRIPT_DIR/migrations-lib.sh"

wait_for_db() {
    local _
    for _ in $(seq "$READY_TRIES"); do
        "${DB_TOOL[@]}" pg_isready -q -h 127.0.0.1 && return 0
        sleep 2
    done
    echo "migrate: the db did not accept TCP connections within $((READY_TRIES * 2)) s" >&2
    return 1
}

case "${1:-}" in
    "") wait_for_db; migrations_apply ;;
    --adopt) wait_for_db; migrations_adopt ;;
    --status)
        wait_for_db
        state=$(migrations_state)
        echo "state: $state"
        if [ "$state" = "adopted" ]; then
            pending=$(migrations_pending)
            applied=$(migrations_applied_count)
            echo "applied: $applied"
            echo "pending: $(grep -c . <<< "$pending" || true)"
            [ -z "$pending" ] || xargs -n1 basename <<< "$pending" | sed 's/^/  /'
        elif [ "$state" = "pre-ledger" ]; then
            echo "pending: run --adopt first; it checks the baseline (db/baseline-check.sql)"
        fi
        ;;
    *) echo "usage: $0 [--status | --adopt]" >&2; exit 2 ;;
esac
