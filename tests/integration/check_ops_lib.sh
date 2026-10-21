#!/usr/bin/env bash
# The host scripts against a real db whose user and database are not the defaults: a temporary
# checkout of scripts/ and db/ with its own db-only compose files (pinned image, anonymous volume,
# no published ports, no prod mounts), POSTGRES_USER=site_user and POSTGRES_DB=site_db in the
# container, and the host's own POSTGRES_USER/POSTGRES_DB set wrong, so only the container's
# identity can work. Run from the project root, with Docker:
#
#     ./tests/integration/check_ops_lib.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK=$(mktemp -d)
TREE="$WORK/checkout"
export COMPOSE_PROJECT_NAME="cc-ops-check-$$"
export POSTGRES_USER=wrong POSTGRES_DB=wrong WEBHOOK_URL=""

mkdir -p "$TREE"
cp -R "$ROOT/scripts" "$ROOT/db" "$TREE/"
cat > "$TREE/docker-compose.yml" <<'YAML'
services:
  db:
    image: timescale/timescaledb:2.26.2-pg16
    environment:
      POSTGRES_USER: site_user
      POSTGRES_PASSWORD: check
      POSTGRES_DB: site_db
    volumes:
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      - ./db/ledger.sql:/docker-entrypoint-initdb.d/ledger.sql:ro
YAML
echo "services: {}" > "$TREE/docker-compose.prod.yml"

# shellcheck source=scripts/ops-lib.sh
. "$TREE/scripts/ops-lib.sh"
cleanup() {
    "${DC[@]}" down -v > /dev/null 2>&1 || echo "WARN could not remove compose project ${COMPOSE_PROJECT_NAME}" >&2
    rm -rf "$WORK"
}
trap cleanup EXIT

die() { echo "FAIL $*" >&2; exit 1; }
ok() { echo "ok   $*"; }

"${DC[@]}" up -d db > /dev/null 2>&1
status=$("$TREE/scripts/migrate.sh" --status) || die "migrate.sh --status failed"
[[ "$status" == *"state: adopted"* && "$status" == *"pending: 0"* ]] || die "migrate.sh --status: $status"
ok "migrate.sh waits for the db and reads the ledger as site_user on site_db"

identity=$("${DB_TOOL[@]}" psql -X -At -c "SELECT current_user || '|' || current_database()")
[ "$identity" = "site_user|site_db" ] || die "DB_TOOL connected as $identity"
identity=$("${DB_TOOL[@]}" psql -X -At -d postgres -c "SELECT current_user || '|' || current_database()")
[ "$identity" = "site_user|postgres" ] || die "DB_TOOL -d postgres connected as $identity"
ok "DB_TOOL connects as the container's user to its database, and -d postgres reaches postgres"

"$TREE/scripts/verify-compression.sh" > /dev/null || die "verify-compression.sh failed"
ok "verify-compression.sh runs against site_db"

BACKUP_DIR="$WORK/backups" BACKUP_LOCK_DIR="$WORK/backup.lock" "$TREE/scripts/backup.sh" > /dev/null \
    || die "backup.sh failed"
dump=$(find "$WORK/backups" -name 'candle_collector-*-timescaledb-2.26.2.dump')
[ -n "$dump" ] || die "backup.sh published no dump"
"${DC[@]}" exec -T db pg_restore --list < "$dump" | grep -q "TABLE public discovery_volumes" \
    || die "the dump does not list site_db's tables"
ok "backup.sh dumps site_db, and the dump reads back"

echo "all ops-lib checks passed"
