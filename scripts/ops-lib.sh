# shellcheck shell=bash
# shellcheck disable=SC2034  # the names below are read by the scripts that source this file
# The host scripts' prod context, sourced by every script that drives the stack: the project
# directory, the prod compose command, and how to reach the database as itself.
#
# DC is the only place a host script builds the compose command; every server compose command
# takes the prod override (deploy/README.md). DB_TOOL runs a Postgres client in the db container as
# the container's POSTGRES_USER against its POSTGRES_DB, which .env may set, so no script names the
# database identity. It is an array, not a function, so restic and `env -u` can run it:
#
#   "${DB_TOOL[@]}" psql -X -Atc 'SELECT 1'
#   "${DB_TOOL[@]}" pg_dump --format=custom > out.dump
#   "${DB_TOOL[@]}" psql -X -d postgres      # psql takes the last -d: the maintenance database
#
# The lib defines names only: no cd, no set -e.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DC=(docker compose -f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker-compose.prod.yml")
# The single quotes are deliberate: the variables expand inside the container.
# shellcheck disable=SC2016
DB_TOOL=("${DC[@]}" exec -T db sh -c 'tool=$1; shift; exec "$tool" -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' db-tool)
# Fixed once sourced, so DB_TOOL can never disagree with DC.
readonly PROJECT_DIR DC DB_TOOL

# The application database's name, for statements that name it (restore's DROP and CREATE).
db_name() {
    # shellcheck disable=SC2016
    "${DC[@]}" exec -T db sh -c 'printf "%s\n" "$POSTGRES_DB"'
}
