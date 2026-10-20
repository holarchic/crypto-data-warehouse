#!/usr/bin/env bash
# Fresh install / full redeploy of crypto-data-warehouse. NOT for routine code updates:
# `compose pull db` + stack-wide `up -d` can recreate the db onto a moved `latest-pg16`.
# Code updates follow deploy/README.md § Code updates (build + `up -d --no-deps <svc>`).
#
# Usage: ./deploy/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DC=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

# -- Helpers ------------------------------------------------------------------

info()  { echo "==> $*"; }
fail()  { echo "==> FATAL: $*" >&2; exit 1; }

# -- Pre-flight checks -------------------------------------------------------

cd "$PROJECT_DIR"

if [ ! -f ".env" ]; then
    fail ".env not found. Create it first:
    cp .env.example .env
    nano .env
    (set POSTGRES_PASSWORD at minimum)"
fi

# Verify required env vars
# shellcheck disable=SC1091
source .env
if [ -z "${POSTGRES_PASSWORD:-}" ]; then
    fail "POSTGRES_PASSWORD not set in .env"
fi

# -- Pull latest code (if git repo) ------------------------------------------

if [ -d ".git" ]; then
    info "Pulling latest code"
    git pull --ff-only origin main
else
    info "Not a git repo — skipping pull"
fi

# -- Build and deploy ---------------------------------------------------------

info "Pulling base images"
"${DC[@]}" pull db

info "Building application images"
# Include profile-gated services (discovery, liquidations-hl, hl-candles-repair) so they don't run
# stale code after a git pull — `compose build` alone skips anything behind a profile flag.
"${DC[@]}" --profile discovery --profile liquidations-hl --profile hl-candles-repair build

info "Starting services"
"${DC[@]}" up -d

# -- Wait for health ----------------------------------------------------------

info "Waiting for API health..."
MAX_RETRIES=30
for i in $(seq 1 $MAX_RETRIES); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        info "API healthy"
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        fail "API did not become healthy after ${MAX_RETRIES} attempts"
    fi
    sleep 2
done

# -- Install crontab entries --------------------------------------------------

"$SCRIPT_DIR/install-cron.sh"

# -- Status -------------------------------------------------------------------

echo ""
info "Deployment complete"
echo ""
"${DC[@]}" ps
echo ""
echo "  API:       http://localhost:8000/health"
echo "  Status:    http://localhost:8000/status"
echo "  Logs:      cd $PROJECT_DIR && ${DC[*]} logs -f"
echo ""
echo "Next: run discovery to populate pairs:"
echo "  cd $PROJECT_DIR && ${DC[*]} --profile discovery run --rm discovery"
