#!/usr/bin/env bash
# Fresh install / full redeploy of crypto-data-warehouse. Idempotent, but NOT for routine code
# updates: `compose pull db` + stack-wide `up -d` can recreate the db onto a moved
# `latest-pg16`. Code updates follow deploy/README.md § Code updates (build + `up -d --no-deps <svc>`).
#
# Usage: ./deploy/deploy.sh
#   Run from the project root (/opt/crypto-data-warehouse).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
REPO_URL="https://github.com/holarchic/crypto-data-warehouse.git"
# DB backups are DISABLED (2026-08-22): on-box dumps were deleted and the backup +
# retention cron (below) removed, pending an OFFSITE DR target (restic -> Backblaze B2
# or a Hetzner Storage Box). On-box dumps on /mnt/pgvol gave no real DR and crowded the
# DB tablespace. To re-enable: BACKUP_DIR=/mnt/pgvol/backups + restore the two cron lines.

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
$COMPOSE pull db

info "Building application images"
# Include profile-gated services (discovery, liquidations-hl) so they don't run stale code after
# a git pull — `compose build` alone skips anything behind a profile flag.
$COMPOSE --profile discovery --profile liquidations-hl build

info "Starting services"
$COMPOSE up -d

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

info "Installing cron jobs"

# Build crontab — preserve existing non-candle-collector entries
EXISTING_CRON=$(crontab -l 2>/dev/null | grep -v "candle-collector" || true)
NEW_CRON="$EXISTING_CRON
# candle-collector: daily discovery (02:00 UTC)
0 2 * * * cd $PROJECT_DIR && $COMPOSE --profile discovery run --rm discovery >> /var/log/candle-collector-discovery.log 2>&1
# candle-collector: daily HL Grade-A liquidation pull (04:00 UTC — T-009.3)
0 4 * * * cd $PROJECT_DIR && $COMPOSE --profile liquidations-hl run --rm liquidations-hl >> /var/log/candle-collector-liquidations-hl.log 2>&1
# candle-collector: host monitor (every 5 min)
*/5 * * * * $PROJECT_DIR/deploy/host-monitor.sh 2>/dev/null
"

# Remove blank lines and install
echo "$NEW_CRON" | grep -v '^$' | crontab -

info "Cron jobs installed:"
crontab -l | grep "candle-collector"

# -- Status -------------------------------------------------------------------

echo ""
info "Deployment complete"
echo ""
$COMPOSE ps
echo ""
echo "  API:       http://localhost:8000/health"
echo "  Status:    http://localhost:8000/status"
echo "  Logs:      cd $PROJECT_DIR && $COMPOSE logs -f"
echo ""
echo "Next: run discovery to populate pairs:"
echo "  cd $PROJECT_DIR && $COMPOSE --profile discovery run --rm discovery"
