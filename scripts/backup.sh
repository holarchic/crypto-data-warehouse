#!/usr/bin/env bash
# Backup candle_collector DB via pg_dump in the db container.
# Creates timestamped custom-format dump in backups/ directory.
set -euo pipefail

BACKUP_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups"
mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
FILENAME="candle_collector-${TIMESTAMP}.dump"
FILEPATH="${BACKUP_DIR}/${FILENAME}"

echo "==> Backing up candle_collector to ${FILEPATH}"

docker compose exec -T db pg_dump \
    -U candles \
    --format=custom \
    --compress=6 \
    candle_collector > "$FILEPATH"

# Verify dump is readable
echo "==> Verifying backup..."
pg_restore --list "$FILEPATH" > /dev/null 2>&1 || {
    # pg_restore may not be on host — try in container
    docker compose exec -T db pg_restore --list /dev/stdin < "$FILEPATH" > /dev/null
}

SIZE=$(du -h "$FILEPATH" | cut -f1)
echo "==> Backup complete: ${FILEPATH} (${SIZE})"
