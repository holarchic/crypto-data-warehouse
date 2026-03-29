#!/usr/bin/env bash
# Restore candle_collector DB from a pg_dump custom-format backup.
# Uses timescaledb_pre_restore/post_restore for clean hypertable restore.
#
# Usage: ./scripts/restore.sh backups/candle_collector-20260329-120000.dump
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <backup-file>"
    echo "Example: $0 backups/candle_collector-20260329-120000.dump"
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: backup file not found: ${BACKUP_FILE}"
    exit 1
fi

echo "==> WARNING: This will DROP and recreate the candle_collector database."
echo "    Backup file: ${BACKUP_FILE}"
read -rp "    Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

echo "==> Preparing TimescaleDB for restore..."
docker compose exec -T db psql -U candles candle_collector -c \
    "SELECT timescaledb_pre_restore();"

echo "==> Restoring from ${BACKUP_FILE}..."
docker compose exec -T db pg_restore \
    -U candles \
    --clean \
    --if-exists \
    --no-owner \
    -d candle_collector < "$BACKUP_FILE"

echo "==> Finalizing TimescaleDB restore..."
docker compose exec -T db psql -U candles candle_collector -c \
    "SELECT timescaledb_post_restore();"

echo "==> Row counts after restore:"
docker compose exec -T db psql -U candles candle_collector -c "
    SELECT 'candles' AS table_name, count(*) FROM candles
    UNION ALL
    SELECT 'funding_rates', count(*) FROM funding_rates
    UNION ALL
    SELECT 'open_interest', count(*) FROM open_interest
    UNION ALL
    SELECT 'collection_status', count(*) FROM collection_status
    ORDER BY 1;
"

echo "==> Restore complete."
