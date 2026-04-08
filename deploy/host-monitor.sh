#!/usr/bin/env bash
# Host-level health monitor for candle-collector.
# Runs via cron every 5 minutes. Sends webhook alerts for:
#   - Disk usage >80%
#   - Available memory <500MB
#   - Expected Docker containers not running
#
# Alert dedup: skips if alert file touched within the last hour.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ALERT_FILE="/tmp/candle-collector-host-alert"
COOLDOWN_SECONDS=3600

# -- Load webhook URL ---------------------------------------------------------

ENV_FILE="$PROJECT_DIR/.env"
WEBHOOK_URL=""
if [ -f "$ENV_FILE" ]; then
    WEBHOOK_URL=$(grep -E '^WEBHOOK_URL=' "$ENV_FILE" | cut -d= -f2- | tr -d '"' || true)
fi

# -- Helpers ------------------------------------------------------------------

send_alert() {
    local message="$1"
    local hostname
    hostname=$(hostname)

    echo "[host-monitor] ALERT: $message"

    if [ -n "$WEBHOOK_URL" ]; then
        curl -sf -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"🚨 candle-collector ($hostname): $message\"}" \
            > /dev/null 2>&1 || true
    fi

    touch "$ALERT_FILE"
}

# -- Dedup check --------------------------------------------------------------

if [ -f "$ALERT_FILE" ]; then
    last_alert=$(stat -c %Y "$ALERT_FILE" 2>/dev/null || stat -f %m "$ALERT_FILE" 2>/dev/null || echo 0)
    now=$(date +%s)
    elapsed=$((now - last_alert))
    if [ "$elapsed" -lt "$COOLDOWN_SECONDS" ]; then
        exit 0
    fi
fi

# -- Disk usage ---------------------------------------------------------------

disk_pct=$(df / --output=pcent 2>/dev/null | tail -1 | tr -d ' %' || df / | tail -1 | awk '{print $5}' | tr -d '%')
if [ "$disk_pct" -gt 80 ]; then
    send_alert "Disk usage at ${disk_pct}% (threshold: 80%)"
    exit 0
fi

# -- Memory -------------------------------------------------------------------

avail_mb=$(awk '/MemAvailable/ {printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 9999)
if [ "$avail_mb" -lt 500 ]; then
    send_alert "Available memory: ${avail_mb}MB (threshold: 500MB)"
    exit 0
fi

# -- Docker containers --------------------------------------------------------

EXPECTED_CONTAINERS=(
    "collector-hyperliquid"
    "collector-binance"
    "collector-bybit"
    "collector-okx"
    "collector-coinbase"
    "collector-dydx"
    "monitor"
    "api"
    "db"
)

cd "$PROJECT_DIR"
running=$(docker compose -f docker-compose.yml -f docker-compose.prod.yml ps --format '{{.Name}}' 2>/dev/null || true)

down_containers=()
for container in "${EXPECTED_CONTAINERS[@]}"; do
    if ! echo "$running" | grep -q "$container"; then
        down_containers+=("$container")
    fi
done

if [ ${#down_containers[@]} -gt 0 ]; then
    send_alert "Containers down: ${down_containers[*]}"
    exit 0
fi
