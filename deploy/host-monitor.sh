#!/usr/bin/env bash
# Host-level health monitor for crypto-data-warehouse.
# Runs via cron every 5 minutes. Sends Discord/webhook alerts for:
#   - Disk usage — tiered 75% / 85% / 95% bands, per mount (/ and /mnt/pgvol)
#   - Available memory <500MB
#   - Expected Docker containers not running
#
# Disk alerts self-dedup per mount+band: an escalation (75->85->95) pages at once,
# an unchanged band re-pings at most hourly, a drop below 75% clears (recovery ping).
# Memory + container alerts share the ALERT_FILE cooldown: skip if touched this hour.
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

post_webhook() {
    local message="$1"
    local hostname
    hostname=$(hostname)
    if [ -n "$WEBHOOK_URL" ]; then
        curl -sf -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"🚨 candle-collector ($hostname): $message\"}" \
            > /dev/null 2>&1 || true
    fi
}

# Memory/container alerts: post + arm the shared ALERT_FILE hourly cooldown.
send_alert() {
    local message="$1"
    echo "[host-monitor] ALERT: $message"
    post_webhook "$message"
    touch "$ALERT_FILE"
}

# Disk alerts: post only — dedup is per-mount+band state below, NOT the shared cooldown.
disk_alert() {
    local message="$1"
    echo "[host-monitor] $message"
    post_webhook "$message"
}

# -- Disk usage (tiered, per mount) -------------------------------------------
# Bands 75/85/95. Each mount records its last-alerted band + timestamp in its own
# state file, so an escalation pages immediately, the same band re-pings at most
# hourly, and dropping below 75% clears. Both the root disk and the data volume
# are watched — the root does NOT overflow to the volume, so each fills alone.
DISK_STATE_DIR="/tmp/candle-collector-disk-bands"
DISK_REMIND_SECONDS=3600
DISK_MOUNTS=("/" "/mnt/pgvol")
mkdir -p "$DISK_STATE_DIR"

disk_band() {
    if   [ "$1" -ge 95 ]; then echo 95
    elif [ "$1" -ge 85 ]; then echo 85
    elif [ "$1" -ge 75 ]; then echo 75
    else echo 0; fi
}

disk_band_label() {
    case "$1" in
        95) echo "🔴 CRITICAL disk";;
        85) echo "🟠 URGENT disk";;
        75) echo "🟡 WARNING disk";;
        *)  echo "disk";;
    esac
}

disk_now=$(date +%s)
for mount in "${DISK_MOUNTS[@]}"; do
    pct=$(df "$mount" --output=pcent 2>/dev/null | tail -1 | tr -d ' %')
    [ -z "${pct:-}" ] && continue                      # mount absent / df failed — skip
    band=$(disk_band "$pct")
    state_file="$DISK_STATE_DIR/$(echo "$mount" | tr '/' '_')"
    prev_band=0
    prev_ts=0
    if [ -f "$state_file" ]; then
        read -r prev_band prev_ts < "$state_file" || true
    fi

    if [ "$band" -eq 0 ]; then                         # below all bands
        if [ "${prev_band:-0}" -ge 75 ]; then
            disk_alert "✅ RECOVERED: ${mount} back to ${pct}% (below 75%)"
        fi
        echo "0 $disk_now" > "$state_file"
    elif [ "$band" -gt "${prev_band:-0}" ]; then       # escalation — page immediately
        disk_alert "$(disk_band_label "$band"): ${mount} at ${pct}% (crossed ${band}%)"
        echo "$band $disk_now" > "$state_file"
    elif [ "$band" -eq "${prev_band:-0}" ]; then       # same band — hourly reminder
        if [ $(( disk_now - ${prev_ts:-0} )) -ge "$DISK_REMIND_SECONDS" ]; then
            disk_alert "$(disk_band_label "$band"): ${mount} still at ${pct}% (>=${band}%)"
            echo "$band $disk_now" > "$state_file"
        fi
    else                                               # de-escalated but still >=75 — update quietly
        echo "$band $disk_now" > "$state_file"
    fi
done

# -- Memory + container dedup gate (shared hourly cooldown) --------------------

if [ -f "$ALERT_FILE" ]; then
    last_alert=$(stat -c %Y "$ALERT_FILE" 2>/dev/null || stat -f %m "$ALERT_FILE" 2>/dev/null || echo 0)
    now=$(date +%s)
    elapsed=$((now - last_alert))
    if [ "$elapsed" -lt "$COOLDOWN_SECONDS" ]; then
        exit 0
    fi
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
