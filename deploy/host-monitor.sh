#!/usr/bin/env bash
# Host-level health monitor for crypto-data-warehouse.
# Runs via cron every 5 minutes. Sends Discord/webhook alerts for:
#   - Disk usage — tiered 75% / 85% / 95% bands, per mount (/ and /mnt/pgvol)
#   - Available memory <500MB
#   - Expected Docker containers not running
#   - Offsite backup older than 26h (once configured)
#
# Disk alerts self-dedup per mount+band: an escalation (75->85->95) pages at once,
# an unchanged band re-pings at most hourly, a drop below 75% clears (recovery ping).
# A stale backup re-pings every 6h on its own reminder file and pings again on recovery.
# Memory and container alerts each re-ping at most hourly on their own reminder file, so
# neither mutes the other. Every check runs every time; none exits early.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# -- Webhook --------------------------------------------------------------------
# A checkout without scripts/webhook-lib.sh still runs every check below; it just can't page.

WEBHOOK_URL="${WEBHOOK_URL:-}"
if [ -f "$PROJECT_DIR/scripts/webhook-lib.sh" ]; then
    # shellcheck source=scripts/webhook-lib.sh
    . "$PROJECT_DIR/scripts/webhook-lib.sh"
    WEBHOOK_URL=$(webhook_url "$PROJECT_DIR")
else
    webhook_post() { return 1; }
fi

# -- Helpers ------------------------------------------------------------------

# 0 only when the page was delivered (webhook_post's codes), so reminder state that must
# not move on an unsent page can check it.
post_webhook() {
    webhook_post "$WEBHOOK_URL" "🚨 candle-collector ($(hostname)): $1"
}

# remind <file> <message>: pages unless <file> was touched within the hour. The file moves
# only on a delivered page, so an undelivered one is retried next run.
ALERT_REMIND_SECONDS=3600
remind() {
    local file="$1" message="$2" now last
    now=$(date +%s)
    last=$(stat -c %Y "$file" 2>/dev/null || stat -f %m "$file" 2>/dev/null || echo 0)
    [ $(( now - last )) -ge "$ALERT_REMIND_SECONDS" ] || return 0
    echo "[host-monitor] ALERT: $message"
    if post_webhook "$message"; then
        touch "$file" || echo "[host-monitor] cannot write ${file}; this page repeats every run"
    else
        echo "[host-monitor] page not delivered; retrying next run"
    fi
}

# Disk alerts dedup on the per-mount band state below, which the caller writes only when
# this returns 0 (delivered), so an undelivered page is retried next run.
disk_alert() {
    local message="$1"
    echo "[host-monitor] $message"
    post_webhook "$message" && return 0
    echo "[host-monitor] page not delivered; retrying next run"
    return 1
}

# -- Disk usage (tiered, per mount) -------------------------------------------
# Bands 75/85/95. Each mount records its last-alerted band + timestamp in its own
# state file, so an escalation pages immediately, the same band re-pings at most
# hourly, and dropping below 75% clears. Both the root disk and the data volume
# are watched — the root does NOT overflow to the volume, so each fills alone.
DISK_STATE_DIR="${DISK_STATE_DIR:-/tmp/candle-collector-disk-bands}"
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
    # Under pipefail a failed df fails the assignment; without `|| pct=""` that aborts the
    # whole monitor instead of skipping this mount.
    pct=$(df "$mount" --output=pcent 2>/dev/null | tail -1 | tr -d ' %') || pct=""
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
            disk_alert "✅ RECOVERED: ${mount} back to ${pct}% (below 75%)" \
                && echo "0 $disk_now" > "$state_file"
        else
            echo "0 $disk_now" > "$state_file"
        fi
    elif [ "$band" -gt "${prev_band:-0}" ]; then       # escalation — page immediately
        disk_alert "$(disk_band_label "$band"): ${mount} at ${pct}% (crossed ${band}%)" \
            && echo "$band $disk_now" > "$state_file"
    elif [ "$band" -eq "${prev_band:-0}" ]; then       # same band — hourly reminder
        if [ $(( disk_now - ${prev_ts:-0} )) -ge "$DISK_REMIND_SECONDS" ]; then
            disk_alert "$(disk_band_label "$band"): ${mount} still at ${pct}% (>=${band}%)" \
                && echo "$band $disk_now" > "$state_file"
        fi
    else                                               # de-escalated but still >=75 — update quietly
        echo "$band $disk_now" > "$state_file"
    fi
done

# -- Offsite backup freshness -------------------------------------------------
# Checked only once offsite backup is configured (the env file scripts/backup-offsite.sh
# reads). A failed run pages from that script; this catches the runs that never start.
# Its own reminder file, so a stale backup never mutes another check's page. The env file and
# marker paths come from scripts/offsite-lib.sh; a checkout without it skips this check, never
# the ones after it. Reminder state moves only
# on a delivered page, so an undelivered one is retried next run.
offsite_lib_loaded=0
if [ -f "$PROJECT_DIR/scripts/offsite-lib.sh" ]; then
    # shellcheck source=scripts/offsite-lib.sh
    . "$PROJECT_DIR/scripts/offsite-lib.sh"
    offsite_lib_loaded=1
fi
BACKUP_ALERT_FILE="${BACKUP_ALERT_FILE:-/tmp/candle-collector-backup-stale-alert}"
BACKUP_STALE_SECONDS=$((26 * 3600))
BACKUP_REMIND_SECONDS=$((6 * 3600))

if [ "$offsite_lib_loaded" -eq 1 ] && [ -f "$RESTIC_ENV_FILE" ]; then
    backup_now=$(date +%s)
    last_ok=$(stat -c %Y "$BACKUP_OK_MARKER" 2>/dev/null || echo 0)
    if [ $(( backup_now - last_ok )) -ge "$BACKUP_STALE_SECONDS" ]; then
        last_alert=$(stat -c %Y "$BACKUP_ALERT_FILE" 2>/dev/null || echo 0)
        if [ $(( backup_now - last_alert )) -ge "$BACKUP_REMIND_SECONDS" ]; then
            if [ "$last_ok" -eq 0 ]; then
                backup_age="never succeeded"
            else
                backup_age="last success $(( (backup_now - last_ok) / 3600 ))h ago"
            fi
            echo "[host-monitor] Offsite backup stale: ${backup_age}"
            if post_webhook "🟠 Offsite backup stale: ${backup_age}"; then
                touch "$BACKUP_ALERT_FILE" \
                    || echo "[host-monitor] cannot write ${BACKUP_ALERT_FILE}; the stale page repeats every run"
            else
                echo "[host-monitor] page not delivered; retrying next run"
            fi
        fi
    elif [ -f "$BACKUP_ALERT_FILE" ]; then
        echo "[host-monitor] Offsite backup recovered"
        if post_webhook "✅ Offsite backup recovered"; then
            rm -f "$BACKUP_ALERT_FILE" || echo "[host-monitor] cannot remove ${BACKUP_ALERT_FILE}"
        else
            echo "[host-monitor] page not delivered; retrying next run"
        fi
    fi
fi

# -- Memory -------------------------------------------------------------------

MEMORY_ALERT_FILE="${MEMORY_ALERT_FILE:-/tmp/candle-collector-memory-alert}"
MEMINFO_FILE="${MEMINFO_FILE:-/proc/meminfo}"

avail_mb=$(awk '/MemAvailable/ {printf "%d", $2/1024}' "$MEMINFO_FILE" 2>/dev/null) || avail_mb=""
if [ -z "$avail_mb" ]; then
    echo "[host-monitor] cannot read MemAvailable from ${MEMINFO_FILE}; memory not checked"
elif [ "$avail_mb" -lt 500 ]; then
    remind "$MEMORY_ALERT_FILE" "Available memory: ${avail_mb}MB (threshold: 500MB)"
fi

# -- Docker containers --------------------------------------------------------
# Services are matched by exact compose service name. A failed `compose ps` pages as a failed
# probe: it says nothing about which containers run.

CONTAINER_ALERT_FILE="${CONTAINER_ALERT_FILE:-/tmp/candle-collector-container-alert}"
EXPECTED_SERVICES=(
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
if running=$(docker compose -f docker-compose.yml -f docker-compose.prod.yml \
        ps --status running --format '{{.Service}}' 2>/dev/null); then
    down_services=()
    for service in "${EXPECTED_SERVICES[@]}"; do
        grep -qxF "$service" <<<"$running" || down_services+=("$service")
    done
    if [ ${#down_services[@]} -gt 0 ]; then
        remind "$CONTAINER_ALERT_FILE" "Containers down: ${down_services[*]}"
    fi
else
    remind "$CONTAINER_ALERT_FILE" "Container check failed: docker compose ps did not answer"
fi
