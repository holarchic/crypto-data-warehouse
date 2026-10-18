# shellcheck shell=bash
# Webhook alerts for the host scripts (scripts/backup.sh, scripts/backup-offsite.sh,
# deploy/host-monitor.sh). collector/monitor.py's build_webhook_payload is the Python twin.

# Sets WEBHOOK_URL from the environment, else from <project dir>/.env. Surrounding whitespace
# and double quotes are trimmed the way compose reads .env: a trailing space there once left
# curl rejecting the URL, and every host page went undelivered without a trace.
load_webhook_url() {
    local env_file="$1/.env" line value
    if [ -n "${WEBHOOK_URL:-}" ]; then
        return 0
    fi
    WEBHOOK_URL=""
    [ -f "$env_file" ] || return 0
    line=$(grep -E '^WEBHOOK_URL=' "$env_file" | head -n 1) || return 0
    value="${line#WEBHOOK_URL=}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    value="${value#\"}"
    value="${value%\"}"
    WEBHOOK_URL="$value"
}

# Posts <message> with both Slack (text) and Discord (content) keys. Returns non-zero when a
# configured webhook was not reached; 0 when delivered or when none is configured.
webhook_post() {
    local text="$1"
    [ -n "${WEBHOOK_URL:-}" ] || return 0
    text="${text//\\/\\\\}"
    text="${text//\"/\\\"}"
    curl -sf --connect-timeout 5 --max-time 15 -X POST "$WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "{\"text\":\"${text}\",\"content\":\"${text}\"}" > /dev/null 2>&1
}
