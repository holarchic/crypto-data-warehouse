# shellcheck shell=bash
# Webhook alerts for the host scripts (scripts/backup.sh, scripts/backup-offsite.sh,
# deploy/host-monitor.sh). collector/monitor.py's build_webhook_payload is the Python twin.

# webhook_url <project dir>: prints WEBHOOK_URL from the environment, else from the project's
# .env read the way compose reads it (surrounding whitespace trimmed; a double- or single-quoted
# value taken up to its closing quote; an unquoted value cut at a ` #` comment), since curl
# rejects a URL that keeps any of that. Prints nothing when unset; warns on an unreadable .env.
webhook_url() {
    local env_file="$1/.env" line value status=0
    if [ -n "${WEBHOOK_URL:-}" ]; then
        printf '%s' "$WEBHOOK_URL"
        return 0
    fi
    [ -f "$env_file" ] || return 0
    line=$(grep -m 1 -E '^[[:space:]]*WEBHOOK_URL[[:space:]]*=' "$env_file") || status=$?
    if [ "$status" -eq 1 ]; then
        return 0
    elif [ "$status" -ne 0 ]; then
        echo "webhook: cannot read ${env_file}; pages are off" >&2
        return 0
    fi
    value="${line#*=}"
    value="${value#"${value%%[![:space:]]*}"}"
    case "$value" in
        \"*) value="${value#\"}"; value="${value%%\"*}" ;;
        \'*) value="${value#\'}"; value="${value%%\'*}" ;;
        *) value="${value%%[[:space:]]#*}"; value="${value%"${value##*[![:space:]]}"}" ;;
    esac
    printf '%s' "$value"
}

# The message as a JSON string body: backslash, quote and every control character escaped.
json_string() {
    local s="$1" code char
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    s="${s//$'\n'/\\n}"
    s="${s//$'\r'/\\r}"
    s="${s//$'\t'/\\t}"
    for code in $(seq 1 31); do
        printf -v char '%b' "\\x$(printf '%02x' "$code")"
        s="${s//"$char"/$(printf '\\u%04x' "$code")}"
    done
    printf '%s' "$s"
}

# webhook_post <url> <message>: posts with both Slack (text) and Discord (content) keys.
# Returns 0 when delivered, 1 when the post failed, 2 when no webhook is configured.
webhook_post() {
    local url="$1" text
    [ -n "$url" ] || return 2
    text=$(json_string "$2")
    curl -sf --connect-timeout 5 --max-time 15 -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "{\"text\":\"${text}\",\"content\":\"${text}\"}" > /dev/null 2>&1 || return 1
}
