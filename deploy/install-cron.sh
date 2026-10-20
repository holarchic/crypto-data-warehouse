#!/usr/bin/env bash
# Installs this checkout's cron jobs as one marked block in the user's crontab. A re-run replaces
# that block and leaves every other entry alone. Entries an earlier, unmarked install wrote are
# recognised by their `# candle-collector:` comments and by running something in this checkout,
# and are replaced too. Cron times are the host's local time; the server runs on UTC.
#
# Usage: deploy/install-cron.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BEGIN="# >>> crypto-data-warehouse (deploy/install-cron.sh) >>>"
END="# <<< crypto-data-warehouse <<<"

dir=$(printf '%q' "$PROJECT_DIR")
compose="cd $dir && docker compose -f docker-compose.yml -f docker-compose.prod.yml"
block="$BEGIN
# daily discovery (02:00 UTC)
0 2 * * * $compose --profile discovery run --rm discovery >> /var/log/candle-collector-discovery.log 2>&1
# daily HL Grade-A liquidation pull (04:00 UTC)
0 4 * * * $compose --profile liquidations-hl run --rm liquidations-hl >> /var/log/candle-collector-liquidations-hl.log 2>&1
# nightly offsite backup (05:30 UTC; a no-op until the restic env file exists)
30 5 * * * $dir/scripts/backup-offsite.sh >> /var/log/candle-collector-backup.log 2>&1
# daily HL 1m repair from REST (06:30 UTC, ~6 h background trickle)
30 6 * * * $compose --profile hl-candles-repair run --rm --no-deps hl-candles-repair >> /var/log/candle-collector-hl-candles-repair.log 2>&1
# host monitor (every 5 min)
*/5 * * * * $dir/deploy/host-monitor.sh >> /var/log/candle-collector-host-monitor.log 2>&1
$END"

# "no crontab" is an empty crontab; any other failure to read it stops before anything is written.
if ! current=$(crontab -l 2>&1); then
    case "$current" in
        *"no crontab"*) current="" ;;
        *) echo "==> FATAL: cannot read the crontab: ${current}" >&2; exit 1 ;;
    esac
fi

kept=$(awk -v begin="$BEGIN" -v end="$END" -v dir="$PROJECT_DIR" -v qdir="$dir" '
    $0 == begin { inside = 1; next }
    $0 == end { inside = 0; next }
    inside { next }
    /^# candle-collector:/ { next }
    index($0, dir "/") || index($0, "cd " dir " ") || index($0, qdir "/") || index($0, "cd " qdir " ") { next }
    { print }
' <<<"$current")

{
    [ -z "$kept" ] || printf '%s\n' "$kept"
    printf '%s\n' "$block"
} | crontab -

echo "==> Cron jobs installed:"
crontab -l | awk -v begin="$BEGIN" -v end="$END" '$0 == begin, $0 == end'
