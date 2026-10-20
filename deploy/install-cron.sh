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

# Cron hands each line to /bin/sh and treats `%` specially, so the checkout path goes in unquoted
# and is limited to characters that need no quoting in either.
case "$PROJECT_DIR" in
    *[!A-Za-z0-9/._-]*)
        echo "==> FATAL: checkout path ${PROJECT_DIR} has characters cron lines can't carry unquoted" >&2
        exit 1
        ;;
esac
dir="$PROJECT_DIR"
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

# A legacy job is one whose command (after the five schedule fields) starts in this checkout:
# `<dir>/…` or `cd <dir> …`. A path elsewhere in a line, such as another job's argument, is not one.
# A marker without its partner (or a nested one) stops the install: dropping lines up to a missing
# end marker would delete every entry after it.
kept=$(BEGIN="$BEGIN" END="$END" DIR="$PROJECT_DIR" awk '
    function ours(cmd) {
        return index(cmd, ENVIRON["DIR"] "/") == 1 || index(cmd, "cd " ENVIRON["DIR"] " ") == 1
    }
    $0 == ENVIRON["BEGIN"] { if (inside) bad = 1; inside = 1; next }
    $0 == ENVIRON["END"] { if (!inside) bad = 1; inside = 0; next }
    inside { next }
    /^# candle-collector:/ { next }
    $1 ~ /^[0-9*]/ {
        cmd = $0
        sub(/^[ \t]*([^ \t]+[ \t]+){5}/, "", cmd)
        if (ours(cmd)) next
    }
    { print }
    END { if (inside || bad) exit 3 }
' <<<"$current") || { echo "==> FATAL: the crontab's crypto-data-warehouse markers don't pair; fix it by hand" >&2; exit 1; }

{
    [ -z "$kept" ] || printf '%s\n' "$kept"
    printf '%s\n' "$block"
} | crontab -

echo "==> Cron jobs installed:"
crontab -l | awk -v begin="$BEGIN" -v end="$END" '$0 == begin, $0 == end'
