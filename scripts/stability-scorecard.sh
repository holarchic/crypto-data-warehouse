#!/usr/bin/env bash
# The v0.8 stability scorecard (roadmap.md): the gate's measurable criteria, read from the data.
# Read-only. Prints a report and exits 1 when any measured check fails or a measurement can't run.
#
#     ./scripts/stability-scorecard.sh [--days N]    # default 14, the gate window
#
# SCORECARD_API_URL (default http://127.0.0.1:8000) and SCORECARD_AS_OF (an ISO-8601 reference time;
# default now) override the API and the clock. deploy/README.md § Stability scorecard says when to
# run it and how to read it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/ops-lib.sh
. "$SCRIPT_DIR/ops-lib.sh"

API_SAMPLES=50
API_P95_RANK=48          # the 48th of 50 sorted times
API_P95_LIMIT_S=0.500
API_ROWS=500

usage() {
    echo "usage: $0 [--days N]   (N a positive whole number of days, default 14)" >&2
    exit 1
}

DAYS=14
while [ $# -gt 0 ]; do
    case "$1" in
        --days) [ $# -ge 2 ] || usage; DAYS="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[[ "$DAYS" =~ ^[1-9][0-9]*$ ]] || usage

API_URL="${SCORECARD_API_URL:-http://127.0.0.1:8000}"
AS_OF="${SCORECARD_AS_OF:-}"
failed=0

# The deployed config, from the monitor's image: the timeframe intervals, the discovery threshold,
# and which venues collect funding and OI.
read -r -d '' META_PY <<'PY' || true
import json, config
configs = config.load_all_configs()
print(json.dumps({
    "intervals": config.load_timeframe_intervals(),
    "volume_threshold_usd": config.load_discovery_config()["volume_threshold_usd"],
    "funding_exchanges": [c.name for c in configs if c.has_funding],
    "oi_exchanges": [c.name for c in configs if c.has_oi],
}))
PY
if ! meta=$("${DC[@]}" exec -T monitor python -c "$META_PY"); then
    echo "Cannot read the deployed config: is the monitor running?" >&2
    exit 1
fi

# The metadata is the collectors' truth only if every running collector carries the same files.
config_hashes() {
    "${DC[@]}" exec -T "$1" sha256sum /app/config.py /app/exchanges.yaml | awk '{print $1}' | tr '\n' ' '
}
echo "=== Config consistency ==="
if ! monitor_hashes=$(config_hashes monitor); then
    echo "Cannot hash the monitor's config files." >&2
    exit 1
fi
collectors=$("${DC[@]}" ps --services --status running | grep '^collector-' || true)
drifted=()
for service in $collectors; do
    service_hashes=$(config_hashes "$service") || service_hashes="unreadable"
    [ "$service_hashes" = "$monitor_hashes" ] || drifted+=("$service")
done
if [ -z "$collectors" ]; then
    echo "config consistent: FAIL (no running collector)"
    failed=1
elif [ ${#drifted[@]} -gt 0 ]; then
    echo "config consistent: FAIL (differs from the monitor: ${drifted[*]})"
    failed=1
else
    echo "config consistent: PASS ($(echo "$collectors" | wc -l | tr -d ' ') collectors match the monitor)"
fi

echo
echo "=== Database (last ${DAYS} days) ==="
started=$(date +%s)
if ! db_out=$("${DB_TOOL[@]}" psql -X -v ON_ERROR_STOP=1 -v days="$DAYS" -v meta="$meta" -v as_of="$AS_OF" \
        < "$SCRIPT_DIR/stability-scorecard.sql"); then
    printf '%s\n' "${db_out:-}"
    echo "database checks: FAIL (the scorecard SQL failed)"
    exit 1
fi
printf '%s\n' "$db_out"
echo "(database checks took $(( $(date +%s) - started )) s)"
markers=$(grep -cE '^SCORECARD-DB: (PASS|FAIL)$' <<<"$db_out" || true)
if [ "$markers" -ne 1 ]; then
    echo "database checks: FAIL (expected one verdict line, found ${markers})"
    failed=1
elif ! grep -qx 'SCORECARD-DB: PASS' <<<"$db_out"; then
    failed=1
fi

echo
echo "=== API latency ==="
bodies=$(mktemp -d)
trap 'rm -rf "$bodies"' EXIT
url="${API_URL}/candles?exchange=binance&asset_id=BTC&timeframe=5m&limit=${API_ROWS}"
times=""
latency_error=""
for i in $(seq 1 "$API_SAMPLES"); do
    if ! result=$(curl --max-time 5 -sS -o "$bodies/$i" -w '%{http_code} %{time_total}' "$url"); then
        latency_error="request ${i} failed"
        break
    fi
    code=${result%% *}
    if [ "$code" != 200 ]; then
        latency_error="request ${i} answered HTTP ${code}"
        break
    fi
    times+="${result##* }"$'\n'
done
# Every timed response must be the full 500-row query, or the time measured something smaller.
if [ -z "$latency_error" ]; then
    latency_error=$(python3 - "$bodies" "$API_SAMPLES" "$API_ROWS" <<'PY'
import json, sys
bodies, samples, rows = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(1, samples + 1):
    try:
        data = json.load(open(f"{bodies}/{i}"))
        got = len(data) if isinstance(data, list) else "a non-list"
    except (OSError, ValueError):
        got = "unreadable"
    if got != rows:
        print(f"request {i} returned {got} rows, not {rows}")
        break
PY
    ) || latency_error="the response check failed"
fi
if [ -n "$latency_error" ]; then
    echo "api p95 (${API_ROWS} rows): FAIL (${latency_error})"
    failed=1
else
    p95=$(printf '%s' "$times" | sort -g | sed -n "${API_P95_RANK}p")
    if awk -v p="$p95" -v limit="$API_P95_LIMIT_S" 'BEGIN { exit !(p < limit) }'; then
        echo "api p95 (${API_ROWS} rows): PASS (${p95} s < ${API_P95_LIMIT_S} s, ${API_SAMPLES} requests)"
    else
        echo "api p95 (${API_ROWS} rows): FAIL (${p95} s, limit < ${API_P95_LIMIT_S} s)"
        failed=1
    fi
fi

echo
echo "NOT MEASURED: funding rows against venue history (funding_repair's report checks them); per-venue"
echo "  discovery coverage (discovery stores the cross-venue maximum); auto-recovery (T-030.2 times it);"
echo "  gap-detection latency; HL 1m (repaired daily); Bybit/OKX 30 s and Binance settlement-window OI density."
if [ "$failed" -eq 0 ]; then
    echo "SCORECARD: PASS (measured checks only)"
else
    echo "SCORECARD: FAIL"
fi
exit "$failed"
