#!/usr/bin/env bash
# Verify TimescaleDB compression policies are active.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DC=(docker compose -f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker-compose.prod.yml")

# Statements go in on stdin, so psql variables (-v) interpolate; any SQL error fails the call.
db_sql() { "${DC[@]}" exec -T db psql -X -v ON_ERROR_STOP=1 -U candles candle_collector "$@"; }

echo "=== Compression status ==="
db_sql <<'SQL'
SELECT hypertable_name, compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'public'
ORDER BY hypertable_name;
SQL

echo "=== Compression stats (per hypertable) ==="
for TABLE in candles funding_rates open_interest; do
    echo "--- ${TABLE} ---"
    db_sql -v tbl="$TABLE" 2>/dev/null <<'SQL' \
        || echo "  (no compression stats yet — chunks may not be old enough)"
SELECT total_chunks, compressed_chunks,
       pg_size_pretty(before_compression_total_bytes) AS before_size,
       pg_size_pretty(after_compression_total_bytes) AS after_size
FROM hypertable_compression_stats(:'tbl');
SQL
done

echo ""
echo "=== Compression policies ==="
db_sql 2>/dev/null <<'SQL' || db_sql <<'SQL2'
SELECT hypertable_name, compress_after
FROM timescaledb_information.jobs j
JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
WHERE j.proc_name = 'policy_compression'
ORDER BY hypertable_name;
SQL
SELECT hypertable_name,
       config->>'compress_after' AS compress_after
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_compression'
ORDER BY hypertable_name;
SQL2

echo ""
echo "=== Retention policies (expect none: all history is kept) ==="
db_sql <<'SQL'
SELECT hypertable_name,
       config->>'drop_after' AS drop_after
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention'
ORDER BY hypertable_name;
SQL

echo ""
echo "=== Done ==="
