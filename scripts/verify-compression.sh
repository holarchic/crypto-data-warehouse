#!/usr/bin/env bash
# Verify TimescaleDB compression and retention policies are active.
set -euo pipefail

DB_CMD="docker compose exec -T db psql -U candles candle_collector --no-align --tuples-only"

echo "=== Compression status ==="
docker compose exec -T db psql -U candles candle_collector -c "
    SELECT hypertable_name, compression_enabled
    FROM timescaledb_information.hypertables
    WHERE hypertable_schema = 'public'
    ORDER BY hypertable_name;
"

echo "=== Compression stats (per hypertable) ==="
for TABLE in candles funding_rates open_interest; do
    echo "--- ${TABLE} ---"
    docker compose exec -T db psql -U candles candle_collector -c "
        SELECT total_chunks, compressed_chunks,
               pg_size_pretty(before_compression_total_bytes) AS before_size,
               pg_size_pretty(after_compression_total_bytes) AS after_size
        FROM hypertable_compression_stats('${TABLE}');
    " 2>/dev/null || echo "  (no compression stats yet — chunks may not be old enough)"
done

echo ""
echo "=== Compression policies ==="
docker compose exec -T db psql -U candles candle_collector -c "
    SELECT hypertable_name, compress_after
    FROM timescaledb_information.jobs j
    JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
    WHERE j.proc_name = 'policy_compression'
    ORDER BY hypertable_name;
" 2>/dev/null || docker compose exec -T db psql -U candles candle_collector -c "
    SELECT hypertable_name,
           config->>'compress_after' AS compress_after
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
    ORDER BY hypertable_name;
"

echo ""
echo "=== Retention policies ==="
docker compose exec -T db psql -U candles candle_collector -c "
    SELECT hypertable_name,
           config->>'drop_after' AS drop_after
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
    ORDER BY hypertable_name;
"

echo ""
echo "=== Done ==="
