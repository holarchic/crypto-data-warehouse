#!/usr/bin/env bash
# The api image's feed metadata and rules, from the built image itself: config.py, feed_health.py and
# exchanges.yaml copied beside main.py, and main.py deriving its intervals, routing, loop names,
# staleness and gap query from them. Run from the project root after a change to api/,
# collector/config.py, collector/feed_health.py or collector/exchanges.yaml:
#
#     ./tests/integration/check_api_metadata.sh
set -euo pipefail

cd "$(dirname "$0")/../.."
docker compose build api > /dev/null
docker compose run --rm --no-deps -T -e POSTGRES_USER=check -e POSTGRES_PASSWORD=check --entrypoint python api - <<'PY'
import config, main
assert config.__file__ == "/app/config.py", config.__file__
assert main.TIMEFRAME_INTERVALS == config.load_timeframe_intervals() | main.RETIRED_TIMEFRAME_INTERVALS
assert len(main.TIMEFRAME_INTERVALS) == 11 and main.TIMEFRAME_INTERVALS["1m"] == 60 and main.TIMEFRAME_INTERVALS["1d"] == 86400
assert main._candle_table("1m") == "candles_1m" and main._candle_table("5m") == "candles" and main._candle_table("1d") == "candles"
assert set(main.LOOP_TIMEFRAMES) == set(config.LOOP_INTERVALS)
assert main.feed_health.__file__ == "/app/feed_health.py", main.feed_health.__file__
sql, params = main.feed_health.gap_query(main._candle_table("1m"), exchange="binance", timeframe="1m",
                                         interval_s=60, lookback_count=10, asset_id="BTC")
assert "FROM candles_1m\n" in sql and params["asset_id"] == "BTC" and params["lookback_s"] == 600
# The real handlers, over a fake pool that answers by query: /status flags a never-succeeded row and
# not a fresh one; /status/gaps runs the shared query on candles_1m with the asset filter.
from contextlib import contextmanager
from datetime import datetime, timezone
t0 = datetime(2026, 10, 21, 10, 0, tzinfo=timezone.utc)
t1 = datetime(2026, 10, 21, 10, 3, tzinfo=timezone.utc)
STATUS_ROWS = [
    {"exchange": "binance", "symbol": "A/USDT:USDT", "timeframe": "5m", "last_success": None,
     "error_count": 0, "last_error": None, "seconds_since_success": None},
    {"exchange": "binance", "symbol": "B/USDT:USDT", "timeframe": "5m", "last_success": t0,
     "error_count": 0, "last_error": None, "seconds_since_success": 30.0},
]
GAP_ROWS = [{"symbol": "BTC/USDT:USDT", "gap_after": t0, "gap_before": t1, "missing_count": 2}]

class Result:
    def __init__(self, rows):
        self.rows = rows
    def fetchall(self):
        return self.rows
    def fetchone(self):
        return self.rows[0]

class Conn:
    calls = []
    def execute(self, query, params=None):
        self.calls.append((query, params))
        if "LEAD(" in query:
            return Result(GAP_ROWS)
        if "FROM collection_status" in query:
            return Result(STATUS_ROWS)
        if "pg_database_size" in query:
            return Result([{"size_bytes": 1}])
        if "hypertable_compression_stats" in query:
            return Result([])
        raise AssertionError(query)

class Pool:
    @contextmanager
    def connection(self):
        yield Conn()

main.pool = Pool()
status = main.get_status()
assert [r["is_stale"] for r in status["collectors"]] == [True, False], status
assert status["stale"] == 1 and status["total"] == 2, status
assert status["collectors"][1]["last_success"] == "2026-10-21T10:00:00+00:00", status

gaps = main.get_status_gaps(exchange="binance", timeframe="1m", asset_id="BTC", lookback=10)
query, params = Conn.calls[-1]
assert "FROM candles_1m\n" in query and "asset_id = %(asset_id)s" in query, query
assert params == {"exchange": "binance", "timeframe": "1m", "lookback_s": 600, "interval_s": 60, "asset_id": "BTC"}, params
assert gaps == {"exchange": "binance", "timeframe": "1m", "total": 1, "gaps": [
    {"symbol": "BTC/USDT:USDT", "gap_after": "2026-10-21T10:00:00+00:00",
     "gap_before": "2026-10-21T10:03:00+00:00", "missing_count": 2}]}, gaps
calls = len(Conn.calls)
assert main.get_status_gaps(exchange="binance", timeframe="7m", lookback=10) == {"error": "Unknown timeframe: 7m", "gaps": []}
assert len(Conn.calls) == calls

print("api metadata ok:", len(main.TIMEFRAME_INTERVALS), "intervals,", len(main.LOOP_TIMEFRAMES), "loop names, feed_health at",
      main.feed_health.__file__, "; /status and /status/gaps handlers ok")
PY
