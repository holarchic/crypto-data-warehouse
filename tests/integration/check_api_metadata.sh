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
docker compose run --rm --no-deps -e POSTGRES_USER=check -e POSTGRES_PASSWORD=check --entrypoint python api -c '
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
rows = [{"timeframe": "5m", "seconds_since_success": None}, {"timeframe": "5m", "seconds_since_success": 30.0}]
stale = [main.feed_health.is_stale_for_status(r["seconds_since_success"], main.TIMEFRAME_INTERVALS.get(r["timeframe"]))
         for r in rows]
assert stale == [True, False], stale
print("api metadata ok:", len(main.TIMEFRAME_INTERVALS), "intervals,", len(main.LOOP_TIMEFRAMES), "loop names, feed_health at",
      main.feed_health.__file__)
'
