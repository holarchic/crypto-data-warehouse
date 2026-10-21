#!/usr/bin/env bash
# The api image's feed metadata, from the built image itself: config.py and exchanges.yaml copied
# beside main.py, and main.py deriving its intervals, routing and loop names from them. Run from the
# project root after a change to api/, collector/config.py or collector/exchanges.yaml:
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
print("api metadata ok:", len(main.TIMEFRAME_INTERVALS), "intervals,", len(main.LOOP_TIMEFRAMES), "loop names")
'
