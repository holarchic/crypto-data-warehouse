"""api/main.py's candle-table routing: every candle-reading query routes its table through
_candle_table(), so a timeframe stored in candles_1m is read from there (T-015). The routing itself
comes from collector/config.py (tests/test_api_feed_metadata.py)."""

from pathlib import Path

_API_MAIN = Path(__file__).resolve().parents[1] / "api" / "main.py"


def test_api_candle_queries_are_table_routed():
    # Both candle-reading queries route via _candle_table(); no hardcoded `FROM candles` remains.
    source = _API_MAIN.read_text()
    assert source.count("FROM {_candle_table(timeframe)}") == 1  # /candles
    assert "feed_health.gap_query(_candle_table(timeframe)," in source  # /status/gaps
    assert "FROM candles" not in source  # no un-routed candle query leaked through
