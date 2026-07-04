"""Regression guard for the candle-table routing mirror (T-015). `api/main.py` routes `/candles`
and `/status/gaps` to the right storage table per timeframe (1m → candles_1m), but the api/ container
can't import collector/, so it mirrors the routing as a literal. Parse that literal from source and
assert it matches config.load_candle_tables() EXACTLY — so a route added on the collector side but
not the API (or a stale extra API route) can't silently diverge. FastAPI isn't in the collector test
env, hence the ast parse rather than an import (same approach as test_api_loop_timeframes.py).
"""

import ast
from pathlib import Path

from config import load_candle_tables

_API_MAIN = Path(__file__).resolve().parents[1] / "api" / "main.py"


def _api_literal(name: str):
    tree = ast.parse(_API_MAIN.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in api/main.py")


def _config_non_default_tables() -> dict[str, str]:
    """The non-default candle-table routes from the collector's single source (default = candles)."""
    return {tf: table for tf, table in load_candle_tables().items() if table != "candles"}


def test_api_candle_table_mirror_matches_config_exactly():
    # EXACT equality (not subset): a missing route OR a stale extra route both fail.
    assert _api_literal("CANDLE_TABLE_BY_TIMEFRAME") == _config_non_default_tables()
    # Sanity: 1m is actually routed (guards a config that silently lost the 1m mapping).
    assert _config_non_default_tables().get("1m") == "candles_1m"


def test_api_timeframe_intervals_has_1m():
    # Without 1m in the map, /status/gaps?timeframe=1m returns "Unknown timeframe" and /status
    # can't compute is_stale for 1m rows.
    assert _api_literal("TIMEFRAME_INTERVALS").get("1m") == 60


def test_api_candle_queries_are_table_routed():
    # Both candle-reading queries route via _candle_table(); no hardcoded `FROM candles` remains.
    source = _API_MAIN.read_text()
    assert source.count("FROM {_candle_table(timeframe)}") == 2  # /candles + /status/gaps
    assert "FROM candles" not in source  # no un-routed candle query leaked through
