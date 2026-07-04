"""Tests for collector/candles.py (poll_limit weight diet) and its wiring into
_collect_pair. The pure helper is tested directly; the wiring is tested against the
REAL main module (conftest stubs `main` as a MagicMock, so we load main.py in
isolation via importlib to exercise the actual limit selection, not a mock).
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import config as _config_mod  # real collector module — gives us the collector dir
from candles import (
    CANDLE_LIMIT_MARGIN,
    CANDLE_LIMIT_MIN,
    CANDLE_SEED_LIMIT,
    poll_limit,
)
from rate_limiter import binance_kline_weight

_MAIN_PATH = Path(_config_mod.__file__).parent / "main.py"


def _load_real_main():
    """Load collector/main.py under a private name so we run the real _collect_pair /
    fetch_candles without disturbing the shared `main` stub other tests rely on."""
    spec = importlib.util.spec_from_file_location("main_real", _MAIN_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- poll_limit pure helper ---------------------------------------------------


def test_poll_limit_steady_state_is_weight_1():
    # Caught up (one interval since last candle) → tiny limit → Binance kline weight 1 (was 5).
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms - interval_s * 1000  # exactly one interval back
    limit = poll_limit(since_ms, now_ms, interval_s)
    assert limit <= CANDLE_LIMIT_MARGIN + 1
    assert binance_kline_weight(limit) == 1


def test_poll_limit_sizes_to_gap_plus_margin():
    # A pair down K cycles fetches ~K + MARGIN candles (exact gap recovery, not a flat 1000).
    interval_s = 300
    k = 40
    now_ms = 1_700_000_000_000
    since_ms = now_ms - k * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == k + CANDLE_LIMIT_MARGIN


def test_poll_limit_caps_at_seed_limit():
    # A huge gap is capped at the per-call ceiling (recovery preserved exactly as flat-1000 gave).
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms - 10_000 * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == CANDLE_SEED_LIMIT


def test_poll_limit_floors_on_clock_skew():
    # now < since (clock skew) → floor, never 0/negative.
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms + 5 * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == CANDLE_LIMIT_MIN


# --- _collect_pair wiring (real main) -----------------------------------------


def _run_collect_pair(main_mod, monkeypatch, last_timestamp, weight_budget):
    """Drive the real _collect_pair with fetch_candles patched to capture its `limit`."""
    captured = {}

    def _fake_fetch(exchange, symbol, market_info, exchange_name, timeframe, interval_s,
                    rate_limiter, since_ms=None, limit=1000):
        captured["limit"] = limit
        captured["since_ms"] = since_ms
        return []  # empty → insert_candles/status paths are no-ops

    monkeypatch.setattr(main_mod, "fetch_candles", _fake_fetch)
    # Freeze wall clock so the poll_limit gap is deterministic.
    fixed_now = 1_700_000_000.0
    monkeypatch.setattr(main_mod.time, "time", lambda: fixed_now)

    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=weight_budget)
    tf = SimpleNamespace(name="5m", interval_s=300, table="candles")
    pair = {"asset_id": "BTC", "symbol": "BTC/USDT:USDT", "native_symbol": "BTCUSDT",
            "market_type": "perp", "last_timestamp": last_timestamp}
    main_mod._collect_pair(object(), pair, config, tf, None, None, "binance/5m")
    return captured, fixed_now


def test_collect_pair_seed_uses_full_limit(monkeypatch):
    # Seed (no stored candle) fetches max history regardless of venue.
    main_mod = _load_real_main()
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=None, weight_budget=2400)
    assert captured["since_ms"] is None
    assert captured["limit"] == CANDLE_SEED_LIMIT


def test_collect_pair_poll_weight_bound_uses_diet(monkeypatch):
    # Poll on a weight-bound venue (Binance) sizes the limit to the gap — small, not 1000.
    main_mod = _load_real_main()
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, now = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=2400)
    expected = poll_limit(captured["since_ms"], int(now * 1000), interval_s)
    assert captured["limit"] == expected
    assert captured["limit"] < CANDLE_SEED_LIMIT  # diet applied


def test_collect_pair_poll_non_weight_bound_keeps_full_limit(monkeypatch):
    # Poll on a non-weight-bound venue keeps the flat 1000 — diet is Binance-only.
    main_mod = _load_real_main()
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=None)
    assert captured["limit"] == CANDLE_SEED_LIMIT


# --- candle-table routing (T-015) ---------------------------------------------


def test_insert_sql_is_keyed_by_table():
    # One INSERT per candle table, targeting the right table (built from CANDLE_TABLES at import).
    main_mod = _load_real_main()
    assert set(main_mod.INSERT_SQL) == {"candles", "candles_1m"}
    assert "INTO candles (" in main_mod.INSERT_SQL["candles"]
    assert "INTO candles_1m (" in main_mod.INSERT_SQL["candles_1m"]


def test_insert_candles_selects_table(monkeypatch):
    # insert_candles(table=...) executes the SQL for that table; unknown table fails fast.
    main_mod = _load_real_main()
    executed = {}

    class _Cur:
        rowcount = 1
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, row): executed["sql"] = sql

    class _Conn:
        def cursor(self): return _Cur()
        def commit(self): pass

    row = {"exchange": "binance", "market_type": "perp", "asset_id": "BTC", "symbol": "B",
           "native_symbol": "B", "timeframe": "1m", "timestamp": 1.0, "open": 1, "high": 1,
           "low": 1, "close": 1, "volume": 1, "quote_volume": None, "trades": None}
    main_mod.insert_candles(_Conn(), [row], "candles_1m")
    assert "INTO candles_1m (" in executed["sql"]
    import pytest
    with pytest.raises(KeyError):
        main_mod.insert_candles(_Conn(), [row], "no_such_table")


def test_collect_pair_threads_tf_table(monkeypatch):
    # _collect_pair must route the insert to tf.table (so 1m lands in candles_1m).
    main_mod = _load_real_main()
    captured = {}
    monkeypatch.setattr(main_mod, "fetch_candles", lambda *a, **k: [{"timestamp": 1.0}])
    monkeypatch.setattr(main_mod, "insert_candles",
                        lambda conn, candles, table="candles": captured.setdefault("table", table) or 1)
    monkeypatch.setattr(main_mod, "upsert_status_success", lambda *a, **k: None)
    monkeypatch.setattr(main_mod.time, "time", lambda: 1_700_000_000.0)
    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=None)
    tf = SimpleNamespace(name="1m", interval_s=60, table="candles_1m")
    pair = {"asset_id": "BTC", "symbol": "B", "native_symbol": "B",
            "market_type": "perp", "last_timestamp": None}
    main_mod._collect_pair(object(), pair, config, tf, None, object(), "binance/1m")
    assert captured["table"] == "candles_1m"
