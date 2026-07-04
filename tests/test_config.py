"""Tests for collector/config.py — YAML config loading."""

import pytest
from config import (
    CANDLE_TABLES,
    _parse_exchange,
    load_all_configs,
    load_candle_tables,
    load_config,
    load_discovery_config,
)

# --- load_config ---

def test_load_config_hyperliquid():
    cfg = load_config("hyperliquid")
    assert cfg.name == "hyperliquid"
    assert cfg.ccxt_class == "hyperliquid"
    assert cfg.market_type == "perp"
    assert cfg.has_funding is True
    assert cfg.has_oi is True


def test_load_config_coinbase_no_funding():
    cfg = load_config("coinbase")
    assert cfg.has_funding is False
    assert cfg.has_oi is False
    assert cfg.market_type == "spot"


def test_has_liquidation_ws_flag():
    # T-009.2: binance + bybit stream liquidations; others (incl. HL) do not (default False).
    assert load_config("binance").has_liquidation_ws is True
    assert load_config("bybit").has_liquidation_ws is True
    assert load_config("hyperliquid").has_liquidation_ws is False
    assert load_config("coinbase").has_liquidation_ws is False


def test_has_premium_index_flag():
    # T-010: only binance polls premiumIndexKlines; others default False.
    assert load_config("binance").has_premium_index is True
    assert load_config("bybit").has_premium_index is False
    assert load_config("hyperliquid").has_premium_index is False
    assert load_config("coinbase").has_premium_index is False


def test_load_config_unknown_exchange_raises():
    with pytest.raises(ValueError, match="Unknown exchange"):
        load_config("doesnotexist")


def test_load_config_timeframes_parsed():
    cfg = load_config("hyperliquid")
    tf_names = [tf.name for tf in cfg.timeframes]
    assert "5m" in tf_names
    assert "1d" in tf_names
    assert len(cfg.timeframes) == 10


# --- load_all_configs ---

def test_load_all_configs_returns_enabled():
    configs = load_all_configs()
    names = [c.name for c in configs]
    assert "hyperliquid" in names
    assert "binance" in names
    assert len(configs) >= 6


def test_load_all_configs_timeframes_populated():
    configs = load_all_configs()
    for cfg in configs:
        assert len(cfg.timeframes) > 0, f"{cfg.name} has no timeframes"


# --- OI cadence knobs (T-007.2) ---

def test_oi_cadence_knobs_per_exchange():
    # HL + Binance dense-tick 60s; Bybit/OKX bulk 30s.
    assert load_config("hyperliquid").oi_poll_interval_s == 60
    assert load_config("binance").oi_poll_interval_s == 60
    assert load_config("bybit").oi_poll_interval_s == 30
    assert load_config("okx").oi_poll_interval_s == 30
    # Binance ±15m window + ~30m baseline refresh + dense parallelism.
    binance = load_config("binance")
    assert binance.oi_settlement_window_s == 900
    assert binance.oi_baseline_interval_s == 1800
    assert binance.oi_dense_concurrency == 6
    # Dense-OI weight lane (T-007.4): binance reserves 600 for the priority dense burst — sized
    # to the dense weight-RATE (~543/min at a mass settlement), not the in-flight count.
    assert binance.rate_limit_priority_reserve == 600


def test_oi_cadence_defaults_for_non_oi_exchange():
    # coinbase declares no OI knobs → dataclass defaults (5-min), inert.
    cfg = load_config("coinbase")
    assert cfg.oi_poll_interval_s == 300
    assert cfg.oi_settlement_window_s == 900
    assert cfg.oi_baseline_interval_s == 1800
    assert cfg.oi_dense_concurrency == 6
    # Priority-reserve lane off by default (non-OI exchange never enters the dense path).
    assert cfg.rate_limit_priority_reserve == 0


# --- candle-table routing (T-015) ---

def test_timeframe_table_routing():
    # 1m routes to candles_1m (ts_1m volume tablespace); every other TF defaults to candles.
    by_name = {tf.name: tf.table for tf in load_config("binance").timeframes}
    assert by_name["1m"] == "candles_1m"
    assert by_name["5m"] == "candles"
    assert by_name["1h"] == "candles"


def test_bybit_1m_enabled_and_rate_retuned():
    # T-015.3: bybit collects 1m (→ candles_1m) and its shared limiter is retuned for the
    # full-universe 1m issue rate (100ms→30ms; still 3.6× under the 120 req/s IP cap).
    cfg = load_config("bybit")
    by_name = {tf.name: tf.table for tf in cfg.timeframes}
    assert by_name["1m"] == "candles_1m"
    assert cfg.rate_limit_ms == 30


def test_load_candle_tables():
    tables = load_candle_tables()
    assert tables["1m"] == "candles_1m"
    assert tables["5m"] == "candles"
    # every mapping is a valid storage table
    assert set(tables.values()) <= set(CANDLE_TABLES)


def test_unknown_table_raises_at_load():
    # A yaml `table:` outside the allowlist fails ONCE at collector config load (via _parse_exchange).
    ex = {"ccxt_class": "x", "btc_symbol": "B", "market_type": "perp", "supported_timeframes": ["5m"]}
    tf_defs = {"5m": {"interval_s": 300, "buffer_s": 30, "table": "bogus_table"}}
    with pytest.raises(ValueError, match="not in allowlist"):
        _parse_exchange("x", ex, tf_defs)


def test_load_candle_tables_validates_bad_table(monkeypatch):
    # The MONITOR path (load_candle_tables) never runs _parse_exchange, so it must validate too —
    # else a bad table: reaches GAP_SQL_BY_TABLE[table] as a per-cycle KeyError (codex correctness).
    import config
    monkeypatch.setattr(config, "_load_yaml",
                        lambda: {"timeframes": {"5m": {"interval_s": 300, "buffer_s": 30, "table": "bogus"}}})
    with pytest.raises(ValueError, match="not in allowlist"):
        load_candle_tables()


def test_timeframe_table_defaults_to_candles():
    # A tf def with no `table:` key → default candles (dataclass default + parse default).
    ex = {"ccxt_class": "x", "btc_symbol": "B", "market_type": "perp", "supported_timeframes": ["5m"]}
    tf_defs = {"5m": {"interval_s": 300, "buffer_s": 30}}
    cfg = _parse_exchange("x", ex, tf_defs)
    assert cfg.timeframes[0].table == "candles"


# --- load_discovery_config ---

def test_load_discovery_config_has_threshold():
    disc = load_discovery_config()
    assert disc["volume_threshold_usd"] == 1_000_000


def test_load_discovery_config_has_overrides():
    disc = load_discovery_config()
    assert disc["symbol_overrides"]["BCHSV"] == "BSV"
