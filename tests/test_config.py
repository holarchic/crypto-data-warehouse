"""Tests for collector/config.py — YAML config loading."""

import pytest
from config import load_all_configs, load_config, load_discovery_config

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
    # Binance ±15m window + ~30m baseline refresh.
    binance = load_config("binance")
    assert binance.oi_settlement_window_s == 900
    assert binance.oi_baseline_interval_s == 1800


def test_oi_cadence_defaults_for_non_oi_exchange():
    # coinbase declares no OI knobs → dataclass defaults (5-min), inert.
    cfg = load_config("coinbase")
    assert cfg.oi_poll_interval_s == 300
    assert cfg.oi_settlement_window_s == 900
    assert cfg.oi_baseline_interval_s == 1800


# --- load_discovery_config ---

def test_load_discovery_config_has_threshold():
    disc = load_discovery_config()
    assert disc["volume_threshold_usd"] == 1_000_000


def test_load_discovery_config_has_overrides():
    disc = load_discovery_config()
    assert disc["symbol_overrides"]["BCHSV"] == "BSV"
