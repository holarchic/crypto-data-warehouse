"""Exchange and timeframe configuration from exchanges.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

YAML_PATH = Path(__file__).parent / "exchanges.yaml"


@dataclass
class TimeframeConfig:
    name: str
    interval_s: int
    buffer_s: int


@dataclass
class ExchangeConfig:
    name: str
    ccxt_class: str
    btc_symbol: str
    market_type: str
    rate_limit_ms: int
    enabled: bool
    has_funding: bool = False
    has_oi: bool = False
    options: dict = field(default_factory=dict)
    timeframes: list[TimeframeConfig] = field(default_factory=list)
    # Weight-budget limiter (Binance): set to enable WeightRateLimiter instead
    # of the interval-based RateLimiter. Leave None to keep interval pacing.
    rate_limit_weight_budget: int | None = None
    rate_limit_window_s: int = 60
    rate_limit_safety: int = 400


def _load_yaml():
    with open(YAML_PATH) as f:
        return yaml.safe_load(f)


def _parse_exchange(exchange_name: str, ex: dict, tf_defs: dict) -> ExchangeConfig:
    timeframes = []
    for tf_name in ex.get("supported_timeframes", []):
        if tf_name not in tf_defs:
            raise ValueError(f"Timeframe '{tf_name}' not defined in timeframes section")
        td = tf_defs[tf_name]
        timeframes.append(TimeframeConfig(name=tf_name, interval_s=td["interval_s"], buffer_s=td["buffer_s"]))

    return ExchangeConfig(
        name=exchange_name,
        ccxt_class=ex["ccxt_class"],
        btc_symbol=ex["btc_symbol"],
        market_type=ex["market_type"],
        rate_limit_ms=ex.get("rate_limit_ms", 200),
        enabled=True,
        has_funding=ex.get("has_funding", False),
        has_oi=ex.get("has_oi", False),
        options=ex.get("options", {}),
        timeframes=timeframes,
        rate_limit_weight_budget=ex.get("rate_limit_weight_budget"),
        rate_limit_window_s=ex.get("rate_limit_window_s", 60),
        rate_limit_safety=ex.get("rate_limit_safety", 400),
    )


def load_config(exchange_name: str | None = None) -> ExchangeConfig:
    """Load config for the given exchange (default: EXCHANGE env var or 'hyperliquid')."""
    exchange_name = exchange_name or os.environ.get("EXCHANGE", "hyperliquid")
    raw = _load_yaml()
    tf_defs = raw["timeframes"]
    exchanges = raw["exchanges"]

    if exchange_name not in exchanges:
        raise ValueError(f"Unknown exchange '{exchange_name}'. Available: {list(exchanges.keys())}")

    ex = exchanges[exchange_name]
    if not ex.get("enabled", True):
        raise ValueError(f"Exchange '{exchange_name}' is disabled in config")

    return _parse_exchange(exchange_name, ex, tf_defs)


def load_all_configs() -> list[ExchangeConfig]:
    """Load configs for all enabled exchanges."""
    raw = _load_yaml()
    tf_defs = raw["timeframes"]
    configs = []
    for name, ex in raw["exchanges"].items():
        if ex.get("enabled", True):
            configs.append(_parse_exchange(name, ex, tf_defs))
    return configs


def load_discovery_config() -> dict:
    """Load discovery section from exchanges.yaml."""
    raw = _load_yaml()
    return raw.get("discovery", {"volume_threshold_usd": 1_000_000, "symbol_overrides": {}})


def load_timeframe_intervals() -> dict[str, int]:
    """Return {timeframe: interval_s} for all configured timeframes."""
    raw = _load_yaml()
    return {name: td["interval_s"] for name, td in raw["timeframes"].items()}
