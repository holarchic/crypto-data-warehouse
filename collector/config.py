"""Exchange and timeframe configuration from exchanges.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


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
    options: dict = field(default_factory=dict)
    timeframes: list[TimeframeConfig] = field(default_factory=list)


def load_config(exchange_name: str | None = None) -> ExchangeConfig:
    """Load config for the given exchange (default: EXCHANGE env var or 'hyperliquid')."""
    exchange_name = exchange_name or os.environ.get("EXCHANGE", "hyperliquid")

    yaml_path = Path(__file__).parent / "exchanges.yaml"
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    tf_defs = raw["timeframes"]
    exchanges = raw["exchanges"]

    if exchange_name not in exchanges:
        raise ValueError(f"Unknown exchange '{exchange_name}'. Available: {list(exchanges.keys())}")

    ex = exchanges[exchange_name]
    if not ex.get("enabled", True):
        raise ValueError(f"Exchange '{exchange_name}' is disabled in config")

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
        options=ex.get("options", {}),
        timeframes=timeframes,
    )
