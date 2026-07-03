"""Exchange and timeframe configuration from exchanges.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

YAML_PATH = Path(__file__).parent / "exchanges.yaml"

# Per-loop liveness heartbeats. The funding/OI threads upsert a sentinel
# collection_status row under these pseudo-timeframes (symbol "*") at the end of
# each poll cycle; the monitor's staleness check flags a silently-dead loop.
# NOT candle timeframes — deliberately kept out of exchanges.yaml. Intervals feed
# the monitor's `interval + max(interval, 1800)` threshold: funding → 2h, oi → 35m.
#
# LIQUIDATION_WS is the streaming variant (collector/ws.py): the heartbeat fires on
# a ~60s connection timer *while the socket is up*, not per event — a WS feed is
# routinely connected-but-silent, so an event-based heartbeat would false-alarm every
# quiet hour. Interval 300 → the monitor's 30-min floor flags a dead WS thread ~35min
# after its last heartbeat (cadence is 60s; alert latency is ~35min).
FUNDING_LOOP = "funding"
OI_LOOP = "oi"
LIQUIDATION_WS = "liq_ws"
LOOP_INTERVALS = {FUNDING_LOOP: 3600, OI_LOOP: 300, LIQUIDATION_WS: 300}


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
    # Weight reserved for priority callers (the Binance dense-OI lane, T-007.4). Positive-weight
    # non-priority callers (candles, funding) self-limit to `budget - safety - reserve`; priority
    # callers (dense `fetch_open_interest` + its premiumIndex trigger) draw up to `budget -
    # safety`, so the dense burst never blocks on the candle-kline burst. (The openInterestHist
    # baseline is weight 0 and bypasses the cap entirely — separate /futures/data bucket.) 0 =
    # lane off (single-cap rollback). Operator invariant: reserve >= oi_dense_concurrency × OI weight.
    rate_limit_priority_reserve: int = 0
    # OI cadence knobs (T-007.2). oi_poll_interval_s is the loop tick for every OI
    # strategy (HL bulk, Bybit/OKX bulk, Binance dense). oi_settlement_window_s and
    # oi_baseline_interval_s are Binance-only (± dense window half-width; per-symbol
    # openInterestHist baseline refresh target). Defaults keep non-OI exchanges inert.
    oi_poll_interval_s: int = 300
    oi_settlement_window_s: int = 900
    oi_baseline_interval_s: int = 1800
    # Binance-dense-only (T-007.4): concurrent workers for the per-symbol dense fetch, so a
    # mass settlement (~541 symbols) stays under the tick instead of ~140s sequential. Set 1
    # to fall back to sequential (rollback for any shared-session anomaly).
    oi_dense_concurrency: int = 6


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
        rate_limit_priority_reserve=ex.get("rate_limit_priority_reserve", 0),
        oi_poll_interval_s=ex.get("oi_poll_interval_s", 300),
        oi_settlement_window_s=ex.get("oi_settlement_window_s", 900),
        oi_baseline_interval_s=ex.get("oi_baseline_interval_s", 1800),
        oi_dense_concurrency=ex.get("oi_dense_concurrency", 6),
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
