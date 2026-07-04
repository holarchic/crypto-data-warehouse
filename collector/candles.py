"""Candle-fetch request sizing.

`poll_limit` sizes the `fetch_ohlcv` `limit` to the actual gap since the last stored
candle instead of a flat 1000. On Binance the kline weight scales with `limit`
(`binance_kline_weight`: <100→1, <500→2, ≤1000→5), so a steady-state poll that needs
one new candle should cost weight 1, not 5 — the flat-1000 poll is what bloated the
mass-settlement candle burst ~5× and starved the dense-OI weight lane (T-007.4).

This is the Poll-path sizing only. Seed (no stored candle) still fetches
`CANDLE_SEED_LIMIT` for maximum history. The caller applies `poll_limit` only on
weight-bound venues (`rate_limit_weight_budget` set) — the weight only binds on
Binance, and there's no reason to change live fetch semantics on venues that aren't
weight-limited. A pure function in a real (non-stubbed) module so it's unit-testable.
"""

CANDLE_LIMIT_MARGIN = 5   # extra candles past the computed gap, covering a boundary-timing
#                           race (a candle closing between `now` and the fetch) so we never
#                           under-fetch — an unfetched closed candle would be a data gap.
CANDLE_LIMIT_MIN = 2      # floor: guards clock skew (now < since ⇒ negative gap) so `limit`
#                           is never 0/negative. Recovery for real gaps comes from the gap
#                           term, not this floor.
CANDLE_SEED_LIMIT = 1000  # Seed / large-gap ceiling: max history the exchange returns per call.


def poll_limit(since_ms: int, now_ms: int, interval_s: int) -> int:
    """Candles to request on a Poll fetch: the closed-candle gap since `since_ms` plus a
    margin, floored at `CANDLE_LIMIT_MIN` and capped at `CANDLE_SEED_LIMIT`. Caught up
    (gap ≈ 1) → a small limit (weight 1 on Binance); a pair down K cycles → ~K + margin
    (capped 1000) so gap recovery is preserved exactly as the flat-1000 ceiling gave."""
    gap_candles = (now_ms - since_ms) // (interval_s * 1000)
    return max(CANDLE_LIMIT_MIN, min(CANDLE_SEED_LIMIT, gap_candles + CANDLE_LIMIT_MARGIN))
