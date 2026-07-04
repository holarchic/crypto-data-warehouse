"""Per-exchange rate limiters.

Two shapes, same interface:

- `RateLimiter` — paces by minimum interval between requests. Used by exchanges
  whose budget is expressed as "N requests per second".
- `WeightRateLimiter` — paces by weight budget per rolling window, synced from
  the server's own used-weight header (e.g. Binance's `X-MBX-USED-WEIGHT-1M`).
  Used where the budget is expressed in request *weight*, not request count.

Both expose:
    acquire(weight: int = 1, priority: bool = False) -> None
    update_from_response(headers: dict | None, weight: int = 1) -> None

Call sites pair `acquire(weight=W)` with `update_from_response(..., weight=W)`
in a try/finally so the reservation is always released — otherwise a failed
fetch would leak `_in_flight` forever. `RateLimiter.update_from_response` is a
no-op; it exists so call sites stay uniform across exchanges. `priority` selects
`WeightRateLimiter`'s reserved lane (ignored by `RateLimiter`); see that class.

`build_rate_limiter(config)` is the single construction seam — it picks the
shape from the config and wires the priority reserve. `paced_call` runs one API
call through acquire→fn→release. Weight helpers for Binance USDM futures live at
the bottom of this module.
"""

import logging
import threading
import time

from config import ExchangeConfig

log = logging.getLogger(__name__)


class RateLimiter:
    """Interval-based limiter. Threads call `acquire()` before each API request;
    the limiter hands out sequential time slots spaced by `interval_s`. No lock
    is held during the network round-trip — multiple requests can be in flight
    at once, each issued at its own allocated slot.
    """

    def __init__(self, interval_s: float):
        self._interval_s = interval_s
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def acquire(self, weight: int = 1, priority: bool = False) -> None:
        # `weight` and `priority` ignored — this limiter paces on request count, not weight;
        # `priority` is accepted so call sites stay uniform across limiter types.
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval_s
            wait = slot - now
        if wait > 0:
            time.sleep(wait)

    def update_from_response(self, headers: dict | None, weight: int = 1) -> None:
        return None

    def used_snapshot(self) -> None:
        """No weight budget to report — this limiter paces on request count. Exists so
        callers can log weight uniformly across limiter types (see `WeightRateLimiter`)."""
        return None


class WeightRateLimiter:
    """Weight-budget limiter synced from response headers.

    State:
    - `_used`: last authoritative value from the server's used-weight header.
    - `_used_at`: monotonic time of that reading.
    - `_in_flight`: weight reserved by `acquire()` but not yet released by a
      matching `update_from_response()` call. Absorbs the window between
      multiple threads reserving before any response returns.

    Algorithm:
    - A **zero-weight** call (`weight <= 0`) bypasses the gate entirely — it
      consumes no budget (e.g. the openInterestHist baseline on the separate
      `/futures/data` bucket) and so must never block on the main-pool cap.
    - If the last reading is older than `window_s + 5s`, assume the rolling
      window has rolled over and reset `_used` and `_in_flight`. This prevents
      permanent stall if no responses come back (e.g. during an IP ban).
    - On `acquire(weight, priority)`: two caps on the one shared budget — a
      **priority** caller (the dense-OI lane) may reserve up to `budget - safety`;
      a **non-priority** caller (candles, funding) is held to
      `budget - safety - priority_reserve`, leaving the reserve spendable by
      priority callers even when candles have filled their lower cap. If the
      relevant cap fits, reserve and return; otherwise sleep `poll_s` and retry.
    - On `update_from_response`: read server's used-weight header (case
      insensitive), set `_used` to it, release the per-request reservation
      (weight-only; `priority` does not affect release).

    `priority_reserve` is clamped ≥0 and neutralized to 0 (with a warning) if it
    claims ≥ half the usable cap — a conservative generic bound that keeps the
    non-priority cap comfortably above any single caller's weight (an over-large
    reserve would otherwise shrink it below a caller's weight and stall
    candle/funding acquires); half-cap is the policy, not the literal deadlock point.

    The server header is the source of truth for cross-process budget use — it
    reflects the IP's actual usage across this and any other process sharing the
    IP. Under concurrent callers on one exchange instance the read is best-effort
    (`last_response_headers` is an exchange-global slot, so a release may sync
    `_used` from a peer's or a failed call's headers); this is usually conservative
    for a short burst, and the `safety` margin absorbs the slack.
    """

    DEFAULT_HEADER = "x-mbx-used-weight-1m"

    def __init__(
        self,
        budget: int,
        window_s: int = 60,
        safety: int = 400,
        poll_s: float = 0.5,
        header: str = DEFAULT_HEADER,
        priority_reserve: int = 0,
    ):
        self._budget = budget
        self._window_s = window_s
        self._safety = safety
        self._poll_s = poll_s
        self._header = header.lower()
        self._lock = threading.Lock()
        self._used = 0
        self._used_at = time.monotonic()
        self._in_flight = 0
        # Weight held back from non-priority callers so a priority caller (the dense-OI lane)
        # always has spendable headroom (see `acquire`). Guard against an over-large reserve: the
        # actual failure is the non-priority cap (`budget - safety - reserve`) falling to/below a
        # caller's weight — at the extreme, a reserve >= the usable cap collapses it to <=0 and
        # stalls every candle/funding acquire. Rather than couple the limiter to caller weights,
        # reject any reserve claiming >= HALF the usable cap: a conservative, generic bound that
        # keeps the non-priority cap > (budget-safety)/2 — comfortably above any single caller's
        # weight — and neutralizes an over-large reserve to 0 (single-cap fallback, collector stays
        # alive) with a loud warning. Half-cap is the policy bound, not the literal deadlock point.
        usable = budget - safety
        if priority_reserve * 2 >= usable:
            log.warning(
                "priority_reserve=%d claims >= half the usable cap (budget=%d safety=%d "
                "usable=%d); neutralizing to 0 to avoid starving non-priority callers",
                priority_reserve, budget, safety, usable,
            )
            priority_reserve = 0
        self._priority_reserve = max(0, priority_reserve)

    def acquire(self, weight: int = 1, priority: bool = False) -> None:
        # A zero-weight call consumes no budget (e.g. the openInterestHist baseline on the
        # separate /futures/data bucket) — it must never block on the main-pool cap.
        if weight <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                # Rolling-window fallback: if no sync for >window, assume reset.
                if now - self._used_at > self._window_s + 5:
                    self._used = 0
                    self._in_flight = 0
                    self._used_at = now

                # Two caps on one shared budget: priority callers (dense OI) may use the full
                # `budget - safety`; non-priority callers (candles/funding) leave the reserve
                # untouched. Holding non-priority below the high cap is what keeps `_used`
                # low enough that the reserve is genuinely spendable by priority callers.
                cap = self._budget - self._safety - (0 if priority else self._priority_reserve)
                if self._used + self._in_flight + weight <= cap:
                    self._in_flight += weight
                    return

            time.sleep(self._poll_s)

    def update_from_response(self, headers: dict | None, weight: int = 1) -> None:
        # Always release this request's reservation, even if headers are
        # missing — otherwise a failed request leaks `_in_flight`.
        used_weight = _read_weight_header(headers, self._header) if headers else None
        with self._lock:
            if used_weight is not None:
                self._used = used_weight
                self._used_at = time.monotonic()
            self._in_flight = max(0, self._in_flight - weight)

    def used_snapshot(self) -> tuple[int, int]:
        """Read-only `(server used-weight, reserved in-flight)` for logging — direct evidence
        the rolling-minute weight stays under the priority cap during the dense burst
        (T-007.4 closed the correlation-only diagnosis with this). Lock-guarded; mutates nothing."""
        with self._lock:
            return self._used, self._in_flight


def _read_weight_header(headers: dict, key: str) -> int | None:
    """Case-insensitive header lookup returning int weight or None."""
    # requests/urllib3 return a case-insensitive dict, but ccxt wraps headers
    # in a plain dict whose keys may be any case. Normalize on read.
    key_lower = key.lower()
    for k, v in headers.items():
        if k.lower() == key_lower:
            try:
                return int(v)
            except (TypeError, ValueError):
                return None
    return None


# ---------------------------------------------------------------------------
# Binance USDM weight helpers
# ---------------------------------------------------------------------------
# Weights sourced from Binance USDM Futures API docs (per-endpoint table):
#   https://binance-docs.github.io/apidocs/futures/en/
# Kline weight scales with the `limit` param, others are fixed per endpoint.

BINANCE_FUNDING_HISTORY_WEIGHT = 1  # fetch_funding_rate_history
BINANCE_FUNDING_BULK_WEIGHT = 1     # fetch_funding_rates (premiumIndex bulk)
BINANCE_OI_WEIGHT = 1               # fetch_open_interest
BINANCE_TICKERS_BULK_WEIGHT = 40    # fetch_tickers (ticker/24hr, all symbols)
# fetch_open_interest_history (openInterestHist) is on the separate /futures/data IP
# bucket, NOT the main X-MBX-USED-WEIGHT-1M pool → weight 0 here. Because weight 0 also
# means the WeightRateLimiter applies no pacing, the OI baseline sweep self-paces its
# calls with BINANCE_OI_HIST_SPACING_S against that bucket's (undocumented) cap.
BINANCE_OI_HIST_WEIGHT = 0
BINANCE_OI_HIST_SPACING_S = 0.15


def binance_kline_weight(limit: int) -> int:
    """Weight for Binance USDM fapi/v1/klines at a given `limit` param."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def paced_call(rate_limiter, exchange, weight, fn, *args, priority: bool = False, **kwargs):
    """Acquire budget, run `fn`, release budget using the exchange's last
    response headers. Release happens in a `finally` so failed calls don't
    leak the reservation. `exchange` may be None for non-ccxt callers.

    `priority` is keyword-only (declared after `*args`) so it drives the limiter's
    priority lane without ever being forwarded into `fn` (a ccxt method that would
    reject an unexpected kwarg).
    """
    rate_limiter.acquire(weight=weight, priority=priority)
    try:
        return fn(*args, **kwargs)
    finally:
        headers = getattr(exchange, "last_response_headers", None) if exchange else None
        rate_limiter.update_from_response(headers, weight=weight)


def build_rate_limiter(config: ExchangeConfig):
    """Construct the rate limiter for an exchange: a header-synced `WeightRateLimiter` when a
    weight budget is set (Binance), else an interval `RateLimiter`. This is the single
    construction seam — it owns the `priority_reserve` wiring so the config field provably
    reaches the limiter, and it is testable without importing `main` (which the test harness
    stubs). Returns the limiter; the caller passes it to every thread."""
    if config.rate_limit_weight_budget is not None:
        log.info(
            "Using WeightRateLimiter: budget=%d/%ds safety=%d priority_reserve=%d",
            config.rate_limit_weight_budget, config.rate_limit_window_s,
            config.rate_limit_safety, config.rate_limit_priority_reserve,
        )
        return WeightRateLimiter(
            budget=config.rate_limit_weight_budget,
            window_s=config.rate_limit_window_s,
            safety=config.rate_limit_safety,
            priority_reserve=config.rate_limit_priority_reserve,
        )
    log.info("Using RateLimiter: interval=%dms", config.rate_limit_ms)
    return RateLimiter(config.rate_limit_ms / 1000)
