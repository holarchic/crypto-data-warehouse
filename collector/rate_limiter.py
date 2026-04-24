"""Per-exchange rate limiters.

Two shapes, same interface:

- `RateLimiter` — paces by minimum interval between requests. Used by exchanges
  whose budget is expressed as "N requests per second".
- `WeightRateLimiter` — paces by weight budget per rolling window, synced from
  the server's own used-weight header (e.g. Binance's `X-MBX-USED-WEIGHT-1M`).
  Used where the budget is expressed in request *weight*, not request count.

Both expose:
    acquire(weight: int = 1) -> None
    update_from_response(headers: dict | None, weight: int = 1) -> None

Call sites pair `acquire(weight=W)` with `update_from_response(..., weight=W)`
in a try/finally so the reservation is always released — otherwise a failed
fetch would leak `_in_flight` forever. `RateLimiter.update_from_response` is a
no-op; it exists so call sites stay uniform across exchanges. Weight helpers
for Binance USDM futures live at the bottom of this module.
"""

import logging
import threading
import time

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

    def acquire(self, weight: int = 1) -> None:
        # `weight` ignored — this limiter paces on request count, not weight.
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval_s
            wait = slot - now
        if wait > 0:
            time.sleep(wait)

    def update_from_response(self, headers: dict | None, weight: int = 1) -> None:
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
    - If the last reading is older than `window_s + 5s`, assume the rolling
      window has rolled over and reset `_used` and `_in_flight`. This prevents
      permanent stall if no responses come back (e.g. during an IP ban).
    - On `acquire(weight)`: if `_used + _in_flight + weight <= budget - safety`,
      reserve and return. Otherwise sleep `poll_s` and retry.
    - On `update_from_response`: read server's used-weight header (case
      insensitive), set `_used` to it, release the per-request reservation.

    The server header is authoritative — it reflects the IP's actual budget use
    across this and any other process sharing the IP, so it can't drift.
    """

    DEFAULT_HEADER = "x-mbx-used-weight-1m"

    def __init__(
        self,
        budget: int,
        window_s: int = 60,
        safety: int = 400,
        poll_s: float = 0.5,
        header: str = DEFAULT_HEADER,
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

    def acquire(self, weight: int = 1) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                # Rolling-window fallback: if no sync for >window, assume reset.
                if now - self._used_at > self._window_s + 5:
                    self._used = 0
                    self._in_flight = 0
                    self._used_at = now

                cap = self._budget - self._safety
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


def binance_kline_weight(limit: int) -> int:
    """Weight for Binance USDM fapi/v1/klines at a given `limit` param."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def paced_call(rate_limiter, exchange, weight, fn, *args, **kwargs):
    """Acquire budget, run `fn`, release budget using the exchange's last
    response headers. Release happens in a `finally` so failed calls don't
    leak the reservation. `exchange` may be None for non-ccxt callers.
    """
    rate_limiter.acquire(weight=weight)
    try:
        return fn(*args, **kwargs)
    finally:
        headers = getattr(exchange, "last_response_headers", None) if exchange else None
        rate_limiter.update_from_response(headers, weight=weight)
