"""RateLimiter pacing tests."""

import logging
import threading
import time

from config import ExchangeConfig
from rate_limiter import (
    RateLimiter,
    WeightRateLimiter,
    binance_kline_weight,
    build_rate_limiter,
    paced_call,
)


def test_single_thread_paces_at_interval():
    limiter = RateLimiter(0.05)
    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 5 acquires at 50ms interval: 4 intervals = 200ms. Generous upper bound for jitter.
    assert 0.18 < elapsed < 0.40, f"elapsed={elapsed:.3f}"


def test_concurrent_threads_share_slots():
    limiter = RateLimiter(0.05)
    results = []
    lock = threading.Lock()

    def worker():
        limiter.acquire()
        with lock:
            results.append(time.monotonic())

    start = time.monotonic()
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    # 10 acquires at 50ms interval: 9 intervals = 450ms. Upper bound allows jitter.
    assert 0.40 < elapsed < 0.80, f"elapsed={elapsed:.3f}"

    # Slot times should be spaced at ~50ms
    results.sort()
    for a, b in zip(results, results[1:]):
        gap = b - a
        assert gap >= 0.04, f"gap {gap:.3f}s too small between slots"


def test_no_wait_after_idle_period():
    limiter = RateLimiter(0.05)
    limiter.acquire()
    time.sleep(0.1)  # > interval, so next acquire should not wait
    start = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.01, f"acquire after idle should not wait, elapsed={elapsed:.3f}"


def test_rate_limiter_accepts_weight_arg():
    # Weight is ignored by interval limiter, but the arg must be accepted so
    # call sites stay uniform across limiter types.
    limiter = RateLimiter(0.01)
    limiter.acquire(weight=10)
    limiter.update_from_response({"x-mbx-used-weight-1m": "999"}, weight=10)  # no-op


# ---------------------------------------------------------------------------
# WeightRateLimiter
# ---------------------------------------------------------------------------


def test_weight_limiter_acquires_within_budget():
    lim = WeightRateLimiter(budget=2400, safety=400, poll_s=0.01)
    start = time.monotonic()
    for _ in range(10):
        lim.acquire(weight=5)
    elapsed = time.monotonic() - start
    assert elapsed < 0.05, f"acquires within budget should not wait, elapsed={elapsed:.3f}"


def test_weight_limiter_blocks_when_over_cap():
    # cap = budget - safety = 500. One acquire of 300 fits; a second of 300 doesn't
    # (300 in_flight + 300 > 500). Releasing via update_from_response lets it proceed.
    lim = WeightRateLimiter(budget=1000, safety=500, poll_s=0.05)
    lim.acquire(weight=300)

    done = threading.Event()

    def worker():
        lim.acquire(weight=300)
        done.set()

    t = threading.Thread(target=worker)
    t.start()
    assert not done.wait(0.15), "second acquire should have blocked"

    # Simulate a response that shows low server-side usage — releases reservation.
    lim.update_from_response({"x-mbx-used-weight-1m": "0"}, weight=300)
    assert done.wait(0.5), "second acquire should unblock after release"
    t.join()


def test_header_update_syncs_used_weight():
    lim = WeightRateLimiter(budget=1000, safety=100, poll_s=0.01)
    # Server says 800 used; with safety=100 cap=900 → only 100 room.
    lim.update_from_response({"x-mbx-used-weight-1m": "800"}, weight=0)
    lim.acquire(weight=100)

    blocked = threading.Event()

    def worker():
        lim.acquire(weight=50)  # 800 + 100 + 50 = 950 > 900
        blocked.set()

    t = threading.Thread(target=worker)
    t.start()
    assert not blocked.wait(0.15), "acquire exceeding synced used should block"

    # Server reports fresh window — used back to 0.
    lim.update_from_response({"x-mbx-used-weight-1m": "0"}, weight=100)
    assert blocked.wait(0.5), "acquire should unblock after server drop"
    t.join()


def test_window_expiry_resets_state():
    lim = WeightRateLimiter(budget=100, safety=0, window_s=1, poll_s=0.01)
    lim.acquire(weight=100)

    unblocked = threading.Event()

    def worker():
        lim.acquire(weight=10)
        unblocked.set()

    t = threading.Thread(target=worker)
    t.start()
    assert not unblocked.wait(0.2), "should be blocked while budget full"

    # Simulate no response for >window+5s → next acquire path should reset.
    lim._used_at = time.monotonic() - (lim._window_s + 10)
    assert unblocked.wait(1.0), "rollover reset should unblock"
    t.join()


def test_case_insensitive_header_lookup():
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.01)
    lim.acquire(weight=100)
    lim.update_from_response({"X-MBX-Used-Weight-1M": "500"}, weight=100)
    assert lim._used == 500


def test_missing_header_still_releases_reservation():
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.01)
    lim.acquire(weight=100)
    lim.update_from_response({}, weight=100)
    assert lim._in_flight == 0


def test_concurrent_acquires_respect_cap():
    # 20 threads × weight=50 = 1000 total; cap=800. At peak, `_used + _in_flight`
    # should not exceed cap by more than a single grant's worth.
    lim = WeightRateLimiter(budget=1000, safety=200, poll_s=0.01)
    peaks = []
    done_count = [0]
    lock = threading.Lock()

    def worker():
        lim.acquire(weight=50)
        with lock:
            peaks.append(lim._used + lim._in_flight)
        time.sleep(0.02)  # simulate network round-trip
        lim.update_from_response({"x-mbx-used-weight-1m": "0"}, weight=50)
        with lock:
            done_count[0] += 1

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert done_count[0] == 20
    cap = 1000 - 200
    assert max(peaks) <= cap, f"exceeded cap: {max(peaks)}"


def test_binance_kline_weight():
    # Matches Binance USDM fapi/v1/klines weight table.
    assert binance_kline_weight(1) == 1
    assert binance_kline_weight(99) == 1
    assert binance_kline_weight(100) == 2
    assert binance_kline_weight(499) == 2
    assert binance_kline_weight(500) == 5
    assert binance_kline_weight(1000) == 5
    assert binance_kline_weight(1500) == 10


# ---------------------------------------------------------------------------
# WeightRateLimiter — priority reserve lane (T-007.4)
# ---------------------------------------------------------------------------


def test_priority_reserve_lets_priority_proceed_while_nonpriority_blocks():
    # budget=1000 safety=0 reserve=100 → non-priority cap 900, priority cap 1000.
    # used=950: above the non-priority cap, below the priority cap.
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.02, priority_reserve=100)
    lim._used = 950

    got = threading.Event()

    def prio():
        lim.acquire(weight=1, priority=True)  # 950+1 <= 1000 → into the reserve
        got.set()

    tp = threading.Thread(target=prio)
    tp.start()
    assert got.wait(0.5), "priority acquire should proceed into the reserved headroom"
    tp.join()

    blocked = threading.Event()

    def nonprio():
        lim.acquire(weight=1, priority=False)  # 950 + in_flight(1) + 1 > 900 → blocks
        blocked.set()

    tn = threading.Thread(target=nonprio)
    tn.start()
    assert not blocked.wait(0.15), "non-priority acquire should block below the reserved cap"

    lim.update_from_response({"x-mbx-used-weight-1m": "0"}, weight=0)  # used → 0
    assert blocked.wait(0.5), "non-priority should unblock once used drops"
    tn.join()


def test_priority_reserve_zero_is_single_cap_for_positive_weight():
    # reserve=0 → priority and non-priority see the same cap (weight>0 backward-compat).
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.02, priority_reserve=0)
    lim._used = 950
    lim.acquire(weight=1, priority=False)  # both fit under 1000
    lim.acquire(weight=1, priority=True)
    assert lim._in_flight == 2


def test_weight_zero_bypasses_cap_gate():
    # A weight-0 call (openInterestHist baseline on the separate bucket) never blocks on the
    # cap, even with used at/above the high cap — it consumes no main-pool budget.
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.02, priority_reserve=100)
    lim._used = 1000
    done = threading.Event()

    def worker():
        lim.acquire(weight=0, priority=False)
        done.set()

    t = threading.Thread(target=worker)
    t.start()
    assert done.wait(0.3), "weight-0 acquire must not block on the cap"
    t.join()
    assert lim._in_flight == 0  # zero-weight reserves nothing


def test_over_large_reserve_neutralized_to_zero(caplog):
    # reserve >= half the usable cap would deadlock non-priority callers → neutralized to 0.
    with caplog.at_level(logging.WARNING):
        lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.02, priority_reserve=600)
    assert lim._priority_reserve == 0
    assert any("neutralizing to 0" in r.message for r in caplog.records)
    lim.acquire(weight=5, priority=False)  # weight-5 candle still proceeds (not deadlocked)
    assert lim._in_flight == 5


def test_priority_release_accounting_unchanged():
    lim = WeightRateLimiter(budget=1000, safety=0, poll_s=0.02, priority_reserve=100)
    lim.acquire(weight=5, priority=True)
    assert lim._in_flight == 5
    lim.update_from_response({"x-mbx-used-weight-1m": "0"}, weight=5)
    assert lim._in_flight == 0  # release is weight-only; priority doesn't leak in-flight


class _SpyLimiter:
    """Records acquire(weight, priority); never blocks. For the paced_call wiring test."""

    def __init__(self):
        self.calls = []

    def acquire(self, weight: int = 1, priority: bool = False) -> None:
        self.calls.append((weight, priority))

    def update_from_response(self, headers, weight: int = 1) -> None:
        pass


def test_paced_call_threads_priority_without_forwarding_to_fn():
    spy = _SpyLimiter()
    seen = {}

    def fn(a, b, kw=None):
        seen["args"] = (a, b)
        seen["kw"] = kw
        return "ok"

    out = paced_call(spy, None, 7, fn, 1, 2, kw="x", priority=True)
    assert out == "ok"
    assert seen == {"args": (1, 2), "kw": "x"}  # 'priority' consumed by paced_call, not leaked to fn
    assert spy.calls == [(7, True)]

    paced_call(spy, None, 3, fn, 9, 8)  # default priority=False
    assert spy.calls[-1] == (3, False)


def test_rate_limiter_accepts_priority_arg():
    # Interval limiter ignores priority (like weight), but must accept it — no throw.
    RateLimiter(0.01).acquire(weight=5, priority=True)


def _cfg(**over):
    base = dict(
        name="x", ccxt_class="x", btc_symbol="B", market_type="perp",
        rate_limit_ms=100, enabled=True,
    )
    base.update(over)
    return ExchangeConfig(**base)


def test_build_rate_limiter_weight_path_wires_priority_reserve():
    # Activation guard: proves the config field reaches the real limiter (a `main` that
    # forgot to pass it would leave reserve=0 and every other test would still pass).
    lim = build_rate_limiter(
        _cfg(rate_limit_weight_budget=2400, rate_limit_safety=400, rate_limit_priority_reserve=100)
    )
    assert isinstance(lim, WeightRateLimiter)
    assert lim._priority_reserve == 100


def test_build_rate_limiter_interval_path_for_no_budget():
    lim = build_rate_limiter(_cfg(rate_limit_weight_budget=None, rate_limit_ms=200))
    assert isinstance(lim, RateLimiter)
