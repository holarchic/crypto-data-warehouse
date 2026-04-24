"""RateLimiter pacing tests."""

import threading
import time

from rate_limiter import RateLimiter, WeightRateLimiter, binance_kline_weight


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
