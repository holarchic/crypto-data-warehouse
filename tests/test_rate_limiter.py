"""RateLimiter pacing tests."""

import threading
import time

from rate_limiter import RateLimiter


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
