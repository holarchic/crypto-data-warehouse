"""Shared per-exchange rate limiter.

Threads call `acquire()` before each API request. The limiter hands out sequential
time slots spaced by `interval_s`; callers sleep just long enough to hit their slot,
then issue the request. Crucially, no lock is held during the network round-trip —
multiple threads can have requests in flight at once as long as each was issued at
its own allocated slot.

Contrast with `threading.Lock` + `sleep(rate_limit_s)` inside the critical section,
which serializes both the pacing AND the network wait. Under contention that pattern
makes effective throughput ~1 request per `(rate_limit_s + network_rtt)` instead of
1 per `rate_limit_s`, which is what the exchange actually allows.
"""

import threading
import time


class RateLimiter:
    def __init__(self, interval_s: float):
        self._interval_s = interval_s
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval_s
            wait = slot - now
        if wait > 0:
            time.sleep(wait)
