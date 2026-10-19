"""Fixed-rate pacing shared by every REST poll loop (candles, premium index, OI).

A loop polls on a slot grid, `k × interval + buffer`. Each cycle waits for the first slot after its
own *start*, not after now: a cycle that overruns that slot is followed at once by a late cycle
instead of skipping to the slot after now. Late cycles follow back to back, never concurrently,
until one ends before its next slot, which puts the loop back on the grid. Cursor-based polls
(candles, premium, the Binance OI baseline) are gap-aware, so a late cycle costs them freshness,
never data. Snapshot feeds (dense and bulk OI) take the sample late instead, where a skipped slot
lost it outright.
"""

import logging
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def next_slot(t: float, interval_s: int, buffer_s: int) -> float:
    """First slot `k × interval_s + buffer` strictly after `t`. The buffer is clamped to half the
    interval so the slot stays mid-period: at a sub-minute cadence (Bybit/OKX OI, 30 s) the shared
    30 s buffer equals the interval and would fold the slot back onto the boundary itself, polling at
    the instant the venue rolls its data."""
    buffer_s = min(buffer_s, interval_s // 2)
    return ((t - buffer_s) // interval_s + 1) * interval_s + buffer_s


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds")


def sleep_until_next_slot(interval_s: int, buffer_s: int, started: float | None, label: str) -> None:
    """Sleep until the first slot after `started`, the start of the cycle that just ended, or not at
    all if that slot has passed. `started` None (no cycle has run yet) waits for the slot after now.

    Logs one line per cycle end, the record the pacing check reads: the cycle's start (or, with no
    previous cycle, the time the target was taken from), its target slot and the wait (the line's
    own timestamp is the cycle end)."""
    now = time.time()
    if started is None:
        slot = next_slot(now, interval_s, buffer_s)
        origin = f"no previous cycle, now {_iso(now)}"
    else:
        slot = next_slot(started, interval_s, buffer_s)
        origin = f"Cycle started {_iso(started)}"
    wait_s = slot - now
    if wait_s <= 0:
        log.info("[%s] %s; next slot %s; slot passed %.0fs ago, starting now", label, origin, _iso(slot), -wait_s)
        return
    log.info("[%s] %s; next slot %s; sleeping %.0fs", label, origin, _iso(slot), wait_s)
    time.sleep(wait_s)
