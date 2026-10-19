"""HyperLiquid 1m daily repair (T-019) → `candles_1m`.

HL 1m arrives only over WebSocket (hl_candles.py), and two kinds of WS error become permanent once
HL's REST history (~5,000 bars, ~83 h at 1m) passes them: all-coin drop windows, and bars stored
before their minute closed (the WS row stops short of the minute's last trades: 1.9% of traded
minutes, 13% for BTC, measured 2026-10-19). This one-shot (`python -m hl_candles_repair`, run daily
by cron) replaces each active coin's recent 1m bars with REST `candleSnapshot`'s closed values.

**Budget.** HL counts REST and WS `post` requests against one per-IP weight budget (1200/min), and
the collector's own polls (themselves `candleSnapshot`) already use ~1,155-1,175 of it. So the job
trickles: every call it makes is charged `20 + ceil(minutes / 60)` and the next call waits
`weight × 60 / budget` seconds (an average 20 weight/min, one 46-weight call per 26 h slice every
138 s). A 429 backs off and halves the budget for the rest of the run.

**Cursor.** `repaired_through` is the `last_timestamp` of the `candle_repair` loop sentinel: every
minute before it has been repaired, or reported lost. A run covers the last 26 h, or back to the
cursor minus a 2 h overlap after a missed day, capped at 72 h so the oldest slice is fetched well
inside REST's history; slices are fetched oldest first. The cursor advances only when every coin and
slice is complete, so a failure self-heals on the next run. A cap that cuts into unrepaired history
is a loss: it is alerted, and only a delivered alert lets the cursor rebase past it.

**Completeness.** REST answers a bar for every listed minute (`n == 0` when untraded), so a slice is
checked on that raw answer: a hole, an early end or an unexplained empty answer is incomplete, and
so is a stored row REST has no traded bar for. A leading gap is the coin's listing: within REST's
history, truncation only cuts the old end, which the expiry guard keeps every slice clear of. Only
traded bars are written (the WS emits only traded minutes), through a change-guarded upsert that
counts inserts (drops) apart from updates (unclosed bars). A malformed bar writes nothing for its
slice, so a stored value is never replaced by a NULL.
"""

import argparse
import json
import logging
import math
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

import psycopg
from config import CANDLE_REPAIR
from monitor import send_webhook
from status import LOOP_SENTINEL_SYMBOL, query_active_pairs, upsert_status_success

from db import build_dsn, connect_db, safe_rollback

log = logging.getLogger(__name__)

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
EXCHANGE = "hyperliquid"
MARKET_TYPE = "perp"
PAIRS_TIMEFRAME = "5m"  # the pair set the WS feed subscribes (ws.py)

MINUTE_MS = 60_000
HOUR_MS = 3_600_000
END_MARGIN_MS = 2 * MINUTE_MS  # keeps clear of the forming bar and the minute the WS still settles
DEFAULT_LOOKBACK_MS = 26 * HOUR_MS  # a daily run with 2 h of overlap
OVERLAP_MS = 2 * HOUR_MS
CAP_MS = 72 * HOUR_MS  # leaves >= 11 h under REST's ~83 h for the run itself
SLICE_MS = 26 * HOUR_MS  # <= 1,560 bars, so a call weighs <= 46
EXPIRY_MS = 82 * HOUR_MS  # a slice starting earlier than this at fetch time may be truncated

BASE_WEIGHT = 20
WEIGHT_PER_MIN = 20.0
MIN_WEIGHT_PER_MIN = 5.0
BACKOFF_429_S = 120
MAX_ATTEMPTS = 4  # the first try plus 3 retries after a 429
LOCK_KEY = 0x484C314D  # "HL1M": one repair at a time, across cron and manual runs

UPSERT_REPAIR_SQL = """
    INSERT INTO candles_1m (exchange, market_type, asset_id, symbol, native_symbol, timeframe, timestamp,
                            open, high, low, close, volume, quote_volume, trades)
    VALUES (%(exchange)s, %(market_type)s, %(asset_id)s, %(symbol)s, %(native_symbol)s, '1m',
            to_timestamp(%(timestamp)s), %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, NULL, %(trades)s)
    ON CONFLICT (exchange, market_type, symbol, timeframe, timestamp) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
        volume = EXCLUDED.volume, trades = EXCLUDED.trades
    WHERE (candles_1m.open, candles_1m.high, candles_1m.low, candles_1m.close, candles_1m.volume,
           candles_1m.trades)
          IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume,
                            EXCLUDED.trades)
    RETURNING (xmax = 0) AS inserted
"""

# The first run seeds the cursor before it fetches anything, so a failed first run is still caught up
# and the sentinel exists for the monitor. last_success here means "tracking since"; a success
# afterwards moves it through status.upsert_status_success.
BOOTSTRAP_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, last_timestamp,
                                   last_success, error_count)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, to_timestamp(%(cursor_s)s), NOW(), 0)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO NOTHING
"""

CURSOR_SQL = """
    SELECT (extract(epoch FROM last_timestamp) * 1000)::bigint FROM collection_status
    WHERE exchange = %(exchange)s AND market_type = %(market_type)s AND symbol = %(symbol)s
      AND timeframe = %(timeframe)s
"""

STORED_MINUTES_SQL = """
    SELECT (extract(epoch FROM timestamp) * 1000)::bigint FROM candles_1m
    WHERE exchange = %(exchange)s AND market_type = %(market_type)s AND symbol = %(symbol)s
      AND timeframe = '1m' AND timestamp >= to_timestamp(%(start_s)s) AND timestamp < to_timestamp(%(end_s)s)
"""

LOCK_SQL = "SELECT pg_try_advisory_lock(%(key)s)"

SENTINEL = {"exchange": EXCHANGE, "market_type": MARKET_TYPE, "symbol": LOOP_SENTINEL_SYMBOL,
            "timeframe": CANDLE_REPAIR}


class RateLimited(Exception):
    """HL answered 429: the shared per-IP weight budget is spent."""


class FetchError(Exception):
    """Any other failed HL call: network, HTTP status, or an unreadable body."""


class LockLost(Exception):
    """A reconnect could not re-take the advisory lock: another repair holds it."""


# ---------------------------------------------------------------------------
# Window, slices and pacing (pure)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Window:
    req: int  # the earliest minute this run must cover to advance the cursor
    end: int  # exclusive, fixed for the run
    lost: tuple[int, int] | None  # [cursor, end - 72 h) when the cap cuts into unrepaired history


def window(now_ms: int, cursor_ms: int | None) -> Window:
    end = now_ms // MINUTE_MS * MINUTE_MS - END_MARGIN_MS
    req = end - DEFAULT_LOOKBACK_MS
    if cursor_ms is not None:
        req = min(req, cursor_ms - OVERLAP_MS)
    floor = end - CAP_MS
    lost = (cursor_ms, floor) if cursor_ms is not None and cursor_ms < floor else None
    return Window(req=max(req, floor), end=end, lost=lost)


def slices(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    """`[start, end)` in <= 26 h pieces, oldest first."""
    out = []
    s = start_ms
    while s < end_ms:
        out.append((s, min(s + SLICE_MS, end_ms)))
        s += SLICE_MS
    return out


def snapshot_weight(start_ms: int, end_ms: int) -> int:
    return BASE_WEIGHT + math.ceil((end_ms - start_ms) / MINUTE_MS / 60)


def next_call_at(last_call_at: float, weight: int, per_min: float) -> float:
    return last_call_at + weight * 60.0 / per_min


@dataclass
class Pacer:
    """Weighted-debt pacing of every HL call the job makes, on an injected clock."""

    clock: object
    sleep: object
    per_min: float = WEIGHT_PER_MIN
    ready_at: float | None = None
    http429: int = 0

    def charge(self, weight: int) -> None:
        now = self.clock()
        if self.ready_at is not None and now < self.ready_at:
            self.sleep(self.ready_at - now)
            now = self.ready_at
        self.ready_at = next_call_at(now, weight, self.per_min)

    def rate_limited(self) -> None:
        self.http429 += 1
        self.per_min = max(self.per_min / 2, MIN_WEIGHT_PER_MIN)
        self.sleep(BACKOFF_429_S)


# ---------------------------------------------------------------------------
# Parsing and coverage (pure)
# ---------------------------------------------------------------------------


@dataclass
class Parsed:
    raw_minutes: list[int] = field(default_factory=list)  # every in-window minute REST answered
    traded: list[dict] = field(default_factory=list)  # in-window bars with n > 0
    malformed: bool = False


def _finite(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _bar(b: dict, coin: str) -> dict | None:
    """A bar's numeric fields, or None when anything in it is malformed."""
    if not isinstance(b, dict) or b.get("s") != coin:
        return None
    t, n = b.get("t"), b.get("n")
    if type(t) is not int or t % MINUTE_MS or type(n) is not int or n < 0:
        return None
    ohlcv = [_finite(b.get(k)) for k in ("o", "h", "l", "c", "v")]
    if None in ohlcv:
        return None
    return {"t": t, "n": n, **dict(zip(("open", "high", "low", "close", "volume"), ohlcv))}


def parse_snapshot(bars, coin: str, start_ms: int, end_ms: int) -> Parsed:
    if not isinstance(bars, list):
        return Parsed(malformed=True)
    parsed = Parsed()
    for raw in bars:
        bar = _bar(raw, coin)
        if bar is None:
            return Parsed(malformed=True)
        if not start_ms <= bar["t"] < end_ms:
            continue  # REST pads the edges; outside the slice is not an anomaly
        parsed.raw_minutes.append(bar["t"])
        if bar["n"] > 0:
            parsed.traded.append(bar)
    parsed.raw_minutes = sorted(set(parsed.raw_minutes))
    return parsed


def coverage(raw_minutes: list[int], start_ms: int, end_ms: int) -> str:
    """`complete`, `leading` (listed mid-slice), `empty`, or `hole` (truncated or a missing minute)."""
    if not raw_minutes:
        return "empty"
    contiguous = (raw_minutes[-1] - raw_minutes[0]) // MINUTE_MS + 1 == len(raw_minutes)
    if not contiguous or raw_minutes[-1] != end_ms - MINUTE_MS:
        return "hole"
    return "leading" if raw_minutes[0] > start_ms else "complete"


def coin_complete(results: list[dict]) -> bool:
    """A coin's slices, oldest first, must read empty* then (leading|complete) then complete*, with no
    stored row REST lacks a traded bar for: empty older slices are its pre-listing history."""
    if any(r["stored_conflict"] for r in results):
        return False
    i = 0
    while i < len(results) and results[i]["status"] == "empty":
        i += 1
    if i == len(results) or results[i]["status"] not in ("leading", "complete"):
        return False
    return all(r["status"] == "complete" for r in results[i + 1:])


# ---------------------------------------------------------------------------
# HL calls
# ---------------------------------------------------------------------------


def _post(payload: dict):
    req = urllib.request.Request(HL_INFO_URL, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise RateLimited() from e
        raise FetchError(f"HTTP {e.code}") from e
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise FetchError(str(e)) from e


def fetch_snapshot(coin: str, start_ms: int, end_ms: int):
    return _post({"type": "candleSnapshot",
                  "req": {"coin": coin, "interval": "1m", "startTime": start_ms, "endTime": end_ms - 1}})


def fetch_meta_names() -> dict[str, str]:
    """cc asset_id → HL coin name. HL names are uppercase except the 1000× tokens' leading `k`
    (`kPEPE`), which ccxt uppercases into the asset_id: the oi.py bridge."""
    meta = _post({"type": "meta"})
    try:
        return {u["name"].upper(): u["name"] for u in meta["universe"]}
    except (KeyError, TypeError, AttributeError) as e:
        raise FetchError(f"unreadable meta: {e}") from e


def paced(pacer: Pacer, weight: int, call):
    """One HL call through the pacer, retrying after a 429. Every attempt is charged."""
    for _ in range(MAX_ATTEMPTS):
        pacer.charge(weight)
        try:
            return call()
        except RateLimited:
            pacer.rate_limited()
    raise FetchError(f"429 on {MAX_ATTEMPTS} attempts")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def take_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(LOCK_SQL, {"key": LOCK_KEY})
        return bool(cur.fetchone()[0])


def read_cursor(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute(CURSOR_SQL, SENTINEL)
        row = cur.fetchone()
    conn.commit()
    return None if row is None or row[0] is None else int(row[0])


def bootstrap_cursor(conn, cursor_ms: int) -> None:
    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL, {**SENTINEL, "cursor_s": cursor_ms / 1000})
    conn.commit()


def stored_minutes(conn, pair: dict, start_ms: int, end_ms: int) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(STORED_MINUTES_SQL, {"exchange": EXCHANGE, "market_type": pair["market_type"],
                                         "symbol": pair["symbol"], "start_s": start_ms / 1000,
                                         "end_s": end_ms / 1000})
        rows = cur.fetchall()
    conn.commit()
    return {int(r[0]) for r in rows}


def write_bars(conn, pair: dict, bars: list[dict]) -> tuple[int, int]:
    """Upsert the traded bars in one transaction; returns (inserted, updated). An unchanged row
    returns no result row, so every result set is read, not just the first."""
    if not bars:
        return 0, 0
    params = [{"exchange": EXCHANGE, "market_type": pair["market_type"], "asset_id": pair["asset_id"],
               "symbol": pair["symbol"], "native_symbol": pair.get("native_symbol") or "",
               "timestamp": b["t"] / 1000, "open": b["open"], "high": b["high"], "low": b["low"],
               "close": b["close"], "volume": b["volume"], "trades": b["n"]} for b in bars]
    inserted = updated = 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_REPAIR_SQL, params, returning=True)
        while True:
            row = cur.fetchone()
            if row is not None:
                inserted += bool(row[0])
                updated += not row[0]
            if not cur.nextset():
                break
    conn.commit()
    return inserted, updated


def advance_cursor(conn, end_ms: int) -> None:
    upsert_status_success(conn, EXCHANGE, MARKET_TYPE, LOOP_SENTINEL_SYMBOL, CANDLE_REPAIR, end_ms / 1000)


@dataclass
class Session:
    """The run's connection and its advisory lock. A session lock ends with its connection, so a
    reconnect re-takes it before anything else touches HL or the table, or the run stops."""

    connect: object
    conn: object = None

    def open(self) -> bool:
        self.conn = self.connect()
        return take_lock(self.conn)

    def call(self, fn, *args):
        try:
            return fn(self.conn, *args)
        except psycopg.OperationalError:
            if not self.conn.closed:
                safe_rollback(self.conn)
                raise
            log.warning("db connection lost; reconnecting once")
            if not self.open():
                raise LockLost("another repair took the lock while the connection was down") from None
            return fn(self.conn, *args)

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001 — a close failure must not mask the run's exit code
                pass


@dataclass
class Deps:
    connect: object
    fetch_snapshot: object
    fetch_meta: object
    send_alert: object  # text -> True when delivered
    sleep: object = time.sleep
    clock: object = time.monotonic
    wall_ms: object = lambda: int(time.time() * 1000)


class SliceExpired(Exception):
    """The slice now starts past REST's reach: its answer could be truncated."""


def _result(status: str, conflict: bool = False, inserted: int = 0, updated: int = 0) -> dict:
    return {"status": status, "stored_conflict": conflict, "inserted": inserted, "updated": updated}


def repair_slice(session: Session, pacer: Pacer, deps: Deps, pair: dict, coin: str | None,
                 start_ms: int, end_ms: int) -> dict:
    if coin is None:
        return _result("failed")

    def call():
        if start_ms < deps.wall_ms() - EXPIRY_MS:
            raise SliceExpired()
        return deps.fetch_snapshot(coin, start_ms, end_ms)

    try:
        bars = paced(pacer, snapshot_weight(start_ms, end_ms), call)
    except SliceExpired:
        return _result("expired")
    except FetchError as e:
        log.warning("%s %s: %s", coin, _span(start_ms, end_ms), e)
        return _result("failed")
    parsed = parse_snapshot(bars, coin, start_ms, end_ms)
    if parsed.malformed:
        return _result("malformed")
    stored = session.call(stored_minutes, pair, start_ms, end_ms)
    conflict = bool(stored - {b["t"] for b in parsed.traded})
    inserted, updated = session.call(write_bars, pair, parsed.traded)
    return _result(coverage(parsed.raw_minutes, start_ms, end_ms), conflict, inserted, updated)


def _span(start_ms: int, end_ms: int) -> str:
    fmt = "%Y-%m-%dT%H:%MZ"
    return f"{time.strftime(fmt, time.gmtime(start_ms / 1000))}..{time.strftime(fmt, time.gmtime(end_ms / 1000))}"


def repair(session: Session, pacer: Pacer, deps: Deps, pairs: list[dict], names: dict[str, str],
           start_ms: int, end_ms: int) -> dict[str, list[dict]]:
    """Every coin over every slice, oldest slice first; returns each coin's slice results."""
    results = {p["symbol"]: [] for p in pairs}
    for s, e in slices(start_ms, end_ms):
        for pair in pairs:
            coin = names.get(pair["asset_id"])
            r = repair_slice(session, pacer, deps, pair, coin, s, e)
            results[pair["symbol"]].append(r)
            if r["inserted"] or r["updated"] or r["status"] not in ("complete", "leading", "empty") \
                    or r["stored_conflict"]:
                log.info("%s %s: %s inserted=%d updated=%d stored_conflict=%s", coin or pair["asset_id"],
                         _span(s, e), r["status"], r["inserted"], r["updated"], r["stored_conflict"])
    return results


def report_loss(lost: tuple[int, int], deps: Deps) -> bool:
    text = (f"🚨 HL 1m repair: {_span(*lost)} UTC is past REST's reach and stays as stored "
            "(drops and unclosed bars there are permanent)")
    log.error("lost span %s", _span(*lost))
    delivered = bool(deps.send_alert(text))
    if not delivered:
        log.error("lost-span alert not delivered; the cursor stays so the next run alerts again")
    return delivered


def run(now_ms: int, hours: int | None, deps: Deps) -> int:
    """One repair. Returns the exit code: 0 for a clean run (or another repair holding the lock),
    1 for anything that withheld or qualified the cursor."""
    session = Session(deps.connect)
    try:
        if not session.open():
            log.info("another repair holds the lock; nothing to do")
            return 0
        cursor = session.call(read_cursor)
        win = window(now_ms, cursor)
        if cursor is None:
            session.call(bootstrap_cursor, win.req)
        start = win.end - hours * HOUR_MS if hours else win.req
        delivered = report_loss(win.lost, deps) if win.lost else True
        pairs = session.call(query_active_pairs, EXCHANGE, PAIRS_TIMEFRAME)
        if not pairs:
            log.error("no active %s %s pairs; nothing certified", EXCHANGE, PAIRS_TIMEFRAME)
            return 1
        pacer = Pacer(deps.clock, deps.sleep)
        try:
            names = paced(pacer, BASE_WEIGHT, deps.fetch_meta)
        except FetchError as e:
            log.error("meta failed: %s", e)
            return 1
        results = repair(session, pacer, deps, pairs, names, start, win.end)
        return _finish(session, win, start, delivered, results, pacer)
    finally:
        session.close()


def _finish(session: Session, win: Window, start: int, delivered: bool, results: dict, pacer: Pacer) -> int:
    incomplete = sorted(sym for sym, rs in results.items() if not coin_complete(rs))
    covers = start <= win.req
    advanced = not incomplete and covers and delivered
    if advanced:
        session.call(advance_cursor, win.end)
    flat = [r for rs in results.values() for r in rs]
    log.info("DONE window=%s coins=%d inserted=%d updated=%d incomplete=%d http429=%d lost=%s advanced=%s%s",
             _span(start, win.end), len(results), sum(r["inserted"] for r in flat),
             sum(r["updated"] for r in flat), len(incomplete), pacer.http429,
             _span(*win.lost) if win.lost else "none", advanced,
             "" if covers else " (manual window short of the required range)")
    if incomplete:
        log.warning("incomplete: %s", " ".join(incomplete[:40]))
    return 1 if incomplete or pacer.http429 or win.lost or not delivered else 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Replace recent HL 1m bars with REST's closed values")
    ap.add_argument("--hours", type=int, choices=range(1, 73), metavar="1-72",
                    help="repair the last N hours instead of the cursor-driven window")
    args = ap.parse_args()
    webhook = os.environ.get("WEBHOOK_URL", "")
    deps = Deps(connect=lambda: connect_db(build_dsn()), fetch_snapshot=fetch_snapshot,
                fetch_meta=fetch_meta_names, send_alert=lambda text: send_webhook(webhook, text))
    try:
        return run(int(time.time() * 1000), args.hours, deps)
    except (LockLost, RuntimeError) as e:  # RuntimeError: connect_db exhausted its retries
        log.error("repair aborted: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
