"""One-shot rebuild of stored funding from each venue's settled history (T-035).

`python -m funding_repair --exchange <name> --until <ISO second> [--apply] [--after <symbol>]`

The collector once stored each venue's "current funding" rate instead of its settled history, so
stored rows hold predictions, later settlements' rates and millisecond duplicates. This reads every
stored symbol's history from the venue with the collector's own functions (`funding.py`) and
compares, row by row, everything up to `--until`.

**Bounds.** `--until` is inclusive by canonical second: the venue is read through `until + 999 ms`,
settlements are kept through second `until`, and `--apply` deletes `timestamp < until + 1 s`. Report,
apply, resume and verification runs pass the same value. Rows newer than `until` are only compared
(`tail_unmatched`), never written.

**Report** per symbol: `equal`; `differ` (rate, interval, or a non-canonical stored time);
`missing` (venue settlement not stored); `unmatched` (stored row on no venue settlement, or an extra
row in a second); `pre_depth` (stored rows older than the venue's history); `interval_unverified`
(the venue's oldest returned settlement has no fetched predecessor, so its stored interval can't be
checked); `tail_unmatched`. A symbol is `unfetchable` (no ccxt market, or no venue history: left
alone) or `failed` (any fetch error: left alone, and the run exits 1).

**Apply** rebuilds each fully fetched symbol that isn't all-equal in one transaction: delete its
rows below `until + 1 s`, upsert the venue's rows. The upsert wins any interleaving with the
collector's `DO NOTHING` insert. A database error aborts the run; resume with `--after`.

**HL budget.** One ≤41-weight call a minute, never while the HL candle repair holds its lock or in
06:10–06:35 UTC (before its 06:30 cron run), and not for 60 s after either clears or after start:
the candle repair releases its lock with its last call still inside HL's minute. The gate checks
the lock every second through every wait, its own spacing included, so a candle repair is missed
only if it starts and ends within one second; the runbook rules out manual HL candle repairs
while this runs.
"""

import argparse
import logging
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType

import ccxt
import psycopg
from config import load_config
from funding import (
    HOUR_MS,
    VENUES,
    FundingFetchError,
    FundingRow,
    fetch_symbol_history,
    funding_rows,
    market_params,
    truncate_to_second,
)
from hl_candles_repair import LOCK_KEY as HL_CANDLE_REPAIR_LOCK_KEY
from status import Instrument

from db import build_dsn, connect_db, create_exchange

log = logging.getLogger(__name__)

LOCK_KEY = 0x46554E44  # "FUND": one funding repair at a time
PREDECESSOR_MS = 24 * HOUR_MS  # read this far before the first stored row, for its interval
REPAIR_SPACING_S = MappingProxyType({"binance": 0.75, "bybit": 0.5, "okx": 0.5, "dydx": 0.5})  # HL: HlGate
REPAIR_WINDOW_H = MappingProxyType({"hyperliquid": 400})  # ≤401 items: 41 weight; the rest use the collector's
HL_QUIET_START, HL_QUIET_END = (6, 10), (6, 35)  # UTC (hour, minute), before the 06:30 HL repair
HL_COOLDOWN_S = 60  # between HL calls, and after the candle repair or the quiet window clears
HL_POLL_S = 1.0

LOCK_SQL = "SELECT pg_try_advisory_lock(%(key)s)"
LOCK_HELD_SQL = """
    SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted
                   AND classid = 0 AND objid::bigint = %(key)s AND objsubid = 1)
"""
SYMBOLS_SQL = """
    SELECT symbol, min(asset_id), (extract(epoch FROM min(timestamp)) * 1000)::bigint,
           (extract(epoch FROM max(timestamp)) * 1000)::bigint
    FROM funding_rates WHERE exchange = %(exchange)s GROUP BY symbol ORDER BY symbol
"""
STORED_SQL = """
    SELECT (extract(epoch FROM timestamp) * 1000)::bigint, rate, interval_hours
    FROM funding_rates
    WHERE exchange = %(exchange)s AND symbol = %(symbol)s
      AND timestamp >= to_timestamp(%(lo_s)s) AND timestamp < to_timestamp(%(hi_s)s)
"""
# psycopg prepares a statement on its 5th run, and Postgres may then plan it generically. A generic
# DELETE leaves TimescaleDB no symbol to pick compressed batches by, so it decompresses every batch in
# range (~2.2M rows) and trips its 100k-per-transaction limit.
CUSTOM_PLAN_SQL = "SET LOCAL plan_cache_mode = force_custom_plan"
DELETE_SQL = """
    DELETE FROM funding_rates
    WHERE exchange = %(exchange)s AND symbol = %(symbol)s AND timestamp < to_timestamp(%(hi_s)s)
"""
UPSERT_SQL = """
    INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, predicted_rate, interval_hours)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s), %(rate)s, NULL, %(interval_hours)s)
    ON CONFLICT (exchange, symbol, timestamp) DO UPDATE
    SET asset_id = EXCLUDED.asset_id, rate = EXCLUDED.rate, predicted_rate = NULL,
        interval_hours = EXCLUDED.interval_hours
"""


@dataclass(frozen=True)
class StoredRow:
    ts_ms: int  # as stored, possibly with milliseconds
    rate: float | None  # the column is nullable
    interval_hours: int | None


@dataclass(frozen=True)
class StoredSymbol:
    symbol: str
    asset_id: str
    first_ms: int
    last_ms: int


@dataclass
class Report:
    symbol: str
    status: str = "ok"  # ok | unfetchable | failed
    equal: int = 0
    differ: int = 0
    missing: int = 0
    unmatched: int = 0
    pre_depth: int = 0
    interval_unverified: int = 0
    tail_unmatched: int = 0
    applied: bool = False

    COUNTS = ("equal", "differ", "missing", "unmatched", "pre_depth", "interval_unverified", "tail_unmatched")

    def needs_apply(self) -> bool:
        return self.status == "ok" and bool(self.differ or self.missing or self.unmatched or self.pre_depth)

    def line(self) -> str:
        counts = " ".join(f"{name}={getattr(self, name)}" for name in self.COUNTS)
        return f"{self.symbol} {self.status} {counts}{' applied' if self.applied else ''}"


def compare(stored: list[StoredRow], venue_rows: list[FundingRow], report: Report) -> None:
    """Count `stored` (the range up to `until`) against the venue's rows for the same range."""
    venue = {r.ts_ms: r for r in venue_rows}
    venue_first = min(venue)
    by_second: dict[int, list[StoredRow]] = {}
    for row in stored:
        by_second.setdefault(truncate_to_second(row.ts_ms), []).append(row)
    for second, rows in by_second.items():
        if second not in venue:
            if second < venue_first:
                report.pre_depth += len(rows)
            else:
                report.unmatched += len(rows)
            continue
        report.unmatched += len(rows) - 1
        want = venue[second]
        row = next((r for r in rows if r.ts_ms == second), rows[0])
        if row.ts_ms != second or row.rate != want.rate:
            report.differ += 1
        elif row.interval_hours == want.interval_hours:
            report.equal += 1
        elif want.interval_hours is None and second == venue_first:
            report.interval_unverified += 1
        else:
            report.differ += 1
    report.missing += len(set(venue) - set(by_second))


def compare_tail(stored: list[StoredRow], venue_rows: list[FundingRow]) -> int:
    """Stored rows after `until` with no venue settlement at their second, or a different rate."""
    venue = {r.ts_ms: r.rate for r in venue_rows}
    return sum(1 for row in stored
               if truncate_to_second(row.ts_ms) not in venue or venue[truncate_to_second(row.ts_ms)] != row.rate)


class HlGate:
    """Spaces HL calls a minute apart and holds them out of the candle repair's minutes (see the
    module docstring)."""

    def __init__(self, conn, clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep):
        self.conn, self.clock, self.sleep = conn, clock, sleep
        self.next_call = clock() + HL_COOLDOWN_S

    def blocked(self) -> bool:
        """True while an HL call could share a minute with the candle repair's."""
        now = datetime.fromtimestamp(self.clock(), UTC)
        if HL_QUIET_START <= (now.hour, now.minute) < HL_QUIET_END:
            return True
        with self.conn.cursor() as cur:
            cur.execute(LOCK_HELD_SQL, {"key": HL_CANDLE_REPAIR_LOCK_KEY})
            held = bool(cur.fetchone()[0])
        self.conn.commit()
        return held

    def wait(self) -> None:
        """Return once HL_COOLDOWN_S have passed since the last call and since the gate last saw it
        blocked, checking every HL_POLL_S."""
        while True:
            if self.blocked():  # it may clear just after this check, so count the poll gap too
                self.next_call = self.clock() + HL_COOLDOWN_S + HL_POLL_S
            elif self.clock() >= self.next_call:
                return
            self.sleep(min(HL_POLL_S, max(self.next_call - self.clock(), 0) or HL_POLL_S))

    def sent(self) -> None:
        self.next_call = max(self.next_call, self.clock() + HL_COOLDOWN_S)


@dataclass
class Pacer:
    """Spaces the run's venue requests: REPAIR_SPACING_S after each, or on HL the gate."""

    venue: str
    gate: HlGate | None = None
    sleep: Callable[[float], None] = time.sleep
    requests: int = field(default=0)

    def call(self, fn, params):
        if self.gate is not None:
            self.gate.wait()
        try:
            return fn(params)
        finally:
            self.requests += 1
            if self.gate is not None:
                self.gate.sent()
            else:
                self.sleep(REPAIR_SPACING_S[self.venue])


def _rows(conn, sql: str, params: dict) -> list:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.commit()
    return rows


def stored_symbols(conn, exchange_name: str) -> list[StoredSymbol]:
    return [StoredSymbol(*row) for row in _rows(conn, SYMBOLS_SQL, {"exchange": exchange_name})]


def stored_rows(conn, exchange_name: str, symbol: str, lo_ms: int, hi_ms: int) -> list[StoredRow]:
    """Stored rows with time in `[lo_ms, hi_ms)`."""
    params = {"exchange": exchange_name, "symbol": symbol, "lo_s": lo_ms / 1000, "hi_s": hi_ms / 1000}
    return [StoredRow(*row) for row in _rows(conn, STORED_SQL, params)]


def replace_rows(conn, exchange_name: str, symbol: str, until_ms: int, rows: list[FundingRow]) -> None:
    """One transaction: delete the symbol's rows below `until + 1 s`, upsert the venue's."""
    try:
        with conn.cursor() as cur:
            cur.execute(CUSTOM_PLAN_SQL)
            cur.execute(DELETE_SQL, {"exchange": exchange_name, "symbol": symbol, "hi_s": (until_ms + 1000) / 1000})
            cur.executemany(UPSERT_SQL, [row.params() for row in rows])
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def repair_symbol(conn, exchange, exchange_name: str, stored: StoredSymbol, until_ms: int,
                  pacer: Pacer, apply: bool) -> Report:
    report = Report(stored.symbol)
    pair = Instrument(symbol=stored.symbol, asset_id=stored.asset_id, native_symbol=None, market_type="perp")
    window_h = REPAIR_WINDOW_H.get(exchange_name)
    try:
        market = market_params(exchange, exchange_name, stored.symbol)
    except ccxt.BadSymbol:
        report.status = "unfetchable"
        return report
    first = truncate_to_second(stored.first_ms)
    try:
        history = fetch_symbol_history(exchange, exchange_name, market, first - PREDECESSOR_MS, until_ms + 999,
                                       pacer.call, window_h)
        tail_end = truncate_to_second(stored.last_ms)
        tail = (fetch_symbol_history(exchange, exchange_name, market, until_ms + 1000, tail_end + 999,
                                     pacer.call, window_h) if tail_end > until_ms else [])
    except (FundingFetchError, ccxt.BaseError) as exc:
        log.warning("%s: fetch failed: %s", stored.symbol, exc)
        report.status = "failed"
        return report
    rows = [r for r in funding_rows(pair, exchange_name, None, history) if r.ts_ms >= first]
    if not rows:
        report.status = "unfetchable"
        return report
    compare(stored_rows(conn, exchange_name, stored.symbol, 0, until_ms + 1000), rows, report)
    tail_rows = funding_rows(pair, exchange_name, None, tail)
    report.tail_unmatched = compare_tail(
        stored_rows(conn, exchange_name, stored.symbol, until_ms + 1000, tail_end + 1000), tail_rows)
    if apply and report.needs_apply():
        replace_rows(conn, exchange_name, stored.symbol, until_ms, rows)
        report.applied = True
    return report


def take_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(LOCK_SQL, {"key": LOCK_KEY})
        return bool(cur.fetchone()[0])


def run(conn, exchange_factory: Callable[[], object], exchange_name: str, until_ms: int, apply: bool,
        after: str | None, sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time) -> int:
    if not take_lock(conn):
        log.error("another funding repair holds the lock")
        return 1
    conn.commit()
    gate = HlGate(conn, clock, sleep) if exchange_name == "hyperliquid" else None
    pacer = Pacer(exchange_name, gate, sleep)
    exchange = pacer.call(lambda _params: exchange_factory(), None)  # HL's markets load is a call too
    totals, statuses, applied = Counter(), Counter(), 0
    for stored in stored_symbols(conn, exchange_name):
        if after is not None and stored.symbol <= after:
            continue
        report = repair_symbol(conn, exchange, exchange_name, stored, until_ms, pacer, apply)
        log.info("%s", report.line())
        totals.update({name: getattr(report, name) for name in Report.COUNTS})
        statuses[report.status] += 1
        applied += report.applied
    until_iso = datetime.fromtimestamp(until_ms / 1000, UTC).isoformat()
    log.info("%s until=%s requests=%d symbols=%s applied=%d %s", exchange_name, until_iso, pacer.requests,
             dict(statuses), applied, " ".join(f"{name}={totals[name]}" for name in Report.COUNTS))
    return 1 if statuses["failed"] else 0


def parse_until(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None or dt.microsecond:
        raise argparse.ArgumentTypeError("--until needs a timezone and a whole second")
    return int(dt.timestamp() * 1000)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Rebuild stored funding from the venue's settled history")
    ap.add_argument("--exchange", required=True, choices=sorted(VENUES))
    ap.add_argument("--until", required=True, type=parse_until, help="inclusive whole second, e.g. 2026-10-27T13:00:00Z")
    ap.add_argument("--apply", action="store_true", help="replace each differing symbol's rows")
    ap.add_argument("--after", help="resume past this symbol (the symbols run in sorted order)")
    args = ap.parse_args()
    try:
        conn = connect_db(build_dsn())
        return run(conn, lambda: create_exchange(load_config(args.exchange)), args.exchange,
                   args.until, args.apply, args.after)
    except (psycopg.Error, RuntimeError) as exc:  # RuntimeError: connect_db exhausted its retries
        log.error("funding repair aborted: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
