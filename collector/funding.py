"""Funding rate collection: one thread per exchange, polls every hour.

Every row is a settlement as the venue's funding-history endpoint records it: the settlement time
truncated to the whole second, the settled rate, and `interval_hours`, the gap to the symbol's
previous settlement. Nothing is read from a venue's "current funding" endpoint: its rate is a
prediction (Bybit, OKX, HL) or belongs to a later settlement than its timestamp implies (Binance
4h/1h symbols), so storing it put estimates and future rates into the history (T-035).

Records are read from the venue's raw response, not ccxt's parsed history: ccxt drops a record
with no timestamp, and a fetch must fail closed on a malformed record rather than lose it.
"""

import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import NamedTuple

import ccxt
from backoff import RETRIABLE_EXCEPTIONS
from config import FUNDING_LOOP, ExchangeConfig
from rate_limiter import BINANCE_FUNDING_HISTORY_WEIGHT, RateLimiter, paced_call
from status import Instrument, active_instruments, upsert_loop_heartbeat

from db import connect_db, safe_rollback

log = logging.getLogger(__name__)

HOUR_MS = 3_600_000
SEED_LOOKBACK_MS = 100 * HOUR_MS  # a symbol with no recent stored settlement starts this far back
CURSOR_WINDOW_MS = 7 * 24 * HOUR_MS  # stored settlements older than this are not cursors

INSERT_FUNDING_SQL = """
    INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, predicted_rate, interval_hours)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s),
            %(rate)s, NULL, %(interval_hours)s)
    ON CONFLICT DO NOTHING
"""

STORED_CURSORS_SQL = """
    SELECT symbol, (extract(epoch FROM max(timestamp)) * 1000)::bigint
    FROM funding_rates
    WHERE exchange = %(exchange)s AND timestamp > now() - %(window_ms)s * interval '1 millisecond'
    GROUP BY symbol
"""


class FundingFetchError(Exception):
    """A symbol's history could not be read completely, so none of it may be written."""


class FullPage(FundingFetchError):
    """A request returned as many records as the venue's page holds, so some may be cut off."""


class MalformedRecord(FundingFetchError):
    """A record has no usable settlement time, or a missing or non-finite rate."""


@dataclass(frozen=True)
class Settlement:
    ts_ms: int  # settlement time truncated to the whole second
    rate: float


@dataclass(frozen=True)
class FundingRow:
    """One funding_rates row."""

    exchange: str
    asset_id: str
    symbol: str
    ts_ms: int
    rate: float
    interval_hours: int | None

    def params(self) -> dict:
        return {"exchange": self.exchange, "asset_id": self.asset_id, "symbol": self.symbol,
                "timestamp": self.ts_ms / 1000, "rate": self.rate, "interval_hours": self.interval_hours}


class _Raw(NamedTuple):
    """One parsed venue record, before truncation."""

    raw_ts: int
    rate: float
    symbol: str | None = None


def _iso_to_ms(value: str) -> int:
    """An ISO-8601 time with a timezone, in ms; a naive time or a non-string raises."""
    if not isinstance(value, str):
        raise TypeError(f"not an ISO time: {value!r}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"no timezone: {value!r}")
    return int(parsed.timestamp() * 1000)


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class Venue:
    """How one venue's funding-history endpoint is asked and read.

    `window_h` is sized so a window `[start, start + window_h − 1 ms]` holds at most `window_h + 1`
    settlements (the 1 h minimum interval plus millisecond jitter), under `page_cap`.
    """

    method: str  # ccxt's raw method for the endpoint
    page_cap: int
    window_h: int
    window_params: Callable[[int, int, int], dict]  # (start_ms, end_ms, page_cap); both bounds inclusive
    records: Callable[[object], list]
    time_field: str
    rate_field: str
    parse_time: Callable[[object], int] = int
    lower_bounded: bool = True  # False: the endpoint also returns rows older than the window


# Keyed by exchange name (exchanges.yaml), not ccxt class: Binance runs as `binanceusdm`.
VENUES = MappingProxyType({
    "binance": Venue(
        method="fapiPublicGetFundingRate", page_cap=1000, window_h=900,
        window_params=lambda start, end, cap: {"startTime": start, "endTime": end, "limit": cap},
        records=lambda resp: resp, time_field="fundingTime", rate_field="fundingRate",
    ),
    "bybit": Venue(
        method="publicGetV5MarketFundingHistory", page_cap=200, window_h=150,
        window_params=lambda start, end, cap: {"startTime": start, "endTime": end, "limit": cap},
        records=lambda resp: resp["result"]["list"], time_field="fundingRateTimestamp", rate_field="fundingRate",
    ),
    # OKX's `before`/`after` bounds are exclusive. `realizedRate` is the settled rate; the record's
    # `fundingRate` is the prediction and is never read.
    "okx": Venue(
        method="publicGetPublicFundingRateHistory", page_cap=100, window_h=80,
        window_params=lambda start, end, cap: {"before": start - 1, "after": end + 1, "limit": cap},
        records=lambda resp: resp["data"], time_field="fundingTime", rate_field="realizedRate",
    ),
    # HL has no page parameter: the window bounds the response and so its weight (20, plus 1 per 20
    # items: an 18 h window weighs 21). 500 items is HL's cap on a time-range response.
    "hyperliquid": Venue(
        method="publicPostInfo", page_cap=500, window_h=18,
        window_params=lambda start, end, cap: {"type": "fundingHistory", "startTime": start, "endTime": end},
        records=lambda resp: resp, time_field="time", rate_field="fundingRate",
    ),
    # dYdX takes only an upper bound and answers the newest `limit` rows at or before it, so a full
    # page is complete once it reaches back to the window's start.
    "dydx": Venue(
        method="indexerGetHistoricalFundingMarket", page_cap=100, window_h=80,
        window_params=lambda start, end, cap: {"effectiveBeforeOrAt": _ms_to_iso(end), "limit": cap},
        records=lambda resp: resp["historicalFunding"], time_field="effectiveAt", rate_field="rate",
        parse_time=_iso_to_ms, lower_bounded=False,
    ),
})


def market_params(exchange, venue_name: str, symbol: str) -> dict:
    """The request parameters naming `symbol`'s market on the venue. Raises ccxt's BadSymbol for a
    symbol the exchange's markets don't list."""
    market = exchange.market(symbol)
    if venue_name == "bybit":
        return {"category": "linear" if market["linear"] else "inverse", "symbol": market["id"]}
    if venue_name == "okx":
        return {"instId": market["id"]}
    if venue_name == "hyperliquid":
        return {"coin": market["baseName"]}
    if venue_name == "dydx":
        return {"market": market["id"]}
    return {"symbol": market["id"]}


def _parse(venue: Venue, record: dict) -> _Raw:
    """One record's raw settlement time and rate, or MalformedRecord."""
    try:
        raw_ts = venue.parse_time(record[venue.time_field])
        rate = float(record[venue.rate_field])
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
        raise MalformedRecord(f"{record!r}: {exc!r}") from exc
    if not math.isfinite(rate):
        raise MalformedRecord(f"{record!r}: rate {rate}")
    return _Raw(raw_ts, rate)


def _records(venue: Venue, response) -> list:
    """The response's record list, or MalformedRecord when the response isn't shaped as expected."""
    try:
        records = venue.records(response)
    except (KeyError, TypeError) as exc:
        raise MalformedRecord(f"unexpected response {response!r:.200}: {exc!r}") from exc
    if not isinstance(records, list):
        raise MalformedRecord(f"unexpected response {response!r:.200}")
    return records


def truncate_to_second(ms: int) -> int:
    """A settlement's canonical time: venues stamp the same settlement up to ~0.3 s apart."""
    return ms // 1000 * 1000


def fetch_symbol_history(exchange, venue_name: str, market: dict, since_ms: int, until_ms: int,
                         call: Callable, window_h: int | None = None) -> list[Settlement]:
    """Every settlement of one market with raw time in `[since_ms, until_ms]`, oldest first.

    Walks windows of `window_h` hours (default the venue's own); `call(fn, params)` makes each
    request, so the caller owns pacing. Raises FundingFetchError rather than return a partial list.
    """
    venue = VENUES[venue_name]
    window_ms = (window_h or venue.window_h) * HOUR_MS
    by_second: dict[int, Settlement] = {}
    start = since_ms
    while start <= until_ms:
        end = min(start + window_ms - 1, until_ms)
        response = call(getattr(exchange, venue.method), {**market, **venue.window_params(start, end, venue.page_cap)})
        page = [_parse(venue, r) for r in _records(venue, response)]
        reaches_start = not venue.lower_bounded and page and min(r.raw_ts for r in page) <= start
        if len(page) >= venue.page_cap and not reaches_start:
            raise FullPage(f"{venue_name} {market}: {len(page)} records in {start}..{end}")
        for r in page:
            if start <= r.raw_ts <= end:
                second = truncate_to_second(r.raw_ts)
                by_second.setdefault(second, Settlement(second, r.rate))
        start = end + 1
    return [by_second[ts] for ts in sorted(by_second)]


def _binance_symbol(record) -> str:
    symbol = record.get("symbol") if isinstance(record, dict) else None
    if not isinstance(symbol, str) or not symbol:
        raise MalformedRecord(f"{record!r}: no symbol")
    return symbol


def fetch_binance_all(exchange, since_ms: int, call: Callable) -> dict[str, list[Settlement]]:
    """Every Binance USDⓈ-M settlement from `since_ms` to now, keyed by Binance's symbol, oldest first.

    Pages on the raw millisecond time: the next page starts at the last row's raw time, inclusive,
    so a page cut inside one settlement re-fetches rows rather than skipping any. A full page that
    can't move past its start (a whole page in one millisecond) raises FullPage: stepping past that
    millisecond would skip its unseen rows. Dedup is per (symbol, second), so symbols settling in the
    same second all stay.
    """
    venue = VENUES["binance"]
    seen: dict[str, dict[int, Settlement]] = {}
    start = since_ms
    while True:
        response = call(getattr(exchange, venue.method), {"startTime": start, "limit": venue.page_cap})
        page = [_parse(venue, record)._replace(symbol=_binance_symbol(record)) for record in _records(venue, response)]
        for r in page:
            second = truncate_to_second(r.raw_ts)
            seen.setdefault(r.symbol, {}).setdefault(second, Settlement(second, r.rate))
        if len(page) < venue.page_cap:
            break
        last = max(r.raw_ts for r in page)
        if last <= start:
            raise FullPage(f"binance all-symbols: {len(page)} records at {start} without progress")
        start = last
    return {symbol: [by_second[ts] for ts in sorted(by_second)] for symbol, by_second in seen.items()}


def funding_rows(instrument: Instrument, exchange_name: str, prev_ts_ms: int | None,
                 settlements: list[Settlement]) -> list[FundingRow]:
    """Rows for one symbol's settlements (oldest first). `interval_hours` is the gap to the previous
    settlement, the first one's to `prev_ts_ms`; None when no predecessor is known."""
    rows = []
    prev = prev_ts_ms
    for s in settlements:
        interval = None if prev is None else round((s.ts_ms - prev) / HOUR_MS)
        rows.append(FundingRow(exchange_name, instrument.asset_id, instrument.symbol, s.ts_ms, s.rate, interval))
        prev = s.ts_ms
    return rows


def stored_cursors(conn, exchange_name: str) -> dict[str, int]:
    """Each symbol's newest stored settlement (ms) within the last CURSOR_WINDOW_MS."""
    with conn.cursor() as cur:
        cur.execute(STORED_CURSORS_SQL, {"exchange": exchange_name, "window_ms": CURSOR_WINDOW_MS})
        return {symbol: ts for symbol, ts in cur.fetchall()}


def _insert_funding(conn, rows: list[FundingRow]) -> int:
    """Insert funding rows and commit. Returns the count inserted."""
    if not rows:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT_FUNDING_SQL, row.params())
            inserted += cur.rowcount
    conn.commit()
    return inserted


def _poll_binance(exchange, config, conn, instruments, cursors, now_ms, call) -> int:
    """One all-symbols walk from the oldest active cursor, then each active symbol's new rows."""
    since = min(cursors[i.symbol] + 1000 if i.symbol in cursors else now_ms - SEED_LOOKBACK_MS for i in instruments)
    try:
        by_symbol = fetch_binance_all(exchange, max(since, now_ms - CURSOR_WINDOW_MS), call)
    except Exception as exc:
        log.warning("[%s/funding] History fetch error: %s", config.name, exc)
        return 0

    rows = []
    for instrument in instruments:
        try:
            venue_symbol = market_params(exchange, "binance", instrument.symbol)["symbol"]
        except ccxt.BadSymbol as exc:
            log.warning("[%s/funding] No market for %s: %s", config.name, instrument.symbol, exc)
            continue
        cursor = cursors.get(instrument.symbol)
        fresh = [s for s in by_symbol.get(venue_symbol, []) if cursor is None or s.ts_ms > cursor]
        rows += funding_rows(instrument, config.name, cursor, fresh)

    # A db restart here would otherwise raise out of funding_loop and kill the thread (T-014).
    try:
        return _insert_funding(conn, rows)
    except Exception as exc:
        log.warning("[%s/funding] Insert error: %s", config.name, exc)
        safe_rollback(conn)
        return 0


def _poll_per_symbol(exchange, config, conn, instruments, cursors, now_ms, call) -> int:
    """Each active symbol from its newest stored settlement. A failed symbol writes nothing and
    keeps its cursor, so the next hour retries the same range."""
    inserted = 0
    for instrument in instruments:
        cursor = cursors.get(instrument.symbol)
        since = cursor + 1000 if cursor is not None else now_ms - SEED_LOOKBACK_MS
        try:
            market = market_params(exchange, config.name, instrument.symbol)
            settlements = fetch_symbol_history(exchange, config.name, market, since, now_ms, call)
            inserted += _insert_funding(conn, funding_rows(instrument, config.name, cursor, settlements))
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s/funding] Retriable error on %s: %s", config.name, instrument.symbol, exc)
            if conn.closed:
                return inserted
            safe_rollback(conn)
        except Exception as exc:
            log.warning("[%s/funding] Error on %s: %s", config.name, instrument.symbol, exc)
            if not conn.closed:
                safe_rollback(conn)
    return inserted


def poll_settled(exchange, config: ExchangeConfig, conn, instruments: list[Instrument],
                 rate_limiter: RateLimiter) -> int:
    """Store every settlement each active symbol has had since its newest stored one. Returns the
    count inserted."""
    def call(fn, params):
        return paced_call(rate_limiter, exchange, BINANCE_FUNDING_HISTORY_WEIGHT, fn, params)

    named = [i for i in instruments if i.asset_id]  # asset_id is NOT NULL in funding_rates
    if len(named) < len(instruments):
        log.warning("[%s/funding] Skipping %d instrument(s) with no asset_id", config.name, len(instruments) - len(named))
    cursors = stored_cursors(conn, config.name)
    conn.commit()  # no read transaction stays open across the network walk
    now_ms = int(time.time() * 1000)
    poll = _poll_binance if config.name == "binance" else _poll_per_symbol
    return poll(exchange, config, conn, named, cursors, now_ms, call)


def _sleep_until_next_hour():
    """Sleep until the next hour boundary + 60s buffer."""
    now = time.time()
    next_hour = (now // 3600 + 1) * 3600 + 60
    sleep_s = next_hour - now
    log.info("[funding] Sleeping %.0fs until next hour boundary", sleep_s)
    time.sleep(sleep_s)


def _heartbeat(conn, config: ExchangeConfig, thread_name: str):
    """Write the per-loop liveness heartbeat, guarded so a write hiccup can't kill
    the loop (it reconnects next cycle via the `if conn.closed` guard).

    Marks loop *liveness*, not data-flow success: it fires once per completed cycle
    (incl. no-pairs and caught-poll-error cycles). Only the reconnect-fail and
    pairs-query-fail paths skip it — they `continue` before the convergence point.
    """
    try:
        upsert_loop_heartbeat(conn, config.name, config.market_type, FUNDING_LOOP)
        conn.commit()
    except Exception:
        log.exception("[%s] Heartbeat upsert failed", thread_name)
        if not conn.closed:
            safe_rollback(conn)


def funding_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Funding rate collection loop. Own DB connection. Runs as daemon thread."""
    thread_name = f"{config.name}/funding"
    conn = connect_db(dsn)

    while True:
        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                time.sleep(30)
                continue

        try:
            pairs = active_instruments(conn, config.name)
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            safe_rollback(conn)
            _sleep_until_next_hour()
            continue

        if pairs:
            try:
                inserted = poll_settled(exchange, config, conn, pairs, rate_limiter)
            except Exception:
                log.exception("[%s] Poll failed", thread_name)
                safe_rollback(conn)
                inserted = 0
            log.info("[%s] Poll: %d funding rates inserted (%d pairs)", thread_name, inserted, len(pairs))
        else:
            log.info("[%s] No active pairs, sleeping", thread_name)

        _heartbeat(conn, config, thread_name)
        _sleep_until_next_hour()
