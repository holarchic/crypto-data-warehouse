"""Every /export/parquet feed's SQL against a real TimescaleDB, through real psycopg and the real serializer.

The unit tests check the SQL text; only a real database runs TimescaleDB's first()/last()/time_bucket and
returns the row shapes rows_to_parquet consumes. Runs outside pytest, inside the api image, against a
throwaway database at the current schema (db/init.sql, the ledger, then any newer migration):

    docker network create cc-check
    docker run -d --name cc-check-db --network cc-check -e POSTGRES_USER=candles \
        -e POSTGRES_PASSWORD=x -e POSTGRES_DB=candle_collector timescale/timescaledb:2.26.2-pg16
    docker exec -i cc-check-db psql -U candles -d candle_collector < db/init.sql
    docker exec -i cc-check-db psql -U candles -d candle_collector < db/ledger.sql
    bash -c 'migrations_psql() { docker exec -i cc-check-db psql -X -U candles -d candle_collector "$@"; }
             . scripts/migrations-lib.sh && migrations_apply'
    docker build -t cc-api-check --build-context project=. api
    docker run --rm --network cc-check -v "$PWD/tests/integration:/checks:ro" -e PYTHONPATH=/app \
        -e CC_PG_TEST_DSN=postgresql://candles:x@cc-check-db:5432/candle_collector \
        cc-api-check python /checks/check_export_queries.py
    docker rm -fv cc-check-db && docker network rm cc-check

Prints one line per check and exits non-zero on the first failure.
"""

import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import NamedTuple

import export
import psycopg
import pyarrow.parquet as pq
from psycopg.rows import dict_row

DAY = datetime(2026, 7, 6, tzinfo=timezone.utc)
FIVE = timedelta(minutes=5)


class Bar(NamedTuple):
    """One stored 5m premium bar."""

    ts: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float | None


def at(hour, minute=0):
    return DAY + timedelta(hours=hour, minutes=minute)


def ms(dt):
    return int(dt.timestamp()) * 1000


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def export_rows(conn, table, start, end, asset_id=None):
    """The window's rows exactly as the endpoint would ship them: query, serialize, read back."""
    sql, params = export.feed_query(table, start, end, asset_id)
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return []
    t = pq.read_table(BytesIO(export.rows_to_parquet(table, rows)))
    check(f"{table} schema is the seed schema", t.schema.equals(export.feed_schema(table)))
    return t.to_pylist()


def premium_bars(hour, base, skip=None):
    """Twelve 5m bars for one hour, bar `skip` left out; the hour's high is bar 7's, its low bar 3's."""
    bars = []
    for i in range(12):
        if i == skip:
            continue
        o = base + i * 1e-5
        bars.append(Bar(ts=at(hour) + i * FIVE, open=o, high=o + 3e-5 + (5e-4 if i == 7 else 0),
                        low=o - 2e-5 - (5e-4 if i == 3 else 0), close=o + 1e-5))
    return bars


def insert_premium(conn, exchange, symbol, native, bars):
    rows = list(bars)
    random.Random(7).shuffle(rows)  # first/last must order by time, not by insertion
    conn.cursor().executemany(
        "INSERT INTO premium_index (exchange, asset_id, symbol, native_symbol, timestamp, premium_open, "
        "premium_high, premium_low, premium_close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        [(exchange, symbol.split("/")[0], symbol, native, b.ts, b.open, b.high, b.low, b.close) for b in rows],
    )


def check_premium(conn):
    btc = premium_bars(10, 0.001) + premium_bars(11, 0.002, skip=5) + premium_bars(12, 0.003) + premium_bars(13, 0.004)
    insert_premium(conn, "binance", "BTC/USDT:USDT", "BTCUSDT", btc)
    insert_premium(conn, "binance", "ETH/USDT:USDT", "ETHUSDT", premium_bars(10, 0.007))
    insert_premium(conn, "binance", "BTC/USDC:USDC", "BTCUSDC", premium_bars(10, 0.009))
    insert_premium(conn, "bybit", "BTC/USDT:USDT", "BTCUSDT", premium_bars(10, 0.008))
    null_hour = premium_bars(20, 0.005)
    null_hour[6] = null_hour[6]._replace(high=None)
    insert_premium(conn, "binance", "ETH/USDT:USDT", "ETHUSDT", null_hour)
    conn.commit()

    both = export_rows(conn, "binance_premium_index", at(10), at(14))
    check("only complete binance USDT hours", sorted((r["coin"], r["open_time_ms"]) for r in both)
          == [("BTCUSDT", ms(at(h))) for h in (10, 12, 13)] + [("ETHUSDT", ms(at(10)))], str(both))
    whole = export_rows(conn, "binance_premium_index", at(10), at(14), asset_id="BTC")
    check("asset_id filter", [r["coin"] for r in whole] == ["BTCUSDT"] * 3, str(whole))
    h10, src = whole[0], premium_bars(10, 0.001)
    check("fixture: the hour's extremes are interior bars", max(src, key=lambda b: b.high) is src[7]
          and min(src, key=lambda b: b.low) is src[3])
    check("first/max/min/last by time", (h10["premium_open"], h10["premium_high"], h10["premium_low"], h10["premium_close"])
          == (src[0].open, max(b.high for b in src), min(b.low for b in src), src[-1].close), str(h10))
    check("close time is open + 3,599,999 ms", all(r["close_time_ms"] - r["open_time_ms"] == 3_599_999 for r in whole))

    a = export_rows(conn, "binance_premium_index", at(10), at(12, 30), asset_id="BTC")
    b = export_rows(conn, "binance_premium_index", at(12, 30), at(14), asset_id="BTC")
    check("a mid-hour seam emits each hour once", a + b == whole,
          f"{[r['open_time_ms'] for r in a]} + {[r['open_time_ms'] for r in b]}")
    check("hours open before start are out",
          export_rows(conn, "binance_premium_index", at(10, 1), at(12, 1), asset_id="BTC") == [whole[1]])
    try:
        export_rows(conn, "binance_premium_index", at(20), at(21))
        check("a NULL price inside an hour raises", False)
    except ValueError as exc:
        check("a NULL price inside an hour raises", "premium_high" in str(exc), str(exc))


def check_plain_feeds(conn):
    ctx = {c: 1.5 for c in ("funding", "open_interest", "prev_day_px", "day_ntl_vlm", "premium", "oracle_px",
                             "mark_px", "mid_px", "impact_bid_px", "impact_ask_px")}
    for ts, funding in ((at(10, 0) + timedelta(microseconds=1500), None), (at(11), 2.0)):
        row = {**ctx, "funding": funding}
        conn.execute("INSERT INTO hl_asset_ctxs (asset_id, symbol, timestamp, " + ", ".join(ctx) + ") VALUES "
                     "(%s, %s, %s, " + ", ".join(["%s"] * len(ctx)) + ")",
                     ("kPEPE", "KPEPE/USDC:USDC", ts, *[row[c] for c in ctx]))
    for exchange, symbol in (("binance", "BTC/USDT:USDT"), ("binance", "BTC/USDC:USDC"), ("bybit", "ETH/USDT:USDT")):
        conn.execute("INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest, oi_value_usd) "
                     "VALUES (%s, %s, %s, %s, %s, %s)", (exchange, symbol.split("/")[0], symbol, at(10), 10.0, 20.0))
    for venue in ("hyperliquid", "binance"):
        conn.execute("INSERT INTO liquidations (timestamp, venue, grade, asset_id, side, dir, px, sz, mark_px, "
                     "closed_pnl, method, liquidated_user, oid, tid, day) VALUES "
                     "(%s, %s, 'a', 'BTC', 'A', NULL, 1.0, 2.0, 3.0, NULL, NULL, '0xabc', 11, 22, '20260706')",
                     (at(10), venue))
    conn.commit()

    ctxs = export_rows(conn, "hl_asset_ctxs", at(10), at(11))
    check("hl_asset_ctxs half-open, ms floor, NULL -> NaN",
          len(ctxs) == 1 and ctxs[0]["coin"] == "kPEPE" and ctxs[0]["time_ms"] == ms(at(10)) + 1
          and math.isnan(ctxs[0]["funding"]), str(ctxs))
    oi = export_rows(conn, "binance_oi", at(10), at(11))
    check("binance_oi binance USDT only, native coin", [r["coin"] for r in oi] == ["BTCUSDT"], str(oi))
    liq = export_rows(conn, "hl_liquidations", at(10), at(11))
    check("hl_liquidations hyperliquid only, seed defaults",
          len(liq) == 1 and liq[0]["dir"] == "" and liq[0]["closed_pnl"] == 0.0 and liq[0]["oid"] == 11, str(liq))


def main():
    conn = psycopg.connect(os.environ["CC_PG_TEST_DSN"], row_factory=dict_row)
    check("real connection", conn.execute("SELECT 1 AS one").fetchone() == {"one": 1})
    check_premium(conn)
    check_plain_feeds(conn)
    print("all checks passed")


if __name__ == "__main__":
    main()
