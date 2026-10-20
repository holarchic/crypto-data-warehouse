"""hl_candles_repair's SQL and DB functions against a real TimescaleDB, with real psycopg.

The unit tests run with psycopg and db stubbed (tests/conftest.py), so this runs outside
pytest, inside the collector image, against a throwaway database created from db/init.sql:

    docker network create cc-check
    docker run -d --name cc-check-db --network cc-check -e POSTGRES_USER=candles \
        -e POSTGRES_PASSWORD=x -e POSTGRES_DB=candle_collector timescale/timescaledb:2.26.2-pg16
    docker exec -i cc-check-db psql -U candles -d candle_collector < db/init.sql
    docker build -t cc-collector-check collector
    docker run --rm --network cc-check -v "$PWD/tests/integration:/checks:ro" -e PYTHONPATH=/app \
        -e CC_PG_TEST_DSN=postgresql://candles:x@cc-check-db:5432/candle_collector \
        cc-collector-check python /checks/check_hl_candles_repair.py
    docker rm -fv cc-check-db && docker network rm cc-check

Prints one line per check and exits non-zero on the first failure.
"""

import os
import sys

import hl_candles_repair as hl
import psycopg
import status

M = hl.MINUTE_MS
T0 = 1_792_400_000_000 // M * M
PAIR = status.Instrument(symbol="BTC/USDC:USDC", asset_id="BTC", native_symbol="0", market_type="perp")


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def bars(*rows):
    return [{"t": t, "open": o, "high": h, "low": low, "close": c, "volume": v, "n": n}
            for t, o, h, low, c, v, n in rows]


def stored(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT (extract(epoch FROM timestamp) * 1000)::bigint, open, high, low, close, volume, "
                    "trades, quote_volume FROM candles_1m WHERE symbol = %s ORDER BY timestamp", (PAIR.symbol,))
        return cur.fetchall()


def main():
    dsn = os.environ["CC_PG_TEST_DSN"]
    conn = psycopg.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        check("real connection", cur.fetchone() == (1,))

    ws = bars((T0, 1, 2, 0.5, 1.5, 10, 100), (T0 + M, 1, 2, 0.5, 1.5, 10, 100))
    hl.write_bars(conn, PAIR, ws)
    rest = bars((T0, 1, 2, 0.5, 1.5, 10, 100),          # unchanged, first: returns no result row
                (T0 + M, 1, 2.5, 0.5, 1.7, 11, 104),    # an unclosed WS bar, corrected
                (T0 + 2 * M, 3, 4, 2.5, 3.5, 7, 40))    # a dropped minute, filled
    counts = hl.write_bars(conn, PAIR, rest)
    check("unchanged, changed, new -> inserted 1, updated 1", counts == (1, 1), str(counts))
    expected = [(b["t"], b["open"], b["high"], b["low"], b["close"], b["volume"], b["n"], None) for b in rest]
    got = [tuple(float(x) if isinstance(x, (int, float)) and i in (1, 2, 3, 4, 5) else x
                 for i, x in enumerate(row)) for row in stored(conn)]
    check("stored values equal REST's", got == expected, str(got))
    check("rerun of the same bars -> 0/0", hl.write_bars(conn, PAIR, rest) == (0, 0))
    check("stored minutes", hl.stored_minutes(conn, PAIR, T0, T0 + 3 * M) == {T0, T0 + M, T0 + 2 * M})

    check("no cursor before bootstrap", hl.read_cursor(conn) is None)
    hl.bootstrap_cursor(conn, T0)
    hl.bootstrap_cursor(conn, T0 + 60 * M)
    check("bootstrap is idempotent and keeps its anchor", hl.read_cursor(conn) == T0, str(hl.read_cursor(conn)))
    hl.advance_cursor(conn, T0 + 120 * M)
    # read_cursor commits on the connection it reads, so a second session proves the advance landed.
    reader = psycopg.connect(dsn)
    check("advance is committed (seen from another session)", hl.read_cursor(reader) == T0 + 120 * M)
    reader.close()
    check("advance moves the cursor", hl.read_cursor(conn) == T0 + 120 * M)
    hl.advance_cursor(conn, T0 + 30 * M)
    check("advance never moves it back (GREATEST)", hl.read_cursor(conn) == T0 + 120 * M)

    status.upsert_discovery_pair(conn, hl.EXCHANGE, hl.MARKET_TYPE, PAIR.symbol, status.UNIVERSE_TIMEFRAME,
                                 PAIR.asset_id, PAIR.native_symbol, 1e6)
    conn.commit()
    check("active instruments: the universe row, never the repair sentinel", hl.active_pairs(conn) == [PAIR],
          str(hl.active_pairs(conn)))

    check("lock taken", hl.take_lock(conn))
    other = psycopg.connect(dsn)
    check("a second session cannot take it", not hl.take_lock(other))
    conn.close()
    check("the lock ends with its session", hl.take_lock(other))
    other.close()
    print("ALL OK")


if __name__ == "__main__":
    main()
