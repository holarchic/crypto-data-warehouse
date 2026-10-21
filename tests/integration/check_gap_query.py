"""feed_health.gap_query's SQL against a real TimescaleDB: both candle tables, with and without the asset
filter, the query the monitor and /status/gaps both run.

The unit tests check the SQL text and its parameters; only a real database runs it. Runs outside pytest,
inside the api image (which carries feed_health.py and config.py), against a throwaway database at the
current schema, with the same setup as check_export_queries.py:

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
        cc-api-check python /checks/check_gap_query.py
    docker rm -fv cc-check-db && docker network rm cc-check

Prints one line per check and exits non-zero on the first failure.
"""

import os
import sys
from datetime import timedelta

import feed_health
import psycopg
from psycopg.rows import dict_row

LOOKBACK = 10
# Bars back from the current bucket. 5 and 4 are missing, so the run between bars 6 and 3 lacks 2.
# Bar 8 is the oldest inside the lookback with two intervals' margin for the clock moving on; bar 30
# is outside it, and would add a gap of its own if the query saw it.
BTC_BARS = (30, 8, 7, 6, 3, 2, 1)
ETH_BARS = (8, 7, 5, 4, 3, 2, 1)       # bar 6 missing: one gap of 1, BTC's filter must drop it


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def plant(conn, table, timeframe, interval_s):
    """Bars for BTC and ETH at the BTC_BARS/ETH_BARS offsets back from the current bucket, which is
    taken from the database's own clock because the query's window is relative to NOW()."""
    step = timedelta(seconds=interval_s)
    bucket = conn.execute("SELECT date_bin(%s, now(), TIMESTAMPTZ '2000-01-01') AS t", (step,)).fetchone()["t"]
    for asset, bars in (("BTC", BTC_BARS), ("ETH", ETH_BARS)):
        for back in bars:
            conn.execute(
                f"INSERT INTO {table} (exchange, market_type, asset_id, symbol, native_symbol, timeframe, timestamp, close) "
                "VALUES ('binance', 'perp', %s, %s, %s, %s, %s, 1.0)",
                (asset, f"{asset}/USDT:USDT", f"{asset}USDT", timeframe, bucket - back * step))
    conn.commit()
    return bucket, step


def gaps(conn, table, timeframe, interval_s, asset_id=None):
    sql, params = feed_health.gap_query(table, exchange="binance", timeframe=timeframe, interval_s=interval_s,
                                        lookback_count=LOOKBACK, asset_id=asset_id)
    return conn.execute(sql, params).fetchall()


def check_table(conn, table, timeframe, interval_s):
    bucket, step = plant(conn, table, timeframe, interval_s)

    every = gaps(conn, table, timeframe, interval_s)
    check(f"{table} finds one gap per asset", [(r["symbol"], float(r["missing_count"])) for r in every]
          == [("BTC/USDT:USDT", 2.0), ("ETH/USDT:USDT", 1.0)], str(every))
    check(f"{table} ignores the bar outside the lookback",
          all(r["gap_after"] > bucket - 30 * step for r in every), str(every))

    btc = gaps(conn, table, timeframe, interval_s, asset_id="BTC")
    check(f"{table} asset filter keeps only BTC's gap", len(btc) == 1, str(btc))
    check(f"{table} BTC gap spans bars 6 to 3",
          (btc[0]["gap_after"], btc[0]["gap_before"], float(btc[0]["missing_count"]))
          == (bucket - 6 * step, bucket - 3 * step, 2.0), str(btc))

    check(f"{table} unknown asset finds nothing", gaps(conn, table, timeframe, interval_s, asset_id="DOGE") == [])


def main():
    conn = psycopg.connect(os.environ["CC_PG_TEST_DSN"], row_factory=dict_row)
    check("real connection", conn.execute("SELECT 1 AS one").fetchone() == {"one": 1})
    check_table(conn, "candles", "5m", 300)
    check_table(conn, "candles_1m", "1m", 60)
    print("all checks passed")


if __name__ == "__main__":
    main()
