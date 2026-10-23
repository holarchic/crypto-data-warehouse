"""funding_repair's and funding's SQL against a real TimescaleDB, with real psycopg.

The unit tests answer the SQL from an in-memory table (tests/test_funding_repair.py), so this runs
outside pytest, inside the collector image, against a throwaway database at the current schema
(db/init.sql, the ledger, then any newer migration), set up as in check_hl_candles_repair.py:

    docker run --rm --network cc-check -v "$PWD/tests/integration:/checks:ro" -e PYTHONPATH=/app \
        -e CC_PG_TEST_DSN=postgresql://candles:x@cc-check-db:5432/candle_collector \
        cc-collector-check python /checks/check_funding_repair.py

Prints one line per check and exits non-zero on the first failure.
"""

import os
import sys
import threading
import time

import funding
import funding_repair as fr
import hl_candles_repair
import psycopg

H = 3_600_000
T0 = 1_780_000_000_000 // H * H  # 2026-05-28, months before now: its chunk can be compressed
SYMBOL, OTHER = "TRB/USDT:USDT", "BTC/USDT:USDT"


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def venue_rows(n, start=T0, step_h=4, rate=lambda i: (i + 1) * 1e-5):
    return [{"exchange": "binance", "asset_id": "TRB", "symbol": SYMBOL, "timestamp": (start + i * step_h * H) / 1000,
             "rate": rate(i), "interval_hours": step_h if i else None} for i in range(n)]


def table(conn, symbol=SYMBOL, exchange="binance"):
    with conn.cursor() as cur:
        cur.execute("SELECT (extract(epoch FROM timestamp) * 1000)::bigint, rate, interval_hours, predicted_rate "
                    "FROM funding_rates WHERE exchange = %s AND symbol = %s ORDER BY timestamp", (exchange, symbol))
        rows = cur.fetchall()
    conn.commit()
    return rows


def expected(rows):
    return [(round(r["timestamp"] * 1000), r["rate"], r["interval_hours"], None) for r in rows]


def seed_legacy(conn):
    """What the old poll left: each settlement's rate 4 h early, interval 8, a millisecond-stamped
    seed copy, a predicted rate, all in one old chunk that is then compressed."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM funding_rates")
        for i in range(12):
            cur.execute("INSERT INTO funding_rates VALUES ('binance', 'TRB', %s, to_timestamp(%s), %s, 0.0001, 8)",
                        (SYMBOL, (T0 + (i - 1) * 4 * H) / 1000, (i + 1) * 1e-5))
        cur.execute("INSERT INTO funding_rates VALUES ('binance', 'TRB', %s, to_timestamp(%s), 1e-5, NULL, 8)",
                    (SYMBOL, (T0 + 2) / 1000))
        cur.execute("INSERT INTO funding_rates VALUES ('binance', 'BTC', %s, to_timestamp(%s), 7e-5, NULL, 8)",
                    (OTHER, T0 / 1000))
        cur.execute("INSERT INTO funding_rates VALUES ('okx', 'TRB', 'TRB/USDT:USDT', to_timestamp(%s), 7e-5, NULL, 8)",
                    (T0 / 1000,))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT count(compress_chunk(c, if_not_compressed => true)) "
                    "FROM show_chunks('funding_rates', older_than => now() - interval '30 days') c")
        compressed = cur.fetchone()[0]
    conn.commit()
    return compressed


def check_replace(conn):
    compressed = seed_legacy(conn)
    check("legacy rows sit in a compressed chunk", compressed >= 1, f"{compressed} chunk(s)")
    until = T0 + 40 * H
    later = {"exchange": "binance", "asset_id": "TRB", "symbol": SYMBOL, "timestamp": (until + 4 * H) / 1000,
             "rate": 9e-5, "interval_hours": 4}
    with conn.cursor() as cur:
        cur.execute(funding.INSERT_FUNDING_SQL, later)
    conn.commit()

    rows = venue_rows(11)
    stored = fr.stored_rows(conn, "binance", SYMBOL, 0, until + 1000)
    report = fr.Report(SYMBOL)
    fr.compare(stored, rows, report)
    check("report sees the legacy defects", report.differ + report.unmatched + report.pre_depth > 0, report.line())

    fr.replace_rows(conn, "binance", SYMBOL, until, rows)
    check("replace leaves exactly the venue rows, and the row after until",
          table(conn) == expected(rows) + expected([later]))
    check("other symbols and venues untouched",
          table(conn, OTHER) == [(T0, 7e-5, 8, None)] and table(conn, SYMBOL, "okx") == [(T0, 7e-5, 8, None)])
    report = fr.Report(SYMBOL)
    fr.compare(fr.stored_rows(conn, "binance", SYMBOL, 0, until + 1000), rows, report)
    check("a second report is all-equal", report.equal == 11 and not report.needs_apply(), report.line())

    before = table(conn)
    bad = rows[:5] + [{**rows[5], "asset_id": None}]  # NOT NULL violation after the delete ran
    try:
        fr.replace_rows(conn, "binance", SYMBOL, until, bad)
        failed = False
    except psycopg.Error:
        failed = True
    check("a failed insert rolls the delete back", failed and table(conn) == before)


def check_race(dsn, conn):
    """The repair's upsert wins whichever way the collector's DO NOTHING insert interleaves."""
    until = T0 + 40 * H
    rows = venue_rows(11)
    key_row = rows[10]
    collector_row = {**key_row, "interval_hours": 8, "rate": 1e-4}

    # The collector commits first: the repair's delete removes it, its upsert writes its own values.
    with conn.cursor() as cur:
        cur.execute("DELETE FROM funding_rates WHERE symbol = %s", (SYMBOL,))
        cur.execute(funding.INSERT_FUNDING_SQL, collector_row)
    conn.commit()
    fr.replace_rows(conn, "binance", SYMBOL, until, rows)
    check("collector row committed first is replaced", table(conn)[-1] == expected([key_row])[0])

    # The collector inserts while the repair's transaction holds the key: it waits, then does nothing.
    with conn.cursor() as cur:
        cur.execute(fr.DELETE_SQL, {"exchange": "binance", "symbol": SYMBOL, "hi_s": (until + 1000) / 1000})
        cur.executemany(fr.UPSERT_SQL, rows)
    result = {}
    collector_conn = psycopg.connect(dsn)
    with collector_conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        collector_pid = cur.fetchone()[0]
    collector_conn.commit()

    def collector():
        with collector_conn.cursor() as cur:
            cur.execute(funding.INSERT_FUNDING_SQL, collector_row)
            result["rowcount"] = cur.rowcount
        collector_conn.commit()

    thread = threading.Thread(target=collector)
    thread.start()
    with psycopg.connect(dsn) as watcher:
        blocked = False
        for _ in range(50):
            blocked = watcher.execute("SELECT %s = ANY(pg_blocking_pids(%s))",
                                      (conn.info.backend_pid, collector_pid)).fetchone()[0]
            if blocked:
                break
            time.sleep(0.1)
    check("collector insert is blocked by the repair's transaction in PostgreSQL", blocked)
    conn.commit()
    thread.join(10)
    collector_conn.close()
    check("then inserts nothing, and the repair's values stand",
          result.get("rowcount") == 0 and table(conn)[-1] == expected([key_row])[0])


def check_locks(dsn, conn):
    check("repair lock taken once", fr.take_lock(conn))
    conn.commit()
    with psycopg.connect(dsn) as other:
        check("a second repair can't take it", not fr.take_lock(other))
    with psycopg.connect(dsn) as candle:
        with candle.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (hl_candles_repair.LOCK_KEY,))
        gate = fr.HlGate(conn, clock=lambda: 1_792_756_800.0)  # 2026-10-23 12:00 UTC, outside the window
        check("HL gate sees the candle repair's lock", gate._blocked())
    check("and sees it released", not gate._blocked())


def check_csv_rollback(conn):
    seed_legacy(conn)
    until = T0 + 40 * H
    with conn.cursor() as cur:
        cur.execute("INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest) "
                    "VALUES ('binance', 'TRB', %s, to_timestamp(%s), 1) ON CONFLICT DO NOTHING", (SYMBOL, T0 / 1000))
        cur.execute(funding.INSERT_FUNDING_SQL, {"exchange": "binance", "asset_id": "TRB", "symbol": SYMBOL,
                                                 "timestamp": (until + 8 * H) / 1000, "rate": 9e-5, "interval_hours": 4})
    conn.commit()
    export = ("COPY (SELECT * FROM funding_rates WHERE exchange = 'binance' AND timestamp < to_timestamp(%s) "
              "ORDER BY symbol, timestamp) TO STDOUT WITH CSV" % ((until + 1000) / 1000))

    def dump():
        with conn.cursor() as cur, cur.copy(export) as copy:
            data = b"".join(bytes(chunk) for chunk in copy)
        conn.commit()
        return data

    saved = dump()
    fr.replace_rows(conn, "binance", SYMBOL, until, venue_rows(11))
    check("the repair changed the saved range", dump() != saved)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM funding_rates WHERE exchange = 'binance' AND timestamp < to_timestamp(%s)",
                    ((until + 1000) / 1000,))
        with cur.copy("COPY funding_rates FROM STDIN WITH CSV") as copy:
            copy.write(saved)
    conn.commit()
    check("the CSV restores the saved range byte for byte", dump() == saved)
    newer = [r for r in table(conn) if r[0] > until]
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM open_interest WHERE symbol = %s", (SYMBOL,))
        oi = cur.fetchone()[0]
    conn.commit()
    check("newer funding, other venues and other tables survive",
          newer == [(until + 8 * H, 9e-5, 4, None)] and table(conn, SYMBOL, "okx") == [(T0, 7e-5, 8, None)] and oi == 1)


def check_cursors(conn):
    now = int(time.time() * 1000) // H * H
    with conn.cursor() as cur:
        cur.execute("DELETE FROM funding_rates WHERE symbol = %s", (OTHER,))
        for ts in (now - 30 * 24 * H, now - 8 * H, now - 2 * H + 5):
            cur.execute(funding.INSERT_FUNDING_SQL, {"exchange": "binance", "asset_id": "BTC", "symbol": OTHER,
                                                     "timestamp": ts / 1000, "rate": 1e-5, "interval_hours": 8})
    conn.commit()
    cursors = funding.stored_cursors(conn, "binance")
    conn.commit()
    check("stored_cursors gives the newest row within 7 days, in ms", cursors.get(OTHER) == now - 2 * H + 5, repr(cursors))


def main():
    dsn = os.environ["CC_PG_TEST_DSN"]
    conn = psycopg.connect(dsn)
    check_replace(conn)
    check_race(dsn, conn)
    check_locks(dsn, conn)
    check_csv_rollback(conn)
    check_cursors(conn)
    print("all funding repair checks passed")


if __name__ == "__main__":
    main()
