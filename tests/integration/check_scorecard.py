"""scripts/stability-scorecard.sql against a real TimescaleDB, with data planted around one fixed
reference time: a complete baseline that passes every check, then one fault at a time, each of
which must flip only its own check.

Runs on the host (python3 and docker), from the project root. It starts a throwaway TimescaleDB
at the current schema (db/init.sql, the ledger, then any newer migration) and removes it:

    python3 tests/integration/check_scorecard.py

Prints one line per check and exits non-zero on the first failure.
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[2]
CONTAINER = "cc-scorecard-check"
IMAGE = "timescale/timescaledb:2.26.2-pg16"
PSQL = ["docker", "exec", "-i", CONTAINER, "psql", "-X", "-q", "-U", "candles", "-d", "candle_collector",
        "-v", "ON_ERROR_STOP=1"]

AS_OF = "2026-10-20 12:00:00+00"
DAYS = 2
META = {
    "intervals": {"5m": 300, "1m": 60},
    "volume_threshold_usd": 1_000_000,
    "funding_exchanges": ["binance", "hyperliquid"],
    "oi_exchanges": ["binance", "hyperliquid"],
}

# The window is 10-18 12:00 → 10-20 12:00. ETH is discovered on a 5m slot inside it.
#   candles: 5m bars due through 10:55 (end 11:00); 1m through 10:59 (end 11:00)
#     binance BTC 5m 564, binance ETH 5m 347 (06:05 → 11:00), hyperliquid BTC 5m 564, binance BTC 1m 2820
#   funding: a settlement is due 2 h after it happens, so 8 h and 1 h slots are due through 10:00
#     binance BTC 6 (10-18 16:00 … 10-20 08:00), ETH 4 (10-19 08:00 …), hyperliquid BTC 47 (10-18 12:00 …)
#   OI: 300 s buckets binance BTC 564, ETH 347; 60 s buckets hyperliquid BTC 2820
CANDLES = 564 + 347 + 564 + 2820
FUNDING = 6 + 4 + 47
OI = 564 + 347 + 2820

PLANT = """
TRUNCATE candles, candles_1m, funding_rates, open_interest, collection_status, discovery_volumes;
INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, asset_id, discovered_at) VALUES
    ('binance', 'perp', 'BTC/USDT:USDT', '5m', TRUE, 'BTC', NULL),
    ('binance', 'perp', 'BTC/USDT:USDT', '1m', TRUE, 'BTC', NULL),
    ('binance', 'perp', 'ETH/USDT:USDT', '5m', TRUE, 'ETH', '2026-10-19 06:05:00+00'),
    ('hyperliquid', 'perp', 'BTC/USDC:USDC', '5m', TRUE, 'BTC', NULL),
    ('binance', 'perp', '*', 'funding', TRUE, NULL, NULL),
    ('binance', 'perp', 'XRP/USDT:USDT', '5m', FALSE, 'XRP', NULL);
-- Every bar from before the window to the reference time, the unclosed tail included.
INSERT INTO candles (exchange, market_type, asset_id, symbol, native_symbol, timeframe, timestamp, close)
SELECT p.exchange, 'perp', p.asset, p.symbol, p.asset, '5m', ts, 1.0
FROM (VALUES ('binance', 'BTC', 'BTC/USDT:USDT'), ('binance', 'ETH', 'ETH/USDT:USDT'),
             ('hyperliquid', 'BTC', 'BTC/USDC:USDC')) p(exchange, asset, symbol),
     generate_series(timestamptz '2026-10-18 10:00+00', timestamptz '2026-10-20 12:00+00', interval '5 min') ts;
INSERT INTO candles_1m (exchange, market_type, asset_id, symbol, native_symbol, timeframe, timestamp, close)
SELECT 'binance', 'perp', 'BTC', 'BTC/USDT:USDT', 'BTCUSDT', '1m', ts, 1.0
FROM generate_series(timestamptz '2026-10-18 10:00+00', timestamptz '2026-10-20 12:00+00', interval '1 min') ts;
-- Binance at 8 h plus one off-schedule row that bins into its 8 h slot; HyperLiquid hourly.
INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, interval_hours)
SELECT p.exchange, p.asset, p.symbol, ts, 0.0001, p.hours
FROM (VALUES ('binance', 'BTC', 'BTC/USDT:USDT', 8), ('binance', 'ETH', 'ETH/USDT:USDT', 8),
             ('hyperliquid', 'BTC', 'BTC/USDC:USDC', 1)) p(exchange, asset, symbol, hours),
     generate_series(timestamptz '2026-10-18 08:00+00', timestamptz '2026-10-20 12:00+00',
                     make_interval(hours => p.hours)) ts;
INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, interval_hours)
VALUES ('binance', 'BTC', 'BTC/USDT:USDT', '2026-10-19 04:00+00', 0.0001, 8);
-- Binance every 5 minutes plus a second row inside one bucket; HyperLiquid every minute.
INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest)
SELECT p.exchange, p.asset, p.symbol, ts, 1.0
FROM (VALUES ('binance', 'BTC', 'BTC/USDT:USDT', 300), ('binance', 'ETH', 'ETH/USDT:USDT', 300),
             ('hyperliquid', 'BTC', 'BTC/USDC:USDC', 60)) p(exchange, asset, symbol, step),
     generate_series(timestamptz '2026-10-18 10:00+00', timestamptz '2026-10-20 12:00+00',
                     make_interval(secs => p.step)) ts;
INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest)
VALUES ('binance', 'BTC', 'BTC/USDT:USDT', '2026-10-19 00:00:30+00', 1.0);
INSERT INTO discovery_volumes (run_date, asset_id, max_volume_usd) VALUES
    ('2026-10-20', 'BTC', 5e9), ('2026-10-20', 'ETH', 1e9), ('2026-10-20', 'DOGE', 5e5);
"""

CHECKS = ("candle capture", "funding-slot occupancy (stored interval)",
          "OI bucket occupancy (60 s HL, 300 s elsewhere)", "primary keys", "discovery (union of venues)")
CANDLE, FUNDING_CHECK, OI_CHECK, PK, DISCOVERY = CHECKS


class Verdict(NamedTuple):
    measured: str
    verdict: str


class Venue(NamedTuple):
    section: str
    exchange: str
    timeframe: str


class Capture(NamedTuple):
    expected: int
    captured: int


class Report(NamedTuple):
    verdicts: dict      # check name -> Verdict
    venues: dict        # Venue -> Capture
    markers: list       # the SCORECARD-DB lines


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def psql(sql, **variables):
    """Runs SQL through psql; `variables` become psql variables, which the SQL reads as :'name'."""
    args = [arg for name, value in variables.items() for arg in ("-v", f"{name}={value}")]
    return subprocess.run([*PSQL, *args], input=sql, capture_output=True, text=True, check=True).stdout


def scorecard(as_of=AS_OF, days=DAYS, meta=META):
    """Runs the scorecard SQL unaligned (-A) and parses its checks, venue rows and markers by column."""
    out = subprocess.run([*PSQL, "-A", "-v", f"days={days}", "-v", f"meta={json.dumps(meta)}", "-v", f"as_of={as_of}"],
                         input=(ROOT / "scripts/stability-scorecard.sql").read_text(),
                         capture_output=True, text=True, check=True).stdout
    verdicts, venues = {}, {}
    for line in out.splitlines():
        cells = line.split("|")
        if len(cells) == 4 and cells[0] in CHECKS:
            name, measured, _target, verdict = cells
            check(f"{name} reported once", name not in verdicts)
            verdicts[name] = Verdict(measured, verdict)
        elif len(cells) == 8 and cells[0] in ("candles", "funding", "oi"):
            section, exchange, timeframe, _pairs, expected, captured, _pct, _measured_to = cells
            venues[Venue(section, exchange, timeframe)] = Capture(int(expected), int(captured))
    markers = [line for line in out.splitlines() if line.startswith("SCORECARD-DB:")]
    return Report(verdicts, venues, markers)


def section_total(report, section):
    rows = [capture for venue, capture in report.venues.items() if venue.section == section]
    return Capture(sum(r.expected for r in rows), sum(r.captured for r in rows))


def assert_counts(name, report, expect):
    """`expect` maps a section name (its total) or a Venue to the Capture it must report."""
    for key, want in expect.items():
        got = section_total(report, key) if isinstance(key, str) else report.venues.get(key)
        check(f"{name}: {key} counts", got == want, f"{got}, want {want}")


def run_case(name, fault="", failing=frozenset(), as_of=AS_OF, expect=None, **variables):
    """Plant the baseline, apply `fault`, and assert that each check is exactly PASS or FAIL as listed."""
    psql(PLANT + fault, **variables)
    report = scorecard(as_of)
    check(f"{name}: every check reported", set(report.verdicts) == set(CHECKS), str(report.verdicts))
    wrong = {c: v for c, v in report.verdicts.items() if v.verdict != ("FAIL" if c in failing else "PASS")}
    check(f"{name}: only {sorted(failing) or 'nothing'} fails", not wrong, str(wrong))
    check(f"{name}: one marker", report.markers == [f"SCORECARD-DB: {'FAIL' if failing else 'PASS'}"], str(report.markers))
    assert_counts(name, report, expect or {})
    return report


def run_boundary(name, fault, target, want, passes, days=DAYS, venues=None, **variables):
    """A threshold case on a reduced universe: only `target`'s verdict and counts are asserted. `venues`
    narrows the metadata's funding and OI venues to the ones left active."""
    psql(PLANT + fault, **variables)
    meta = META if venues is None else {**META, "funding_exchanges": venues, "oi_exchanges": venues}
    report = scorecard(days=days, meta=meta)
    verdict = report.verdicts.get(target)
    check(f"{name}: {target} is {'PASS' if passes else 'FAIL'}",
          verdict is not None and verdict.verdict == ("PASS" if passes else "FAIL"), str(verdict))
    section = {CANDLE: "candles", FUNDING_CHECK: "funding", OI_CHECK: "oi"}[target]
    assert_counts(name, report, {section: want})


# Faults. Values reach the SQL as psql variables (:'name'), quoted and cast.
DROP_BTC_5M_BARS = """
DELETE FROM candles WHERE exchange = 'binance' AND symbol = 'BTC/USDT:USDT' AND timeframe = '5m'
  AND timestamp >= :'from'::timestamptz AND timestamp < :'from'::timestamptz + make_interval(mins => 5 * :'n'::int);
"""
DROP_HL_OI_MINUTES = """
DELETE FROM open_interest WHERE exchange = 'hyperliquid'
  AND timestamp >= :'from'::timestamptz AND timestamp < :'from'::timestamptz + make_interval(mins => :'n'::int);
"""
DROP_HL_FUNDING_HOURS = """
DELETE FROM funding_rates WHERE exchange = 'hyperliquid'
  AND timestamp >= :'from'::timestamptz AND timestamp < :'from'::timestamptz + make_interval(hours => :'n'::int);
"""
CHANGE_ETH_INTERVAL = """
DELETE FROM funding_rates WHERE symbol = 'ETH/USDT:USDT';
INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, interval_hours)
SELECT 'binance', 'ETH', 'ETH/USDT:USDT', ts, 0.0001, 8
FROM generate_series(timestamptz '2026-10-18 08:00+00', timestamptz '2026-10-19 08:00+00', interval '8 hours') ts;
INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, interval_hours)
SELECT 'binance', 'ETH', 'ETH/USDT:USDT', ts, 0.0001, 4
FROM generate_series(timestamptz '2026-10-19 12:00+00', timestamptz '2026-10-20 12:00+00', interval '4 hours') ts;
"""
# Exact-threshold universes: one pair whose window yields a round number of expected slots.
ONLY_BTC_5M_1000_BARS = """
UPDATE collection_status SET active = (exchange = 'binance' AND symbol = 'BTC/USDT:USDT' AND timeframe = '5m');
UPDATE collection_status SET discovered_at = '2026-10-16 23:40+00' WHERE active;     -- 1,000 bars to 11:00
INSERT INTO candles (exchange, market_type, asset_id, symbol, native_symbol, timeframe, timestamp, close)
SELECT 'binance', 'perp', 'BTC', 'BTC/USDT:USDT', 'BTCUSDT', '5m', ts, 1.0
FROM generate_series(timestamptz '2026-10-16 23:40+00', timestamptz '2026-10-18 09:55+00', interval '5 min') ts;
"""
ONLY_HL_100_FUNDING_HOURS = """
UPDATE collection_status SET active = (exchange = 'hyperliquid');
UPDATE collection_status SET discovered_at = '2026-10-16 07:00+00' WHERE active;     -- 100 settlements through 10:00
INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, interval_hours)
SELECT 'hyperliquid', 'BTC', 'BTC/USDC:USDC', ts, 0.0001, 1
FROM generate_series(timestamptz '2026-10-16 07:00+00', timestamptz '2026-10-18 07:00+00', interval '1 hour') ts;
"""
ONLY_HL_2000_OI_MINUTES = """
UPDATE collection_status SET active = (exchange = 'hyperliquid');
UPDATE collection_status SET discovered_at = '2026-10-19 01:40+00' WHERE active;     -- 2,000 buckets to 11:00
"""


def start_db():
    subprocess.run(["docker", "run", "-d", "--name", CONTAINER, "-e", "POSTGRES_USER=candles",
                    "-e", "POSTGRES_PASSWORD=x", "-e", "POSTGRES_DB=candle_collector", IMAGE],
                   check=True, capture_output=True)
    for _ in range(60):
        ready = subprocess.run(["docker", "exec", CONTAINER, "pg_isready", "-q", "-h", "127.0.0.1", "-U", "candles"])
        if ready.returncode == 0:
            break
        time.sleep(1)
    psql((ROOT / "db/init.sql").read_text())
    psql((ROOT / "db/ledger.sql").read_text())
    subprocess.run(["bash", "-c", f'migrations_psql() {{ docker exec -i {CONTAINER} psql -X -U candles -d candle_collector "$@"; }}; '
                                  ". scripts/migrations-lib.sh && migrations_apply"], cwd=ROOT, check=True, capture_output=True)


def main():
    subprocess.run(["docker", "rm", "-fv", CONTAINER], capture_output=True)
    try:
        start_db()
        run_case("baseline", expect={"candles": Capture(CANDLES, CANDLES), "funding": Capture(FUNDING, FUNDING),
                                     "oi": Capture(OI, OI), Venue("funding", "hyperliquid", ""): Capture(47, 47)})

        # Single faults: each flips only its own check.
        run_case("candles missing five bars", DROP_BTC_5M_BARS, {CANDLE}, **{"from": "2026-10-19 00:00+00", "n": 5},
                 expect={"candles": Capture(CANDLES, CANDLES - 5)})
        run_case("funding missing one settlement", DROP_HL_FUNDING_HOURS, {FUNDING_CHECK},
                 expect={"funding": Capture(FUNDING, FUNDING - 1)}, **{"from": "2026-10-19 05:00+00", "n": 1})
        # HyperLiquid with no funding rows: nothing stored names its interval, so its slots are taken
        # at 8 h (10-18 16:00 … 10-20 08:00), and it captures none of them.
        run_case("funding venue with no rows", "DELETE FROM funding_rates WHERE exchange = 'hyperliquid';", {FUNDING_CHECK},
                 expect={Venue("funding", "hyperliquid", ""): Capture(6, 0)})
        # ETH moves from 8 h to 4 h settlements mid-window: complete, but with no single slot length,
        # so it leaves the denominator and is named as not measured.
        report = run_case("funding interval changed in the window", CHANGE_ETH_INTERVAL,
                          expect={"funding": Capture(6 + 47, 6 + 47)})
        check("funding interval changed: the pair is named not measured",
              "1 pair(s) not measured (interval changed)" in report.verdicts[FUNDING_CHECK].measured,
              report.verdicts[FUNDING_CHECK].measured)
        # Lag edge: a settlement exactly 2 h before the reference time is due; a second earlier it isn't.
        run_case("funding settlement exactly at the lag edge", as_of="2026-10-20 10:00:00+00",
                 expect={Venue("funding", "binance", ""): Capture(10, 10), Venue("funding", "hyperliquid", ""): Capture(47, 47)})
        run_case("funding one second before the lag edge", as_of="2026-10-20 09:59:59+00",
                 expect={Venue("funding", "binance", ""): Capture(8, 8), Venue("funding", "hyperliquid", ""): Capture(46, 46)})
        run_case("HL OI every 5 minutes only",
                 "DELETE FROM open_interest WHERE exchange = 'hyperliquid' AND extract(minute FROM timestamp)::int % 5 <> 0;",
                 {OI_CHECK}, expect={Venue("oi", "hyperliquid", ""): Capture(2820, 564)})
        run_case("non-HL OI venue below 95 %",
                 "DELETE FROM open_interest WHERE exchange = 'binance' AND symbol = 'ETH/USDT:USDT'"
                 " AND timestamp < '2026-10-20 00:00+00';", {OI_CHECK})
        run_case("OI venue with no rows", "DELETE FROM open_interest WHERE exchange = 'hyperliquid';", {OI_CHECK},
                 expect={Venue("oi", "hyperliquid", ""): Capture(2820, 0)})
        run_case("qualifying asset not collected",
                 "INSERT INTO discovery_volumes VALUES ('2026-10-20', 'SOL', 2e9);", {DISCOVERY})
        run_case("stale discovery run", "UPDATE discovery_volumes SET run_date = '2026-10-18';", {DISCOVERY})
        run_case("no discovery history", "TRUNCATE discovery_volumes;", {DISCOVERY})

        report = run_case("zero active pairs", "UPDATE collection_status SET active = FALSE;",
                          {CANDLE, FUNDING_CHECK, OI_CHECK, DISCOVERY})
        check("zero active pairs: each capture names its empty denominator",
              all(report.verdicts[c].measured.startswith("no expected slots") for c in (CANDLE, FUNDING_CHECK, OI_CHECK)),
              str(report.verdicts))

        # Exact thresholds: 999 of 1,000 bars is 99.9 %, 99 of 100 settlements 99 %, 1,900 of 2,000
        # buckets 95 %. Each passes, and one slot fewer fails.
        for n, passes in ((1, True), (2, False)):
            run_boundary(f"candles {1000 - n} of 1000", ONLY_BTC_5M_1000_BARS + DROP_BTC_5M_BARS, CANDLE,
                         Capture(1000, 1000 - n), passes, days=4, **{"from": "2026-10-17 00:00+00", "n": n})
        for n, passes in ((1, True), (2, False)):
            run_boundary(f"funding {100 - n} of 100", ONLY_HL_100_FUNDING_HOURS + DROP_HL_FUNDING_HOURS, FUNDING_CHECK,
                         Capture(100, 100 - n), passes, days=5, venues=["hyperliquid"],
                         **{"from": "2026-10-17 00:00+00", "n": n})
        for n, passes in ((100, True), (101, False)):
            run_boundary(f"OI {2000 - n} of 2000", ONLY_HL_2000_OI_MINUTES + DROP_HL_OI_MINUTES, OI_CHECK,
                         Capture(2000, 2000 - n), passes, venues=["hyperliquid"],
                         **{"from": "2026-10-19 12:00+00", "n": n})

        # Lag edge: at 12:00 the 10:55 bar is due; a second earlier it is not (one 5m bar per 5m pair,
        # one 1m bar). On the discovery edge, ETH found a second after 06:05 loses that slot.
        run_case("one second before the lag edge", as_of="2026-10-20 11:59:59+00",
                 expect={"candles": Capture(CANDLES - 3 - 1, CANDLES - 3 - 1)})
        run_case("discovered just after a slot",
                 "UPDATE collection_status SET discovered_at = '2026-10-19 06:05:01+00' WHERE symbol = 'ETH/USDT:USDT';",
                 expect={Venue("candles", "binance", "5m"): Capture(564 + 346, 564 + 346)})

        # Last: planting doesn't restore a dropped constraint.
        run_case("primary key dropped", "ALTER TABLE funding_rates DROP CONSTRAINT funding_rates_pkey;", {PK})
        print("all checks passed")
    finally:
        subprocess.run(["docker", "rm", "-fv", CONTAINER], capture_output=True)


if __name__ == "__main__":
    main()
