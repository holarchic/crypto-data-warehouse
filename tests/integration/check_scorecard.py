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
    "funding_interval_hours": {"binance": 8, "hyperliquid": 1},
}

# The window is 10-18 12:00 → 10-20 12:00. ETH is discovered on a 5m slot inside it.
#   candles: 5m bars due through 10:55 (end 11:00); 1m through 10:59 (end 11:00)
#     binance BTC 5m 564, binance ETH 5m 347 (06:05 → 11:00), hyperliquid BTC 5m 564, binance BTC 1m 2820
#   funding: 8 h slots due through 00:00 (end 08:00), 1 h through 09:00 (end 10:00)
#     binance BTC 5 (10-18 16:00 … 10-20 00:00), ETH 3 (10-19 08:00 …), hyperliquid BTC 46
#   OI: 300 s buckets binance BTC 564, ETH 347; 60 s buckets hyperliquid BTC 2820
CANDLES = 564 + 347 + 564 + 2820
FUNDING = 5 + 3 + 46
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

CHECKS = ("candle capture", "funding-slot occupancy (collector-assumed interval)",
          "OI bucket occupancy (60 s HL, 300 s elsewhere)", "primary keys", "discovery (union of venues)")


def check(name, ok, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}")
    if not ok:
        sys.exit(1)


def psql(sql, *args):
    return subprocess.run([*PSQL, *args], input=sql, capture_output=True, text=True, check=True).stdout


def scorecard(as_of=AS_OF, meta=META):
    """Runs the scorecard SQL unaligned; returns its check verdicts, venue rows and marker lines."""
    out = psql((ROOT / "scripts/stability-scorecard.sql").read_text(), "-A",
               "-v", f"days={DAYS}", "-v", f"meta={json.dumps(meta)}", "-v", f"as_of={as_of}")
    lines = out.splitlines()
    verdicts = {}
    venues = {}
    for line in lines:
        cells = line.split("|")
        if len(cells) == 4 and cells[0] in CHECKS:
            verdicts[cells[0]] = (cells[1], cells[3])
        elif len(cells) == 8 and cells[0] in ("candles", "funding", "oi"):
            venues[(cells[0], cells[1], cells[2])] = (int(cells[4]), int(cells[5]))
    markers = [line for line in lines if line.startswith("SCORECARD-DB:")]
    return verdicts, venues, markers


def totals(venues, section):
    rows = [v for k, v in venues.items() if k[0] == section]
    return sum(e for e, _ in rows), sum(c for _, c in rows)


def run_case(name, fault, failing, as_of=AS_OF, expect=None):
    """Plant the baseline, apply `fault`, and assert that exactly the `failing` checks FAIL."""
    psql(PLANT + fault)
    verdicts, venues, markers = scorecard(as_of)
    check(f"{name}: every check reported", set(verdicts) == set(CHECKS), str(verdicts))
    wrong = {c: v for c, v in verdicts.items() if (v[1] == "FAIL") != (c in failing)}
    check(f"{name}: only {sorted(failing) or 'nothing'} fails", not wrong, str(wrong))
    check(f"{name}: one marker", markers == [f"SCORECARD-DB: {'FAIL' if failing else 'PASS'}"], str(markers))
    for section, (expected, captured) in (expect or {}).items():
        got = totals(venues, section) if isinstance(section, str) else venues.get(section)
        check(f"{name}: {section} expected/captured", got == (expected, captured), f"{got}, want {(expected, captured)}")
    return verdicts, venues


def start_db():
    subprocess.run(["docker", "rm", "-fv", CONTAINER], capture_output=True)
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
    start_db()
    try:
        cand, fund, oi, pk, disc = CHECKS
        run_case("baseline", "", set(), expect={"candles": (CANDLES, CANDLES), "funding": (FUNDING, FUNDING),
                                                 "oi": (OI, OI), ("funding", "hyperliquid", ""): (46, 46)})

        # Candles: 99.9 % of 4,295 needs 4,291 bars. Four missing passes, five fails.
        gap = "DELETE FROM candles WHERE exchange = 'binance' AND symbol = 'BTC/USDT:USDT' AND timestamp IN ({});"
        bars = [f"'2026-10-19 00:{m:02d}:00+00'" for m in range(0, 25, 5)]
        run_case("candles at the threshold", gap.format(", ".join(bars[:4])), set(),
                 expect={"candles": (CANDLES, CANDLES - 4)})
        run_case("candles below the threshold", gap.format(", ".join(bars)), {cand},
                 expect={"candles": (CANDLES, CANDLES - 5)})

        # Funding: 99 % of 54 needs all 54, so one missing settlement fails.
        run_case("funding missing one settlement",
                 "DELETE FROM funding_rates WHERE exchange = 'binance' AND symbol = 'ETH/USDT:USDT'"
                 " AND timestamp = '2026-10-19 16:00+00';", {fund}, expect={"funding": (FUNDING, FUNDING - 1)})
        # HyperLiquid with no funding rows: its expected slots come from the collector's 1 h assumption.
        run_case("funding venue with no rows", "DELETE FROM funding_rates WHERE exchange = 'hyperliquid';", {fund},
                 expect={("funding", "hyperliquid", ""): (46, 0)})

        # OI: 95 % of 3,731 needs 3,545 buckets. 186 missing HL minutes pass, 187 fail.
        hl_minutes = ("DELETE FROM open_interest WHERE exchange = 'hyperliquid'"
                      " AND timestamp >= '2026-10-19 00:00+00' AND timestamp < timestamptz '2026-10-19 00:00+00'"
                      " + make_interval(mins => {});")
        run_case("OI at the threshold", hl_minutes.format(186), set(), expect={"oi": (OI, OI - 186)})
        run_case("OI below the threshold", hl_minutes.format(187), {oi}, expect={"oi": (OI, OI - 187)})
        run_case("HL OI every 5 minutes only",
                 "DELETE FROM open_interest WHERE exchange = 'hyperliquid'"
                 " AND extract(minute FROM timestamp)::int % 5 <> 0;", {oi},
                 expect={("oi", "hyperliquid", ""): (2820, 564)})
        run_case("non-HL OI venue below 95 %",
                 "DELETE FROM open_interest WHERE exchange = 'binance' AND symbol = 'ETH/USDT:USDT'"
                 " AND timestamp < '2026-10-20 00:00+00';", {oi})
        run_case("OI venue with no rows", "DELETE FROM open_interest WHERE exchange = 'hyperliquid';", {oi},
                 expect={("oi", "hyperliquid", ""): (2820, 0)})

        run_case("qualifying asset not collected",
                 "INSERT INTO discovery_volumes VALUES ('2026-10-20', 'SOL', 2e9);", {disc})
        run_case("stale discovery run", "UPDATE discovery_volumes SET run_date = '2026-10-18';", {disc})
        run_case("no discovery history", "TRUNCATE discovery_volumes;", {disc})

        verdicts, _ = run_case("zero active pairs", "UPDATE collection_status SET active = FALSE;",
                               {cand, fund, oi, disc})
        check("zero active pairs: each capture names its empty denominator",
              all(verdicts[c][0].startswith("no expected slots") for c in (cand, fund, oi)), str(verdicts))

        # Lag edge: at 12:00 the 10:55 bar is due; a second earlier it is not (one 5m bar per 5m pair,
        # one 1m bar). On the discovery edge, ETH found a second after 06:05 loses that slot.
        run_case("one second before the lag edge", "", set(), as_of="2026-10-20 11:59:59+00",
                 expect={"candles": (CANDLES - 3 - 1, CANDLES - 3 - 1)})
        run_case("discovered just after a slot",
                 "UPDATE collection_status SET discovered_at = '2026-10-19 06:05:01+00'"
                 " WHERE symbol = 'ETH/USDT:USDT';", set(),
                 expect={("candles", "binance", "5m"): (564 + 346, 564 + 346)})

        # Last: planting doesn't restore a dropped constraint.
        run_case("primary key dropped", "ALTER TABLE funding_rates DROP CONSTRAINT funding_rates_pkey;", {pk})
        print("all checks passed")
    finally:
        subprocess.run(["docker", "rm", "-fv", CONTAINER], capture_output=True)


if __name__ == "__main__":
    main()
