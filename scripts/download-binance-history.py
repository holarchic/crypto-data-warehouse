#!/usr/bin/env python3
"""Download Binance USDT-M futures 15m klines and import to candle-collector PostgreSQL.

Downloads monthly zip files from data.binance.vision for all coins that overlap
with HyperLiquid's perp universe. Imports directly to candle-collector's candles
hypertable via psycopg COPY for speed.

Usage:
    # 1. Open SSH tunnel to Hetzner PostgreSQL:
    #    ssh -L 5432:localhost:5432 <server>
    #
    # 2. Dry run (download only, no DB import):
    #    python3 scripts/download-binance-history.py --months 12 --dry-run
    #
    # 3. Full run (download + import):
    #    python3 scripts/download-binance-history.py --months 12
    #
    # 4. Specific coins only:
    #    python3 scripts/download-binance-history.py --coins ETH,BTC,SOL --months 6

Requires: psycopg[binary], requests
"""

import argparse
import csv
import io
import logging
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DOWNLOAD_DIR = PROJECT_ROOT / "data" / "binance-history"

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
TIMEFRAME = "15m"
BATCH_SIZE = 10000

# Binance kline CSV columns
# open_time, open, high, low, close, volume, close_time, quote_volume, count,
# taker_buy_volume, taker_buy_quote_volume, ignore
KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count",
    "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]

INSERT_SQL = """
    INSERT INTO candles (exchange, market_type, asset_id, symbol, native_symbol,
                         timeframe, timestamp, open, high, low, close,
                         volume, quote_volume, trades)
    VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""


def get_hl_coins(api_url: str) -> list[str]:
    """Fetch HyperLiquid coin list from candle-collector API."""
    r = requests.get(f"{api_url}/candles/symbols?exchange=hyperliquid", timeout=10)
    r.raise_for_status()
    return sorted(set(s["asset_id"] for s in r.json()))


def generate_months(n_months: int) -> list[tuple[int, int]]:
    """Generate (year, month) tuples for the last n_months."""
    now = datetime.now(timezone.utc)
    months = []
    y, m = now.year, now.month
    for _ in range(n_months):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        months.append((y, m))
    months.reverse()
    return months


def download_klines(coin: str, year: int, month: int) -> list[dict] | None:
    """Download and parse one month of 15m klines for a Binance USDT-M perp.

    Returns list of candle dicts or None if the symbol/month doesn't exist.
    """
    symbol = f"{coin}USDT"
    filename = f"{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{filename}"

    # Check local cache first
    local_path = DOWNLOAD_DIR / filename
    if local_path.exists():
        log.debug("Using cached %s", filename)
        with zipfile.ZipFile(local_path) as zf:
            csv_name = zf.namelist()[0]
            return _parse_csv(zf.read(csv_name).decode(), coin)

    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning("Failed to download %s: %s", filename, e)
        return None

    # Cache the zip
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(r.content)

    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            csv_name = zf.namelist()[0]
            return _parse_csv(zf.read(csv_name).decode(), coin)
    except zipfile.BadZipFile:
        log.warning("Bad zip file for %s", filename)
        local_path.unlink(missing_ok=True)
        return None


def _parse_csv(csv_text: str, coin: str) -> list[dict]:
    """Parse Binance kline CSV into candle dicts."""
    candles = []
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if len(row) < 11:
            continue
        try:
            open_time_ms = int(row[0])
        except ValueError:
            continue  # skip header row if present

        candles.append({
            "exchange": "binance",
            "market_type": "perp",
            "asset_id": coin,
            "symbol": f"{coin}/USDT:USDT",
            "native_symbol": f"{coin}USDT",
            "timeframe": TIMEFRAME,
            "timestamp": open_time_ms / 1000,  # epoch seconds
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "quote_volume": float(row[7]),
            "trades": int(row[8]),
        })
    return candles


def import_to_postgres(candles: list[dict], pg_dsn: str) -> int:
    """Bulk insert candles to PostgreSQL. Returns count of new rows."""
    import psycopg

    conn = psycopg.connect(pg_dsn)
    inserted = 0
    try:
        for i in range(0, len(candles), BATCH_SIZE):
            batch = candles[i:i + BATCH_SIZE]
            with conn.cursor() as cur:
                for c in batch:
                    cur.execute(INSERT_SQL, (
                        c["exchange"], c["market_type"], c["asset_id"],
                        c["symbol"], c["native_symbol"], c["timeframe"],
                        c["timestamp"], c["open"], c["high"], c["low"],
                        c["close"], c["volume"], c["quote_volume"], c["trades"],
                    ))
                    inserted += cur.rowcount
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return inserted


def export_to_sqlite(candles: list[dict], db_path: str) -> int:
    """Export candles to local SQLite in scanner.db format for backtesting."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            coin TEXT, interval TEXT, timestamp TEXT,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, exchange TEXT,
            PRIMARY KEY (coin, interval, timestamp)
        )
    """)

    inserted = 0
    for i in range(0, len(candles), BATCH_SIZE):
        batch = candles[i:i + BATCH_SIZE]
        for c in batch:
            ts = datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).isoformat()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO candles (coin, interval, timestamp, open, high, low, close, volume, exchange) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (c["asset_id"], c["timeframe"], ts, c["open"], c["high"], c["low"], c["close"], c["volume"], "binance"),
                )
                inserted += conn.total_changes  # approximate
            except sqlite3.IntegrityError:
                pass
        conn.commit()
    conn.close()
    return inserted


def load_env(env_path: Path) -> dict[str, str]:
    """Load key=value pairs from a .env file."""
    env = {}
    if not env_path.exists():
        return env
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip("'\"")
    return env


def build_dsn(env: dict[str, str]) -> str:
    """Build PostgreSQL DSN from .env values."""
    user = env.get("POSTGRES_USER", "candles")
    password = env.get("POSTGRES_PASSWORD")
    if not password:
        print("Error: POSTGRES_PASSWORD not found in .env — use --sqlite-only to skip PostgreSQL")
        sys.exit(1)
    host = env.get("POSTGRES_HOST", "localhost")
    port = env.get("POSTGRES_PORT", "5432")
    db = env.get("POSTGRES_DB", "candle_collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def main():
    parser = argparse.ArgumentParser(description="Download Binance USDT-M 15m klines and import to candle-collector")
    parser.add_argument("--months", type=int, default=12, help="Months of history to download (default: 12)")
    parser.add_argument("--coins", type=str, default="", help="Comma-separated coin list (default: all HL-overlap)")
    parser.add_argument("--api-url", default="http://localhost:8000", help="candle-collector API URL")
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env", help="Path to .env file")
    parser.add_argument("--dry-run", action="store_true", help="Download only, no DB import")
    parser.add_argument("--sqlite-only", type=str, default="", help="Export to SQLite file instead of PostgreSQL (path)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Get coin list
    if args.coins:
        coins = [c.strip().upper() for c in args.coins.split(",")]
    else:
        log.info("Fetching HL coin list from candle-collector API...")
        try:
            coins = get_hl_coins(args.api_url)
        except Exception as e:
            log.error("Could not fetch HL coins: %s — use --coins to specify manually", e)
            sys.exit(1)

    months = generate_months(args.months)
    log.info("Downloading %d months of 15m klines for %d coins (%s – %s)",
             len(months), len(coins), f"{months[0][0]}-{months[0][1]:02d}", f"{months[-1][0]}-{months[-1][1]:02d}")

    # Setup DB connection
    pg_dsn = None
    if not args.dry_run and not args.sqlite_only:
        env = load_env(args.env_file)
        pg_dsn = build_dsn(env)

    # Build all (coin, year, month) tasks
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tasks = [(coin, year, month) for coin in coins for year, month in months]
    log.info("Total downloads: %d (using 10 concurrent workers)", len(tasks))

    # Download concurrently, write to SQLite/PG as results arrive
    coins_found = set()
    coins_missing = set(coins)
    total_downloaded = 0
    sqlite_conn = None

    if args.sqlite_only:
        import sqlite3
        sqlite_conn = sqlite3.connect(args.sqlite_only)
        sqlite_conn.execute("PRAGMA journal_mode=WAL")
        sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                coin TEXT, interval TEXT, timestamp TEXT,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, exchange TEXT,
                PRIMARY KEY (coin, interval, timestamp)
            )
        """)
        sqlite_conn.commit()

    completed = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(download_klines, coin, year, month): (coin, year, month) for coin, year, month in tasks}

        for future in as_completed(futures):
            coin, year, month = futures[future]
            completed += 1

            try:
                candles = future.result()
            except Exception as e:
                log.warning("Error downloading %s %d-%02d: %s", coin, year, month, e)
                continue

            if candles is None:
                continue

            coins_found.add(coin)
            coins_missing.discard(coin)
            total_downloaded += len(candles)

            # Stream to SQLite — executemany + commit every 500 downloads
            if sqlite_conn:
                rows = [
                    (c["asset_id"], c["timeframe"],
                     datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).isoformat(),
                     c["open"], c["high"], c["low"], c["close"], c["volume"], "binance")
                    for c in candles
                ]
                sqlite_conn.executemany(
                    "INSERT OR IGNORE INTO candles (coin, interval, timestamp, open, high, low, close, volume, exchange) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows,
                )
                if completed % 500 == 0:
                    sqlite_conn.commit()
            elif pg_dsn and not args.dry_run:
                try:
                    import_to_postgres(candles, pg_dsn)
                except Exception as e:
                    log.error("PostgreSQL import failed for %s %d-%02d: %s", coin, year, month, e)

            # Progress every 100 downloads
            if completed % 100 == 0:
                elapsed = time.time() - t_start
                rate = completed / elapsed
                remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                log.info("Progress: %d/%d downloads (%.0f/s), %d candles, %d coins found, ~%.0fs remaining",
                         completed, len(tasks), rate, total_downloaded, len(coins_found), remaining)

    if sqlite_conn:
        sqlite_conn.commit()
        sqlite_conn.close()

    elapsed = time.time() - t_start

    # Summary
    log.info("\n=== Summary ===")
    log.info("Coins with data: %d/%d", len(coins_found), len(coins))
    log.info("Total candles: %d", total_downloaded)
    log.info("Time: %.0fs (%.0f downloads/s)", elapsed, len(tasks) / elapsed if elapsed > 0 else 0)
    if coins_missing:
        missing_sorted = sorted(coins_missing)
        log.info("Missing on Binance: %s", ", ".join(missing_sorted[:20]))
        if len(missing_sorted) > 20:
            log.info("  ... and %d more", len(missing_sorted) - 20)


if __name__ == "__main__":
    main()
