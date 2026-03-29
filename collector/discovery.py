"""Discovery service — finds qualifying pairs across all exchanges.

Union qualification: $1M 24h volume on ANY exchange → collect on ALL exchanges where listed.
Run as one-shot: `docker compose --profile discovery run --rm discovery`
"""

import logging
import sys
from collections import defaultdict

from config import load_all_configs, load_discovery_config
from dotenv import load_dotenv
from main import build_dsn, connect_db, create_exchange
from status import deactivate_stale_pairs, upsert_discovery_pair
from symbols import resolve_asset_id

log = logging.getLogger(__name__)


def fetch_exchange_tickers(exchange, config):
    """Fetch all tickers for an exchange, filtered by market type. Returns list of (asset_id, symbol, market_type, native_symbol, volume)."""
    try:
        # OKX ignores ccxt unified "type" param — needs native instType
        if config.market_type == "perp" and config.ccxt_class == "okx":
            tickers = exchange.fetch_tickers(params={"instType": "SWAP"})
        elif config.market_type == "perp":
            tickers = exchange.fetch_tickers(params={"type": "swap"})
        else:
            tickers = exchange.fetch_tickers()
    except Exception:
        log.exception("fetch_tickers() failed for %s — skipping", config.name)
        return []

    overrides = load_discovery_config().get("symbol_overrides", {})
    results = []

    for symbol, ticker in tickers.items():
        market = exchange.markets.get(symbol)
        if not market:
            continue

        # Filter by market type
        if config.market_type == "perp":
            if not market.get("swap"):
                continue
        # spot: accept all (Coinbase)

        base = market.get("base")
        if not base:
            continue

        volume = ticker.get("quoteVolume") or 0
        asset_id = resolve_asset_id(base, overrides)
        native_symbol = market.get("id", "")

        results.append({
            "asset_id": asset_id,
            "symbol": symbol,
            "market_type": config.market_type,
            "native_symbol": native_symbol,
            "volume": volume,
        })

    log.info("[%s] Fetched %d tickers, %d passed market_type filter (%s)", config.name, len(tickers), len(results), config.market_type)
    return results


def discover():
    """Run discovery: fetch tickers from all exchanges, qualify pairs, write to collection_status."""
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    configs = load_all_configs()
    discovery_config = load_discovery_config()
    threshold = discovery_config.get("volume_threshold_usd", 1_000_000)
    dsn = build_dsn()

    log.info("Discovery starting: %d exchanges, volume threshold=$%s", len(configs), f"{threshold:,.0f}")

    # Phase 1: Fetch tickers from all exchanges
    # volumes[asset_id][exchange_name] = {symbol, market_type, native_symbol, volume}
    volumes = defaultdict(dict)

    # Filter to perp exchanges only — spot (Coinbase) uses BTC fallback
    perp_configs = [c for c in configs if c.market_type == "perp"]
    log.info("Filtered to %d perp exchanges (skipping spot)", len(perp_configs))

    for config in perp_configs:
        log.info("Loading markets for %s...", config.name)
        try:
            exchange = create_exchange(config)
        except Exception:
            log.exception("Failed to create exchange %s — skipping", config.name)
            continue

        tickers = fetch_exchange_tickers(exchange, config)
        for t in tickers:
            volumes[t["asset_id"]][config.name] = {
                "symbol": t["symbol"],
                "market_type": t["market_type"],
                "native_symbol": t["native_symbol"],
                "volume": t["volume"],
            }

    # Phase 2: Union qualification — max volume across any exchange >= threshold
    qualifying_assets = set()
    for asset_id, exchange_data in volumes.items():
        max_vol = max(d["volume"] for d in exchange_data.values())
        if max_vol >= threshold:
            qualifying_assets.add(asset_id)

    log.info("Qualifying assets: %d / %d total (threshold=$%s)", len(qualifying_assets), len(volumes), f"{threshold:,.0f}")

    # Phase 3: Write to collection_status
    conn = connect_db(dsn)
    total_upserted = 0
    total_deactivated = 0

    # Track active symbols per (exchange, timeframe) for deactivation
    active_symbols_map = defaultdict(list)  # (exchange, timeframe) -> [symbols]

    try:
        for asset_id in sorted(qualifying_assets):
            for exchange_name, data in volumes[asset_id].items():
                # Find the config for this exchange to get timeframes
                config = next((c for c in perp_configs if c.name == exchange_name), None)
                if not config:
                    continue

                for tf in config.timeframes:
                    upsert_discovery_pair(
                        conn,
                        exchange=exchange_name,
                        market_type=data["market_type"],
                        symbol=data["symbol"],
                        timeframe=tf.name,
                        asset_id=asset_id,
                        native_symbol=data["native_symbol"],
                        volume=data["volume"],
                    )
                    active_symbols_map[(exchange_name, tf.name)].append(data["symbol"])
                    total_upserted += 1

        conn.commit()
        log.info("Upserted %d pair×timeframe rows", total_upserted)

        # Phase 4: Deactivate stale pairs
        for (exchange_name, timeframe), symbols in active_symbols_map.items():
            count = deactivate_stale_pairs(conn, exchange_name, timeframe, symbols)
            total_deactivated += count

        conn.commit()
        log.info("Deactivated %d stale pair×timeframe rows", total_deactivated)

    except Exception:
        conn.rollback()
        log.exception("Discovery failed during DB writes")
        sys.exit(1)
    finally:
        conn.close()

    # Summary
    log.info("Discovery complete: %d qualifying assets, %d rows upserted, %d deactivated", len(qualifying_assets), total_upserted, total_deactivated)

    # Log top assets by max volume
    asset_vols = []
    for asset_id in qualifying_assets:
        max_vol = max(d["volume"] for d in volumes[asset_id].values())
        n_exchanges = len(volumes[asset_id])
        asset_vols.append((asset_id, max_vol, n_exchanges))
    asset_vols.sort(key=lambda x: x[1], reverse=True)
    for asset_id, vol, n_ex in asset_vols[:20]:
        log.info("  %-10s $%12s  (%d exchanges)", asset_id, f"{vol:,.0f}", n_ex)


if __name__ == "__main__":
    discover()
