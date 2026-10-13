# candle-collector

A self-hosted crypto market-data warehouse. It collects candles, funding rates, open interest, liquidations and the premium index from six venues into TimescaleDB, and serves them over a small read-only API.

It's built for research and backtesting. It is a historical store, not a live feed: trading systems should keep their own real-time connections.

## Attribution

For those who care about such things, the majority of the analysis and code in this repo was produced by Claude Opus 4.8 (Anthropic), with David (@holarchic) guiding Claude, providing insights, direction and critical review throughout the process.

## What it collects

| Venue | Candles | Funding | Open interest | Liquidations | Other |
|---|---|---|---|---|---|
| Binance (USDT perps) | 1m – 12h | yes | yes | live stream | premium index, 5m |
| Bybit (perps) | 1m – 12h | yes | yes | live stream | |
| HyperLiquid (perps) | 1m (WebSocket), 5m – 12h | yes | yes, every minute | full daily tape from HyperLiquid's S3 node data | mark, oracle and premium context every minute |
| OKX (perps) | 5m – 12h | yes | yes | | |
| dYdX (perps) | 5m – 4h | yes | | | |
| Coinbase (spot) | 5m – 2h | | | | |

Collectors start with BTC on each venue. A daily discovery job adds every pair above a 24h volume threshold ($1M to add, $500k to keep; set in `collector/exchanges.yaml`). Symbols use the ccxt format (`BTC/USDT:USDT`) plus a canonical `asset_id` (`BTC`) for joining across venues.

## How it works

- **Collectors:** one container per venue. Polling is aligned to candle boundaries, rate-limited by each venue's request-weight budget, and resumes from the last stored bar after a restart. Writes are idempotent (`INSERT … ON CONFLICT DO NOTHING`).
- **Storage:** TimescaleDB hypertables, compressed after 7 days. Candles are kept for 2 years, everything else for 1 year. 1m candles have their own table, so they can sit on a separate volume.
- **Monitor:** staleness and gap alerts to a Discord or Slack webhook, a weekly heartbeat, and an optional dead-man's-switch ping (e.g. healthchecks.io).
- **API:** FastAPI, read-only, plus a parquet export for bulk pulls into backtests.

Module and table detail is in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Quick start

Docker with Compose.

```bash
git clone https://github.com/holarchic/candle-collector
cd candle-collector
cp .env.example .env            # set POSTGRES_PASSWORD
docker compose up -d
docker compose --profile discovery run --rm discovery    # optional: widen beyond BTC
curl "http://localhost:8000/candles?exchange=binance&asset_id=BTC&timeframe=5m&limit=5"
```

The first candles land within one bar interval. The HyperLiquid liquidation pull is a separate one-shot job (`docker compose --profile liquidations-hl run --rm liquidations-hl`). It reads a requester-pays S3 bucket, so it needs AWS credentials in `.env`, and AWS bills your account for the transfer.

## API

| Endpoint | Returns |
|---|---|
| `GET /candles` | OHLCV by `exchange`, `asset_id`, `timeframe`, `start`, `end`, `limit` |
| `GET /candles/exchanges` · `/timeframes` · `/symbols` | What's being collected |
| `GET /funding` · `/funding/latest` | Funding rates |
| `GET /oi` · `/oi/latest` | Open interest |
| `GET /liquidations` | Recent liquidation events |
| `GET /status` · `/status/gaps` | Per-collector health and detected gaps |
| `GET /export/parquet` | A time window of HyperLiquid context, Binance open interest or HyperLiquid liquidations, as parquet |
| `GET /health` | Liveness |

More example queries are in [`deploy/README.md`](deploy/README.md).

## Configuration

- `collector/exchanges.yaml`: venues, timeframes, rate limits and discovery thresholds.
- `.env`: Postgres credentials, the monitor webhook and thresholds, and optional AWS credentials for the HyperLiquid tape. See `.env.example`.

## Running it in production

It runs on one small VPS. The reference install is 4 vCPU and 8 GB RAM, with a block-storage volume for the 1m candles. [`deploy/README.md`](deploy/README.md) covers the production compose file, firewall, cron jobs, backups and how to ship code updates without restarting the database.

## Limits

- Not a live feed. Expect data up to one bar interval old.
- HyperLiquid serves only about 5,000 bars of 1m history (about 3.5 days), so a 1m gap older than that can't be backfilled.
- Binance and Bybit liquidation streams start when you start collecting. There's no history.
- Coinbase is spot only.

## Development

```bash
uv run --extra dev ruff check .
uv run --extra dev pytest -q
```

## License

MIT
