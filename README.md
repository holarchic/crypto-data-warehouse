# candle-collector

Your own crypto market-data warehouse. It collects candles, funding rates, open interest, liquidations and the premium index from six exchanges into TimescaleDB, and serves them over a read-only API. It's built to run unattended on a server you control.

## Who it's for

Anyone who needs crypto derivatives data for research or backtesting but doesn't want to pay a data vendor, or to wire up and babysit six exchange APIs by hand. Every feed uses public endpoints, so you need no exchange API keys.

## Attribution

For those who care about such things, the majority of the code in this repo was produced by Claude Opus 4.8 (Anthropic), with David (@holarchic) guiding Claude, providing insights, direction and critical review throughout the process.

## What it collects

| Exchange | Candles | Funding | Open interest | Liquidations | Also |
|---|---|---|---|---|---|
| Binance (USDT perps) | from 1m | ✓ | ✓ | live stream | premium index, 5m |
| Bybit (perps) | from 1m | ✓ | ✓ | live stream | |
| HyperLiquid (perps) | from 1m | ✓ | ✓ | every event, pulled daily | mark, oracle and premium, every minute |
| OKX (perps) | from 5m | ✓ | ✓ | | |
| dYdX (perps) | from 5m | ✓ | | | |
| Coinbase (spot) | from 5m | | | | |

Timeframes are 1m, 5m, 15m, 30m, 1h, 2h, 4h and 12h, where the exchange offers them.

Each exchange starts with BTC. A daily discovery job adds every pair trading over $1M a day and drops any that fall below $500k (thresholds in `collector/exchanges.yaml`). Symbols use ccxt format (`BTC/USDT:USDT`); `asset_id` (`BTC`) joins them across exchanges.

## How it works

- **Collectors:** one container per exchange. Each polls on candle boundaries, stays inside the exchange's rate limit, and resumes from its last stored bar, so restarts and short outages backfill themselves. Writes are idempotent.
- **Storage:** TimescaleDB, compressed after 7 days. Candles are kept for 2 years, everything else for 1 year. 1m candles have their own table, so they can sit on a separate volume.
- **API:** read-only FastAPI, plus a parquet export for bulk pulls.

Module and table detail: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Monitoring

Set `WEBHOOK_URL` in `.env` to a Discord or Slack webhook and it tells you when something breaks.

- **Collector monitor**, every 5 minutes: alerts when a feed goes stale, when a gap appears, and when the number of failing collectors jumps above its recent baseline. Repeats are deduplicated.
- **Host monitor**, every 5 minutes via cron: disk usage at 75%, 85% and 95% per mount, low memory, and stopped containers.
- **Weekly heartbeat:** a summary message, so a silent channel means the alerting itself is down.
- **Dead-man's switch:** set `HEALTHCHECK_PING_URL` (e.g. healthchecks.io) and the monitor pings it every cycle. If the pings stop, that service alerts you.
- **On demand:** `GET /status` and `GET /status/gaps` show each collector's last success, error count and gaps.

Thresholds: [`deploy/README.md`](deploy/README.md#monitor-env-vars).

## Hosting

It runs anywhere Docker Compose runs: a VPS, a cloud VM or a home server. The reference install runs all six exchanges on 4 vCPU and 8 GB RAM. 1m candles add about 19 GB a year after compression. There are no Kubernetes manifests yet; contributions welcome.

On a server, always start it with the production override. It keeps Postgres off the network, binds the API to localhost and sets memory limits. Reach the API over an SSH tunnel. [`deploy/README.md`](deploy/README.md) covers provisioning, firewall, cron, backups and updates.

## Quick start

On a laptop:

```bash
git clone https://github.com/holarchic/candle-collector
cd candle-collector
cp .env.example .env     # set POSTGRES_PASSWORD
docker compose up -d
curl "http://localhost:8000/candles?exchange=binance&asset_id=BTC&timeframe=5m&limit=5"
```

The first 5m candles land within about five minutes. To collect beyond BTC, run discovery: `docker compose --profile discovery run --rm discovery`.

This compose file publishes Postgres (5432) and the API (8000) on every interface. Don't use it on a public server.

HyperLiquid's liquidation history comes from a separate daily job (`--profile liquidations-hl`). It reads a requester-pays S3 bucket, so it needs AWS credentials in `.env`, and AWS bills you for the transfer.

## API

| Endpoint | Returns |
|---|---|
| `GET /candles` | OHLCV, filtered by `exchange`, `asset_id`, `timeframe`, `start`, `end`, `limit` |
| `GET /candles/exchanges` · `/timeframes` · `/symbols` | What's being collected |
| `GET /funding` · `/funding/latest` | Funding rates |
| `GET /oi` · `/oi/latest` | Open interest |
| `GET /liquidations` | Recent liquidations |
| `GET /status` · `/status/gaps` | Collector health and gaps |
| `GET /export/parquet` | HyperLiquid context, Binance open interest or HyperLiquid liquidations for a time window, as parquet |
| `GET /health` | Liveness |

Example queries: [`deploy/README.md`](deploy/README.md#api-queries).

## Configuration

- `collector/exchanges.yaml`: exchanges, timeframes, rate limits, discovery thresholds.
- `.env`: Postgres credentials, alert webhook and thresholds, optional AWS credentials. See `.env.example`.

## For coding agents

If you're installing this for a user, do these steps in order and run every check.

1. **Prerequisites.** `docker compose version` reports v2. On a laptop, ports 5432 and 8000 are free. For production, the host is a fresh Ubuntu 24.04 server.
2. **Provision (server only).** As root: `git clone https://github.com/holarchic/candle-collector.git /opt/candle-collector && /opt/candle-collector/deploy/setup.sh`. It installs Docker, log rotation and UFW (SSH only). Tell the user to keep port 8000 closed in their provider's firewall too.
3. **Configure.** `cp .env.example .env` and set `POSTGRES_PASSWORD` to a random value (`openssl rand -hex 24`). Ask the user for a Discord or Slack webhook for `WEBHOOK_URL`. Leave the AWS variables empty unless the user wants HyperLiquid liquidation history and accepts the S3 transfer cost. No exchange API keys are needed.
4. **Start.** Laptop: `docker compose up -d`. Server: `./deploy/deploy.sh`, which builds, starts with the production override, waits for the API and installs the cron jobs.
5. **Verify.**
   - `curl -s localhost:8000/health` returns `{"status":"ok"}`.
   - After 6 minutes, `curl -s "localhost:8000/candles?exchange=binance&asset_id=BTC&timeframe=5m&limit=1"` returns one candle with every OHLCV field set.
   - `curl -s localhost:8000/status` shows the 5m rows with `"is_stale": false`.
6. **Widen.** Run discovery (above). On a server, add `-f docker-compose.yml -f docker-compose.prod.yml`; cron repeats it daily.

Rules:

- Never publish ports 5432 or 8000 to the internet. Use an SSH tunnel.
- Ship updates the way [`deploy/README.md`](deploy/README.md#code-updates) describes. Don't rerun `deploy.sh` on a live install: it can recreate the database.
- Don't write to the database by hand. Schema changes go in `db/migrations/` and `db/init.sql`.
- Without AWS credentials, the daily `liquidations-hl` cron job exits with code 2. That's expected; remove its crontab line if the user doesn't want the job.

## Limits

- Not a live feed: data is up to one bar old.
- HyperLiquid serves only about 5,000 bars of 1m history (about 3.5 days), so an older 1m gap there can't be backfilled.
- Binance and Bybit liquidation streams start when you start collecting. There's no history.

## Contributing

Contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT
