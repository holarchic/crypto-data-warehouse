# crypto-data-warehouse

[![CI](https://github.com/holarchic/crypto-data-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/holarchic/crypto-data-warehouse/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/holarchic/crypto-data-warehouse)](https://github.com/holarchic/crypto-data-warehouse/releases)
[![License: MIT](https://img.shields.io/github/license/holarchic/crypto-data-warehouse)](LICENSE)

Self-hosted crypto data warehouse: OHLCV, funding rate, open interest, liquidations. Binance, Bybit, HyperLiquid, OKX

> **Status:** running in production since April 2026, collecting about 720 assets. Pre-1.0: the schema may change between releases.

```console
$ curl "localhost:8000/candles?exchange=binance&asset_id=BTC&timeframe=5m&limit=1"
[
  {
    "exchange": "binance",
    "market_type": "perp",
    "asset_id": "BTC",
    "symbol": "BTC/USDT:USDT",
    "native_symbol": "BTCUSDT",
    "timeframe": "5m",
    "timestamp": "2026-10-13T19:10:00+00:00",
    "open": 83213.9,
    "high": 83238.7,
    "low": 83157.4,
    "close": 83157.4,
    "volume": 100.932,
    "quote_volume": null,
    "trades": null
  }
]
```

## Why

For anyone who wants crypto derivatives data for research or backtesting without paying a data vendor or babysitting six exchange APIs by hand. I built it for my own backtests.

Public endpoints only, so no exchange API keys. It runs unattended on a server you control and tells you when something breaks.

**When not to use it:**

- You need tick-level trades or order books. Use a paid archive such as [Tardis.dev](https://tardis.dev), or stream them yourself with [cryptofeed](https://github.com/bmoscon/cryptofeed), which handles the connections but leaves storage to you.
- You need years of history on day one. crypto-data-warehouse starts with what each exchange returns in one call (up to 1,000 bars per timeframe) and collects forward. For deep Binance history, use its [bulk files](https://data.binance.vision).
- You need a live feed. Data lands once a bar closes, so trading systems should keep their own real-time connection.

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

Which assets:

- **Every exchange starts with BTC.** A daily discovery job adds any asset trading over $1M a day on one of the perp exchanges and collects it everywhere it's listed.
- **An asset is dropped** once its highest nightly volume over the past seven days is below $500k, so a weekend lull doesn't cut its history. Thresholds are in `collector/exchanges.yaml`.
- **The reference install** tracks 674 assets on Binance, 642 on Bybit, 421 on OKX and 174 on HyperLiquid (October 2026).
- **Coinbase and dYdX are BTC only:** discovery covers perps, and ccxt can't list dYdX's tickers.

## Data notes

- **Timestamps** are UTC, at the candle's open.
- **Bars are stored once closed** and never change. HyperLiquid 1m is the exception: it arrives over WebSocket, so the latest bar updates until it closes. A daily job then replaces each bar with HyperLiquid's own closed value, since the stream can stop short of a minute's last trades or drop minutes.
- **Gaps:** short outages backfill on the next poll. `GET /status/gaps` lists any stretch where bars are missing, with the count.
- **HyperLiquid 1m** history only reaches back about 5,000 bars (about 3.5 days), so the daily repair has to run inside that window. A gap older than that can't be filled.
- **Liquidations:** Binance and Bybit come from live streams, which start when you start collecting and have no history. HyperLiquid's come from its S3 archive, pulled once a day.
- **`quote_volume` and `trades`** are `null` where the exchange doesn't provide them.
- **Symbols** use ccxt format (`BTC/USDT:USDT`); `asset_id` (`BTC`) joins them across exchanges.

## How it works

```mermaid
flowchart LR
    Ex[Exchange APIs<br/>REST and WebSocket] --> Col[Collectors<br/>one per exchange]
    Disc[Daily discovery] -->|pairs to collect| Col
    S3[HyperLiquid S3 archive] --> Liq[Daily liquidation pull]
    Col --> DB[(TimescaleDB)]
    Liq --> DB
    DB --> API[Read-only API]
    API --> You[Your research<br/>and backtests]
    DB --> Mon[Monitors]
    Mon --> Alerts[Discord or Slack]
```

- **Collectors:** one container per exchange. Each polls on candle boundaries, stays inside the exchange's rate limit, and resumes from its last stored bar after a restart. Writes are idempotent.
- **Storage:** TimescaleDB, compressed after 7 days. Nothing is deleted, so plan disk for growth (Requirements below). 1m candles have their own table, so they can sit on a separate volume.
- **API:** read-only FastAPI, plus a parquet export for bulk pulls.

Module and table detail: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Monitoring

Set `WEBHOOK_URL` in `.env` to a Discord or Slack webhook and it tells you when something breaks.

<img src="docs/images/discord-alerts.png" alt="Discord channel showing a weekly heartbeat and an alert for 255 stale Binance 1m collectors" width="640">

- **Collector monitor**, every 5 minutes: alerts when a feed goes stale, when a gap appears, and when the number of failing collectors jumps above its recent baseline. Repeats are deduplicated.
- **Host monitor**, every 5 minutes via cron: disk usage at 75%, 85% and 95% per mount, low memory, and stopped containers.
- **Weekly heartbeat:** a summary message, so a silent channel means the alerting itself is down.
- **Dead-man's switch:** set `HEALTHCHECK_PING_URL` (e.g. healthchecks.io) and the monitor pings it every cycle. If the pings stop, that service alerts you.
- **On demand:** `GET /status` shows each collector's last success, error count and staleness.

Thresholds: [`deploy/README.md`](deploy/README.md#monitor-env-vars).

## Requirements

Docker with Compose v2, on anything that runs it, from a VPS to a box at home. No Kubernetes manifests yet (contributions welcome).

Measured on the reference install in October 2026 (4 vCPU, 8 GB RAM, all six exchanges, about 720 assets):

| | |
|---|---|
| Memory | about 2.4 GB across all containers, 1.3 GB of it the database |
| Disk | database 32 GB; it grows about 4.5 GB a month compressed, 1.8 GB of that 1m candles |
| CPU | near idle, with short bursts at candle boundaries |

It hasn't been tested on less. Start from 4 GB of RAM, and put the database on local SSD, not a network share.

> [!WARNING]
> This is a database. Back it up: `scripts/backup-offsite.sh` keeps nightly encrypted snapshots in a bucket off the server ([Offsite backups](deploy/README.md#offsite-backups)), and `scripts/backup.sh` makes a local dump. To upgrade, follow [Code updates](deploy/README.md#code-updates); don't rerun `deploy/deploy.sh` on a live install, because it can recreate the database.

## Install and use

Paste this into Claude Code, Codex, Cursor or similar:

```text
Install crypto-data-warehouse for me by following
https://raw.githubusercontent.com/holarchic/crypto-data-warehouse/v0.26.1/docs/agent-install.md
Ask me before anything that costs money, opens a public port, or deletes data.
```

The agent asks whether you want a laptop trial or a server, writes `.env` (asking you for an alert webhook), starts the stack and checks that candles are landing. A server install follows [`docs/agent-install.md`](docs/agent-install.md) and [`deploy/README.md`](deploy/README.md).

**Laptop steps.** Run them in order; don't move on until a step's check passes.

1. **Prerequisites.** `docker compose version` reports v2, and `lsof -i :5432 -i :8000` prints nothing.
2. **Configure.**
   ```bash
   git clone https://github.com/holarchic/crypto-data-warehouse
   cd crypto-data-warehouse
   cp .env.example .env     # set POSTGRES_PASSWORD
   ```
   Check: `POSTGRES_PASSWORD` in `.env` isn't empty. Ask the user for a Discord or Slack webhook for `WEBHOOK_URL`; leave it empty if they have none.
3. **Start.** `docker compose up -d db && ./scripts/migrate.sh && docker compose up -d`. Check: `curl -s localhost:8000/health` prints `{"status":"ok"}`.
4. **First data.** Within about five minutes, the `curl` at the top of this page returns one BTC bar.
5. **Stop and ask the user** before either of these:
   - **More assets:** `docker compose --profile discovery run --rm discovery` adds every asset over the volume threshold, about 720 on the reference install (see Requirements for the disk that takes). Check: `curl -s "localhost:8000/candles/symbols?exchange=binance" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))'` prints more than 1.
   - **HyperLiquid liquidation history:** a separate daily job (`--profile liquidations-hl`) that reads a requester-pays S3 bucket. It needs AWS credentials in `.env`, and AWS bills the transfer.

> [!CAUTION]
> This compose file publishes Postgres (5432) and the API (8000) on every interface. On a server, use the production override instead. It keeps Postgres off the network, binds the API to localhost and sets memory limits. [`deploy/README.md`](deploy/README.md) covers provisioning, firewall, cron and updates.

## Development

Needs [uv](https://docs.astral.sh/uv/). Neither command needs a database or network.

```bash
uv run --extra dev ruff check .    # prints "All checks passed!"
uv run --extra dev pytest -q       # ends "N passed, 2 skipped" with no failures
```

CI runs the same two commands. Conventions and PR rules: [`CONTRIBUTING.md`](CONTRIBUTING.md).

## API

| Endpoint | Returns |
|---|---|
| `GET /candles` | OHLCV, filtered by `exchange`, `asset_id`, `timeframe`, `start`, `end`, `limit` |
| `GET /candles/exchanges` · `/timeframes` · `/symbols` | What's being collected |
| `GET /funding` · `/funding/latest` | Funding rates |
| `GET /oi` · `/oi/latest` | Open interest |
| `GET /liquidations` | Recent liquidations |
| `GET /status` · `/status/gaps` | Collector health and gaps |
| `GET /export/parquet` | HyperLiquid context, Binance open interest, HyperLiquid liquidations or Binance 1h premium index for a time window, as parquet. Longer spans: `scripts/export-feed.py` |
| `GET /health` | Liveness |

Example queries: [`deploy/README.md`](deploy/README.md#api-queries). Reading the data for analysis or a backtest (row semantics, long pulls, joins without lookahead): [`docs/query-patterns.md`](docs/query-patterns.md).

## Configuration

- `collector/exchanges.yaml`: exchanges, timeframes, rate limits, discovery thresholds.
- `.env`: Postgres credentials, alert webhook and thresholds, optional AWS credentials. See `.env.example`.

## Disclaimer

The data is provided as is, without warranty, and may contain gaps or errors. Nothing here is financial advice. You're responsible for following each exchange's terms of service and rate limits.

## Contributing and security

Contributions are welcome: see [`CONTRIBUTING.md`](CONTRIBUTING.md). Report vulnerabilities privately: see [`SECURITY.md`](SECURITY.md).

## Attribution

For those who care about such things, the majority of the code in this repo was produced by Claude Opus 4.8 (Anthropic), with david guiding Claude, providing insights, direction and critical review throughout the process.

## License

[MIT](LICENSE)
