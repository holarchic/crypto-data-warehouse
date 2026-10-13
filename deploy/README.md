# candle-collector — Ops & Deployment

## Environment

- Python 3.12+
- PostgreSQL 16 + TimescaleDB
- Docker + Docker Compose
- Copy `.env.example` → `.env` and set `POSTGRES_PASSWORD`

### Monitor env vars

| Var | Purpose |
|-----|---------|
| `WEBHOOK_URL` | Discord/Slack webhook for staleness, gap, and heartbeat messages |
| `HEALTHCHECK_PING_URL` | Optional uptime-monitor URL hit every cycle (dead-man's switch — e.g. healthchecks.io) |
| `HEARTBEAT_HOUR_UTC` | UTC hour for the weekly status heartbeat. Default `0` |
| `HEARTBEAT_DAY_UTC` | UTC weekday for the weekly status heartbeat: `0`=Mon … `6`=Sun. Default `0` (Monday) |
| `MONITOR_INTERVAL_S` | Monitor cycle interval. Default `300` |
| `ALERT_COOLDOWN_S` | Dedup window for staleness/gap alerts. Default `3600` |
| `ALARMING_DELTA_THRESHOLD` | Alarming-collector count must exceed its rolling-1h median by this much to page. Default `25` |
| `ALARMING_SUSTAIN_CYCLES` | Consecutive breaching cycles required before the alarming alert fires. Default `3` — settlement rate-limit waves breach for ≤2 cycles |
| `ALARMING_ERROR_THRESHOLD` | Consecutive `error_count` a collector must reach to enter the alarming count via the error channel. Default `3` — screens out transient single-cycle rate-limit blips that backfill losslessly |

## Local Dev Commands

| Action | Command |
|--------|---------|
| Start all | `docker compose up -d` |
| Stop all | `docker compose down` |
| Run discovery | `docker compose --profile discovery run --rm discovery` |
| Logs (one exchange) | `docker compose logs -f collector-hyperliquid` |
| Logs (all collectors) | `docker compose logs -f collector-hyperliquid collector-binance collector-bybit collector-okx collector-coinbase collector-dydx` |
| Logs (API) | `docker compose logs -f api` |
| Logs (monitor) | `docker compose logs -f monitor` |
| DB shell | `docker compose exec db psql -U candles candle_collector` |
| Apply migration | `docker compose exec db psql -U candles candle_collector < db/migrations/004-compression-retention.sql` |
| Backup DB | `./scripts/backup.sh` |
| Restore DB | `./scripts/restore.sh backups/<file>.dump` |
| Verify compression | `./scripts/verify-compression.sh` |

## API Queries

| Action | Command |
|--------|---------|
| API health | `curl http://localhost:8000/health` |
| Query candles | `curl "http://localhost:8000/candles?exchange=hyperliquid&asset_id=BTC&timeframe=5m&limit=5"` |
| List exchanges | `curl http://localhost:8000/candles/exchanges` |
| List timeframes | `curl http://localhost:8000/candles/timeframes` |
| List symbols | `curl http://localhost:8000/candles/symbols` |
| Symbols (exchange) | `curl "http://localhost:8000/candles/symbols?exchange=binance"` |
| Query funding | `curl "http://localhost:8000/funding?exchange=binance&asset_id=BTC&limit=5"` |
| Latest funding | `curl "http://localhost:8000/funding/latest?asset_id=BTC"` |
| Query OI | `curl "http://localhost:8000/oi?exchange=binance&asset_id=BTC&limit=5"` |
| Latest OI | `curl "http://localhost:8000/oi/latest?asset_id=BTC"` |
| Collection status | `curl http://localhost:8000/status` |
| Status (filtered) | `curl "http://localhost:8000/status?exchange=binance&timeframe=5m"` |
| Gap check | `curl "http://localhost:8000/status/gaps?exchange=binance&timeframe=5m"` |

### DB-direct inspection

| Action | Command |
|--------|---------|
| Collection status | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, timeframe, error_count, last_success FROM collection_status ORDER BY 1,2"` |
| Discovery pairs | `docker compose exec db psql -U candles candle_collector -c "SELECT count(DISTINCT asset_id), count(*) FROM collection_status WHERE active=TRUE"` |
| Funding counts | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, count(*) FROM funding_rates GROUP BY 1"` |
| OI counts | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, count(*) FROM open_interest GROUP BY 1"` |

## Integration smoke test

```bash
docker compose up -d && sleep 320 && curl -s "http://localhost:8000/candles?exchange=hyperliquid&asset_id=BTC&timeframe=5m&limit=5" | python3 -m json.tool
```

Pass: returns ≥1 candle with all OHLCV fields populated, correct exchange/symbol/timeframe.

## Production

- **Server:** one VPS. The reference install is a Hetzner CX33 (x86, 4 vCPU, 8 GB RAM, 80 GB disk) plus a Hetzner Cloud Volume mounted at `/mnt/pgvol` for the 1m candles tablespace
- **API access:** SSH tunnel, `ssh -L 8000:localhost:8000 <server>`, then query `localhost:8000`
- **Project dir:** `/opt/candle-collector`

### Production commands

| Action | Command |
|--------|---------|
| Start prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d` |
| Stop prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml down` |
| Prod logs | `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f` |
| Code update | `git pull` → `build <changed svcs>` → `up -d --no-deps <svcs>` (see Code updates below) |
| Fresh install / full redeploy | `./deploy/deploy.sh` — pulls `latest-pg16` + stack-wide `up -d`; can recreate the db |
| Server setup | `./deploy/setup.sh [GCP_IP...]` |
| Run discovery (prod) | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile discovery run --rm discovery` |

### Code updates

- Code updates never touch the db container. `git pull`, then `build` only the services whose code or config changed, including the profile one-shots `discovery` and `liquidations-hl` (they bake in `collector/` and `exchanges.yaml`), then `up -d --no-deps <svc…>`.
- Deploy mid-interval (e.g. xx:20 – xx:40), away from the 00/08/16 UTC funding settlements.
- `deploy/deploy.sh` is for fresh installs and full redeploys only: its `compose pull db` and stack-wide `up -d` can recreate the db onto a moved `latest-pg16`.

### DB tuning

TimescaleDB auto-tunes `max_connections` low (~25 on 8GB). Override via `command: ["-c", "max_connections=150"]` in `docker-compose.yml`. Current usage: ~68 connections (61 collector threads + API pool + monitor + internal).

### Firewall

- **Provider firewall** (primary, e.g. Hetzner Cloud Firewall): inbound SSH; keep port 8000 closed
- **API bind:** `127.0.0.1:8000` (`docker-compose.prod.yml`). Docker-published ports bypass UFW, so the loopback bind — not UFW — is what keeps the API off the public interface
- **UFW** (defense-in-depth): SSH only
- **Local access:** SSH tunnel — no firewall rule needed

### Cron jobs (installed by `deploy.sh`)

| Schedule | Job |
|----------|-----|
| `0 2 * * *` | Daily discovery |
| `0 4 * * *` | Daily HL Grade-A liquidation pull (last 3 complete days) |
| `*/5 * * * *` | Host monitor |
