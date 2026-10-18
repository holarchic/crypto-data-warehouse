# crypto-data-warehouse — Ops & Deployment

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
| Backup DB (local dump) | `./scripts/backup.sh` |
| Restore DB (local dump) | Stop the collectors, api and monitor, then `./scripts/restore.sh backups/<file>.dump`. It recreates `candle_collector` from the dump with the Disaster recovery steps: a fresh database, a strict restore, and migration 013 before the scheduler restarts |
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
- **Project dir:** `/opt/crypto-data-warehouse`

### Production commands

| Action | Command |
|--------|---------|
| Start prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d` |
| Stop prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml down` |
| Prod logs | `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f` |
| Code update | `git pull` → `build <changed svcs>` → `up -d --no-deps <svcs>` (see Code updates below) |
| Fresh install / full redeploy | `./deploy/deploy.sh` — pulls `latest-pg16` + stack-wide `up -d`; can recreate the db |
| Server setup | `./deploy/setup.sh [CLIENT_IP...]` |
| Run discovery (prod) | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile discovery run --rm discovery` |
| Offsite backup now | `./scripts/backup-offsite.sh` (see Offsite backups) |
| List offsite snapshots | `(set -a; . /etc/candle-collector/restic.env; set +a; restic snapshots)` |

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
| `30 5 * * *` | Nightly offsite backup (does nothing until configured, see below) |
| `*/5 * * * *` | Host monitor |

## Offsite backups

`scripts/backup-offsite.sh` streams `pg_dump` from the db container into a [restic](https://restic.net) repository in an S3-compatible bucket every night at 05:30 UTC. That slot is clear of discovery, the HL pull and the compression jobs.

- **No local copy:** the dump never touches local disk.
- **Encrypted:** restic encrypts the repository with its password.
- **Dedup:** the dump is uncompressed, so restic dedups the unchanged history night to night.
- **Retention:** 7 daily, 5 weekly and 12 monthly snapshots, then `restic check`.
- **Paging:** a failed run pages through `WEBHOOK_URL`. `host-monitor.sh` pages when the last success is more than 26 h old.
- **RPO:** about 24 h.

### Setup

1. **Bucket.** Create a private bucket for the backups alone. Add a lifecycle rule that deletes hidden (noncurrent) versions after 30 days.
2. **Key.** Create a key restricted to that bucket that can list, read and write but **not delete file versions**. On Backblaze B2 that's `listBuckets,listFiles,readFiles,writeFiles`.
   - restic's S3 backend removes files by hiding them, so prune and lock cleanup work.
   - A stolen key can hide files but can't destroy versions, which leaves 30 days to recover.
3. **Credentials.** Write them to `/etc/candle-collector/` (directory `700`, files `600`), outside the checkout:

   ```bash
   # /etc/candle-collector/restic.env
   RESTIC_REPOSITORY=s3:https://<s3-endpoint>/<bucket>/candle-collector
   RESTIC_PASSWORD_FILE=/etc/candle-collector/restic-password
   AWS_ACCESS_KEY_ID=<key id>
   AWS_SECRET_ACCESS_KEY=<key>
   ```

   `restic-password` holds a long random password (`openssl rand -base64 48`). **Keep a copy of the password and repository off the server**, in a password manager. A lost server takes its copy with it, and without the password the backups can't be read.
4. **Install restic and initialise the repository:**

   ```bash
   ./deploy/install-restic.sh
   (set -a; . /etc/candle-collector/restic.env; set +a; restic init)
   ./scripts/backup-offsite.sh   # the first run, by hand
   ```
5. **Cron.** Add the cron line from `deploy.sh`. `host-monitor.sh` pages "never succeeded" until the first run completes.

### Restore drill

`scripts/restore-drill.sh` restores the latest snapshot into a throwaway TimescaleDB container on any machine with Docker, restic and a copy of the env file, and never touches prod. It:

- runs the strict restore;
- checks that job owners match table owners, a restored compression job runs, and a new `candles_1m` chunk lands in `ts_1m`;
- prints row counts from `scripts/drill-counts.sql` over a window.

Run the same SQL on prod just before and just after the backup being drilled, with the bounds the drill prints. Each restored count must fall between the two.

```bash
RESTIC_ENV_FILE=~/.config/candle-collector/restic.env DRILL_DIR=<base dir with ~25 GB free> scripts/restore-drill.sh
```

### Disaster recovery

The drill's fresh-target steps on a new host. Do **not** use `deploy.sh` or `scripts/restore.sh` here. `deploy.sh` starts every writer and re-pulls `latest-pg16`. `scripts/restore.sh` runs these steps for a local dump on a running local stack; on a new host, follow them by hand.

```bash
DC=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
# Loads the credentials for one restic command only.
restic_env() { (set -a; . /root/restore/restic.env; set +a; exec restic "$@"); }
# SQL as the candles superuser; pass -d postgres or -d candle_collector.
db_sql() { "${DC[@]}" exec -T db psql -X -v ON_ERROR_STOP=1 -U candles "$@"; }
```

**Credentials.** Keep them in `/root/restore/` until step 7. A `/etc/candle-collector/restic.env` would let the 05:30 job back up a half-restored database. Keep every writer stopped until step 7.

1. **Checkout and storage first.**
   - Clone the repo into the project dir and `cd` into it. Every command below runs from there.
   - Run `./deploy/setup.sh`, then write `.env` from `.env.example`. New secrets are fine.
   - Mount the volume at `/mnt/pgvol` (fstab) before anything starts the db container. Docker's bind mount doesn't pick up a mount made after the container starts.
   - Create empty `/mnt/pgvol/pgdata_1m` and `/mnt/pgvol/restore`, owned by uid 70 (the image's `postgres`).
2. **Credentials and snapshot.**
   - Run `./deploy/install-restic.sh`.
   - Copy the off-server `restic.env` and `restic-password` into `/root/restore/`, with `RESTIC_PASSWORD_FILE` pointing there. A new bucket key from the account works too; only the password must be the original.
   - Pick the snapshot with `restic_env snapshots --host candle-collector --latest 1`. Note its ID and its `timescaledb-<ver>` and `pg<major>` tags.
3. **Image before first start.** On first start, the image installs TimescaleDB in every database at its own version, and the restore needs the snapshot's.
   - `docker pull timescale/timescaledb:<ver>-pg<major>`
   - `docker tag timescale/timescaledb:<ver>-pg<major> timescale/timescaledb:latest-pg16`
4. **Start only the db and stage the dump.**
   - `"${DC[@]}" up -d db`
   - Wait for the real server. The image's first-start init server is up briefly on the socket only, so check over TCP:
     `for _ in $(seq 90); do "${DC[@]}" exec -T db pg_isready -q -h 127.0.0.1 -U candles -d postgres && break; sleep 2; done`
     Then rerun that `pg_isready` once on its own; it must succeed before going on.
   - `db_sql -d postgres -c "CREATE TABLESPACE ts_1m LOCATION '/mnt/pgvol/pgdata_1m'"` creates the tablespace, once; it outlives any database.
   - `restic_env dump <snapshot id> /candle_collector.dump > /mnt/pgvol/restore/candle_collector.dump` puts the dump on the volume, which the db container sees at the same path.
5. **Fresh database.** If anything below fails, start again here.
   - `db_sql -d postgres -c 'DROP DATABASE IF EXISTS candle_collector' -c 'CREATE DATABASE candle_collector TEMPLATE template0'`
   - `db_sql -d candle_collector -c "CREATE EXTENSION timescaledb VERSION '<ver>'" -c 'SELECT timescaledb_pre_restore()'`
6. **Restore.**
   - `"${DC[@]}" exec -T db pg_restore -U candles -d candle_collector --exit-on-error /mnt/pgvol/restore/candle_collector.dump`
   - `db_sql -d candle_collector < db/migrations/013-keep-all-history.sql`, then check that `SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention'` returns 0. A snapshot taken before 013 brings its retention jobs back, and `post_restore` restarts the scheduler that runs them.
   - `db_sql -d candle_collector -c 'SELECT timescaledb_post_restore()'`
7. **Bring it back.**
   - Build and start the rest with `"${DC[@]}" build` and `up -d --no-deps <services>`.
   - Add the cron lines from `deploy.sh`.
   - Move the credentials to `/etc/candle-collector/` and fix `RESTIC_PASSWORD_FILE`.
   - Run `./scripts/backup-offsite.sh` once, then delete the dump.
   - Never run `compose pull db` or `deploy.sh` on this host unless you mean to move TimescaleDB versions.

**Stuck repository lock.** Each run clears stale locks itself. Before a manual `restic unlock --remove-all`, check that no backup or drill is running: `pgrep restic` on the server, and any machine running a drill.

**Compromised server.**
1. Revoke the bucket key first.
2. With the account's master key, restore each file's last version from before the compromise. Unhiding isn't enough if files were overwritten.
3. Run `restic check --read-data`, which flags any altered pack, since restic's file names are content hashes.
4. Issue a new key.
