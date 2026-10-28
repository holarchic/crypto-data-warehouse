# crypto-data-warehouse — Ops & Deployment

## Environment

- Python 3.12+
- PostgreSQL 16 + TimescaleDB
- Docker + Docker Compose 2.24.4 or later (the prod override's `!override`; the api build's named context needs 2.17), with BuildKit, the default builder
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
| Start all | `docker compose up -d db && ./scripts/migrate.sh && docker compose up -d` (the `&&` keeps the services down if a migration fails) |
| Stop all | `docker compose down` |
| Run discovery | `docker compose --profile discovery run --rm discovery` |
| Logs (one exchange) | `docker compose logs -f collector-hyperliquid` |
| Logs (all collectors) | `docker compose logs -f collector-hyperliquid collector-binance collector-bybit collector-okx collector-coinbase collector-dydx` |
| Logs (API) | `docker compose logs -f api` |
| Logs (monitor) | `docker compose logs -f monitor` |
| DB shell | `docker compose exec db psql -U candles candle_collector` |
| Migrations | `./scripts/migrate.sh --status` shows what is applied and pending; `./scripts/migrate.sh` applies the pending ones in order, each in one transaction with its `schema_migrations` row. A new schema change is a new `db/migrations/NNN-<name>.sql`; `db/init.sql` is frozen at the 001–013 baseline |
| Backup DB (local dump) | `./scripts/backup.sh` |
| Restore DB (local dump) | Stop the collectors, api and monitor, then `./scripts/restore.sh backups/<file>.dump`. It recreates `candle_collector` from the dump with the Disaster recovery steps: a fresh database, a strict restore, and migration 013 before the scheduler restarts. Then `./scripts/migrate.sh --status` (`--adopt` first for a dump taken before the ledger) and `./scripts/migrate.sh`, before starting the services again |
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
| Export one window | `curl -o premium.parquet "http://localhost:8000/export/parquet?table=binance_premium_index&start=2026-10-01T00:00:00Z&end=2026-10-02T00:00:00Z"` |
| Export a long span | `python3 scripts/export-feed.py --api http://localhost:8000 --table hl_asset_ctxs --start 2026-07-01T00:00:00Z --end 2026-10-01T00:00:00Z --out hl-asset-ctxs.parquet` (needs `pyarrow`) |

### DB-direct inspection

| Action | Command |
|--------|---------|
| Collection status | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, timeframe, error_count, last_success FROM collection_status ORDER BY 1,2"` |
| Discovery pairs | `docker compose exec db psql -U candles candle_collector -c "SELECT count(DISTINCT asset_id), count(*) FROM collection_status WHERE active=TRUE"` |
| Funding counts | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, count(*) FROM funding_rates GROUP BY 1"` |
| OI counts | `docker compose exec db psql -U candles candle_collector -c "SELECT exchange, count(*) FROM open_interest GROUP BY 1"` |

## Integration smoke test

```bash
docker compose up -d db && ./scripts/migrate.sh && docker compose up -d && sleep 320 && curl -s "http://localhost:8000/candles?exchange=hyperliquid&asset_id=BTC&timeframe=5m&limit=5" | python3 -m json.tool
```

Pass: returns ≥1 candle with all OHLCV fields populated, correct exchange/symbol/timeframe.

## Production

- **Server:** one VPS. The reference install is a Hetzner CX33 (x86, 4 vCPU, 8 GB RAM, 80 GB disk) plus a Hetzner Cloud Volume mounted at `/mnt/pgvol` for the 1m candles tablespace
- **API access:** SSH tunnel, `ssh -L 8000:localhost:8000 <server>`, then query `localhost:8000`
- **Project dir:** `/opt/crypto-data-warehouse`

### Production commands

| Action | Command |
|--------|---------|
| Start prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d db && ./scripts/migrate.sh && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d` |
| Stop prod | `docker compose -f docker-compose.yml -f docker-compose.prod.yml down` |
| Prod logs | `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f` |
| Code update | `git pull` → `./scripts/migrate.sh --status` (apply where the migration's ROLLOUT says) → `build <changed svcs>` → `up -d --no-deps <svcs>` (see Code updates below) |
| Fresh install / full redeploy | `./deploy/deploy.sh` — pulls `latest-pg16` + stack-wide `up -d`; can recreate the db |
| Server setup | `./deploy/setup.sh [CLIENT_IP...]` |
| Run discovery (prod) | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile discovery run --rm --no-deps discovery` |
| Offsite backup now | `./scripts/backup-offsite.sh` (see Offsite backups) |
| List offsite snapshots | `(. scripts/offsite-lib.sh; restic_run snapshots)` |
| Funding repair | see Funding repair below |
| HL 1m repair, manual window | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile hl-candles-repair run --rm --no-deps hl-candles-repair python -m hl_candles_repair --hours N` (1–72; moves the cursor only when it covers the required range) |

### Code updates

- Code updates never touch the db container. `git pull`, then `./scripts/migrate.sh --status`: apply a pending migration (`./scripts/migrate.sh`) at the point its ROLLOUT note says, before or after the rebuild. Then `build` only the services whose code or config changed, including the profile one-shots `discovery` and `liquidations-hl` (they bake in `collector/` and `exchanges.yaml`), then `up -d --no-deps <svc…>`. A one-shot `run` on the server takes `--no-deps` too: without it, `run` recreates the db when its service definition changed since the db started.
- **Releases.** `pyproject.toml` holds the release version, and the api reports it on `/openapi.json`. A deploy that rebuilds images bumps it (minor for a feature, patch for a fix) and tags the deployed commit `vX.Y.Z`. Every release rebuilds and recreates `api` too, collector-only ones included, since the api image carries the version and the collector's `config.py`, `feed_health.py` and `exchanges.yaml`. After the deploy, `curl -s localhost:8000/openapi.json | jq -r .info.version` on the server prints the tag's version.
- Deploy mid-interval (e.g. xx:20 – xx:40), away from the 00/08/16 UTC funding settlements.
- Restart the HyperLiquid collector only outside a repair run: not while `hl-candles-repair` is running (`docker ps`), not within 1 min of its exit, and not from 06:15 until the 06:30 cron run exits. Its first funding poll reads each pair from its newest stored settlement, one 21-weight `fundingHistory` call per pair (six for a pair with no row in 7 days), and shares HL's per-IP weight budget with the repair.
- `deploy/deploy.sh` is for fresh installs and full redeploys only: its `compose pull db` and stack-wide `up -d` can recreate the db onto a moved `latest-pg16`.

### DB tuning

TimescaleDB auto-tunes `max_connections` low (~25 on 8GB). Override via `command: ["-c", "max_connections=150"]` in `docker-compose.yml`. Current usage: ~68 connections (61 collector threads + API pool + monitor + internal).

### Firewall

- **Provider firewall** (primary, e.g. Hetzner Cloud Firewall): inbound SSH; keep port 8000 closed
- **API bind:** `127.0.0.1:8000` (`docker-compose.prod.yml`). Docker-published ports bypass UFW, so the loopback bind — not UFW — is what keeps the API off the public interface
- **UFW** (defense-in-depth): SSH only
- **Local access:** SSH tunnel — no firewall rule needed

### Cron jobs (installed by `deploy/install-cron.sh`)

`deploy.sh` runs it. It keeps the jobs as one marked block in the crontab, so a re-run replaces that block (and any unmarked entries an older install left) and keeps every other entry.

| Schedule | Job |
|----------|-----|
| `0 2 * * *` | Daily discovery |
| `0 4 * * *` | Daily HL Grade-A liquidation pull (last 3 complete days) |
| `30 5 * * *` | Nightly offsite backup (does nothing until configured, see below) |
| `30 6 * * *` | Daily HL 1m repair from REST (a ~6 h trickle at 20 weight/min; see HL 1m repair) |
| `*/5 * * * *` | Host monitor (`/var/log/candle-collector-host-monitor.log`) |

## HL 1m repair

`python -m hl_candles_repair` replaces recent HyperLiquid 1m bars with REST's closed values: the WebSocket feed can drop minutes or stop short of a minute's last trades, and REST only keeps ~3.5 days. It shares HL's per-IP request budget with the collector, so it trickles at 20 weight a minute.

- **Logs:** `/var/log/candle-collector-hl-candles-repair.log`. The `DONE` line counts inserted (missing minutes filled) and updated (unclosed bars corrected) bars, incomplete coins, 429s and 5xx. A 429 makes the run exit 1; a 5xx that a retry got past doesn't.
- **Paging:** the monitor pages when the `candle_repair` sentinel has no success for 36 h. A span that aged past REST's history before it could be repaired is posted to `WEBHOOK_URL`; the cursor moves past it only once that post is delivered.
- **After a failure** nothing is needed: the next run reaches back to the last repaired minute (up to 72 h).
- **Building:** it bakes `collector/`, so rebuild it with `--profile hl-candles-repair build hl-candles-repair` after a code update.
- **Turning it off:** remove the cron line, stop a running one (`docker ps --filter name=hl-candles-repair -q | xargs -r docker stop`), and deactivate its sentinel so it stops paging: `UPDATE collection_status SET active = FALSE WHERE exchange = 'hyperliquid' AND symbol = '*' AND timeframe = 'candle_repair'`. To turn it back on, set `active = TRUE` again and restore the cron line.

## Funding repair

`python -m funding_repair` rebuilds stored funding from each venue's settled history, one venue per run. It is for rows the collector wrote before it stored settled history. It reports by default and writes only with `--apply`.

> [!CAUTION]
> `--apply` deletes and rewrites a venue's funding rows. Save the venue's rows first (step 3); rows older than a venue's history (OKX keeps about 92 days) are deleted, not rebuilt.

1. **Pick `--until`.** Note the time D the release that stores settled history was deployed. `--until` is the first whole hour at or after D (e.g. `2026-10-27T13:00:00Z`). Run no earlier than 2 h after it, and pass the same value to every run below.
2. **Report.** After a code update, build it first: `"${DC[@]}" --profile funding-repair build funding-repair`.
   ```bash
   . scripts/ops-lib.sh
   "${DC[@]}" --profile funding-repair run --rm --no-deps funding-repair --exchange binance --until <until>
   ```
   One line per symbol, then a total with the `until` used. `unfetchable` symbols (no market, or no history) are left as they are.
3. **Save the venue's rows** on the host, with their count and checksum:
   ```bash
   "${DB_TOOL[@]}" psql -X -c "COPY (SELECT * FROM funding_rates WHERE exchange = 'binance' AND timestamp < '<until>'::timestamptz + interval '1 second' ORDER BY symbol, timestamp) TO STDOUT WITH CSV" > backups/funding-pre-repair-binance-<until>.csv
   cd backups && sha256sum funding-pre-repair-binance-<until>.csv > funding-pre-repair-binance-<until>.csv.sha256; wc -l funding-pre-repair-binance-<until>.csv; cd ..
   ```
4. **Apply:** step 2's command with `--apply`. It exits 1 if any symbol's fetch failed; those symbols are untouched. A database error stops it; rerun with `--after <last symbol logged>`. HyperLiquid makes one call a minute and waits out the HL 1m repair, so start it in the background (`nohup … &`) and expect it to run across two or three nights. Don't start a manual HL 1m repair while it runs.
5. **Verify:** step 2's command again. `differ`, `missing`, `unmatched` and `tail_unmatched` are 0 and no symbol is `failed`. On OKX, rows that aged out of its history since the apply show as `pre_depth` and `interval_unverified`.
6. **Back up:** `./scripts/backup.sh`, then `./scripts/backup-offsite.sh`, and check the new snapshot (Offsite backups).

**Rolling one venue back** restores step 3's file and leaves newer rows, other venues and other tables alone:

```bash
saved=backups/funding-pre-repair-binance-<until>.csv
(cd backups && sha256sum -c "${saved##*/}.sha256") &&
{ echo "BEGIN; DELETE FROM funding_rates WHERE exchange = 'binance' AND timestamp < '<until>'::timestamptz + interval '1 second';"
  echo "COPY funding_rates FROM STDIN WITH CSV;"
  cat "$saved" || exit 1          # a failed read never reaches COMMIT: psql rolls the delete back
  echo '\.'; echo "COMMIT;"
} | "${DB_TOOL[@]}" psql -X -v ON_ERROR_STOP=1
```

The checksum check runs first, so a missing or changed file stops before anything is deleted. Then export the same range as in step 3 and compare it with the saved file (`sha256sum`).

## Stability scorecard

`./scripts/stability-scorecard.sh [--days N]` measures the roadmap's v0.8 stability gate from the stored data. It is read-only. It exits 1 when any measured check fails and prints `SCORECARD: PASS (measured checks only)` or `SCORECARD: FAIL`.

- **When.** Run it mid-interval, away from the 00/08/16 UTC settlements and the 06:30 repair. The default `--days 14` scans two weeks of candles, funding and OI, under a 15-minute statement timeout. Time a `--days 1` run before the first full one. During a full run, watch the newest stored 1m bar (`/candles?exchange=binance&asset_id=BTC&timeframe=1m&limit=1`, now minus its time). Stop the run (Ctrl-C) if that lag passes 1,200 s.
- **Config consistency.** The intervals, threshold and venue lists come from the running monitor's config. Every running collector must carry the same `config.py` and `exchanges.yaml`. A `config consistent: FAIL` names the collectors built from another commit. Rebuild them before reading the rest.
- **Database checks.** These cover the active universe only; a pair deactivated before the run doesn't count. Each pair's window opens at its first discovery, and a slot counts once it should be stored: a bar 1 h after it closes, a funding settlement 2 h after it happens, an OI bucket 1 h after it ends.
  - **Candle capture** (≥ 99.9 %): stored bars against expected bars, by venue and timeframe. HL 1m is streamed and not covered. A venue whose API omits untraded bars (Coinbase) shows them as missed, so check the per-venue rows before reading a FAIL as loss.
  - **Funding-slot occupancy** (≥ 99 %): slots at each pair's stored interval that hold a settlement. A pair whose interval changed inside the window is named and not measured; a pair with no row in the window counts as missed.
  - **OI bucket occupancy** (≥ 95 %): 60 s buckets on HyperLiquid and 300 s elsewhere that hold a snapshot.
  - **Primary keys** on `candles`, `candles_1m`, `funding_rates` and `open_interest`.
  - **Discovery:** the latest run is dated today or yesterday (UTC), and every asset it qualified is active on some venue.
  - A pair re-activated inside the window (a weekend volume dip) counts its inactive time as missed.
- **API p95.** 50 requests for 500 BTC 5m rows, each of which must return all 500. Under 0.5 s passes.
- **Not measured:**
  - funding rows against the venue's own history (the funding repair's report checks them);
  - per-venue discovery (discovery stores each asset's cross-venue maximum);
  - auto-recovery and gap-detection latency;
  - HL 1m;
  - the Bybit/OKX 30 s and Binance settlement-window OI density.

## Offsite backups

`scripts/backup-offsite.sh` streams `pg_dump` from the db container into a [restic](https://restic.net) repository in a bucket every night at 05:30 UTC. That slot is clear of discovery, the HL pull and the compression jobs. On Backblaze B2, restic reaches the bucket through [rclone](https://rclone.org)'s native B2 backend; B2's S3 endpoint can cut off reads of intact files.

- **No local copy:** the dump never touches local disk.
- **Encrypted:** restic encrypts the repository with its password.
- **Dedup:** the dump is uncompressed, so restic dedups the unchanged history night to night.
- **Retention:** 7 daily, 5 weekly and 12 monthly snapshots, then `restic check`.
- **Read-back:** Sunday runs also read back a quarter of the stored data, a different quarter each week, so every file in the bucket is read within about 4 weeks.
- **Paging:** a failed run pages through `WEBHOOK_URL`. `host-monitor.sh` pages when the last success is more than 26 h old.
- **RPO:** about 24 h.

### Setup

1. **Bucket.** Create a private bucket for the backups alone. Add a lifecycle rule that deletes hidden (noncurrent) versions after 30 days.
2. **Key.** Create a key restricted to that bucket that can list, read and write but **not delete file versions**. On Backblaze B2 that's `listBuckets,listFiles,readFiles,writeFiles`.
   - restic removes files by hiding them, so prune and lock cleanup work. Over rclone that holds because `restic_run` (`scripts/offsite-lib.sh`) starts rclone without restic's default `--b2-hard-delete`. Run every manual restic command through `restic_run`, as below: plain `restic` over rclone can't remove its own lock with this key.
   - A stolen key can hide files but can't destroy versions, which leaves 30 days to recover.
3. **Credentials.** Write them to `/etc/candle-collector/` (directory `700`, files `600`), outside the checkout:

   ```bash
   # /etc/candle-collector/restic.env: one NAME=value per line, LF line endings
   RESTIC_REPOSITORY=rclone:b2:<bucket>/candle-collector
   RESTIC_PASSWORD_FILE=/etc/candle-collector/restic-password
   RCLONE_CONFIG_B2_TYPE=b2
   RCLONE_CONFIG_B2_ACCOUNT=<key id>
   RCLONE_CONFIG_B2_KEY=<key>
   ```

   The `RCLONE_CONFIG_B2_*` lines define rclone's `b2` remote, so no rclone config file is needed. For another S3-compatible store, use `RESTIC_REPOSITORY=s3:https://<s3-endpoint>/<bucket>/candle-collector` with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` instead. The dump command never sees any variable this file sets.

   `restic-password` holds a long random password (`openssl rand -base64 48`). **Keep a copy of the password and repository off the server**, in a password manager. A lost server takes its copy with it, and without the password the backups can't be read.
4. **Install restic and rclone, and initialise the repository:**

   ```bash
   ./deploy/install-restic.sh
   ./deploy/install-rclone.sh   # pinned: Ubuntu's rclone can't authorize a bucket-restricted B2 key
   (. scripts/offsite-lib.sh; restic_run init)
   ./scripts/backup-offsite.sh   # the first run, by hand
   ```
5. **Cron.** Run `deploy/install-cron.sh`. `host-monitor.sh` pages "never succeeded" until the first run completes.

### Restore drill

`scripts/restore-drill.sh` restores the latest snapshot into a throwaway TimescaleDB container on any machine with Docker, restic, rclone (a current release: Ubuntu 24.04's 1.60 can't authorize a bucket-restricted B2 key) and a copy of the env file, and never touches prod. It:

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
# The scripts take these from scripts/ops-lib.sh; here they are spelled out to paste by hand.
DC=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
# One restic command with the restore credentials, through the backup's own restic_run.
restic_env() { (RESTIC_ENV_FILE=/root/restore/restic.env; . scripts/offsite-lib.sh; restic_run "$@"); }
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
   - Run `./deploy/install-restic.sh` and `./deploy/install-rclone.sh`.
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
   - `./scripts/migrate.sh --status`. A snapshot taken before the ledger reports `pre-ledger`: run `./scripts/migrate.sh --adopt`, which records 001–013 only if `db/baseline-check.sql` passes, and stop to reconcile any check it names. Then `./scripts/migrate.sh` applies anything newer, before any consumer starts.
7. **Bring it back.**
   - Build and start the rest with `"${DC[@]}" build` and `up -d --no-deps <services>`.
   - Run `deploy/install-cron.sh`.
   - Move the credentials to `/etc/candle-collector/` and fix `RESTIC_PASSWORD_FILE`.
   - Run `./scripts/backup-offsite.sh` once, then delete the dump.
   - Never run `compose pull db` or `deploy.sh` on this host unless you mean to move TimescaleDB versions.

**Stuck repository lock.** Each run clears stale locks itself. Before a manual `(. scripts/offsite-lib.sh; restic_run unlock --remove-all)`, check that no backup or drill is running: `pgrep restic` on the server, and any machine running a drill.

**Read-back failure.** A Sunday page names the quarter that failed, as `--read-data-subset n/4`. A later night's success refreshes the success marker without rereading that quarter, so the page is not closed by it.
1. Fetch each failing pack through the bucket's native API and compare its SHA-256 with its file name. A match means the transport cut the read off, not that the pack is damaged; repair nothing.
2. Close the page only when `(. scripts/offsite-lib.sh; restic_run check --read-data-subset n/4)` rereads that quarter clean.

**Compromised server.**
1. Revoke the bucket key first.
2. With the account's master key, restore each file's last version from before the compromise. Unhiding isn't enough if files were overwritten.
3. Issue a new key with the same restrictions, and put it in an env file on a trusted machine (not the compromised server).
4. Run `(RESTIC_ENV_FILE=<that env file>; . scripts/offsite-lib.sh; restic_run check --read-data)`, which flags any altered pack, since restic's file names are content hashes.
