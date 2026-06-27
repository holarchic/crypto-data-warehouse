# candle-collector Architecture

## Components

### collector/

Fetches OHLCV candles, funding rates, and open interest from exchanges. Writes directly to PostgreSQL. One container per exchange (6 total), one thread per timeframe + optional funding thread + optional OI thread per container. Config in `exchanges.yaml` (`has_funding` / `has_oi` flags per exchange).

`collect_loop` calls `maybe_reload_markets()` (in `db.py`) at the top of each cycle. The helper is age-gated (`MARKETS_RELOAD_INTERVAL_S`, default 86400s) and lock-guarded so the per-timeframe threads sharing one ccxt `exchange` reload at most once per interval. Without this, symbols added by daily discovery stay invisible to the long-running collector container.

The funding and OI threads upsert a per-loop **liveness heartbeat** each cycle (`status.upsert_loop_heartbeat`): a sentinel `collection_status` row with `symbol = "*"` and a pseudo-timeframe from `config.LOOP_INTERVALS` (`funding` / `oi`). These threads write no per-pair candle status, so without the heartbeat a silently-dead funding/OI thread produces no staleness footprint; the monitor flags the sentinel row when it goes stale.

### monitor (collector/monitor.py)

Separate always-on container (`python -m monitor`). One `collection_status` fetch per cycle (`fetch_status_rows`) feeds two independent alert signals plus gap detection:

- **Loop-liveness** — presence-based: any stale funding/OI heartbeat sentinel row pages (own cooldown key). A single dead loop is one row, so it is kept off the count-delta signal below.
- **Candle alarming-mass** — `compute_alarming` counts collectors that are *alarming* (`error_count > 0`, or was-working-now-dark past `4×interval`; excludes the benign new-listing floor + the HL 5m within-cycle cursor). `evaluate_alarming` pages when that count exceeds its rolling-1h median by `ALARMING_DELTA_THRESHOLD` for `ALARMING_SUSTAIN_CYCLES` consecutive cycles, returning a named `AlarmingDecision` that owns the record-only-quiescent baseline invariant.
- **Gap detection** (tiered by timeframe) + a daily heartbeat summary + a per-cycle healthcheck dead-man's-switch ping.

### api/

FastAPI read-only REST API. Queries PostgreSQL, serves consumers on GCP.

### db/

Schema migrations and TimescaleDB hypertable setup. Candles hypertable partitioned on timestamp, 1-week chunks. Compression (>7d) and retention policies (2y candles, 1y funding/OI).

## Data Flow

```
Exchange API → Collector → Normalize (symbol, timestamp UTC) → PostgreSQL
                                                                    ↓
Consumer ← FastAPI ← Query ← PostgreSQL
```

## Data Model

### `candles` hypertable

PK: `(exchange, market_type, symbol, timeframe, timestamp)`

| Column | Type | Notes |
|--------|------|-------|
| exchange | TEXT NOT NULL | 6 exchanges active |
| market_type | TEXT NOT NULL | `perp` or `spot` |
| asset_id | TEXT NOT NULL | Canonical: `BTC` |
| symbol | TEXT NOT NULL | ccxt-normalized: `BTC/USDT:USDT` |
| timeframe | TEXT NOT NULL | 10 timeframes: 5m→1w |
| timestamp | TIMESTAMPTZ NOT NULL | Candle open time, UTC |
| open, high, low, close | DOUBLE PRECISION | |
| volume | DOUBLE PRECISION | Base currency |
| quote_volume | DOUBLE PRECISION | Nullable |
| trades | INTEGER | Nullable |

### `collection_status`

PK: `(exchange, market_type, symbol, timeframe)` — health tracking + discovery metadata per pair.
Also holds per-loop **liveness heartbeats** for the funding/OI threads: sentinel rows with
`symbol = "*"` and `timeframe ∈ {funding, oi}` (one per exchange/loop). Candle-facing readers
(`query_active_pairs`, the `/candles/*` and `/status` API endpoints) exclude these by pseudo-timeframe;
the monitor's staleness check is the only consumer that acts on them.

| Column | Type | Notes |
|--------|------|-------|
| asset_id | TEXT | Canonical base asset, set by discovery + collectors |
| native_symbol | TEXT | Exchange-native symbol ID |
| last_24h_volume_usd | DOUBLE PRECISION | From discovery's `fetch_tickers()` |
| discovered_at | TIMESTAMPTZ | When first discovered |
| deactivated_at | TIMESTAMPTZ | When dropped below threshold |
| active, last_timestamp, last_success, error_count, last_error | — | Health tracking |

### `funding_rates` hypertable

PK: `(exchange, symbol, timestamp)` — 1-month chunks.

| Column | Type | Notes |
|--------|------|-------|
| exchange | TEXT NOT NULL | |
| asset_id | TEXT NOT NULL | Canonical: `BTC` |
| symbol | TEXT NOT NULL | ccxt-normalized |
| timestamp | TIMESTAMPTZ NOT NULL | Funding settlement time |
| rate | DOUBLE PRECISION | Funding rate |
| predicted_rate | DOUBLE PRECISION | Next predicted rate (nullable) |
| interval_hours | INTEGER | 1 (HL), 8 (others) |

### `open_interest` hypertable

PK: `(exchange, symbol, timestamp)` — 1-month chunks.

| Column | Type | Notes |
|--------|------|-------|
| exchange | TEXT NOT NULL | |
| asset_id | TEXT NOT NULL | Canonical: `BTC` |
| symbol | TEXT NOT NULL | ccxt-normalized |
| timestamp | TIMESTAMPTZ NOT NULL | Snapshot time |
| open_interest | DOUBLE PRECISION | Base currency units |
| oi_value_usd | DOUBLE PRECISION | USD notional value |
