# candle-collector Architecture

## Components

### collector/

Fetches OHLCV candles, funding rates, and open interest from exchanges. Writes directly to PostgreSQL. One container per exchange (6 total), one thread per timeframe + optional funding thread + optional OI thread per container. Config in `exchanges.yaml` (`has_funding` / `has_oi` flags per exchange).

`collect_loop` calls `maybe_reload_markets()` (in `db.py`) at the top of each cycle. The helper is age-gated (`MARKETS_RELOAD_INTERVAL_S`, default 86400s) and lock-guarded so the per-timeframe threads sharing one ccxt `exchange` reload at most once per interval. Without this, symbols added by daily discovery stay invisible to the long-running collector container.

The funding and OI threads upsert a per-loop **liveness heartbeat** each cycle (`status.upsert_loop_heartbeat`): a sentinel `collection_status` row with `symbol = "*"` and a pseudo-timeframe from `config.LOOP_INTERVALS` (`funding` / `oi`). These threads write no per-pair candle status, so without the heartbeat a silently-dead funding/OI thread produces no staleness footprint; the monitor flags the sentinel row when it goes stale.

The OI loop (`oi.py`) dispatches each `has_oi` exchange to one of **three strategies** (`oi_loop`; an unrecognized OI venue raises rather than defaulting), all cadences from `ExchangeConfig` (`oi_poll_interval_s` / `oi_settlement_window_s` / `oi_baseline_interval_s`):

- **HyperLiquid** (`_oi_loop_hyperliquid`): one bulk `metaAndAssetCtxs` POST every **1 min** — matching the backtester's Grade-B seed cadence — **dual-writing** OI to `open_interest` plus the full per-asset context bundle (funding/premium/oracle/mark/mid/impact/prev-day/day-volume) to `hl_asset_ctxs`, both under one transaction (`_insert_hl_snapshot`) so the two tables never diverge on a mid-cycle failure. `_build_hl_snapshots` owns the response boundary: it validates the positional `universe[i]`↔`asset_ctxs[i]` contract (length mismatch → abort the cycle) and applies a **parse-or-drop** rule per asset (`_to_float`/`_parse_hl_ctx` raise `_CtxParseError` on a present-but-malformed field, wrong-container `impactPxs`, or non-dict row → that asset is skipped and logged; legitimately-absent fields → SQL NULL).
- **Bybit/OKX** (`_oi_loop_bulk`): one bulk call/cycle at a flat **30 s** — Bybit `fetch_tickers` (OI + native USD read from `ticker["info"]`, parse-or-drop), OKX `fetch_open_interests` (contracts; USD from a `_fetch_price_map` since the bulk endpoint omits `oiUsd`).
- **Binance** (`_oi_loop_binance`, the only per-symbol-expensive venue): a **two-source** loop. A 5-min **baseline** via `openInterestHist` on the separate `/futures/data` weight bucket (`BINANCE_OI_HIST_WEIGHT=0`, self-paced by `BINANCE_OI_HIST_SPACING_S`) — cursor-driven (`_binance_baseline_sweep`): the cursor is DB-seeded from 5-min-aligned rows only (`SEED_BASELINE_CURSOR_SQL`, so dense rows can't poison it), `since`-based so a restart backfills the gap, advanced only after commit, and gated on a successful seed. Plus a sub-5-min **dense** `openInterest` (main weight pool) for symbols within ±`oi_settlement_window_s` of their own settlement — the window from a bulk `premiumIndex` (`nextFundingTime` trigger + `markPx` for USD; the post-settlement half via `nextFundingTime`-change detection, no hardcoded hours). Dense and baseline rows coexist in `open_interest` by distinct timestamps. Cross-tick state is pruned to the active set each cycle. Since Binance has no bulk instantaneous OI, the dense fetch is per-symbol; at a mass settlement (~541 symbols aligned) a sequential loop overruns the 60 s tick, so it runs on `_DenseFetchPool` — K worker threads (`oi_dense_concurrency`) each borrowing their **own** ccxt exchange from a checkout queue (isolated `requests.Session`/`last_response_headers`, no shared-session bet; the global `WeightRateLimiter` still gates all — budget is per-IP), workers returning rows the main thread collects (single writer). The pool is built lazily with per-cycle retry (transient init self-heals; degraded = sequential that cycle) and refreshes its exchanges' markets age-gated. `oi_dense_concurrency: 1` is the pure-sequential rollback.

**Shared-limiter coupling:** the Binance dense burst shares this container's `WeightRateLimiter` (unprioritised) with candle collection; the baseline is kept off the main pool and the dense tick is 60 s to bound peak OI weight — candle freshness is prod-gated (see SPEC failure mode).

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

### `hl_asset_ctxs` hypertable

HyperLiquid-only per-asset context bundle (full `metaAndAssetCtxs`), 1-min cadence,
1-week chunks. Columns mirror the backtester Grade-B seed (`hl-asset-ctxs.parquet`) 1:1 so
the forward tape is a schema-faithful continuation. PK: `(asset_id, timestamp)`.

| Column | Type | Notes |
|--------|------|-------|
| asset_id | TEXT NOT NULL | HL universe name = seed `coin` |
| symbol | TEXT NOT NULL | ccxt-normalized |
| timestamp | TIMESTAMPTZ NOT NULL | Poll time, UTC (= seed `time_ms`) |
| funding, open_interest, prev_day_px, day_ntl_vlm, premium | DOUBLE PRECISION | |
| oracle_px, mark_px, mid_px, impact_bid_px, impact_ask_px | DOUBLE PRECISION | nullable; missing → NULL |
