# candle-collector Architecture

## Components

### collector/

Fetches OHLCV candles, funding rates, and open interest from exchanges. Writes directly to PostgreSQL. One container per exchange (6 total), one thread per timeframe + optional funding thread + optional OI thread per container. Config in `exchanges.yaml` (`has_funding` / `has_oi` flags per exchange).

Candle fetching lives in `main.py` (`collect_loop` → `_collect_pair` → `fetch_candles`); request-size policy is factored into `candles.py` (`poll_limit` + `CANDLE_LIMIT_*`), a pure module so the weight diet is unit-testable (see Shared-limiter coupling below).

`collect_loop` calls `maybe_reload_markets()` (in `db.py`) at the top of each cycle. The helper is age-gated (`MARKETS_RELOAD_INTERVAL_S`, default 86400s) and lock-guarded so the per-timeframe threads sharing one ccxt `exchange` reload at most once per interval. Without this, symbols added by daily discovery stay invisible to the long-running collector container.

The funding, OI, and liquidation-WS threads upsert a per-loop **liveness heartbeat** (`status.upsert_loop_heartbeat`): a sentinel `collection_status` row with `symbol = "*"` and a pseudo-timeframe from `config.LOOP_INTERVALS` (`funding` / `oi` / `liq_ws`). The poll loops fire it each cycle; the WS loop fires it on a ~60 s connection timer while the socket is up (a quiet feed is healthy). These threads write no per-pair candle status, so without the heartbeat a silently-dead loop produces no staleness footprint; the monitor flags the sentinel row when it goes stale.

The OI loop (`oi.py`) dispatches each `has_oi` exchange to one of **three strategies** (`oi_loop`; an unrecognized OI venue raises rather than defaulting), all cadences from `ExchangeConfig` (`oi_poll_interval_s` / `oi_settlement_window_s` / `oi_baseline_interval_s`):

- **HyperLiquid** (`_oi_loop_hyperliquid`): one bulk `metaAndAssetCtxs` POST every **1 min** — matching the backtester's Grade-B seed cadence — **dual-writing** OI to `open_interest` plus the full per-asset context bundle (funding/premium/oracle/mark/mid/impact/prev-day/day-volume) to `hl_asset_ctxs`, both under one transaction (`_insert_hl_snapshot`) so the two tables never diverge on a mid-cycle failure. `_build_hl_snapshots` owns the response boundary: it validates the positional `universe[i]`↔`asset_ctxs[i]` contract (length mismatch → abort the cycle) and applies a **parse-or-drop** rule per asset (`_to_float`/`_parse_hl_ctx` raise `_CtxParseError` on a present-but-malformed field, wrong-container `impactPxs`, or non-dict row → that asset is skipped and logged; legitimately-absent fields → SQL NULL).
- **Bybit/OKX** (`_oi_loop_bulk`): one bulk call/cycle at a flat **30 s** — Bybit `fetch_tickers` (OI + native USD read from `ticker["info"]`, parse-or-drop), OKX `fetch_open_interests` (contracts; USD from a `_fetch_price_map` since the bulk endpoint omits `oiUsd`).
- **Binance** (`_oi_loop_binance`, the only per-symbol-expensive venue): a **two-source** loop. A 5-min **baseline** via `openInterestHist` on the separate `/futures/data` weight bucket (`BINANCE_OI_HIST_WEIGHT=0`, self-paced by `BINANCE_OI_HIST_SPACING_S`) — cursor-driven (`_binance_baseline_sweep`): the cursor is DB-seeded from 5-min-aligned rows only (`SEED_BASELINE_CURSOR_SQL`, so dense rows can't poison it), `since`-based so a restart backfills the gap, advanced only after commit, and gated on a successful seed. Plus a sub-5-min **dense** `openInterest` (main weight pool) for symbols within ±`oi_settlement_window_s` of their own settlement — the window from a bulk `premiumIndex` (`nextFundingTime` trigger + `markPx` for USD; the post-settlement half via `nextFundingTime`-change detection, no hardcoded hours). Dense and baseline rows coexist in `open_interest` by distinct timestamps. Cross-tick state is pruned to the active set each cycle. Since Binance has no bulk instantaneous OI, the dense fetch is per-symbol; at a mass settlement (~541 symbols aligned) a sequential loop overruns the 60 s tick, so it runs on `_DenseFetchPool` — K worker threads (`oi_dense_concurrency`) each borrowing their **own** ccxt exchange from a checkout queue (isolated `requests.Session`/`last_response_headers`, no shared-session bet; the global `WeightRateLimiter` still gates all — budget is per-IP), workers returning rows the main thread collects (single writer). The pool is built lazily with per-cycle retry (transient init self-heals; degraded = sequential that cycle) and refreshes its exchanges' markets age-gated. `oi_dense_concurrency: 1` is the pure-sequential rollback.

**Shared-limiter coupling:** the Binance dense burst shares this container's `WeightRateLimiter` with candle collection (the budget is per-IP). `build_rate_limiter(config)` is the single construction seam (weight vs interval limiter + reserve wiring). A **priority reserve lane** (`rate_limit_priority_reserve`) stops the dense burst blocking on the coincident candle-kline burst: the dense per-symbol `fetch_open_interest` + its `premiumIndex` trigger `acquire(priority=True)` and may draw up to `budget − safety`, while candles/funding self-limit to `budget − safety − reserve`. The reserve is sized to the dense weight-**rate** at a mass settlement (in-window symbols × OI-weight ≈ 543/min), **not** the instantaneous in-flight count — the binding constraint is the server's rolling-minute used-weight. Two things keep the burst from pinning it: (1) the reserve (600) exceeds that rate, so `candle_cap + dense < priority_cap`; (2) a **candle weight diet** — `_collect_pair` sizes the Poll-path kline `limit` to the actual gap via `candles.poll_limit` (only on weight-bound venues, `rate_limit_weight_budget` set), so a caught-up poll costs Binance weight 1 not 5 and the mass-settlement candle burst is ~5× smaller; Seed still fetches `CANDLE_SEED_LIMIT`. Weight-0 calls (the `openInterestHist` baseline on the separate `/futures/data` bucket) bypass the cap entirely. An over-large reserve (≥ half the usable cap) is neutralized to 0 with a warning; `_oi_loop_binance` logs an **edge-triggered runtime** warning when a cycle's dense weight (`len(dense) × weight`) exceeds the reserve band, and logs `used_snapshot()` (server used-weight + in-flight) per dense cycle as direct evidence the burst stays under the priority cap. The dense tick is 60 s to bound peak OI weight; candle freshness stays the prod re-verify gate (see SPEC failure mode).

### collector/ws.py — streaming engine (Binance + Bybit liquidations live)

`ws_loop` is a generic threaded WebSocket engine driven by a `WSFeedSpec` injection record, so it carries **zero venue branches** — the four planned feeds (Binance/Bybit × liquidations/mark) reuse one engine. Venue behavior (URL, topic mapping, sub/unsub frames, message parser, batch insert, app-ping, proactive-reconnect) is entirely spec-injected. The engine owns:

- **Connection lifecycle** via `websocket-client` (`create_connection` + an explicit recv loop with a 1 s socket timeout so ping/heartbeat/resubscribe timers are serviced even on a silent feed). A `recv` timeout is quiet; `recv()==""` with `not connected` is a graceful server close.
- **Reconnect/backoff** on a WS-local retriable set (`WS_RETRIABLE` — `backoff.RETRIABLE_EXCEPTIONS` is ccxt/psycopg-shaped and would never catch a WS drop), reusing only `backoff_sleep`. The in-session DB reconnect (`_ensure_conn`) is **single-shot/non-blocking** so a Postgres blip never freezes the recv loop. Only a *proactive* reconnect skips backoff; a *server-initiated* close backs off (no tight-spin).
- **Subscription management** — initial subscribe + diff-based resubscribe when discovery changes the active-pair set; venue callables own their own arg-cap batching. The subscribed set is cached in `_WSRuntime.topics` and reused on a reconnect that lands during a DB outage (so a per-symbol feed isn't dropped to zero).
- **Batch insert** (`spec.insert_batch`, flushed by size or ~1 s) with the buffer owned by `ws_loop` so it survives a reconnect, bounded at `WS_MAX_BUFFER` (drop oldest) on a sustained DB outage.
- **Connection-timer liveness heartbeat** (`liq_ws` pseudo-TF, same sentinel path as funding/oi) — fires while the socket is up, *not* per event, since a quiet feed is healthy.

`_run_session` (one connection) is factored from `ws_loop` (the reconnect wrapper) for testability; `connect_fn`/`clock` are injected. `run()` launches a `ws_loop` thread per `has_liquidation_ws` exchange (the same daemon-thread-per-loop model as the poll loops).

### collector/liquidations.py — Binance + Bybit liquidation feed specs (Grade C)

The venue data for `ws.py` (the engine stays venue-agnostic). Two `WSFeedSpec` builders + `build_liq_spec(venue)` (the resolver `run()` calls; raises on an unknown venue). Binance `!forceOrder@arr` is one all-market stream on the **routed `/market` path** (an unrouted `/ws` gets only Public data); Bybit `allLiquidation.{native}` is per-symbol, batched-subscribe, 20 s ping. **asset_id resolution:** `parse_message` receives only the raw frame, so each builder closes over a shared `native_to_asset` map that `topics_for_pairs` rebuilds (engine-called at subscribe + resub) and `parse_message` reads — no lock, because `_run_session` calls both sequentially on the one feed thread, subscribe before recv. Binance's all-market stream is filtered to cc's tracked pairs (untracked → no map entry → skipped). Side normalized to `liquidated_side ∈ {long, short}` across the venues' opposite conventions. Grade-C insert (`ON CONFLICT` inert via NULLS-DISTINCT — every event lands). One Bybit connection holds the universe under the 21 k-char args ceiling; a char-based guard warns before a growth breach (sharding deferred).

**Collector concurrency invariant:** each loop (candle per-TF, funding, OI, WS) runs on its own daemon thread with its **own DB connection and direct writes** to disjoint tables — there is no single-writer queue, by design (one container per exchange, no cross-loop write contention). The WS engine conforms: it buffers events and batch-flushes on its own connection.

### monitor (collector/monitor.py)

Separate always-on container (`python -m monitor`). One `collection_status` fetch per cycle (`fetch_status_rows`) feeds two independent alert signals plus gap detection:

- **Loop-liveness** — presence-based: any stale per-loop heartbeat sentinel row (funding/OI/liq_ws) pages (own cooldown key). A single dead loop is one row, so it is kept off the count-delta signal below.
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
Also holds per-loop **liveness heartbeats** for the funding/OI/liquidation-WS threads: sentinel rows with
`symbol = "*"` and `timeframe ∈ {funding, oi, liq_ws}` (one per exchange/loop). Candle-facing readers
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

### `liquidations` hypertable

All-venue liquidation events, two fidelity grades: HL `node_fills` (Grade A, untruncated tape, `.3`) + Binance/Bybit WS (Grade C, forward directional sidecar, no historical seed, `.2`). **One** table (not per-venue) — a superset of the HL seed's 13 columns + `venue`/`grade`/`liquidated_side`; HL rows fill all seed columns, Grade-C rows leave the HL-specific ones NULL. 1-week chunks; compress after 7 days segmentby `venue, asset_id`; retention 1 year. `asset_id` holds the seed's `coin` value (T-011 export aliases it). Full column list in SPEC.

**Dedup (the one structural subtlety):** a UNIQUE INDEX `(venue, asset_id, timestamp, oid, tid)`, **not** a PK — a PK is NOT NULL but `oid`/`tid` are NULL for Grade-C rows, and a hypertable needs the partition column in any unique key. HL rows carry non-null `oid`/`tid`, so distinct pairs preserve real same-ms clustered events (the backtester's structural hash is duplicate-sensitive) while an exact re-pull dedups via `ON CONFLICT DO NOTHING` (idempotency for the daily HL batch). Grade-C rows (NULL `oid`/`tid`, Postgres NULLS DISTINCT) never conflict → all land. The index's `(venue, asset_id, timestamp)` prefix serves range queries + the export. Table exists (migration 008); the writers are `.2`/`.3`.
