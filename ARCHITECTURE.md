# candle-collector Architecture

## Components

### collector/

Fetches OHLCV candles, funding rates, and open interest from exchanges. Writes directly to PostgreSQL. One container per exchange (6 total), one thread per timeframe + optional funding thread + optional OI thread per container. Config in `exchanges.yaml` (`has_funding` / `has_oi` flags per exchange).

Candle fetching lives in `main.py` (`collect_loop` → `_collect_pair` → `fetch_candles`); request-size policy is factored into `candles.py` (`poll_limit` + `CANDLE_LIMIT_*`), a pure module so the weight diet is unit-testable (see Shared-limiter coupling below).

**Candle-table routing (T-015):** which table a timeframe's candles land in is a data-driven property — `table:` in `exchanges.yaml` (default `candles`; `1m` → `candles_1m`). `config._validate_candle_table` is the single validation seam (both `_parse_exchange` and `load_candle_tables` call it, so a bad `table:` fails once at load in the collector AND monitor processes); `TimeframeConfig.table` / `config.load_candle_tables()` is the single routing source, consumed by the collector insert (`main.INSERT_SQL`, a dict keyed by table, built at import from the closed `CANDLE_TABLES` allowlist via one template — no runtime identifier interpolation), the monitor gap-scan (`monitor.GAP_SQL_BY_TABLE`), and — mirrored, since `api/` can't import `collector/` — the API (`_candle_table`, pinned to config by an exact-equality drift test). No hardcoded `1m` branch anywhere.

`collect_loop` calls `maybe_reload_markets()` (in `db.py`) at the top of each cycle. The helper is age-gated (`MARKETS_RELOAD_INTERVAL_S`, default 86400s) and lock-guarded so the per-timeframe threads sharing one ccxt `exchange` reload at most once per interval. Without this, symbols added by daily discovery stay invisible to the long-running collector container.

**DB-restart resilience:** every loop owns one long-lived connection and reconnects at the top of each cycle on `if conn.closed:` (`closed` == `pgconn.status == BAD`; psycopg detects a server-dropped socket only when the conn is next exercised). The hazard is the error handler itself: a `conn.rollback()` or a status/insert write on a just-dropped connection re-raises `OperationalError`, and unhandled that kills the loop thread (root cause of two total outages). So every error-path rollback goes through `db.safe_rollback` (swallows), and the only DB *writes* reachable on a restart raise — `funding._poll_bulk`'s bulk insert and the candle per-pair status write (`main._record_pair_error`) — are individually wrapped; a live-conn pre-check is not protection because the write itself can raise. With no exception able to escape a loop body, the next cycle's `if conn.closed:` reconnects and collection self-heals within ~1 cycle. `ws.py` already had this shape (`_ensure_conn` + a swallowing `_guarded_rollback`).

The funding, OI, premium, liquidation-WS, and HL candle-WS threads upsert a per-loop **liveness heartbeat** (`status.upsert_loop_heartbeat`): a sentinel `collection_status` row with `symbol = "*"` and a pseudo-timeframe from `config.LOOP_INTERVALS` (`funding` / `oi` / `liq_ws` / `premium` / `candle_ws`). The poll loops fire it each cycle; the WS loops fire it on a ~60 s connection timer while the socket is up (a quiet feed is healthy). These threads write no per-pair candle status, so without the heartbeat a silently-dead loop produces no staleness footprint; the monitor flags the sentinel row when it goes stale.

The OI loop (`oi.py`) dispatches each `has_oi` exchange to one of **three strategies** (`oi_loop`; an unrecognized OI venue raises rather than defaulting), all cadences from `ExchangeConfig` (`oi_poll_interval_s` / `oi_settlement_window_s` / `oi_baseline_interval_s`):

- **HyperLiquid** (`_oi_loop_hyperliquid`): one bulk `metaAndAssetCtxs` POST every **1 min** — matching the backtester's Grade-B seed cadence — **dual-writing** OI to `open_interest` plus the full per-asset context bundle (funding/premium/oracle/mark/mid/impact/prev-day/day-volume) to `hl_asset_ctxs`, both under one transaction (`_insert_hl_snapshot`) so the two tables never diverge on a mid-cycle failure. `_build_hl_snapshots` owns the response boundary: it validates the positional `universe[i]`↔`asset_ctxs[i]` contract (length mismatch → abort the cycle) and applies a **parse-or-drop** rule per asset (`_to_float`/`_parse_hl_ctx` raise `_CtxParseError` on a present-but-malformed field, wrong-container `impactPxs`, a non-dict row, or a non-string `name` → that asset is skipped and logged; legitimately-absent fields → SQL NULL). The response names each asset by HL's **native** coin name (`kPEPE` for 1000× tokens); the active-pair map is keyed on cc's **ccxt-canonical** `asset_id` (`KPEPE`), so the lookup bridges via `name.upper()` (HL names are uppercase but for the leading `k`) — without it every 1000× token silently missed the lookup. The two tables store `asset_id` by their role: `open_interest` keeps the **ccxt-canonical** id (cross-venue grouping), while `hl_asset_ctxs` stores the **HL-native name** (= the backtester seed `coin`, so the T-011 export is a passthrough) — they differ only for 1000× tokens, and nothing joins the two on `asset_id`.
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

`_run_session` (one connection) is factored from `ws_loop` (the reconnect wrapper) for testability; `connect_fn`/`clock` are injected. `run()` launches a `ws_loop` thread per `has_liquidation_ws` exchange **and** per `has_candle_ws` exchange (HL 1m candles — see `hl_candles.py`), the same daemon-thread-per-loop model as the poll loops.

### collector/liquidations.py — Binance + Bybit liquidation feed specs (Grade C)

The venue data for `ws.py` (the engine stays venue-agnostic). Two `WSFeedSpec` builders + `build_liq_spec(venue)` (the resolver `run()` calls; raises on an unknown venue). Binance `!forceOrder@arr` is one all-market stream on the **routed `/market` path** (an unrouted `/ws` gets only Public data); Bybit `allLiquidation.{native}` is per-symbol, batched-subscribe, 20 s ping. **asset_id resolution:** `parse_message` receives only the raw frame, so each builder closes over a shared `native_to_asset` map that `topics_for_pairs` rebuilds (engine-called at subscribe + resub) and `parse_message` reads — no lock, because `_run_session` calls both sequentially on the one feed thread, subscribe before recv. Binance's all-market stream is filtered to cc's tracked pairs (untracked → no map entry → skipped). Side normalized to `liquidated_side ∈ {long, short}` across the venues' opposite conventions. Grade-C insert (`ON CONFLICT` inert via NULLS-DISTINCT — every event lands). One Bybit connection holds the universe under the 21 k-char args ceiling; a char-based guard warns before a growth breach (sharding deferred).

### collector/liquidations_hl.py — HyperLiquid Grade-A liquidation daily pull (T-009.3)

A one-shot batch (`python -m liquidations_hl`, run by cron via a profile-gated `liquidations-hl` compose service, mirroring `discovery`) — NOT a `ws.py` feed: `node_fills` is an hourly S3 tape, not a stream. Pulls HL forced-liquidation events from the requester-pays `s3://hl-mainnet-node-data` (scoped read-only IAM key `cc-node-fills-ro`, creds via the compose `${AWS_*:?}` env) into `liquidations` (`venue='hyperliquid'`, `grade='a'`, all 16 columns). Ports the backtester's proven extractor: two on-disk formats across the `2025-07-27` cutover (`_prefix_for`/`_iter_fills`), **streaming lz4 decompress** (1 MiB chunks + rolling-buffer line split, so a peak cascade hour never lands whole), and the **`liquidatedUser==addr` dedup-at-source** rule (a liquidation fill appears on both sides — emit once, from the liquidated side). `_row` maps to the 16 cc columns with **per-event parse-or-drop** (bad identity → skip+count; bad magnitude → NULL; `oid`/`tid` DROP-not-default — they are the dedup key), `liquidated_side` from `dir`, ms→epoch-seconds for `to_timestamp`, and **HL-native `asset_id`** (`kPEPE`, matching the seed + `hl_asset_ctxs`, so the T-011 export is a passthrough — NOT canonicalized). `_stream_insert` flushes in `BATCH_SIZE` chunks; `ON CONFLICT (venue,asset_id,timestamp,oid,tid) DO NOTHING` (idempotent re-pull; distinct `oid`/`tid` at one ms preserve clustered cascade events). Default pulls the last 3 complete UTC days (`default_days`, bounded idempotent lookback); `--start/--end` for backfill. **Error model:** object-level failures (DB `OperationalError` / S3 GET / corrupt-lz4 `RuntimeError`) and day-level list failures are counted + isolated (run continues, exits non-zero); a `<24`-object day counts as a failure (surfaces an incomplete day); a run-fatal DB outage (`_DBUnavailable` = `connect_db` exhaustion) → clean exit 1; deterministic DB faults (`ProgrammingError`/`DataError`) + logic bugs abort loudly.

### collector/premium_index.py — Binance premium-index poll (T-010)

Binance-only boundary-scheduled 5-min poll of `premiumIndexKlines` (ccxt `fetch_premium_index_ohlcv`) into `premium_index` — the perp mark-vs-index spread (funding term structure), a conditioning input for downstream backtests. Reuses the candle poll's shape: gap-aware `since` cursor + the T-007.4 weight diet (`candles.poll_limit` ⇒ a caught-up poll costs kline weight 1, not 5) + the T-014 error discipline (`safe_rollback` on every error path). The per-symbol cursor is seeded from the table so a restart resumes from the gap; **any active symbol missing from the in-memory map is re-seeded from the DB before polling** (so a reactivation resumes from its last stored bar, never a forward-only skip), and advances only after commit. First run is **forward-only** (a few recent bars via `PREMIUM_SEED_LIMIT`, not the 1000-bar candle seed — deep history is backfillable via T-011, so the empty-table rollout carries no weight spike). Stays **non-priority** on the shared `WeightRateLimiter` (yields to candles + the dense-OI reserve — premium is neither the primary product nor unbackfillable). Cadence (`PREMIUM_TIMEFRAME` / `PREMIUM_INTERVAL_S`) is single-sourced in `config.py` — the ccxt timeframe, the incomplete-bar guard, and the `premium` monitor staleness floor (`LOOP_INTERVALS`) all derive from it. `premium_index_loop` **raises fast** on a venue without `fetchPremiumIndexOHLCV` (Binance-only capability guard, matching `oi_loop` / `build_liq_spec`). Launched behind `has_premium_index`; the active-symbol universe comes from `query_active_pairs(…, "5m")` (the candle-TF anchor, not the poll cadence).

### collector/hl_candles.py — HyperLiquid 1-minute candles (WS-only, T-015.1)

HL 1m into the live `candles_1m`, **with zero live REST** — HL's ~1 req/s info budget is reserved for the unbackfillable dense 1-min OI, and a live REST backfill was deploy-measured to starve HL's tight single limiter (5m…1w per-bar coverage 161→33; HL has no priority-reserve lane). So this is a single thread behind `has_candle_ws`:

- **WS candle feed** — a `WSFeedSpec` on `ws.py` (zero REST), one connection for the whole ~161-coin universe. `topics_for_pairs` resolves each pair to its **HL WS coin name** via ccxt `market['info']['name']` (`kPEPE`/`kBONK`/`kSHIB` for 1000× tokens — subscribing the wrong-cased `KPEPE` makes HL drop the *entire* connection; `KAITO`/`KAS` stay uppercase), rebuilt each resub so a new listing is picked up as ccxt reloads. The shared map is keyed on the HL name (routes the frame's `s`) and carries cc's canonical `asset_id`; `native_symbol` (a numeric index) is unused here (no lock, the liquidations pattern); the frame builders wrap each id in HL's `{method,subscription:{type:candle,coin,interval:1m}}` JSON; `parse_message` turns a `{channel:candle,data}` frame into a `candles_1m` row and returns `[]` for control frames (`subscriptionResponse`/`pong`/non-candle). The insert is an **UPSERT** (`ON CONFLICT … DO UPDATE`, hl_candles owns its own SQL) because HL streams the forming bar continuously with no closed flag, so the row converges to the closed value (last-write-wins) — never the generic `DO NOTHING`. `ping_frame` + `ping_interval_s=30` (HL closes idle sockets at 60 s). Loop sentinel `candle_ws`.

A WS-drop gap (no history replay on reconnect) is repaired **out-of-band by a daily REST refetch off the collector's limiter** (T-019, not yet built), not a live-contending sweep. HL's REST serves only the last ~5,000 bars (~3.5 days at 1m), so the repair must land inside that window. The WS emits only traded minutes — a thin coin's no-trade minute has no row by design.

**Streamed, not polled:** HL 1m is deliberately absent from `supported_timeframes`, so `run()` launches **no** per-symbol REST 1m `collect_loop` (which would starve the OI) and discovery writes no per-pair 1m status rows. Liveness is the `candle_ws` loop sentinel. `/candles?timeframe=1m` + `/status/gaps?timeframe=1m` are table-routed so HL 1m is queryable; the `collection_status`-driven metadata endpoints (`/candles/timeframes`, `/candles/symbols`) do not list it.

**Collector concurrency invariant:** each loop (candle per-TF, funding, OI, premium, WS-candle, WS-liq) runs on its own daemon thread with its **own DB connection and direct writes** to disjoint tables — there is no single-writer queue, by design (one container per exchange, no cross-loop write contention). The WS engine conforms: it buffers events and batch-flushes on its own connection.

### collector/discovery.py — daily universe selection

A one-shot (`python -m discovery`, cron 02:00 UTC via the profile-gated `discovery` compose service). Fetches perp tickers per exchange, keeps the highest-volume symbol per `asset_id`, then `qualify_assets` (pure, unit-tested) applies union qualification with hysteresis: an asset enters when its max 24h volume across venues reaches `volume_threshold_usd` and, once active (`status.query_active_asset_ids`), stays while that max holds at `volume_keep_threshold_usd`. Qualifying assets are upserted on every venue that lists them; pairs that drop out are deactivated (`active=FALSE`, history kept).

### monitor (collector/monitor.py)

Separate always-on container (`python -m monitor`). One `collection_status` fetch per cycle (`fetch_status_rows`) feeds two independent alert signals plus gap detection:

- **Loop-liveness** — presence-based: any stale per-loop heartbeat sentinel row (funding/OI/liq_ws/premium/candle_ws) pages (own cooldown key). A single dead loop is one row, so it is kept off the count-delta signal below. The `candle_ws` sentinel is HL 1m's liveness surface (it is streamed, not per-pair-status tracked).
- **Candle alarming-mass** — `compute_alarming` counts collectors that are *alarming* (`error_count >= ALARMING_ERROR_THRESHOLD`, default 3, or was-working-now-dark past `4×interval`; excludes the benign new-listing floor, the HL 5m within-cycle cursor, and transient single-cycle rate-limit blips whose `error_count` resets on the next success). `evaluate_alarming` pages when that count exceeds its rolling-1h median by `ALARMING_DELTA_THRESHOLD` for `ALARMING_SUSTAIN_CYCLES` consecutive cycles, returning a named `AlarmingDecision` that owns the record-only-quiescent baseline invariant.
- **Gap detection** (tiered by timeframe, **routed to the timeframe's candle table** via `load_candle_tables()` so 1m gaps scan `candles_1m`, not the 1m-empty `candles`) + a weekly heartbeat summary (24h candle count summed across `candles` + `candles_1m`) + a per-cycle healthcheck dead-man's-switch ping.

### api/

FastAPI read-only REST API. Queries PostgreSQL, serves downstream consumers. Cannot import `collector/`, so any collector-shared convention is mirrored + drift-tested (`_candle_table`, `LOOP_TIMEFRAMES`).

**Parquet export (`GET /export/parquet`, T-011)** — streams a feed's window as parquet for the backtester's forward-ingest, which reads the raw file and validates each feed by a structural hash (`feed_provenance.rs`: name-sorted schema + commutative per-row digest sum, so column/row order don't affect it — but names/types and per-cell content, including the null-mask, do). The byte-contract lives in one pure module, `api/export.py` (pyarrow only — no fastapi/psycopg, so it unit-tests against real pyarrow): `FEED_SPECS[table]` maps each seed column (a `Column`) to its cc source, a transform `kind`, and an `on_null` policy. `_KINDS` is the single registry of each kind → a `KindSpec(arrow, default_on_null)` (`string`/`epoch_ms`/`float`/`native_symbol`/`int`); the kind is explicit per column, never inferred from the arrow type (int64 is shared by `int` and `epoch_ms`, which datetime-converts). **Missing-value parity is per-column, not universal** — each seed's convention differs, so `on_null` is a fill value, `RAISE`, or the kind default (`float`→NaN value; every other kind→`RAISE`). A NULL in a seed-required column fails closed rather than mint a NaN/parquet-null the extractor never emitted. `_seed_value` is the single null-policy-then-transform source; `feed_schema`/`select_columns`/`rows_to_parquet` build the seed-schema parquet (cc `timestamp`→floor-to-ms epoch int64). The `main.py` endpoint is a thin wrapper — `FEED_SPECS` table whitelist, tz-aware bounds normalized to UTC, half-open `[start,end)`, 400 (unknown table / naive / reversed), 204 on empty (the hash rejects an empty feed), 413 above `EXPORT_MAX_ROWS` (reject, not truncate; cap sized by an in-image peak-RSS smoke). Three feeds registered: `hl_asset_ctxs` (HL context passthrough), `binance_oi` (`open_interest` filtered to binance USDT-perps, coin = ccxt `symbol`→Binance-native via the `native_symbol` kind, `symbol_like` restricting to the seed universe), and `hl_liquidations` (`liquidations WHERE venue='hyperliquid'`, the Grade-A tape — `oid`/`tid` the `int` kind, `dir`/`method`→`""` and `closed_pnl`→`0.0` per the seed defaults, the required `side`/`px`/`sz`/`mark_px` fail-closed). `premium_index` (needs 5m→1h downsample) and `candles` (multi-seed, per venue×tf) fan out next.

**Liquidation read (`GET /liquidations`)** — recent liquidation events (`ORDER BY timestamp DESC`, `limit` bounded `[1, 5000]`), `venue`/`asset_id`/`start`/`end` all optional; cc's monitoring surface for the new feed (peer of `/oi`, `/funding`, keyed on `venue`). The bulk backtester path is the parquet export above.

### db/

Schema migrations and TimescaleDB hypertable setup. Candles hypertable partitioned on timestamp, 1-week chunks; `candles_1m` (1m only) on the `ts_1m` volume tablespace, 1-day chunks (migration 010, operator-applied). Compression (>7d) and retention policies (2y candles + candles_1m, 1y funding/OI).

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
| timeframe | TEXT NOT NULL | 7 collected: 5m→12h (1d/3d/1w retained history only — collection dropped v0.23) |
| timestamp | TIMESTAMPTZ NOT NULL | Candle open time, UTC |
| open, high, low, close | DOUBLE PRECISION | |
| volume | DOUBLE PRECISION | Base currency |
| quote_volume | DOUBLE PRECISION | Nullable |
| trades | INTEGER | Nullable |

### `candles_1m` hypertable

Identical column shape + PK to `candles`, holding ONLY `1m` (Binance + Bybit). A **separate hypertable pinned to the `ts_1m` tablespace** (a Hetzner Cloud Volume) so 1m's ~5× density (~1.4M rows/day, ~19 GB/yr compressed) sits on cheap block storage off the 34 GB root disk — without migrating the in-place `candles` data. Chunk interval **1 day** (vs candles' 1 week — density: a 1-week chunk would be ~3.8 GB uncompressed, half of RAM). Compress >7d, retention 2y (the volume holds it, no shorter-retention compromise). On prod the chunks live in `ts_1m` (migration 010, applied after the operator mounts the volume); `init.sql` (fresh/dev, no volume) creates it in the **default** tablespace — the drift guard compares column/PK/policy shape, not the tablespace clause. Readers treat `candles` + `candles_1m` as one series split by TF; the T-011 export unions them.

### `collection_status`

PK: `(exchange, market_type, symbol, timeframe)` — health tracking + discovery metadata per pair.
Also holds per-loop **liveness heartbeats** for the funding/OI/premium/liquidation-WS/candle-WS threads: sentinel rows with
`symbol = "*"` and `timeframe ∈ {funding, oi, liq_ws, premium, candle_ws}` (one per exchange/loop). Candle-facing readers
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
`asset_id` here is the **HL-native coin name** (= the seed `coin`, e.g. `kPEPE`) — not cc's
ccxt-canonical id — so the T-011 parquet export is a passthrough.

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

### `premium_index` hypertable

Binance-only premium-index term structure (mark-vs-index spread), 5-min OHLC klines (`premiumIndexKlines`), 1-week chunks, compress after 7 days segmentby `(exchange, symbol)`, retention 1 year. A **superset** of the backtester's 1h `binance-premium-index` seed (downsamples to 1h for the T-011 export). Migration 009. PK: `(exchange, symbol, timestamp)`.

| Column | Type | Notes |
|--------|------|-------|
| exchange | TEXT NOT NULL | `binance` |
| asset_id | TEXT NOT NULL | canonical base (`BTC`) |
| symbol | TEXT NOT NULL | ccxt-normalized |
| native_symbol | TEXT NOT NULL | exchange-native (`BTCUSDT`) = seed `coin` |
| timestamp | TIMESTAMPTZ NOT NULL | bar OPEN time, UTC (= seed `open_time_ms`) |
| premium_open, premium_high, premium_low, premium_close | DOUBLE PRECISION | premium-index OHLC over the bar |
