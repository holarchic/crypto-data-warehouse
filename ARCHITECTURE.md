# crypto-data-warehouse Architecture

## Components

### collector/

Fetches OHLCV candles, funding rates, and open interest from exchanges. Writes directly to PostgreSQL. One container per exchange (6 total). Config in `exchanges.yaml` (`has_*` flags per exchange).

**Process root (`main.py`).** `build_feeds(config, exchange, dsn, rate_limiter)` is the feed table: one `Feed(name, target, args)` per thread the venue's config enables, in launch order. That is a candle feed per timeframe, then funding, OI, liquidation WS, premium and HL candle WS behind their `has_*` flags. `run()` starts one daemon thread per feed, then `wait_for_dead_feed` polls them every `SUPERVISE_POLL_S` (10 s). A thread that stops, by an exception or a return, makes `run()` log CRITICAL naming it and call `os._exit(1)`, so `restart: always` restarts the container with fresh process state. It uses `os._exit` because interpreter shutdown joins the dense-OI pool's executor workers, and one mid-request would hold the exit open.

The REST candle feed is `candles.py` (`candle_loop` → `_collect_pair` → `fetch_candles`, with the request sizing `poll_limit` + `CANDLE_LIMIT_*`; see Shared-limiter coupling below). A venue that charges by bars returned (HL) sets `poll_max_bars`: `_collect_pair` passes a ccxt `until` 1 ms before the boundary `poll_max_bars` buckets after `since`'s bucket on every request (HL returns every bucket touching either end of the range), a seed starting `CANDLE_SEED_LIMIT` bars back, so a catch-up poll costs no more than a caught-up one; when a capped window behind now comes back empty, `candle_loop`'s per-pair `scan_from` moves the next poll past it, so a cursor older than the venue's history is stepped over window by window, never with an uncapped request.

**Candle-table routing (T-015):** which table a timeframe's candles land in is a data-driven property — `table:` in `exchanges.yaml` (default `candles`; `1m` → `candles_1m`). `config._validate_candle_table` is the single validation seam (both `_parse_exchange` and `load_candle_tables` call it, so a bad `table:` fails once at load in the collector AND monitor processes); `TimeframeConfig.table` / `config.load_candle_tables()` is the single routing source, consumed by the collector insert (`candles.INSERT_SQL`, a dict keyed by table, built at import from the closed `CANDLE_TABLES` allowlist via one template — no runtime identifier interpolation), the monitor gap-scan (`monitor.GAP_SQL_BY_TABLE`), and — mirrored, since `api/` can't import `collector/` — the API (`_candle_table`, pinned to config by an exact-equality drift test). No hardcoded `1m` branch anywhere.

`candle_loop` calls `maybe_reload_markets()` (in `db.py`) at the top of each cycle. The helper is age-gated (`MARKETS_RELOAD_INTERVAL_S`, default 86400s) and lock-guarded so the per-timeframe threads sharing one ccxt `exchange` reload at most once per interval. Without this, symbols added by daily discovery stay invisible to the long-running collector container.

**DB-restart resilience:** every loop owns one long-lived connection and reconnects at the top of each cycle on `if conn.closed:` (`closed` == `pgconn.status == BAD`; psycopg detects a server-dropped socket only when the conn is next exercised). The hazard is the error handler itself: a `conn.rollback()` or a status/insert write on a just-dropped connection re-raises `OperationalError`, and unhandled that kills the loop thread (root cause of two total outages). So every error-path rollback goes through `db.safe_rollback` (swallows), and the only DB *writes* reachable on a restart raise — `funding._poll_bulk`'s bulk insert and the candle per-pair status write (`candles._record_pair_error`) — are individually wrapped; a live-conn pre-check is not protection because the write itself can raise. With no exception able to escape a loop body, the next cycle's `if conn.closed:` reconnects and collection self-heals within ~1 cycle. `ws.py` already had this shape (`_ensure_conn` + a swallowing `_guarded_rollback`).

The funding, OI, premium, liquidation-WS, and HL candle-WS threads upsert a per-loop **liveness heartbeat** (`status.upsert_loop_heartbeat`): a sentinel `collection_status` row with `symbol = "*"` and a pseudo-timeframe from `config.LOOP_INTERVALS` (`funding` / `oi` / `liq_ws` / `premium` / `candle_ws`; the daily HL 1m repair's `candle_repair` row is written on success through `status.upsert_status_success`, which also carries its cursor). The poll loops fire it each cycle; the WS loops fire it on a ~60 s connection timer while the socket is up (a quiet feed is healthy). These threads write no per-pair candle status, so without the heartbeat a silently-dead loop produces no staleness footprint; the monitor flags the sentinel row when it goes stale.

**Status contract (`status.py`):** every `collection_status` helper runs inside the caller's transaction and never commits or rolls back. The candle loop commits a pair's bars and its cursor (`upsert_status_success`) as one transaction, so the cursor never runs ahead of or behind its bars; the error record, each loop's heartbeat wrapper and the repair's cursor advance commit beside their call. A venue's instrument universe has one reader, `active_instruments(conn, exchange)`, which returns frozen `Instrument` records (symbol, asset_id, native_symbol, market_type) from the venue's active rows at `UNIVERSE_TIMEFRAME` (5m, the one timeframe every venue polls). Funding, OI, premium, the WS feeds and the HL 1m repair collect that universe; `query_active_pairs` is the candle loop's per-timeframe read, which also carries each pair's cursor.

The OI loop (`oi.py`) dispatches each `has_oi` exchange to one of **three strategies** (`oi_loop`; an unrecognized OI venue raises rather than defaulting), all cadences from `ExchangeConfig` (`oi_poll_interval_s` / `oi_settlement_window_s` / `oi_baseline_interval_s`):

- **HyperLiquid** (`_oi_loop_hyperliquid`): one bulk `metaAndAssetCtxs` POST every **1 min** — matching the backtester's Grade-B seed cadence — **dual-writing** OI to `open_interest` plus the full per-asset context bundle (funding/premium/oracle/mark/mid/impact/prev-day/day-volume) to `hl_asset_ctxs`, both under one transaction (`_insert_hl_snapshot`) so the two tables never diverge on a mid-cycle failure. `_build_hl_snapshots` owns the response boundary: it validates the positional `universe[i]`↔`asset_ctxs[i]` contract (length mismatch → abort the cycle) and applies a **parse-or-drop** rule per asset (`_to_float`/`_parse_hl_ctx` raise `_CtxParseError` on a present-but-malformed field, wrong-container `impactPxs`, a non-dict row, or a non-string `name` → that asset is skipped and logged; legitimately-absent fields → SQL NULL). The response names each asset by HL's **native** coin name (`kPEPE` for 1000× tokens); the active-pair map is keyed on cc's **ccxt-canonical** `asset_id` (`KPEPE`), so the lookup bridges via `name.upper()` (HL names are uppercase but for the leading `k`) — without it every 1000× token silently missed the lookup. The two tables store `asset_id` by their role: `open_interest` keeps the **ccxt-canonical** id (cross-venue grouping), while `hl_asset_ctxs` stores the **HL-native name** (= the backtester seed `coin`, so the T-011 export is a passthrough) — they differ only for 1000× tokens, and nothing joins the two on `asset_id`.
- **Bybit/OKX** (`_oi_loop_bulk`): one bulk call/cycle at a flat **30 s** — Bybit `fetch_tickers` (OI + native USD read from `ticker["info"]`, parse-or-drop), OKX `fetch_open_interests` (contracts; USD from a `_fetch_price_map` since the bulk endpoint omits `oiUsd`).
- **Binance** (`_oi_loop_binance`, the only per-symbol-expensive venue): a **two-source** loop. A 5-min **baseline** via `openInterestHist` on the separate `/futures/data` weight bucket (`BINANCE_OI_HIST_WEIGHT=0`, self-paced by `BINANCE_OI_HIST_SPACING_S`) — cursor-driven (`_binance_baseline_sweep`): the cursor is DB-seeded from 5-min-aligned rows only (`SEED_BASELINE_CURSOR_SQL`, so dense rows can't poison it), `since`-based so a restart backfills the gap, advanced only after commit, and gated on a successful seed. Plus a sub-5-min **dense** `openInterest` (main weight pool) for symbols within ±`oi_settlement_window_s` of their own settlement — the window from a bulk `premiumIndex` (`nextFundingTime` trigger + `markPx` for USD; the post-settlement half via `nextFundingTime`-change detection, no hardcoded hours). Dense and baseline rows coexist in `open_interest` by distinct timestamps. Cross-tick state is pruned to the active set each cycle. Since Binance has no bulk instantaneous OI, the dense fetch is per-symbol; at a mass settlement (~690 symbols aligned) a sequential loop overruns the 60 s tick, so it runs on `_DenseFetchPool` — K worker threads (`oi_dense_concurrency`) each borrowing their **own** ccxt exchange from a checkout queue (isolated `requests.Session`/`last_response_headers`, no shared-session bet; the global `WeightRateLimiter` still gates all — budget is per-IP), workers returning rows the main thread collects (single writer). The pool is built lazily with per-cycle retry (transient init self-heals; degraded = sequential that cycle) and refreshes its exchanges' markets age-gated. `oi_dense_concurrency: 1` is the pure-sequential rollback.

**Poll pacing (`slots.py`).** Every REST poll loop (candle `candle_loop`, the OI loops, the premium loop) paces with `slots.sleep_until_next_slot(interval, buffer, started, label)`: it waits for the first `k × interval + buffer` slot (`next_slot`, buffer clamped to half the interval) after the **start** of the cycle that just ended, and starts at once when that slot has already passed. Each loop takes `started = time.time()` as its cycle begins and passes it in; `None` (no cycle yet, the deferred first cycle of a 30m–12h candle loop, the premium loop's first wait) waits for the slot after now. A cycle that ends between a boundary and its buffer point still takes that period's slot (a ~37 s dense OI cycle keeps 60 s), and one that overruns its slot is followed by a late cycle instead of a skipped slot; late cycles run back to back, never concurrently, until one ends before its next slot. Each cycle end logs one line naming the loop, the cycle's start, the target slot and the wait, the record the pacing check reads. Funding's hourly `_sleep_until_next_hour` is a separate, settlement-aligned schedule.

**Shared-limiter coupling:** the Binance dense burst shares this container's `WeightRateLimiter` with candle collection (the budget is per-IP). `build_rate_limiter(config)` is the single construction seam (weight vs interval limiter + reserve wiring). A **priority reserve lane** (`rate_limit_priority_reserve`) stops the dense burst blocking on the coincident candle-kline burst: the dense per-symbol `fetch_open_interest` + its `premiumIndex` trigger `acquire(priority=True)` and may draw up to `budget − safety`, while candles/funding self-limit to `budget − safety − reserve`. The reserve is sized to the dense weight-**rate** at a mass settlement (in-window symbols × OI-weight plus the `premiumIndex` trigger ≈ 700/min at the 60 s cadence), **not** the instantaneous in-flight count — the binding constraint is the server's rolling-minute used-weight. Two things keep the burst from pinning it: (1) the reserve (800) exceeds that rate, so `candle_cap + dense < priority_cap`; (2) a **candle weight diet** — `_collect_pair` sizes the Poll-path kline `limit` to the actual gap via `candles.poll_limit` (only on weight-bound venues, `rate_limit_weight_budget` set), so a caught-up poll costs Binance weight 1 not 5 and the mass-settlement candle burst is ~5× smaller; Seed still fetches `CANDLE_SEED_LIMIT`. Weight-0 calls (the `openInterestHist` baseline on the separate `/futures/data` bucket) bypass the cap entirely. An over-large reserve (≥ half the usable cap) is neutralized to 0 with a warning; `_oi_loop_binance` logs an **edge-triggered runtime** warning when a cycle's dense weight (`len(dense) × weight`) exceeds the reserve band, and logs `used_snapshot()` (server used-weight + in-flight) per dense cycle as direct evidence the burst stays under the priority cap. The dense tick is 60 s to bound peak OI weight; candle freshness stays the prod re-verify gate (see SPEC failure mode).

### collector/ws.py — streaming engine (Binance + Bybit liquidations live)

`ws_loop` is a generic threaded WebSocket engine driven by a `WSFeedSpec` injection record, so it carries **zero venue branches** — the four planned feeds (Binance/Bybit × liquidations/mark) reuse one engine. Venue behavior (URL, topic mapping, sub/unsub frames, message parser, batch insert, app-ping, proactive-reconnect) is entirely spec-injected. The engine owns:

- **Connection lifecycle** via `websocket-client` (`create_connection` + an explicit recv loop with a 1 s socket timeout so ping/heartbeat/resubscribe timers are serviced even on a silent feed). A `recv` timeout is quiet; `recv()==""` with `not connected` is a graceful server close.
- **Reconnect/backoff** on a WS-local retriable set (`WS_RETRIABLE` — `backoff.RETRIABLE_EXCEPTIONS` is ccxt/psycopg-shaped and would never catch a WS drop), reusing only `backoff_sleep`. The in-session DB reconnect (`_ensure_conn`) is **single-shot/non-blocking** so a Postgres blip never freezes the recv loop. Only a *proactive* reconnect skips backoff; a *server-initiated* close backs off (no tight-spin).
- **Subscription management** — initial subscribe + diff-based resubscribe when discovery changes the active-pair set; venue callables own their own arg-cap batching. The subscribed set is cached in `_WSRuntime.topics` and reused on a reconnect that lands during a DB outage (so a per-symbol feed isn't dropped to zero).
- **Batch insert** (`spec.insert_batch`, flushed by size or ~1 s) with the buffer owned by `ws_loop` so it survives a reconnect, bounded at `WS_MAX_BUFFER` (drop oldest) on a sustained DB outage.
- **Connection-timer liveness heartbeat** (`liq_ws` pseudo-TF, same sentinel path as funding/oi) — fires while the socket is up, *not* per event, since a quiet feed is healthy.

`_run_session` (one connection) is factored from `ws_loop` (the reconnect wrapper) for testability; `connect_fn`/`clock` are injected. `build_feeds` adds a `ws_loop` feed per `has_liquidation_ws` exchange **and** per `has_candle_ws` exchange (HL 1m candles — see `hl_candles.py`), the same daemon-thread-per-loop model as the poll loops.

### collector/liquidations.py — Binance + Bybit liquidation feed specs (Grade C)

The venue data for `ws.py` (the engine stays venue-agnostic). Two `WSFeedSpec` builders + `build_liq_spec(venue)` (the resolver `build_feeds` calls; raises on an unknown venue). Binance `!forceOrder@arr` is one all-market stream on the **routed `/market` path** (an unrouted `/ws` gets only Public data); Bybit `allLiquidation.{native}` is per-symbol, batched-subscribe, 20 s ping. **asset_id resolution:** `parse_message` receives only the raw frame, so each builder closes over a shared `native_to_asset` map that `topics_for_pairs` rebuilds (engine-called at subscribe + resub) and `parse_message` reads — no lock, because `_run_session` calls both sequentially on the one feed thread, subscribe before recv. Binance's all-market stream is filtered to cc's tracked pairs (untracked → no map entry → skipped). Side normalized to `liquidated_side ∈ {long, short}` across the venues' opposite conventions. Grade-C insert (`ON CONFLICT` inert via NULLS-DISTINCT — every event lands). One Bybit connection holds the universe under the 21 k-char args ceiling; a char-based guard warns before a growth breach (sharding deferred).

### collector/liquidations_hl.py — HyperLiquid Grade-A liquidation daily pull (T-009.3)

A one-shot batch (`python -m liquidations_hl`, run by cron via a profile-gated `liquidations-hl` compose service, mirroring `discovery`) — NOT a `ws.py` feed: `node_fills` is an hourly S3 tape, not a stream. Pulls HL forced-liquidation events from the requester-pays `s3://hl-mainnet-node-data` (a scoped read-only IAM key, creds from `.env`; `main` exits 2 when they're missing, so compose never requires them) into `liquidations` (`venue='hyperliquid'`, `grade='a'`, all 16 columns). Ports the backtester's proven extractor: two on-disk formats across the `2025-07-27` cutover (`_prefix_for`/`_iter_fills`), **streaming lz4 decompress** (1 MiB chunks + rolling-buffer line split, so a peak cascade hour never lands whole), and the **`liquidatedUser==addr` dedup-at-source** rule (a liquidation fill appears on both sides — emit once, from the liquidated side). `_row` maps to the 16 cc columns with **per-event parse-or-drop** (bad identity → skip+count; bad magnitude → NULL; `oid`/`tid` DROP-not-default — they are the dedup key), `liquidated_side` from `dir`, ms→epoch-seconds for `to_timestamp`, and **HL-native `asset_id`** (`kPEPE`, matching the seed + `hl_asset_ctxs`, so the T-011 export is a passthrough — NOT canonicalized). `_stream_insert` flushes in `BATCH_SIZE` chunks; `ON CONFLICT (venue,asset_id,timestamp,oid,tid) DO NOTHING` (idempotent re-pull; distinct `oid`/`tid` at one ms preserve clustered cascade events). Default pulls the last 3 complete UTC days (`default_days`, bounded idempotent lookback); `--start/--end` for backfill. **Error model:** object-level failures (DB `OperationalError` / S3 GET / corrupt-lz4 `RuntimeError`) and day-level list failures are counted + isolated (run continues, exits non-zero); a `<24`-object day counts as a failure (surfaces an incomplete day); a run-fatal DB outage (`_DBUnavailable` = `connect_db` exhaustion) → clean exit 1; deterministic DB faults (`ProgrammingError`/`DataError`) + logic bugs abort loudly.

### collector/premium_index.py — Binance premium-index poll (T-010)

Binance-only slot-paced (`slots.py`) 5-min poll of `premiumIndexKlines` (ccxt `fetch_premium_index_ohlcv`) into `premium_index` — the perp mark-vs-index spread (funding term structure), a conditioning input for downstream backtests. Reuses the candle poll's shape: gap-aware `since` cursor + the T-007.4 weight diet (`candles.poll_limit` ⇒ a caught-up poll costs kline weight 1, not 5) + the T-014 error discipline (`safe_rollback` on every error path). The per-symbol cursor is seeded from the table so a restart resumes from the gap; **any active symbol missing from the in-memory map is re-seeded from the DB before polling** (so a reactivation resumes from its last stored bar, never a forward-only skip), and advances only after commit. First run is **forward-only** (a few recent bars via `PREMIUM_SEED_LIMIT`, not the 1000-bar candle seed — deep history is backfillable via T-011, so the empty-table rollout carries no weight spike). Stays **non-priority** on the shared `WeightRateLimiter` (yields to candles + the dense-OI reserve — premium is neither the primary product nor unbackfillable). Cadence (`PREMIUM_TIMEFRAME` / `PREMIUM_INTERVAL_S`) is single-sourced in `config.py` — the ccxt timeframe, the incomplete-bar guard, and the `premium` monitor staleness floor (`LOOP_INTERVALS`) all derive from it. `premium_index_loop` **raises fast** on a venue without `fetchPremiumIndexOHLCV` (Binance-only capability guard, matching `oi_loop` / `build_liq_spec`). Launched behind `has_premium_index`; the active-symbol universe comes from `status.active_instruments` (the venue's 5m candle rows, not the poll cadence).

### collector/hl_candles.py — HyperLiquid 1-minute candles (WS-only, T-015.1)

HL 1m into the live `candles_1m`, **with zero live REST** — HL's ~1 req/s info budget is reserved for the unbackfillable dense 1-min OI, and a live REST backfill was deploy-measured to starve HL's tight single limiter (5m…1w per-bar coverage 161→33; HL has no priority-reserve lane). So this is a single thread behind `has_candle_ws`:

- **WS candle feed** — a `WSFeedSpec` on `ws.py` (zero REST), one connection for the whole ~161-coin universe. `topics_for_pairs` resolves each pair to its **HL WS coin name** via ccxt `market['info']['name']` (`kPEPE`/`kBONK`/`kSHIB` for 1000× tokens — subscribing the wrong-cased `KPEPE` makes HL drop the *entire* connection; `KAITO`/`KAS` stay uppercase), rebuilt each resub so a new listing is picked up as ccxt reloads. The shared map is keyed on the HL name (routes the frame's `s`) and carries cc's canonical `asset_id`; `native_symbol` (a numeric index) is unused here (no lock, the liquidations pattern); the frame builders wrap each id in HL's `{method,subscription:{type:candle,coin,interval:1m}}` JSON; `parse_message` turns a `{channel:candle,data}` frame into a `candles_1m` row and returns `[]` for control frames (`subscriptionResponse`/`pong`/non-candle). The insert is an **UPSERT** (`ON CONFLICT … DO UPDATE`, hl_candles owns its own SQL) because HL streams the forming bar continuously with no closed flag, so the row converges to the closed value (last-write-wins) — never the generic `DO NOTHING`. `ping_frame` + `ping_interval_s=30` (HL closes idle sockets at 60 s). Loop sentinel `candle_ws`.

A WS-drop gap (no history replay on reconnect), and a bar the stream left short of its minute's last trades, are repaired **out-of-band by `hl_candles_repair.py`**, a daily one-shot, not a live-contending sweep. HL's REST serves only the last ~5,000 bars (~3.5 days at 1m), so the repair must land inside that window. The WS emits only traded minutes: a thin coin's no-trade minute has no row by design.

**Streamed, not polled:** HL 1m is deliberately absent from `supported_timeframes`, so `build_feeds` adds **no** per-symbol REST 1m `candle_loop` (which would starve the OI) and discovery writes no per-pair 1m status rows. Liveness is the `candle_ws` loop sentinel. `/candles?timeframe=1m` + `/status/gaps?timeframe=1m` are table-routed so HL 1m is queryable; the `collection_status`-driven metadata endpoints (`/candles/timeframes`, `/candles/symbols`) do not list it.

**Collector concurrency invariant:** each loop (candle per-TF, funding, OI, premium, WS-candle, WS-liq) runs on its own daemon thread, supervised by the root, with its **own DB connection and direct writes** — there is no single-writer queue, by design (one container per exchange). Loops that share a table write disjoint rows: candle timeframes differ in `timeframe`, and each loop's status and heartbeat rows carry their own key. The WS engine conforms: it buffers events and batch-flushes on its own connection.

### collector/hl_candles_repair.py — HyperLiquid 1m daily repair (T-019)

A one-shot (`python -m hl_candles_repair`, the profile-gated `hl-candles-repair` compose service, cron at 06:30 UTC) that replaces each active HL coin's recent 1m bars in `candles_1m` with REST `candleSnapshot`'s closed values.

- **Window.** The last 26 h, or back to its cursor minus 2 h after a missed run, capped at 72 h, ending 2 min before the run starts. The cursor is the `last_timestamp` of the `candle_repair` loop sentinel; the first run seeds it before fetching. `--hours N` repairs a manual window and moves the cursor only when it covers the required range.
- **Fetch.** One raw `meta` POST maps `asset_id` to the HL name (`name.upper()`, as `oi.py`), then one `candleSnapshot` per coin per ≤ 26 h slice, oldest slice first, refusing a slice older than 82 h at fetch time.
- **Budget.** REST and WS `post` share HL's per-IP weight budget with the collector's own polls, so every call is charged `20 + ceil(minutes / 60)` and the next waits `weight × 60 / 20` s; a 429 backs off 120 s and halves the rate.
- **Completeness.** REST answers every listed minute, so each slice's raw answer must run without a hole to its end (a leading gap is the coin's listing; an empty older slice resolves when a later one shows the listing), and every stored minute must have a REST traded bar. A malformed bar writes nothing for its slice.
- **Write.** Traded bars only (`n > 0`, matching the WS), through an upsert whose `IS DISTINCT FROM` guard skips unchanged rows and whose `RETURNING (xmax = 0)` counts inserts (drops) apart from updates (unclosed bars); every result set is read.
- **Outcome.** The cursor advances (`status.upsert_status_success`, GREATEST) only when every coin and slice is complete. A cap that cuts into unrepaired history alerts through `monitor.send_webhook`, and only a delivered alert lets the cursor rebase past it. A Postgres advisory lock keeps one run at a time and is re-taken after any reconnect. The monitor pages on the sentinel 36 h after the last success.

### collector/discovery.py — daily universe selection

A one-shot (`python -m discovery`, cron 02:00 UTC via the profile-gated `discovery` compose service). Fetches perp tickers per exchange (a venue whose ccxt has no `fetchTickers`, dYdX, is skipped with one INFO line and contributes no volume), keeps the highest-volume symbol per `asset_id`, then records each asset's max 24h volume across venues for the UTC run date (`status.record_discovery_volumes` → `discovery_volumes`) and reads the per-asset maximum over the last `volume_keep_window_days` run dates (`status.query_recent_max_volumes`, window start from `keep_window_start`). `qualify_assets` (pure, unit-tested) applies union qualification with hysteresis: an asset enters when tonight's max reaches `volume_threshold_usd` and, once active (`status.query_active_asset_ids`), stays while the higher of tonight's max and its window maximum holds at `volume_keep_threshold_usd`; an asset no venue lists tonight never qualifies. The history write and read precede every upsert in the run's first transaction, so a failure there rolls back and exits 1 with `collection_status` untouched. Qualifying assets are upserted on every venue that lists them; pairs that drop out are deactivated (`active=FALSE`, history kept).

### monitor (collector/monitor.py)

Separate always-on container (`python -m monitor`). One `collection_status` fetch per cycle (`fetch_status_rows`) feeds two independent alert signals plus gap detection:

- **Loop-liveness** — presence-based: any stale per-loop heartbeat sentinel row (funding/OI/liq_ws/premium/candle_ws/candle_repair) pages (own cooldown key). Every alert's cooldown starts only when the webhook accepts it (`send_alert`), and a gap scan whose alert failed stays due, so a failed delivery is retried next cycle. A single dead loop is one row, so it is kept off the count-delta signal below. The `candle_ws` sentinel is HL 1m's liveness surface (it is streamed, not per-pair-status tracked).
- **Candle alarming-mass** — `compute_alarming` counts collectors that are *alarming* (`error_count >= ALARMING_ERROR_THRESHOLD`, default 3, or was-working-now-dark past `max(4×interval, ALARMING_DARK_FLOOR_S)` — the 1200 s floor lifts only 1m, whose serial RTT-bound REST cycle runs minutes, not the interval; excludes the benign new-listing floor, the HL 5m and REST 1m within-cycle cursors, and transient single-cycle rate-limit blips whose `error_count` resets on the next success). `evaluate_alarming` pages when that count exceeds its rolling-1h median by `ALARMING_DELTA_THRESHOLD` for `ALARMING_SUSTAIN_CYCLES` consecutive cycles, returning a named `AlarmingDecision` that owns the record-only-quiescent baseline invariant.
- **Gap detection** (tiered by timeframe, **routed to the timeframe's candle table** via `load_candle_tables()` so 1m gaps scan `candles_1m`, not the 1m-empty `candles`) + a weekly heartbeat summary (24h candle count summed across `candles` + `candles_1m`; its stale count is that cycle's `compute_staleness` count, per timeframe) + a per-cycle healthcheck dead-man's-switch ping.

### api/

FastAPI read-only REST API. Queries PostgreSQL, serves downstream consumers. Cannot import `collector/`, so any collector-shared convention is mirrored + drift-tested (`_candle_table`, `LOOP_TIMEFRAMES`).

**Parquet export (`GET /export/parquet`, T-011)** — streams a feed's window as parquet for the backtester's forward-ingest, which reads the raw file and validates each feed by a structural hash (`feed_provenance.rs`: name-sorted schema + commutative per-row digest sum, so column/row order don't affect it — but names/types and per-cell content, including the null-mask, do). The byte-contract lives in one pure module, `api/export.py` (pyarrow only — no fastapi/psycopg, so it unit-tests against real pyarrow): `FEED_SPECS[table]` maps each seed column (a `Column`) to its cc source, a transform `kind`, and an `on_null` policy. `_KINDS` is the single registry of each kind → a `KindSpec(arrow, default_on_null)` (`string`/`epoch_ms`/`float`/`native_symbol`/`int`); the kind is explicit per column, never inferred from the arrow type (int64 is shared by `int` and `epoch_ms`, which datetime-converts). **Missing-value parity is per-column, not universal** — each seed's convention differs, so `on_null` is a fill value, `RAISE`, or the kind default (`float`→NaN value; every other kind→`RAISE`). A NULL in a seed-required column fails closed rather than mint a NaN/parquet-null the extractor never emitted. `_seed_value` is the single null-policy-then-transform source; `feed_schema`/`rows_to_parquet` build the seed-schema parquet (cc `timestamp`→floor-to-ms epoch int64). `feed_query(table, start, end, asset_id)` builds every feed's SQL (half-open `[start,end)`, venue/universe/asset filters, `LIMIT EXPORT_MAX_ROWS+1`); identifiers and bucket widths come from the spec literal, values are parameters. A feed with a `Resample(bucket_s, source_s)` is aggregated in SQL: each `Column.agg` is a group key, `first`/`last`/`max`/`min` (each NULL unless every source value in the bucket is non-NULL, so a NULL reaches the column's `RAISE` instead of being skipped by `max`/`min`), or `bucket_open`/`bucket_close`; only buckets with all `bucket_s/source_s` source rows are emitted (`HAVING count(*)`), and source rows are read over `[ceil(start), ceil(end))` on bucket boundaries, so a bucket is in a window iff its open time is. The `main.py` endpoint is a thin wrapper — `FEED_SPECS` table whitelist, tz-aware bounds normalized to UTC, `feed_query`, 400 (unknown table / naive / reversed), 204 on empty (the hash rejects an empty feed), 413 above `EXPORT_MAX_ROWS` (reject, not truncate; cap sized by an in-image peak-RSS smoke). Four feeds registered: `hl_asset_ctxs` (HL context passthrough), `binance_oi` (`open_interest` filtered to binance USDT-perps, coin = ccxt `symbol`→Binance-native via the `native_symbol` kind, `symbol_like` restricting to the seed universe), and `hl_liquidations` (`liquidations WHERE venue='hyperliquid'`, the Grade-A tape — `oid`/`tid` the `int` kind, `dir`/`method`→`""` and `closed_pnl`→`0.0` per the seed defaults, the required `side`/`px`/`sz`/`mark_px` fail-closed), and `binance_premium_index` (`premium_index` binance USDT 5m resampled to complete 1h bars, coin = `native_symbol`, `close_time_ms` = open + 3,599,999 as in the seed, NULL prices raise). `tests/integration/check_export_queries.py` runs every feed's query against a real TimescaleDB through the serializer.

**Bulk export (`scripts/export-feed.py`)** — a client of the endpoint, not a second DB reader: it walks a span in windows (halving on 413, skipping 204), writes each window as a row group of a staging file only that run owns (`.<out>.<random>.partial`, so two runs aimed at one `<out>` never share it) through one `pq.ParquetWriter`, and renames it to `<out>` after the last window; any failure (status, timeout, schema change, a failed close or rename) removes the staging file and writes no `<out>`, and an empty span writes none.

**Liquidation read (`GET /liquidations`)** — recent liquidation events (`ORDER BY timestamp DESC`, `limit` bounded `[1, 5000]`), `venue`/`asset_id`/`start`/`end` all optional; cc's monitoring surface for the new feed (peer of `/oi`, `/funding`, keyed on `venue`). The bulk backtester path is the parquet export above.

### db/

Schema migrations and TimescaleDB hypertable setup. Candles hypertable partitioned on timestamp, 1-week chunks; `candles_1m` (1m only) on the `ts_1m` volume tablespace, 1-day chunks (migration 010, operator-applied). Compression (>7d) on every hypertable, and no retention: all history is kept (migration 013 removed the old policies, and no file in `db/` adds one). `discovery_volumes` (migration 012) is a plain table with no policies.

### Offsite backup (scripts/backup-offsite.sh, host cron)

A nightly host-cron job at 05:30 UTC, outside compose, that does nothing until `/etc/candle-collector/restic.env` exists.

- **Dump.** It streams `pg_dump --format=custom --compress=0` from the db container into a restic repository in a bucket, via `restic backup --stdin-from-command`. A failed dump therefore makes no snapshot.
  - Snapshots carry a fixed `--host candle-collector` and `timescaledb-<ver>`/`pg<major>` tags.
  - Retention is 7 daily, 5 weekly and 12 monthly snapshots, pruned nightly.
- **After the dump.** `restic check`, then a success marker (`/var/lib/candle-collector/backup-ok`). On Sundays the check adds `--read-data-subset n/4` from `read_back_subset` (ISO week mod 4, plus 1), so the stored data is read back a quarter at a time.
- **Shared contract.** `scripts/offsite-lib.sh` is sourced by the backup, the drill and the monitor. It owns the env-file and marker paths, the snapshot host and file name, `restic_run`, `restic_strip_env_args` and `read_back_subset`.
- **Transport.** On B2 the repository is `rclone:b2:…`: restic starts rclone, whose `b2` remote is defined by `RCLONE_CONFIG_B2_*` in the env file. `restic_run` overrides restic's `rclone.args` default to drop `--b2-hard-delete`, so removals are hides and the no-`deleteFiles` key works; other backends ignore it.
- **Credentials.** `restic_run` keeps the env file's variables in restic's process and the rclone it starts, and every caller handles its failure. The dump command runs under `env` with every name `restic_strip_env_args` reads from the env file removed, so compose keeps reading the HL pull's `AWS_*` from `.env`.
- **Locking.** A local `flock` stops runs from overlapping, and `restic unlock` clears stale locks only.
- **Alerts.** Each restic step pages its own failure through `fail()`, and the `ERR` trap pages any other failing command. `deploy/host-monitor.sh` pages on a marker older than 26 h, with its own 6 h reminder file and a recovery ping. Reminder state moves only on a delivered page.
- **Webhook.** `scripts/webhook-lib.sh` is the host scripts' alert transport, used by `backup.sh`, `backup-offsite.sh` and `host-monitor.sh`. It reads `WEBHOOK_URL` from the environment or `.env`, trimmed the way compose trims it, and posts both `text` (Slack) and `content` (Discord), as `collector/monitor.py` does. It returns whether the page was delivered.
- **Drill.** `scripts/restore-drill.sh` restores the newest snapshot, by parsed instant, into a throwaway container of the tagged image. It cleans up only the run directory and container it created, and the container's PGDATA volume with it. The target mirrors prod: the `candles` superuser, a `ts_1m` tablespace, a `template0` database and the exact extension version.
  - The restore runs `--exit-on-error`. The drill then checks owners, runs a restored compression job, and confirms a new `candles_1m` chunk lands in `ts_1m`.
  - Last, it prints `scripts/drill-counts.sql`: counts over a window that only grows, so prod's counts before and after the dump bracket them.
- **Installers.** `deploy/install-restic.sh` and `deploy/install-rclone.sh` each install a SHA-256-pinned release.

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

Identical column shape + PK to `candles`, holding ONLY `1m` (Binance + Bybit by REST, HyperLiquid by WS). A **separate hypertable pinned to the `ts_1m` tablespace** (a Hetzner Cloud Volume) so 1m's ~5× density (~1.4M rows/day, ~19 GB/yr compressed) sits on cheap block storage off the 34 GB root disk — without migrating the in-place `candles` data. Chunk interval **1 day** (vs candles' 1 week — density: a 1-week chunk would be ~3.8 GB uncompressed, half of RAM). Compress >7d, no retention. On prod the chunks live in `ts_1m` (migration 010, applied after the operator mounts the volume); `init.sql` (fresh/dev, no volume) creates it in the **default** tablespace — the drift guard compares column/PK/policy shape, not the tablespace clause. Readers treat `candles` + `candles_1m` as one series split by TF; the T-011 export unions them.

### `collection_status`

PK: `(exchange, market_type, symbol, timeframe)` — health tracking + discovery metadata per pair.
Also holds per-loop **liveness heartbeats** for the funding/OI/premium/liquidation-WS/candle-WS threads and the daily HL 1m repair: sentinel rows with
`symbol = "*"` and `timeframe ∈ {funding, oi, liq_ws, premium, candle_ws, candle_repair}` (one per exchange/loop). Candle-facing readers
(`query_active_pairs`, `active_instruments`, the `/candles/*` and `/status` API endpoints) exclude these by pseudo-timeframe;
the monitor's staleness check acts on them, and the repair also reads and writes its own row's `last_timestamp` as its repaired-through cursor.

| Column | Type | Notes |
|--------|------|-------|
| asset_id | TEXT | Canonical base asset, set by discovery + collectors |
| native_symbol | TEXT | Exchange-native symbol ID |
| last_24h_volume_usd | DOUBLE PRECISION | From discovery's `fetch_tickers()` |
| discovered_at | TIMESTAMPTZ | When first discovered |
| deactivated_at | TIMESTAMPTZ | When dropped below threshold |
| active, last_timestamp, last_success, error_count, last_error | — | Health tracking |

### `discovery_volumes`

PK: `(run_date, asset_id)` — a plain table (not a hypertable), migration 012. One row per asset in a night's fetched perp tickers (~920/night), no retention. The keep window reads it by `run_date` range; a same-day re-run keeps the higher reading (`GREATEST` on conflict).

| Column | Type | Notes |
|--------|------|-------|
| run_date | DATE NOT NULL | UTC date of the discovery run |
| asset_id | TEXT NOT NULL | Canonical base asset |
| max_volume_usd | DOUBLE PRECISION NOT NULL | Max 24h volume across perp venues that night |

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

PK: `(exchange, symbol, timestamp)` — 1-week chunks (migration 007).

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

All-venue liquidation events, two fidelity grades: HL `node_fills` (Grade A, untruncated tape, `.3`) + Binance/Bybit WS (Grade C, forward directional sidecar, no historical seed, `.2`). **One** table (not per-venue) — a superset of the HL seed's 13 columns + `venue`/`grade`/`liquidated_side`; HL rows fill all seed columns, Grade-C rows leave the HL-specific ones NULL. 1-week chunks; compress after 7 days segmentby `venue, asset_id`; no retention. `asset_id` holds the seed's `coin` value (T-011 export aliases it). Full column list in SPEC.

**Dedup (the one structural subtlety):** a UNIQUE INDEX `(venue, asset_id, timestamp, oid, tid)`, **not** a PK — a PK is NOT NULL but `oid`/`tid` are NULL for Grade-C rows, and a hypertable needs the partition column in any unique key. HL rows carry non-null `oid`/`tid`, so distinct pairs preserve real same-ms clustered events (the backtester's structural hash is duplicate-sensitive) while an exact re-pull dedups via `ON CONFLICT DO NOTHING` (idempotency for the daily HL batch). Grade-C rows (NULL `oid`/`tid`, Postgres NULLS DISTINCT) never conflict → all land. The index's `(venue, asset_id, timestamp)` prefix serves range queries + the export. Table exists (migration 008); the writers are `.2`/`.3`.

### `premium_index` hypertable

Binance-only premium-index term structure (mark-vs-index spread), 5-min OHLC klines (`premiumIndexKlines`), 1-week chunks, compress after 7 days segmentby `(exchange, symbol)`, no retention. A **superset** of the backtester's 1h `binance-premium-index` seed (the `binance_premium_index` export resamples it to complete 1h bars). Migration 009. PK: `(exchange, symbol, timestamp)`.

| Column | Type | Notes |
|--------|------|-------|
| exchange | TEXT NOT NULL | `binance` |
| asset_id | TEXT NOT NULL | canonical base (`BTC`) |
| symbol | TEXT NOT NULL | ccxt-normalized |
| native_symbol | TEXT NOT NULL | exchange-native (`BTCUSDT`) = seed `coin` |
| timestamp | TIMESTAMPTZ NOT NULL | bar OPEN time, UTC (= seed `open_time_ms`) |
| premium_open, premium_high, premium_low, premium_close | DOUBLE PRECISION | premium-index OHLC over the bar |
