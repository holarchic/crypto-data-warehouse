-- v0.14 (SPEC): candles_1m — 1-minute OHLCV on a dedicated volume tablespace (T-015.2/.3)
--
-- 1-minute candles for the highest-volume venues (Binance, Bybit) — the native resolution of
-- 1-minute backtests, which candles' 5m floor can't serve. Identical column shape to
-- `candles` (drift-guarded), but a SEPARATE hypertable pinned to the `ts_1m` tablespace (a
-- Hetzner Cloud Volume) so 1m's ~5× row density (~19 GB/yr) lands on cheap block storage,
-- off the 34 GB root disk, without migrating the in-place `candles` data. Chunk interval is
-- 1 DAY (not candles' 1 week) — density-driven: a 1-week chunk would be ~10M rows / ~3.8 GB
-- uncompressed (half of RAM); 1-day keeps the active chunk ~550 MB.
--
-- ROLLOUT (no migration runner — applied MANUALLY, ONCE, in order). PRECONDITIONS:
--   1. The Hetzner volume is attached + mounted at /mnt/pgvol on the host.
--   2. The db compose service bind-mounts /mnt/pgvol into the container.
--   3. /mnt/pgvol/pgdata_1m exists and is owned by the container's postgres OS user
--      (verify: `docker compose exec db id postgres`).
-- Apply this BEFORE restarting the binance/bybit collectors with `1m` in supported_timeframes,
-- else the 1m threads error on a missing table each cycle (isolated per-pair; self-heals on apply).
-- Then run discovery to populate the `1m` collection_status rows (else only the BTC fallback
-- collects at 1m until the next daily discovery). Tablespace mechanics (incl. compressed-chunk
-- placement) validated on timescaledb:latest-pg16.

-- CREATE TABLESPACE is apply-ONCE: it has no IF NOT EXISTS, and Postgres forbids it inside a
-- DO block / function / transaction, so it can't be guarded in-SQL. LOCATION must be an existing
-- empty dir owned by the container's postgres user (runbook precondition 3). Everything below IS
-- idempotent (IF NOT EXISTS / if_not_exists), so a re-apply only errors on this one line; if you
-- must re-run, comment this line out (the tablespace already exists) — do NOT DROP it (it holds data).
CREATE TABLESPACE ts_1m LOCATION '/mnt/pgvol/pgdata_1m';

CREATE TABLE IF NOT EXISTS candles_1m (
    exchange        TEXT NOT NULL,
    market_type     TEXT NOT NULL,
    asset_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    native_symbol   TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    quote_volume    DOUBLE PRECISION,
    trades          INTEGER,
    PRIMARY KEY (exchange, market_type, symbol, timeframe, timestamp)
) TABLESPACE ts_1m;

SELECT create_hypertable('candles_1m', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- Pin new chunks (incl. compressed) to ts_1m. Redundant with the table-level TABLESPACE clause
-- above (validated), but explicit so the placement can't silently regress if the table is ever
-- recreated without the clause.
SELECT attach_tablespace('ts_1m', 'candles_1m', if_not_attached => TRUE);

CREATE INDEX IF NOT EXISTS idx_candles_1m_asset_ts
    ON candles_1m (asset_id, timeframe, timestamp DESC);

-- Compression: segmentby = PK minus timestamp (identical to candles).
ALTER TABLE candles_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, market_type, symbol, timeframe',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('candles_1m', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('candles_1m', INTERVAL '2 years', if_not_exists => TRUE);
