-- v0.13 (SPEC): premium_index — Binance premium-index term structure
--
-- The Binance perp premium-index (mark-vs-index spread that drives funding), polled
-- as 5-min OHLC klines (premiumIndexKlines). Binance-only (HL premium lives in
-- hl_asset_ctxs; there is no bybit-premium seed). Captured at 5m — a superset of the
-- backtester's 1h binance-premium-index seed (downsamples to 1h for the T-011 export).
-- The writer lands in T-010 (collector/premium_index.py); this migration is the table.
--
-- ROLLOUT (no migration runner — applied manually): apply this BEFORE restarting the
-- binance collector with has_premium_index, else the premium loop errors on a missing
-- table every cycle.

CREATE TABLE IF NOT EXISTS premium_index (
    exchange       TEXT NOT NULL,
    asset_id       TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    native_symbol  TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    premium_open   DOUBLE PRECISION,
    premium_high   DOUBLE PRECISION,
    premium_low    DOUBLE PRECISION,
    premium_close  DOUBLE PRECISION,
    PRIMARY KEY (exchange, symbol, timestamp)
);

SELECT create_hypertable('premium_index', 'timestamp',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_premium_index_asset_ts
    ON premium_index (asset_id, timestamp DESC);

ALTER TABLE premium_index SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('premium_index', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('premium_index', INTERVAL '1 year', if_not_exists => TRUE);
