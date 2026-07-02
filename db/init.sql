CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS candles (
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
);

SELECT create_hypertable('candles', 'timestamp',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_candles_asset_timeframe_ts
    ON candles (asset_id, timeframe, timestamp DESC);

-- Compression: segmentby = PK minus timestamp
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, market_type, symbol, timeframe',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('candles', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('candles', INTERVAL '2 years', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS funding_rates (
    exchange        TEXT NOT NULL,
    asset_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    rate            DOUBLE PRECISION,
    predicted_rate  DOUBLE PRECISION,
    interval_hours  INTEGER,
    PRIMARY KEY (exchange, symbol, timestamp)
);

SELECT create_hypertable('funding_rates', 'timestamp',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_funding_rates_asset_ts
    ON funding_rates (asset_id, timestamp DESC);

ALTER TABLE funding_rates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('funding_rates', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('funding_rates', INTERVAL '1 year', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS open_interest (
    exchange        TEXT NOT NULL,
    asset_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    open_interest   DOUBLE PRECISION,
    oi_value_usd    DOUBLE PRECISION,
    PRIMARY KEY (exchange, symbol, timestamp)
);

SELECT create_hypertable('open_interest', 'timestamp',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_open_interest_asset_ts
    ON open_interest (asset_id, timestamp DESC);

ALTER TABLE open_interest SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('open_interest', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('open_interest', INTERVAL '1 year', if_not_exists => TRUE);

-- HyperLiquid per-asset context bundle (full metaAndAssetCtxs), 1-min cadence.
-- Single-venue; columns mirror the backtester Grade-B seed (hl-asset-ctxs.parquet) 1:1.
CREATE TABLE IF NOT EXISTS hl_asset_ctxs (
    asset_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    funding         DOUBLE PRECISION,
    open_interest   DOUBLE PRECISION,
    prev_day_px     DOUBLE PRECISION,
    day_ntl_vlm     DOUBLE PRECISION,
    premium         DOUBLE PRECISION,
    oracle_px       DOUBLE PRECISION,
    mark_px         DOUBLE PRECISION,
    mid_px          DOUBLE PRECISION,
    impact_bid_px   DOUBLE PRECISION,
    impact_ask_px   DOUBLE PRECISION,
    PRIMARY KEY (asset_id, timestamp)
);

SELECT create_hypertable('hl_asset_ctxs', 'timestamp',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_hl_asset_ctxs_asset_ts
    ON hl_asset_ctxs (asset_id, timestamp DESC);

ALTER TABLE hl_asset_ctxs SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('hl_asset_ctxs', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('hl_asset_ctxs', INTERVAL '1 year', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS collection_status (
    exchange            TEXT NOT NULL,
    market_type         TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    timeframe           TEXT NOT NULL,
    active              BOOLEAN DEFAULT TRUE,
    asset_id            TEXT,
    native_symbol       TEXT,
    last_24h_volume_usd DOUBLE PRECISION,
    discovered_at       TIMESTAMPTZ,
    deactivated_at      TIMESTAMPTZ,
    last_timestamp      TIMESTAMPTZ,
    last_success        TIMESTAMPTZ,
    error_count         INTEGER DEFAULT 0,
    last_error          TEXT,
    PRIMARY KEY (exchange, market_type, symbol, timeframe)
);
