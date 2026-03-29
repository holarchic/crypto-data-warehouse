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
