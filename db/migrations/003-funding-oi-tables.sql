-- v0.4: Add funding_rates and open_interest tables

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
