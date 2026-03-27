-- v0.2: collection_status table for health tracking per exchange/symbol/timeframe
-- Migration for existing deployments (init.sql handles fresh installs)

CREATE TABLE IF NOT EXISTS collection_status (
    exchange        TEXT NOT NULL,
    market_type     TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    active          BOOLEAN DEFAULT TRUE,
    last_timestamp  TIMESTAMPTZ,
    last_success    TIMESTAMPTZ,
    error_count     INTEGER DEFAULT 0,
    last_error      TEXT,
    PRIMARY KEY (exchange, market_type, symbol, timeframe)
);
