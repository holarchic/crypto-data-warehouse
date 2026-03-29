-- v0.6: Compression policies (>7 days) + retention safety nets
--
-- Compression constraint: TimescaleDB does not allow INSERT into compressed
-- chunks. Safe because collectors only insert current-period data (never
-- backfill >7 days old). If backfill is ever needed, manually decompress
-- the target chunk first: SELECT decompress_chunk('<chunk_name>');

-- Candles compression: segmentby = PK minus timestamp
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, market_type, symbol, timeframe',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('candles', INTERVAL '7 days', if_not_exists => TRUE);

-- Funding rates compression
ALTER TABLE funding_rates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('funding_rates', INTERVAL '7 days', if_not_exists => TRUE);

-- Open interest compression
ALTER TABLE open_interest SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('open_interest', INTERVAL '7 days', if_not_exists => TRUE);

-- Retention safety nets (generous — prevents unbounded growth over years)
SELECT add_retention_policy('candles', INTERVAL '2 years', if_not_exists => TRUE);
SELECT add_retention_policy('funding_rates', INTERVAL '1 year', if_not_exists => TRUE);
SELECT add_retention_policy('open_interest', INTERVAL '1 year', if_not_exists => TRUE);
