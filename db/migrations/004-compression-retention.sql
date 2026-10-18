-- v0.6: Compression policies (>7 days). No retention: every row is kept (migration 013).
--
-- Compressed chunks still accept writes, at extra cost per write. Collectors write
-- current-period data, so compression after 7 days rarely meets one.

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
