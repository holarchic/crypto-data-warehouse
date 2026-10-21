-- Read-only: does this database hold every effect of the baseline migrations 001-013? One row per
-- check, `check|ok`. `scripts/migrate.sh --adopt` records the baseline only when every row is `t`.
--
-- `expected` is the baseline's catalog as `actual` projects it: column types, nullability and
-- defaults; constraints; index definitions and validity; hypertable chunk intervals; compression
-- settings and policy intervals. It was taken from a database built by db/init.sql and is frozen
-- with it; tests/integration/check_migrations.sh proves `expected` and `actual` are equal on a fresh
-- init.sql database. A database may hold more than the baseline; every expected row must be
-- present. Tablespace placement (prod's ts_1m) is not baseline.
WITH baseline_tables(t) AS (VALUES ('candles'), ('candles_1m'), ('funding_rates'), ('open_interest'),
    ('hl_asset_ctxs'), ('liquidations'), ('premium_index'), ('collection_status'), ('discovery_volumes')),
actual(kind, object, detail) AS (
SELECT 'column', c.table_name || '.' || c.column_name,
       c.data_type || CASE WHEN c.is_nullable = 'NO' THEN ' not null' ELSE '' END
           || coalesce(' default ' || c.column_default, '')
FROM information_schema.columns c JOIN baseline_tables b ON b.t = c.table_name
WHERE c.table_schema = 'public'
UNION ALL
SELECT 'constraint', con.conname, con.contype::text || ' ' || pg_get_constraintdef(con.oid)
FROM pg_constraint con JOIN pg_class tc ON tc.oid = con.conrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace JOIN baseline_tables b ON b.t = tc.relname
WHERE n.nspname = 'public'
UNION ALL
SELECT 'index', ic.relname,
       regexp_replace(pg_get_indexdef(x.indexrelid), '^CREATE (UNIQUE )?INDEX \S+ ON public\.', 'CREATE \1INDEX ON ')
           || CASE WHEN x.indisvalid AND x.indisready THEN '' ELSE ' (not valid)' END
FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid JOIN pg_class tc ON tc.oid = x.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace JOIN baseline_tables b ON b.t = tc.relname
WHERE n.nspname = 'public' AND ic.relname NOT LIKE '%_timestamp_idx'
UNION ALL
SELECT 'chunk interval', d.hypertable_name, d.time_interval::text
FROM timescaledb_information.dimensions d WHERE d.hypertable_schema = 'public'
UNION ALL
SELECT 'compression', s.hypertable_name,
       string_agg(s.attname || CASE WHEN s.segmentby_column_index IS NOT NULL THEN ' segmentby ' || s.segmentby_column_index
                                    ELSE ' orderby ' || s.orderby_column_index || CASE WHEN s.orderby_asc THEN ' asc' ELSE ' desc' END END,
                  ', ' ORDER BY s.attname)
FROM timescaledb_information.compression_settings s WHERE s.hypertable_schema = 'public' GROUP BY s.hypertable_name
UNION ALL
SELECT 'compression policy', j.hypertable_name, j.config ->> 'compress_after'
FROM timescaledb_information.jobs j WHERE j.proc_name = 'policy_compression' AND j.hypertable_schema = 'public'
),
expected(kind, object, detail) AS (VALUES
    ('chunk interval', 'candles', '7 days'),
    ('chunk interval', 'candles_1m', '1 day'),
    ('chunk interval', 'funding_rates', '30 days'),
    ('chunk interval', 'hl_asset_ctxs', '7 days'),
    ('chunk interval', 'liquidations', '7 days'),
    ('chunk interval', 'open_interest', '7 days'),
    ('chunk interval', 'premium_index', '7 days'),
    ('column', 'candles.asset_id', 'text not null'),
    ('column', 'candles.close', 'double precision'),
    ('column', 'candles.exchange', 'text not null'),
    ('column', 'candles.high', 'double precision'),
    ('column', 'candles.low', 'double precision'),
    ('column', 'candles.market_type', 'text not null'),
    ('column', 'candles.native_symbol', 'text not null'),
    ('column', 'candles.open', 'double precision'),
    ('column', 'candles.quote_volume', 'double precision'),
    ('column', 'candles.symbol', 'text not null'),
    ('column', 'candles.timeframe', 'text not null'),
    ('column', 'candles.timestamp', 'timestamp with time zone not null'),
    ('column', 'candles.trades', 'integer'),
    ('column', 'candles.volume', 'double precision'),
    ('column', 'candles_1m.asset_id', 'text not null'),
    ('column', 'candles_1m.close', 'double precision'),
    ('column', 'candles_1m.exchange', 'text not null'),
    ('column', 'candles_1m.high', 'double precision'),
    ('column', 'candles_1m.low', 'double precision'),
    ('column', 'candles_1m.market_type', 'text not null'),
    ('column', 'candles_1m.native_symbol', 'text not null'),
    ('column', 'candles_1m.open', 'double precision'),
    ('column', 'candles_1m.quote_volume', 'double precision'),
    ('column', 'candles_1m.symbol', 'text not null'),
    ('column', 'candles_1m.timeframe', 'text not null'),
    ('column', 'candles_1m.timestamp', 'timestamp with time zone not null'),
    ('column', 'candles_1m.trades', 'integer'),
    ('column', 'candles_1m.volume', 'double precision'),
    ('column', 'collection_status.active', 'boolean default true'),
    ('column', 'collection_status.asset_id', 'text'),
    ('column', 'collection_status.deactivated_at', 'timestamp with time zone'),
    ('column', 'collection_status.discovered_at', 'timestamp with time zone'),
    ('column', 'collection_status.error_count', 'integer default 0'),
    ('column', 'collection_status.exchange', 'text not null'),
    ('column', 'collection_status.last_24h_volume_usd', 'double precision'),
    ('column', 'collection_status.last_error', 'text'),
    ('column', 'collection_status.last_success', 'timestamp with time zone'),
    ('column', 'collection_status.last_timestamp', 'timestamp with time zone'),
    ('column', 'collection_status.market_type', 'text not null'),
    ('column', 'collection_status.native_symbol', 'text'),
    ('column', 'collection_status.symbol', 'text not null'),
    ('column', 'collection_status.timeframe', 'text not null'),
    ('column', 'discovery_volumes.asset_id', 'text not null'),
    ('column', 'discovery_volumes.max_volume_usd', 'double precision not null'),
    ('column', 'discovery_volumes.run_date', 'date not null'),
    ('column', 'funding_rates.asset_id', 'text not null'),
    ('column', 'funding_rates.exchange', 'text not null'),
    ('column', 'funding_rates.interval_hours', 'integer'),
    ('column', 'funding_rates.predicted_rate', 'double precision'),
    ('column', 'funding_rates.rate', 'double precision'),
    ('column', 'funding_rates.symbol', 'text not null'),
    ('column', 'funding_rates.timestamp', 'timestamp with time zone not null'),
    ('column', 'hl_asset_ctxs.asset_id', 'text not null'),
    ('column', 'hl_asset_ctxs.day_ntl_vlm', 'double precision'),
    ('column', 'hl_asset_ctxs.funding', 'double precision'),
    ('column', 'hl_asset_ctxs.impact_ask_px', 'double precision'),
    ('column', 'hl_asset_ctxs.impact_bid_px', 'double precision'),
    ('column', 'hl_asset_ctxs.mark_px', 'double precision'),
    ('column', 'hl_asset_ctxs.mid_px', 'double precision'),
    ('column', 'hl_asset_ctxs.open_interest', 'double precision'),
    ('column', 'hl_asset_ctxs.oracle_px', 'double precision'),
    ('column', 'hl_asset_ctxs.premium', 'double precision'),
    ('column', 'hl_asset_ctxs.prev_day_px', 'double precision'),
    ('column', 'hl_asset_ctxs.symbol', 'text not null'),
    ('column', 'hl_asset_ctxs.timestamp', 'timestamp with time zone not null'),
    ('column', 'liquidations.asset_id', 'text not null'),
    ('column', 'liquidations.closed_pnl', 'double precision'),
    ('column', 'liquidations.day', 'text'),
    ('column', 'liquidations.dir', 'text'),
    ('column', 'liquidations.grade', 'text not null'),
    ('column', 'liquidations.liquidated_side', 'text'),
    ('column', 'liquidations.liquidated_user', 'text'),
    ('column', 'liquidations.mark_px', 'double precision'),
    ('column', 'liquidations.method', 'text'),
    ('column', 'liquidations.oid', 'bigint'),
    ('column', 'liquidations.px', 'double precision'),
    ('column', 'liquidations.side', 'text'),
    ('column', 'liquidations.sz', 'double precision'),
    ('column', 'liquidations.tid', 'bigint'),
    ('column', 'liquidations.timestamp', 'timestamp with time zone not null'),
    ('column', 'liquidations.venue', 'text not null'),
    ('column', 'open_interest.asset_id', 'text not null'),
    ('column', 'open_interest.exchange', 'text not null'),
    ('column', 'open_interest.oi_value_usd', 'double precision'),
    ('column', 'open_interest.open_interest', 'double precision'),
    ('column', 'open_interest.symbol', 'text not null'),
    ('column', 'open_interest.timestamp', 'timestamp with time zone not null'),
    ('column', 'premium_index.asset_id', 'text not null'),
    ('column', 'premium_index.exchange', 'text not null'),
    ('column', 'premium_index.native_symbol', 'text not null'),
    ('column', 'premium_index.premium_close', 'double precision'),
    ('column', 'premium_index.premium_high', 'double precision'),
    ('column', 'premium_index.premium_low', 'double precision'),
    ('column', 'premium_index.premium_open', 'double precision'),
    ('column', 'premium_index.symbol', 'text not null'),
    ('column', 'premium_index.timestamp', 'timestamp with time zone not null'),
    ('compression', 'candles', 'exchange segmentby 1, market_type segmentby 2, symbol segmentby 3, timeframe segmentby 4, timestamp orderby 1 desc'),
    ('compression', 'candles_1m', 'exchange segmentby 1, market_type segmentby 2, symbol segmentby 3, timeframe segmentby 4, timestamp orderby 1 desc'),
    ('compression', 'funding_rates', 'exchange segmentby 1, symbol segmentby 2, timestamp orderby 1 desc'),
    ('compression', 'hl_asset_ctxs', 'asset_id segmentby 1, timestamp orderby 1 desc'),
    ('compression', 'liquidations', 'asset_id segmentby 2, timestamp orderby 1 desc, venue segmentby 1'),
    ('compression', 'open_interest', 'exchange segmentby 1, symbol segmentby 2, timestamp orderby 1 desc'),
    ('compression', 'premium_index', 'exchange segmentby 1, symbol segmentby 2, timestamp orderby 1 desc'),
    ('compression policy', 'candles', '7 days'),
    ('compression policy', 'candles_1m', '7 days'),
    ('compression policy', 'funding_rates', '7 days'),
    ('compression policy', 'hl_asset_ctxs', '7 days'),
    ('compression policy', 'liquidations', '7 days'),
    ('compression policy', 'open_interest', '7 days'),
    ('compression policy', 'premium_index', '7 days'),
    ('constraint', 'candles_1m_pkey', 'p PRIMARY KEY (exchange, market_type, symbol, timeframe, "timestamp")'),
    ('constraint', 'candles_pkey', 'p PRIMARY KEY (exchange, market_type, symbol, timeframe, "timestamp")'),
    ('constraint', 'collection_status_pkey', 'p PRIMARY KEY (exchange, market_type, symbol, timeframe)'),
    ('constraint', 'discovery_volumes_pkey', 'p PRIMARY KEY (run_date, asset_id)'),
    ('constraint', 'funding_rates_pkey', 'p PRIMARY KEY (exchange, symbol, "timestamp")'),
    ('constraint', 'hl_asset_ctxs_pkey', 'p PRIMARY KEY (asset_id, "timestamp")'),
    ('constraint', 'open_interest_pkey', 'p PRIMARY KEY (exchange, symbol, "timestamp")'),
    ('constraint', 'premium_index_pkey', 'p PRIMARY KEY (exchange, symbol, "timestamp")'),
    ('index', 'candles_1m_pkey', 'CREATE UNIQUE INDEX ON candles_1m USING btree (exchange, market_type, symbol, timeframe, "timestamp")'),
    ('index', 'candles_pkey', 'CREATE UNIQUE INDEX ON candles USING btree (exchange, market_type, symbol, timeframe, "timestamp")'),
    ('index', 'collection_status_pkey', 'CREATE UNIQUE INDEX ON collection_status USING btree (exchange, market_type, symbol, timeframe)'),
    ('index', 'discovery_volumes_pkey', 'CREATE UNIQUE INDEX ON discovery_volumes USING btree (run_date, asset_id)'),
    ('index', 'funding_rates_pkey', 'CREATE UNIQUE INDEX ON funding_rates USING btree (exchange, symbol, "timestamp")'),
    ('index', 'hl_asset_ctxs_pkey', 'CREATE UNIQUE INDEX ON hl_asset_ctxs USING btree (asset_id, "timestamp")'),
    ('index', 'idx_candles_1m_asset_ts', 'CREATE INDEX ON candles_1m USING btree (asset_id, timeframe, "timestamp" DESC)'),
    ('index', 'idx_candles_asset_timeframe_ts', 'CREATE INDEX ON candles USING btree (asset_id, timeframe, "timestamp" DESC)'),
    ('index', 'idx_funding_rates_asset_ts', 'CREATE INDEX ON funding_rates USING btree (asset_id, "timestamp" DESC)'),
    ('index', 'idx_hl_asset_ctxs_asset_ts', 'CREATE INDEX ON hl_asset_ctxs USING btree (asset_id, "timestamp" DESC)'),
    ('index', 'idx_liquidations_dedup', 'CREATE UNIQUE INDEX ON liquidations USING btree (venue, asset_id, "timestamp", oid, tid)'),
    ('index', 'idx_open_interest_asset_ts', 'CREATE INDEX ON open_interest USING btree (asset_id, "timestamp" DESC)'),
    ('index', 'idx_premium_index_asset_ts', 'CREATE INDEX ON premium_index USING btree (asset_id, "timestamp" DESC)'),
    ('index', 'open_interest_pkey', 'CREATE UNIQUE INDEX ON open_interest USING btree (exchange, symbol, "timestamp")'),
    ('index', 'premium_index_pkey', 'CREATE UNIQUE INDEX ON premium_index USING btree (exchange, symbol, "timestamp")')
)
SELECT e.kind || ' ' || e.object || ': ' || e.detail,
       EXISTS (SELECT 1 FROM actual a WHERE (a.kind, a.object, a.detail) = (e.kind, e.object, e.detail))
FROM expected e
UNION ALL
SELECT 'no retention policy', NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention')
UNION ALL
SELECT 'no active 1d/3d/1w collection_status rows (011)', NOT EXISTS (
    SELECT 1 FROM collection_status WHERE timeframe IN ('1d', '3d', '1w') AND active);
