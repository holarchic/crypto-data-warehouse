-- Read-only: does this database have every effect of the baseline migrations 001-013? One row per
-- check, `check|ok`. `scripts/migrate.sh --adopt` runs it and records the baseline only when every
-- row is `t`. Tablespace placement (010's ts_1m) is prod-only, so it is not part of the baseline.
SELECT 'table ' || t, to_regclass('public.' || t) IS NOT NULL
FROM unnest(ARRAY['candles', 'candles_1m', 'funding_rates', 'open_interest', 'hl_asset_ctxs',
                  'liquidations', 'premium_index', 'collection_status', 'discovery_volumes']) AS t
UNION ALL
SELECT 'collection_status.' || c, EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'collection_status' AND column_name = c)
FROM unnest(ARRAY['asset_id', 'native_symbol', 'last_24h_volume_usd', 'discovered_at',
                  'deactivated_at']) AS c
UNION ALL
SELECT 'compression policy ' || h, EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression' AND hypertable_schema = 'public' AND hypertable_name = h)
FROM unnest(ARRAY['candles', 'candles_1m', 'funding_rates', 'open_interest', 'hl_asset_ctxs',
                  'liquidations', 'premium_index']) AS h
UNION ALL
SELECT 'no retention policy', NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention')
UNION ALL
SELECT 'open_interest chunks of 7 days', EXISTS (
    SELECT 1 FROM timescaledb_information.dimensions
    WHERE hypertable_schema = 'public' AND hypertable_name = 'open_interest'
      AND time_interval = INTERVAL '7 days')
UNION ALL
SELECT 'no active 1d/3d/1w rows', NOT EXISTS (
    SELECT 1 FROM collection_status WHERE timeframe IN ('1d', '3d', '1w') AND active)
UNION ALL
SELECT 'index idx_liquidations_dedup', to_regclass('public.idx_liquidations_dedup') IS NOT NULL;
