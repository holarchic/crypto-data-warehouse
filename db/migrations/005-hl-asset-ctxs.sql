-- v0.4 (SPEC): hl_asset_ctxs — HyperLiquid per-asset context bundle
--
-- The full HL metaAndAssetCtxs per-asset context, persisted at HL's OI cadence
-- (1-min, per T-007.1). Single-venue (HyperLiquid only). Columns mirror the
-- backtester's Grade-B seed (hl-asset-ctxs.parquet) 1:1 so the forward tape is a
-- schema-faithful continuation. Self-contained: no dependency on 004 ordering.
--
-- ROLLOUT (no migration runner — applied manually): apply this BEFORE restarting
-- the HL collector, else the 1-min loop errors on a missing table every cycle.

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
