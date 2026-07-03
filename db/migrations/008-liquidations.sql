-- v0.7 (SPEC): liquidations — liquidation events, all venues, two fidelity grades
--
-- One hypertable for HL node_fills (Grade A, untruncated tape) + Binance/Bybit WS
-- (Grade C, forward directional sidecar, no historical seed). Superset of the HL
-- seed's 13 columns + venue/grade/liquidated_side. Writers land in .2 (WS) and .3
-- (HL daily S3 batch); this migration is the table only.
--
-- DEDUP: a UNIQUE INDEX (venue, asset_id, timestamp, oid, tid) — NOT a primary key.
-- A PK must be NOT NULL, but oid/tid are NULL for Grade-C rows; and a hypertable
-- requires the partition column in any unique key. HL rows carry non-null oid/tid, so
-- distinct oid/tid preserve real same-ms clustered events AND an exact re-pull dedups
-- via `ON CONFLICT (venue, asset_id, timestamp, oid, tid) DO NOTHING` (idempotency: a
-- daily-batch retry can't double the Grade-A tape). Grade-C rows have NULL oid/tid;
-- under Postgres NULLS DISTINCT they never conflict, so every stream event lands (a
-- reconnect-replay dup is the accepted graded-sidecar cost). `asset_id` holds the
-- seed's `coin` value (matches hl_asset_ctxs; the T-011 export aliases it).
--
-- Verified end-to-end on timescaledb:latest-pg16: clustered HL events preserved, C rows
-- land, HL re-pull dedups, and a chunk compresses with data intact. The compression
-- WARNING that oid/tid aren't in segmentby/orderby is harmless — inserts only ever
-- target recent (uncompressed) chunks, so the unique index is never exercised on
-- compressed data.
--
-- ROLLOUT (no migration runner — applied manually): apply this BEFORE the .2/.3 writers
-- ship. The HL writer pulls D-1 (a still-uncompressed chunk), so its ON CONFLICT never
-- touches a compressed chunk. Replaying HL data older than 7 days would cross into a
-- compressed chunk — decompress the target chunks first (`decompress_chunk`), as in 004.

CREATE TABLE IF NOT EXISTS liquidations (
    timestamp        TIMESTAMPTZ NOT NULL,
    venue            TEXT NOT NULL,
    grade            TEXT NOT NULL,
    asset_id         TEXT NOT NULL,
    liquidated_side  TEXT,
    side             TEXT,
    dir              TEXT,
    px               DOUBLE PRECISION,
    sz               DOUBLE PRECISION,
    mark_px          DOUBLE PRECISION,
    closed_pnl       DOUBLE PRECISION,
    method           TEXT,
    liquidated_user  TEXT,
    oid              BIGINT,
    tid              BIGINT,
    day              TEXT
);

SELECT create_hypertable('liquidations', 'timestamp',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

-- Dedup + query index. Prefix (venue, asset_id, timestamp) serves range queries + the
-- T-011 export (`WHERE venue='hyperliquid'`).
CREATE UNIQUE INDEX IF NOT EXISTS idx_liquidations_dedup
    ON liquidations (venue, asset_id, timestamp, oid, tid);

ALTER TABLE liquidations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, asset_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('liquidations', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('liquidations', INTERVAL '1 year', if_not_exists => TRUE);
