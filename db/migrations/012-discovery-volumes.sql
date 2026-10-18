-- v0.25 (SPEC): discovery_volumes — nightly union max 24h volume per asset
--
-- Discovery keeps an active asset while its highest nightly union max over the last
-- volume_keep_window_days runs holds at the keep threshold. A single night's volume
-- dropped ~79 marginal assets every Sunday on Saturday's dip, each a permanent hole in
-- their OI and HL-context feeds. This table is that history: one row per asset in the
-- night's fetched perp tickers (~920/night). Plain table, not a hypertable (~17 MB/yr),
-- no retention. The PK leads with run_date so the window read is a range scan.
--
-- ROLLOUT (no migration runner — applied manually): apply with psql -v ON_ERROR_STOP=1
-- BEFORE rebuilding the discovery image; a discovery run against a missing table rolls
-- back and exits 1 without touching collection_status. Idempotent (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS discovery_volumes (
    run_date        DATE NOT NULL,
    asset_id        TEXT NOT NULL,
    max_volume_usd  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_date, asset_id)
);
