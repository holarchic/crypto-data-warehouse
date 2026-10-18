-- v0.27 (SPEC Decision "keep all history"): the warehouse keeps every row it collects. Removes
-- the retention policy from each hypertable that ever carried one. Idempotent (`if_exists`), and
-- removes only retention jobs: compression policies stay.
--
-- ROLLOUT: apply with ON_ERROR_STOP, then confirm no retention job is left:
--   psql -v ON_ERROR_STOP=1 -U candles -d candle_collector < db/migrations/013-keep-all-history.sql
--   SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention';  -- 0
-- Restoring a snapshot taken before 013 brings its retention jobs back: apply 013 after
-- pg_restore and before timescaledb_post_restore() (scripts/restore.sh, scripts/restore-drill.sh
-- and the deploy/README.md disaster-recovery runbook do).
SELECT remove_retention_policy('candles', if_exists => TRUE);
SELECT remove_retention_policy('candles_1m', if_exists => TRUE);
SELECT remove_retention_policy('funding_rates', if_exists => TRUE);
SELECT remove_retention_policy('open_interest', if_exists => TRUE);
SELECT remove_retention_policy('hl_asset_ctxs', if_exists => TRUE);
SELECT remove_retention_policy('liquidations', if_exists => TRUE);
SELECT remove_retention_policy('premium_index', if_exists => TRUE);
