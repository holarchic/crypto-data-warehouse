-- v0.3: Add discovery columns to collection_status for multi-pair support
-- Migration for existing deployments (init.sql handles fresh installs)

ALTER TABLE collection_status ADD COLUMN IF NOT EXISTS asset_id TEXT;
ALTER TABLE collection_status ADD COLUMN IF NOT EXISTS native_symbol TEXT;
ALTER TABLE collection_status ADD COLUMN IF NOT EXISTS last_24h_volume_usd DOUBLE PRECISION;
ALTER TABLE collection_status ADD COLUMN IF NOT EXISTS discovered_at TIMESTAMPTZ;
ALTER TABLE collection_status ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ;
