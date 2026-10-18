-- Row counts the restore drill compares between a restored snapshot and prod
-- (scripts/restore-drill.sh prints them for the restore; run them on prod just before and
-- just after the dump being drilled, with the same :since/:until).
-- The [since, until) window sits behind every late writer (the HL liquidation pull rewrites
-- only the last 3 days), no table deletes rows (no retention), and both totals only gain rows
-- (discovery upserts and deactivates by UPDATE), so each count only grows while a dump runs:
-- prod before <= restored <= prod after.
\set QUIET on
SET TIME ZONE 'UTC';

SELECT 'candles' AS series, count(*) FROM candles
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'candles_1m', count(*) FROM candles_1m
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'funding_rates', count(*) FROM funding_rates
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'open_interest', count(*) FROM open_interest
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'hl_asset_ctxs', count(*) FROM hl_asset_ctxs
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'premium_index', count(*) FROM premium_index
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'liquidations', count(*) FROM liquidations
    WHERE timestamp >= :'since' AND timestamp < :'until'
UNION ALL SELECT 'discovery_volumes', count(*) FROM discovery_volumes
    WHERE run_date >= :'since' AND run_date < :'until'
UNION ALL SELECT 'discovery_volumes (total)', count(*) FROM discovery_volumes
UNION ALL SELECT 'collection_status (total)', count(*) FROM collection_status;
