-- The migration ledger: one row per migration a database has had. Creating it records the
-- baseline, the migrations db/init.sql already contains (001-013), in the same transaction, so a
-- ledger is either absent or complete. initdb runs this file after init.sql on an empty volume;
-- `scripts/migrate.sh --adopt` runs it once on a database built before the ledger existed, after
-- db/baseline-check.sql passes. Later migrations add their own row as they apply.
BEGIN;

CREATE TABLE public.schema_migrations (
    version     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.schema_migrations (version, name) VALUES
    ('001', '001-collection-status.sql'),
    ('002', '002-discovery-columns.sql'),
    ('003', '003-funding-oi-tables.sql'),
    ('004', '004-compression-retention.sql'),
    ('005', '005-hl-asset-ctxs.sql'),
    ('007', '007-oi-chunk-interval.sql'),
    ('008', '008-liquidations.sql'),
    ('009', '009-premium-index.sql'),
    ('010', '010-candles-1m.sql'),
    ('011', '011-drop-slow-timeframes.sql'),
    ('012', '012-discovery-volumes.sql'),
    ('013', '013-keep-all-history.sql');

COMMIT;
