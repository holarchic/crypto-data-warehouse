-- The database half of the v0.8 stability scorecard (scripts/stability-scorecard.sh, which passes
-- the psql variables): candle, funding and OI capture for the active universe, primary keys, and
-- the discovery diagnostic. Read-only: temp tables only. Ends with exactly one marker line,
-- `SCORECARD-DB: PASS` or `SCORECARD-DB: FAIL`.
--
--   days   the window, in days back from the reference time
--   meta   JSON from the deployed config: intervals, volume_threshold_usd, funding_exchanges,
--          oi_exchanges
--   as_of  the reference time (ISO-8601); empty means now(). One clock for the whole run.
--
-- A pair's window opens at the later of the window start and its discovered_at, rounded up to a
-- slot. A candle or OI slot t is expected once it should be stored: t + slot + lag <= the reference
-- time; a funding slot is a settlement, so it is expected once t + lag <= the reference time.

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off
SET statement_timeout = '15min';

CREATE TEMP TABLE sc_run AS
SELECT t0, t0 - make_interval(days => (:'days')::int) AS window_start, (:'meta')::jsonb AS meta
FROM (SELECT coalesce(nullif(:'as_of', ''), now()::text)::timestamptz AS t0) AS clock;

-- Literal bounds, so each hypertable scan excludes chunks at plan time.
SELECT t0, window_start FROM sc_run \gset

CREATE FUNCTION pg_temp.slot_floor(ts timestamptz, slot_s int) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE AS $$ SELECT to_timestamp(floor(extract(epoch FROM ts) / slot_s) * slot_s) $$;
CREATE FUNCTION pg_temp.slot_ceil(ts timestamptz, slot_s int) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE AS $$ SELECT to_timestamp(ceil(extract(epoch FROM ts) / slot_s) * slot_s) $$;

-- One row per (section, pair) with its slot length and expected slot range [first_slot, end_slot).
CREATE TEMP TABLE sc_expect (
    section     text NOT NULL,
    exchange    text NOT NULL,
    market_type text,
    symbol      text NOT NULL,
    timeframe   text,
    slot_s      int  NOT NULL,
    first_slot  timestamptz NOT NULL,
    end_slot    timestamptz NOT NULL
);

-- Candles: each active pair on a configured timeframe. Loop sentinels carry pseudo-timeframes and
-- HL 1m has no per-pair rows, so neither joins. A bar is due one hour after it closes.
INSERT INTO sc_expect
SELECT 'candles', cs.exchange, cs.market_type, cs.symbol, cs.timeframe, tf.interval_s,
       pg_temp.slot_ceil(greatest(r.window_start, coalesce(cs.discovered_at, r.window_start)), tf.interval_s),
       pg_temp.slot_floor(r.t0 - make_interval(secs => tf.interval_s) - interval '1 hour', tf.interval_s)
           + make_interval(secs => tf.interval_s)
FROM sc_run r
CROSS JOIN LATERAL (SELECT key AS timeframe, value::int AS interval_s FROM jsonb_each_text(r.meta -> 'intervals')) tf
JOIN collection_status cs ON cs.timeframe = tf.timeframe AND cs.active;

-- The derivatives universe: the active 5m rows, as the funding and OI loops read it
-- (status.active_instruments, at UNIVERSE_TIMEFRAME).
CREATE TEMP TABLE sc_derivatives AS
SELECT exchange, symbol, discovered_at FROM collection_status WHERE active AND timeframe = '5m';

-- Funding: the slot is the pair's stored interval_hours (the gap to its previous settlement). A
-- pair whose interval changed inside the window has no single slot, so it is not measured (counted
-- apart). A pair with no row in the window has captured nothing: its slots are taken at 8 h, and any
-- interval gives 0 %. A settlement is due two hours after it happens (the loop polls hourly).
CREATE TEMP TABLE sc_funding_pairs AS
SELECT d.exchange, d.symbol, d.discovered_at, i.intervals, i.newest
FROM sc_run r
JOIN sc_derivatives d ON r.meta -> 'funding_exchanges' ? d.exchange
CROSS JOIN LATERAL (
    SELECT count(DISTINCT f.interval_hours) AS intervals,
           (array_agg(f.interval_hours ORDER BY f.timestamp DESC) FILTER (WHERE f.interval_hours IS NOT NULL))[1] AS newest
    FROM funding_rates f
    WHERE f.exchange = d.exchange AND f.symbol = d.symbol
      AND f.timestamp >= :'window_start'::timestamptz AND f.timestamp < :'t0'::timestamptz
) i;

INSERT INTO sc_expect
SELECT 'funding', p.exchange, NULL, p.symbol, NULL, h.slot_s,
       pg_temp.slot_ceil(greatest(r.window_start, coalesce(p.discovered_at, r.window_start)), h.slot_s),
       pg_temp.slot_floor(r.t0 - interval '2 hours', h.slot_s) + make_interval(secs => h.slot_s)
FROM sc_run r
JOIN sc_funding_pairs p ON p.intervals <= 1
CROSS JOIN LATERAL (SELECT 3600 * coalesce(p.newest, 8) AS slot_s) h;

-- OI: the target cadence, a snapshot per minute on HyperLiquid and per 5 minutes elsewhere.
-- A bucket is due an hour after it closes (the Binance baseline refreshes every 30 minutes).
INSERT INTO sc_expect
SELECT 'oi', d.exchange, NULL, d.symbol, NULL, b.slot_s,
       pg_temp.slot_ceil(greatest(r.window_start, coalesce(d.discovered_at, r.window_start)), b.slot_s),
       pg_temp.slot_floor(r.t0 - make_interval(secs => b.slot_s) - interval '1 hour', b.slot_s)
           + make_interval(secs => b.slot_s)
FROM sc_run r
JOIN sc_derivatives d ON r.meta -> 'oi_exchanges' ? d.exchange
CROSS JOIN LATERAL (SELECT CASE WHEN d.exchange = 'hyperliquid' THEN 60 ELSE 300 END AS slot_s) b;

-- Stored slots per pair inside its expected range. Candles count rows (the primary key makes each
-- bar one row); funding and OI count the distinct slots that hold at least one row.
CREATE TEMP TABLE sc_seen AS
SELECT e.section, e.exchange, e.symbol, e.timeframe, count(*) AS observed
FROM sc_expect e
JOIN (SELECT exchange, market_type, symbol, timeframe, timestamp FROM candles
      WHERE timestamp >= :'window_start'::timestamptz AND timestamp < :'t0'::timestamptz
      UNION ALL
      SELECT exchange, market_type, symbol, timeframe, timestamp FROM candles_1m
      WHERE timestamp >= :'window_start'::timestamptz AND timestamp < :'t0'::timestamptz) c
  ON c.exchange = e.exchange AND c.market_type = e.market_type AND c.symbol = e.symbol AND c.timeframe = e.timeframe
WHERE e.section = 'candles' AND c.timestamp >= e.first_slot AND c.timestamp < e.end_slot
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT e.section, e.exchange, e.symbol, NULL,
       count(DISTINCT date_bin(make_interval(secs => e.slot_s), f.timestamp, TIMESTAMPTZ 'epoch'))
FROM sc_expect e
JOIN funding_rates f ON f.exchange = e.exchange AND f.symbol = e.symbol
WHERE e.section = 'funding'
  AND f.timestamp >= :'window_start'::timestamptz AND f.timestamp < :'t0'::timestamptz
  AND f.timestamp >= e.first_slot AND f.timestamp < e.end_slot
GROUP BY 1, 2, 3
UNION ALL
SELECT e.section, e.exchange, e.symbol, NULL,
       count(DISTINCT date_bin(make_interval(secs => e.slot_s), o.timestamp, TIMESTAMPTZ 'epoch'))
FROM sc_expect e
JOIN open_interest o ON o.exchange = e.exchange AND o.symbol = e.symbol
WHERE e.section = 'oi'
  AND o.timestamp >= :'window_start'::timestamptz AND o.timestamp < :'t0'::timestamptz
  AND o.timestamp >= e.first_slot AND o.timestamp < e.end_slot
GROUP BY 1, 2, 3;

-- Per pair: expected slots and the stored ones, capped at expected so one pair's surplus can't
-- cover another's shortfall.
CREATE TEMP TABLE sc_pair AS
SELECT e.section, e.exchange, coalesce(e.timeframe, '') AS timeframe, e.end_slot,
       greatest(0, floor((extract(epoch FROM e.end_slot) - extract(epoch FROM e.first_slot)) / e.slot_s))::bigint AS expected,
       coalesce(s.observed, 0) AS observed
FROM sc_expect e
LEFT JOIN sc_seen s ON s.section = e.section AND s.exchange = e.exchange AND s.symbol = e.symbol
                   AND s.timeframe IS NOT DISTINCT FROM e.timeframe;

-- Every enabled venue gets a row, so a venue with no active pairs shows (and fails) instead of
-- leaving the denominator.
CREATE TEMP TABLE sc_venue AS
SELECT v.section, v.exchange, coalesce(p.timeframe, '') AS timeframe,
       count(p.expected) AS pairs, coalesce(sum(p.expected), 0) AS expected,
       coalesce(sum(least(p.observed, p.expected)), 0) AS captured, max(p.end_slot) AS measured_to
FROM (SELECT 'candles' AS section, exchange FROM sc_expect WHERE section = 'candles'
      UNION SELECT 'funding', jsonb_array_elements_text(meta -> 'funding_exchanges') FROM sc_run
      UNION SELECT 'oi', jsonb_array_elements_text(meta -> 'oi_exchanges') FROM sc_run) v
LEFT JOIN sc_pair p ON p.section = v.section AND p.exchange = v.exchange
GROUP BY 1, 2, 3;

CREATE TEMP TABLE sc_checks (ord int, check_name text, measured text, target text, pass boolean);

-- A section passes on its overall capture, and only with something to measure on every venue.
INSERT INTO sc_checks
SELECT t.ord, t.check_name,
       CASE WHEN coalesce(sum(v.expected), 0) = 0 THEN 'no expected slots'
            ELSE round(100.0 * sum(v.captured) / sum(v.expected), 3) || ' % of ' || sum(v.expected) || ' slots'
                 || coalesce(', no slots on ' || string_agg(v.exchange, ', ') FILTER (WHERE v.expected = 0), '') END
       || CASE WHEN t.section = 'funding' AND u.pairs > 0
               THEN ', ' || u.pairs || ' pair(s) not measured (interval changed)' ELSE '' END,
       '>= ' || t.target_pct || ' %',
       coalesce(sum(v.expected) > 0 AND bool_and(v.expected > 0)
                AND sum(v.captured) * 1000 >= sum(v.expected) * t.target_permille, false)
FROM (VALUES (1, 'candles', 'candle capture', '99.9', 999),
             (2, 'funding', 'funding-slot occupancy (stored interval)', '99', 990),
             (3, 'oi', 'OI bucket occupancy (60 s HL, 300 s elsewhere)', '95', 950)) t(ord, section, check_name, target_pct, target_permille)
LEFT JOIN (SELECT section, exchange, sum(expected) AS expected, sum(captured) AS captured
           FROM sc_venue GROUP BY 1, 2) v ON v.section = t.section
CROSS JOIN (SELECT count(*) AS pairs FROM sc_funding_pairs WHERE intervals > 1) u
GROUP BY t.ord, t.section, t.check_name, t.target_pct, t.target_permille, u.pairs;

INSERT INTO sc_checks
SELECT 4, 'primary keys', string_agg(tbl || CASE WHEN has_pk THEN '' ELSE ' (missing)' END, ', '),
       'all four tables', bool_and(has_pk)
FROM (SELECT tbl, EXISTS (SELECT 1 FROM pg_constraint c
                          WHERE c.conrelid = to_regclass('public.' || tbl) AND c.contype = 'p') AS has_pk
      FROM unnest(ARRAY['candles', 'candles_1m', 'funding_rates', 'open_interest']) AS tbl) k;

-- Discovery stores only each asset's cross-venue maximum, so this checks the union: a qualifying
-- asset on the latest run must be active somewhere, and the latest run must be recent.
CREATE TEMP TABLE sc_uncollected AS
SELECT dv.asset_id, dv.max_volume_usd
FROM discovery_volumes dv, sc_run r
WHERE dv.run_date = (SELECT max(run_date) FROM discovery_volumes)
  AND dv.max_volume_usd >= (r.meta ->> 'volume_threshold_usd')::double precision
  AND NOT EXISTS (SELECT 1 FROM collection_status cs
                  WHERE cs.active AND cs.timeframe = '5m' AND cs.asset_id = dv.asset_id);

INSERT INTO sc_checks
SELECT 5, 'discovery (union of venues)',
       coalesce('latest run ' || last_run, 'no discovery run') || ', ' || uncollected || ' qualifying asset(s) not collected',
       'run within a day, 0 uncollected',
       last_run IS NOT NULL AND last_run >= (r.t0 AT TIME ZONE 'UTC')::date - 1 AND uncollected = 0
FROM sc_run r,
     (SELECT max(run_date) AS last_run FROM discovery_volumes) d,
     (SELECT count(*) AS uncollected FROM sc_uncollected) u;

\echo
\echo '=== Window ==='
SELECT t0 AS reference_time, window_start, (:'days')::int AS days FROM sc_run;
\echo 'Covers the active universe only; HL 1m (streamed, no per-pair rows) is not covered.'

\echo
\echo '=== Capture by venue ==='
SELECT section, exchange, timeframe, pairs, expected, captured,
       CASE WHEN expected = 0 THEN NULL ELSE round(100.0 * captured / expected, 3) END AS pct,
       measured_to
FROM sc_venue ORDER BY section, exchange, timeframe;

\echo
\echo '=== Qualifying assets with no active row ==='
SELECT asset_id, round(max_volume_usd) AS max_volume_usd FROM sc_uncollected ORDER BY max_volume_usd DESC;

\echo
\echo '=== Funding pairs not measured (interval changed in the window) ==='
SELECT 'unmeasured' AS funding, exchange, symbol FROM sc_funding_pairs WHERE intervals > 1 ORDER BY exchange, symbol;

\echo
\echo '=== Database checks ==='
SELECT check_name AS check, measured, target, CASE WHEN pass THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM sc_checks ORDER BY ord;

SELECT bool_and(pass) AS sc_all_pass FROM sc_checks \gset
\if :sc_all_pass
\echo 'SCORECARD-DB: PASS'
\else
\echo 'SCORECARD-DB: FAIL'
\endif
