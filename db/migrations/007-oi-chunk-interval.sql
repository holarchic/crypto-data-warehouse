-- v0.5 (SPEC): open_interest chunk interval 1 month -> 1 week (T-007.2)
--
-- Densifying OI (Bybit/OKX flat 30s, Binance sub-5-min dense window) multiplies
-- open_interest row volume ~5-10x, so align its chunk interval with candles /
-- hl_asset_ctxs (1 week). Affects NEW chunks only — existing 1-month chunks are
-- untouched (harmless). Self-contained: no dependency on 004/005 ordering.
--
-- ROLLOUT (no migration runner — applied manually): safe to apply any time; it
-- does not rewrite existing data and does not require a collector restart.

SELECT set_chunk_time_interval('open_interest', INTERVAL '1 week');
