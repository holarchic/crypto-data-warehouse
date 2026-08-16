-- v0.23 (SPEC): drop 1d/3d/1w from active collection (derive on demand)
--
-- The 1d/3d/1w collectors stalled silently (3d stuck 2026-07-30, 1w 2026-07-13)
-- while last_success kept updating, so the monitor only paged once the 4×interval
-- alarming gate was crossed. These TFs are exact integer rollups of 4h and ~0.09%
-- of row volume; SPEC Decision v0.23 drops them from collection and derives on
-- demand. They are removed from exchanges.yaml (timeframes + supported_timeframes),
-- so discovery and the collectors no longer touch these rows — this one-shot
-- deactivation therefore sticks (nothing re-sets active=TRUE for them). Mirrors the
-- existing deactivate_stale_pairs idiom (active=FALSE, deactivated_at=NOW()).
--
-- Candle HISTORY is RETAINED: this only flips collection_status.active. The candles
-- hypertable is untouched — existing 1d/3d/1w rows stay queryable (frozen).
--
-- ROLLOUT (no migration runner — applied manually): idempotent (WHERE active = TRUE
-- ⇒ re-running is a no-op). Apply AFTER the config deploy that removes these TFs, so
-- no in-flight collector cycle re-activates a row between UPDATE and container reload.

UPDATE collection_status
SET active = FALSE, deactivated_at = NOW()
WHERE timeframe IN ('1d', '3d', '1w') AND active = TRUE;
