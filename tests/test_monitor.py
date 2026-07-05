"""Tests for collector/monitor.py — pure function tests for staleness, gaps, dedup, webhooks."""

import json
import time
from collections import deque
from datetime import date, datetime, timezone

from config import (
    CANDLE_BACKFILL,
    CANDLE_WS,
    FUNDING_LOOP,
    LIQUIDATION_WS,
    LOOP_INTERVALS,
    OI_LOOP,
    PREMIUM_LOOP,
)
from monitor import (
    _TF_SCAN_INTERVALS,
    GAP_SQL_BY_TABLE,
    HEARTBEAT_CANDLES_SQL,
    LOOP_TIMEFRAMES,
    build_heartbeat_text,
    build_webhook_payload,
    compute_alarming,
    compute_staleness,
    detect_gaps,
    evaluate_alarming,
    format_alarming_alert,
    format_gap_alert,
    format_stale_alert,
    gap_scan_due,
    should_alert,
    should_send_heartbeat,
)

TF_INTERVALS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
# The monitor merges LOOP_INTERVALS into the candle-TF map at startup (monitor.run()).
LOOP_AWARE_INTERVALS = {**TF_INTERVALS, **LOOP_INTERVALS}


# --- candle-table gap-scan routing (T-015) ---


def test_gap_sql_by_table_routes():
    assert set(GAP_SQL_BY_TABLE) == {"candles", "candles_1m"}
    assert "FROM candles\n" in GAP_SQL_BY_TABLE["candles"]
    assert "FROM candles_1m\n" in GAP_SQL_BY_TABLE["candles_1m"]
    assert "candles_1m" not in GAP_SQL_BY_TABLE["candles"]  # candles query must NOT hit 1m table


def test_detect_gaps_selects_table():
    # detect_gaps(table='candles_1m') must scan candles_1m (else 1m gaps are invisible).
    captured = {}

    class _Cur:
        description = [("symbol",), ("gap_after",), ("gap_before",), ("missing_count",)]
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params): captured["sql"] = sql
        def fetchall(self): return []

    class _Conn:
        def cursor(self): return _Cur()

    detect_gaps(_Conn(), "binance", "1m", 60, "candles_1m")
    assert "FROM candles_1m" in captured["sql"]


def test_heartbeat_candles_sql_sums_both_tables():
    # The weekly heartbeat count must include candles_1m (the densest feed), not only candles.
    assert "FROM candles WHERE" in HEARTBEAT_CANDLES_SQL
    assert "FROM candles_1m WHERE" in HEARTBEAT_CANDLES_SQL


def test_tf_scan_intervals_has_1m():
    assert _TF_SCAN_INTERVALS["1m"] == 300


# --- compute_staleness ---


def test_compute_staleness_none_stale():
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 100, "error_count": 0},
        {"exchange": "binance", "symbol": "ETH/USDT:USDT", "timeframe": "5m", "seconds_since_success": 500, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


def test_compute_staleness_one_stale():
    # 5m threshold = 300 + max(300, 1800) = 2100s
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 100, "error_count": 0},
        {"exchange": "binance", "symbol": "ETH/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2500, "error_count": 3},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["symbol"] == "ETH/USDT:USDT"


def test_compute_staleness_null_last_success():
    rows = [
        {"exchange": "bybit", "symbol": "BTC/USDT:USDT", "timeframe": "1h", "seconds_since_success": None, "error_count": 0},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1


def test_compute_staleness_at_threshold():
    """Exactly at threshold = not stale (needs >). 5m threshold = 2100s."""
    rows = [
        {"exchange": "okx", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2100, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


def test_compute_staleness_different_timeframes():
    """5m pair stale, 1d pair fine — only 5m returned.
    5m threshold = 2100s; 1d threshold = 86400 + max(86400, 1800) = 172800s.
    """
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2500, "error_count": 0},
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "1d", "seconds_since_success": 50000, "error_count": 0},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["timeframe"] == "5m"


def test_compute_staleness_unknown_timeframe_skipped():
    """Rows with timeframes not in the interval map are ignored."""
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "2m", "seconds_since_success": 1, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


# --- funding/OI loop heartbeats (T-004) ---


def test_loop_timeframes_match_config():
    """Producer (config), consumer (monitor gap-skip), and the pseudo-TF set agree —
    pins against name drift between the heartbeat writers and the monitor."""
    assert LOOP_TIMEFRAMES == set(LOOP_INTERVALS)
    assert {FUNDING_LOOP, OI_LOOP, LIQUIDATION_WS, PREMIUM_LOOP,
            CANDLE_WS, CANDLE_BACKFILL} == set(LOOP_INTERVALS)


def test_compute_staleness_flags_dead_ws_loop():
    """WS liveness pseudo-TF: liq_ws threshold = 300 + max(300, 1800) = 2100s. A heartbeat older
    than that = a dead WS thread (the connection-timer heartbeat stopped). Same path as funding/oi."""
    rows = [
        {"exchange": "bybit", "symbol": "*", "timeframe": LIQUIDATION_WS, "seconds_since_success": 2500, "error_count": 0},
    ]
    stale = compute_staleness(rows, LOOP_AWARE_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["timeframe"] == LIQUIDATION_WS


def test_ws_loop_excluded_from_gap_detection():
    """The liq_ws sentinel carries no candle series — it must be a LOOP_TIMEFRAME so the monitor's
    gap scan skips it (mirrors funding/oi; a gap query on a pseudo-TF would be meaningless)."""
    assert LIQUIDATION_WS in LOOP_TIMEFRAMES


def test_compute_staleness_flags_dead_funding_loop():
    """funding threshold = 3600 + max(3600, 1800) = 7200s; an older heartbeat = dead loop."""
    rows = [
        {"exchange": "binance", "symbol": "*", "timeframe": FUNDING_LOOP, "seconds_since_success": 7500, "error_count": 0},
    ]
    stale = compute_staleness(rows, LOOP_AWARE_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["timeframe"] == FUNDING_LOOP


def test_compute_staleness_flags_dead_oi_loop():
    """oi threshold = 300 + max(300, 1800) = 2100s."""
    rows = [
        {"exchange": "bybit", "symbol": "*", "timeframe": OI_LOOP, "seconds_since_success": 2500, "error_count": 0},
    ]
    stale = compute_staleness(rows, LOOP_AWARE_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["timeframe"] == OI_LOOP


def test_compute_staleness_fresh_loop_heartbeats_ok():
    """Live funding/OI loops (recent heartbeat) are not flagged."""
    rows = [
        {"exchange": "binance", "symbol": "*", "timeframe": FUNDING_LOOP, "seconds_since_success": 120, "error_count": 0},
        {"exchange": "hyperliquid", "symbol": "*", "timeframe": OI_LOOP, "seconds_since_success": 90, "error_count": 0},
    ]
    assert compute_staleness(rows, LOOP_AWARE_INTERVALS) == []


def test_compute_staleness_loop_heartbeat_ignored_without_loop_intervals():
    """Without the loop intervals merged in, a loop row is skipped like any unknown TF —
    guards against the monitor silently checking loop rows on the bare candle map."""
    rows = [
        {"exchange": "okx", "symbol": "*", "timeframe": OI_LOOP, "seconds_since_success": 99999, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


# --- format_stale_alert ---


def test_format_stale_alert_single():
    stale = [{"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 700, "error_count": 2}]
    text = format_stale_alert(stale)
    assert "1 stale" in text
    assert "binance" in text
    assert "BTC/USDT:USDT" in text
    assert "5m" in text


def test_format_stale_alert_multiple():
    stale = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 700, "error_count": 0},
        {"exchange": "bybit", "symbol": "ETH/USDT:USDT", "timeframe": "1h", "seconds_since_success": None, "error_count": 5},
    ]
    text = format_stale_alert(stale)
    assert "2 stale" in text
    assert "never" in text


def test_format_stale_alert_empty():
    assert format_stale_alert([]) == ""


def test_format_stale_alert_truncates_large_lists():
    """Discord caps webhook content at 2000 chars — long lists must truncate."""
    stale = [
        {"exchange": "binance", "symbol": f"SYM{i}/USDT:USDT", "timeframe": "5m",
         "seconds_since_success": 2500, "error_count": 1}
        for i in range(500)
    ]
    text = format_stale_alert(stale)
    assert "500 stale" in text
    assert "plus 485 more" in text
    assert len(text) < 2000


# --- format_gap_alert ---


def test_format_gap_alert():
    gaps = [
        {"symbol": "BTC/USDT:USDT", "gap_after": "2026-03-29T10:00:00+00:00", "gap_before": "2026-03-29T10:15:00+00:00", "missing_count": 2},
    ]
    text = format_gap_alert("binance", "5m", gaps)
    assert "1 candle gap" in text
    assert "binance" in text
    assert "BTC/USDT:USDT" in text


def test_format_gap_alert_empty():
    assert format_gap_alert("binance", "5m", []) == ""


# --- gap_scan_due (per-(exchange, timeframe) tiered scheduling) ---


def test_gap_scan_due_unseen_key():
    assert gap_scan_due({}, ("binance", "5m"), 300, now=1000) is True


def test_gap_scan_due_within_interval():
    last = {("binance", "5m"): 1000}
    assert gap_scan_due(last, ("binance", "5m"), 300, now=1100) is False


def test_gap_scan_due_after_interval():
    last = {("binance", "5m"): 1000}
    assert gap_scan_due(last, ("binance", "5m"), 300, now=1400) is True


def test_gap_scan_due_distinct_exchanges_do_not_suppress():
    """Regression: binance having just scanned 5m must NOT suppress bybit's 5m scan —
    the keying is per (exchange, timeframe), not timeframe alone."""
    last = {("binance", "5m"): 1000}
    assert gap_scan_due(last, ("bybit", "5m"), 300, now=1100) is True


# --- should_alert (dedup) ---


def test_should_alert_first_call():
    cooldowns = {}
    assert should_alert("test_key", cooldowns, 3600) is True
    assert "test_key" in cooldowns


def test_should_alert_within_cooldown():
    cooldowns = {"test_key": time.time()}
    assert should_alert("test_key", cooldowns, 3600) is False


def test_should_alert_after_cooldown():
    cooldowns = {"test_key": time.time() - 4000}
    assert should_alert("test_key", cooldowns, 3600) is True


# --- build_webhook_payload ---


def test_build_webhook_payload():
    payload = build_webhook_payload("hello")
    data = json.loads(payload)
    assert data["text"] == "hello"
    assert data["content"] == "hello"
    assert isinstance(payload, bytes)


# --- build_heartbeat_text ---


HEARTBEAT_STATS = {
    "fresh_1h": 8500,
    "stale_3h": 40,
    "total_active": 8540,
    "total_errors": 312,
    "candles_24h": 124382,
}


def test_build_heartbeat_text_contains_all_fields():
    text = build_heartbeat_text(HEARTBEAT_STATS)
    assert "8500/8540" in text
    assert "40 stale" in text
    assert "124,382" in text  # thousands separator
    assert "312 errors" in text


def test_build_heartbeat_text_zero_errors():
    stats = {**HEARTBEAT_STATS, "total_errors": 0, "stale_3h": 0}
    text = build_heartbeat_text(stats)
    assert "0 errors" in text
    assert "0 stale" in text


# --- should_send_heartbeat ---


def test_should_send_heartbeat_wrong_hour():
    now = datetime(2026, 4, 17, 3, 0, tzinfo=timezone.utc)
    assert should_send_heartbeat(now, None, target_hour=0, target_weekday=now.weekday()) is False


def test_should_send_heartbeat_wrong_weekday():
    # Right hour but not the target weekday → the weekly cadence holds fire (this is the T-013 fix:
    # a daily send is what trained the operator to mute the channel).
    now = datetime(2026, 4, 17, 0, 30, tzinfo=timezone.utc)
    assert should_send_heartbeat(now, None, target_hour=0, target_weekday=(now.weekday() + 1) % 7) is False


def test_should_send_heartbeat_right_slot_never_sent():
    now = datetime(2026, 4, 17, 0, 30, tzinfo=timezone.utc)
    assert should_send_heartbeat(now, None, target_hour=0, target_weekday=now.weekday()) is True


def test_should_send_heartbeat_already_sent_this_slot():
    now = datetime(2026, 4, 17, 0, 30, tzinfo=timezone.utc)
    assert should_send_heartbeat(now, date(2026, 4, 17), target_hour=0, target_weekday=now.weekday()) is False


def test_should_send_heartbeat_prior_week_sent():
    # Sent on the same weekday a week ago → fires again this week (date idempotency key differs).
    now = datetime(2026, 4, 17, 0, 30, tzinfo=timezone.utc)
    assert should_send_heartbeat(now, date(2026, 4, 10), target_hour=0, target_weekday=now.weekday()) is True


def test_should_send_heartbeat_non_zero_target_hour():
    now_ok = datetime(2026, 4, 17, 7, 5, tzinfo=timezone.utc)
    now_bad = datetime(2026, 4, 17, 6, 55, tzinfo=timezone.utc)
    assert should_send_heartbeat(now_ok, None, target_hour=7, target_weekday=now_ok.weekday()) is True
    assert should_send_heartbeat(now_bad, None, target_hour=7, target_weekday=now_bad.weekday()) is False


# --- compute_alarming (T-005): the high-signal subset, 5m → 4×interval = 1200s ---


def test_compute_alarming_excludes_hl_5m_cursor():
    """HL 5m within-cycle cursor: ~12 min behind, no errors — below the 4×interval (1200s) gate."""
    rows = [{"exchange": "hyperliquid", "symbol": "BTC/USDC:USDC", "timeframe": "5m",
             "seconds_since_success": 720, "error_count": 0}]
    assert compute_alarming(rows, TF_INTERVALS) == []


def test_compute_alarming_excludes_new_listing_floor():
    """Never-succeeded new listing: last_success NULL (since None), no errors — excluded."""
    rows = [{"exchange": "binance", "symbol": "PUMP/USDT:USDT", "timeframe": "1d",
             "seconds_since_success": None, "error_count": 0}]
    assert compute_alarming(rows, TF_INTERVALS) == []


def test_compute_alarming_error_channel_catches_never_succeeded_bug():
    """The 136-stuck class: NEVER succeeded (since None) but errors accruing — the error
    channel must catch it, which a lone time gate (since > 4×interval) would miss."""
    rows = [{"exchange": "bybit", "symbol": "AAPL/USDT:USDT", "timeframe": "5m",
             "seconds_since_success": None, "error_count": 57}]
    alarming = compute_alarming(rows, TF_INTERVALS)
    assert len(alarming) == 1
    assert alarming[0]["symbol"] == "AAPL/USDT:USDT"


def test_compute_alarming_time_channel_catches_gone_dark():
    """A previously-working collector gone dark past 4×interval (3000 > 1200), no errors."""
    rows = [{"exchange": "okx", "symbol": "ETH/USDT:USDT", "timeframe": "5m",
             "seconds_since_success": 3000, "error_count": 0}]
    assert len(compute_alarming(rows, TF_INTERVALS)) == 1


def test_compute_alarming_time_gate_is_strict():
    """Exactly at 4×interval (1200s for 5m) = not alarming (needs >)."""
    rows = [{"exchange": "okx", "symbol": "ETH/USDT:USDT", "timeframe": "5m",
             "seconds_since_success": 1200, "error_count": 0}]
    assert compute_alarming(rows, TF_INTERVALS) == []


def test_compute_alarming_skips_loop_pseudo_tfs():
    """A dead funding/OI loop heartbeat is NOT counted here — it pages via the separate
    presence alert; padding the candle count would dilute the +delta signal."""
    rows = [{"exchange": "dydx", "symbol": "*", "timeframe": FUNDING_LOOP,
             "seconds_since_success": 99999, "error_count": 99}]
    assert compute_alarming(rows, LOOP_AWARE_INTERVALS) == []


def test_compute_alarming_skips_unknown_timeframe():
    rows = [{"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "2m",
             "seconds_since_success": 99999, "error_count": 5}]
    assert compute_alarming(rows, TF_INTERVALS) == []


def test_compute_alarming_benign_floor_is_empty():
    """A whole cycle of only-benign staleness (cursor + new-listing floor) → 0 alarming."""
    rows = [
        {"exchange": "hyperliquid", "symbol": f"C{i}/USDC:USDC", "timeframe": "5m",
         "seconds_since_success": 720, "error_count": 0} for i in range(60)
    ] + [
        {"exchange": "binance", "symbol": f"NEW{i}/USDT:USDT", "timeframe": "1d",
         "seconds_since_success": None, "error_count": 0} for i in range(49)
    ]
    assert compute_alarming(rows, TF_INTERVALS) == []


# --- evaluate_alarming (T-005): rolling-median baseline + sustained breach ---


def test_evaluate_alarming_below_threshold_no_fire():
    d = evaluate_alarming(10, deque([0, 0]), 25, 2, 0)
    assert d.baseline == 0 and d.should_record is True and d.next_consecutive == 0 and d.should_fire is False


def test_evaluate_alarming_single_cycle_breach_no_fire():
    d = evaluate_alarming(50, deque([0, 0]), 25, 2, 0)
    assert d.should_record is False and d.next_consecutive == 1 and d.should_fire is False


def test_evaluate_alarming_two_consecutive_fires():
    d = evaluate_alarming(50, deque([0, 0]), 25, 2, 1)
    assert d.should_record is False and d.next_consecutive == 2 and d.should_fire is True


def test_evaluate_alarming_breach_then_clear_resets():
    d = evaluate_alarming(5, deque([0, 0]), 25, 2, 1)
    assert d.should_record is True and d.next_consecutive == 0 and d.should_fire is False


def test_evaluate_alarming_empty_history_baseline_zero():
    d = evaluate_alarming(30, deque(), 25, 2, 0)
    assert d.baseline == 0 and d.should_record is False  # breaching → not recorded into baseline


def test_evaluate_alarming_median_over_benign_window():
    """Median is outlier-robust: a window of mostly-0 with a couple small values still ~0."""
    d = evaluate_alarming(30, deque([0, 0, 0, 2, 1]), 25, 2, 0)
    assert d.baseline == 0 and d.should_record is False


def _drive_alarming(counts, delta=25, sustain=2, poll_s=300):
    """Mirror monitor_loop's record-only-quiescent rule; return the 1-based cycle index at
    which it first fires, or None."""
    history = deque(maxlen=max(1, 3600 // poll_s))
    consec = 0
    for i, count in enumerate(counts, start=1):
        d = evaluate_alarming(count, history, delta, sustain, consec)
        consec = d.next_consecutive
        if d.should_fire:
            return i
        if d.should_record:
            history.append(count)
    return None


def test_evaluate_alarming_cold_start_into_outage_fires():
    """The codex cold-start regression: a steady high count from an EMPTY history must fire at
    `sustain_cycles` — breaching counts must not enter the baseline and self-suppress."""
    assert _drive_alarming([136, 136]) == 2


def test_evaluate_alarming_transient_spike_does_not_fire():
    """A single-cycle spike that clears the next cycle never reaches sustain."""
    assert _drive_alarming([50, 0, 0]) is None


def test_evaluate_alarming_benign_steady_state_quiet():
    assert _drive_alarming([0, 1, 0, 2, 0, 1]) is None


# --- format_alarming_alert (T-005) ---


def test_format_alarming_alert_empty():
    assert format_alarming_alert([], 0, 0, 25) == ""


def test_format_alarming_alert_content():
    rows = [{"exchange": "binance", "symbol": "AAPL/USDT:USDT", "timeframe": "5m",
             "seconds_since_success": None, "error_count": 57}]
    text = format_alarming_alert(rows, 30, 0, 25)
    assert "30 alarming-stale" in text
    assert "baseline ~0" in text
    assert "+25" in text
    assert "binance" in text and "AAPL/USDT:USDT" in text
    assert "never" in text  # last_success NULL


def test_format_alarming_alert_truncates_large_lists():
    rows = [{"exchange": "binance", "symbol": f"SYM{i}/USDT:USDT", "timeframe": "5m",
             "seconds_since_success": 3000, "error_count": 1} for i in range(500)]
    text = format_alarming_alert(rows, 500, 0, 25)
    assert "500 alarming-stale" in text
    assert "plus 485 more" in text
    assert len(text) < 2000
