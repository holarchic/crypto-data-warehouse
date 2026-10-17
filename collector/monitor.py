"""Health monitor — stale collector detection, gap detection, webhook alerts.

Runs as always-on container: `python -m monitor`
"""

import json
import logging
import os
import statistics
import time
import urllib.request
from collections import deque, namedtuple
from datetime import datetime, timezone

from config import CANDLE_TABLES, LOOP_INTERVALS, load_candle_tables, load_timeframe_intervals
from dotenv import load_dotenv
from status import LOOP_SENTINEL_SYMBOL

log = logging.getLogger(__name__)

# Per-loop heartbeat pseudo-timeframes (config.LOOP_INTERVALS keys: funding, oi, liq_ws, premium,
# candle_ws). Their sentinel collection_status rows are checked for staleness like any
# collector, but carry no candle series — so gap detection skips them.
LOOP_TIMEFRAMES = frozenset(LOOP_INTERVALS)

# Floor on the alarming time gate. A REST candle loop fetches its pairs serially, so its cycle
# is bound by per-pair RTT × pair count, not by the interval: ~680 Binance 1m pairs take ~3 min
# (up to ~10 min at a settlement), far past 4×60s. Only 1m sits below the floor; 5m is at it.
ALARMING_DARK_FLOOR_S = 1200

# Result of one alarming-signal evaluation (named, not positional). `should_record` carries the
# baseline-update invariant (record only non-breaching counts) so the caller doesn't re-derive it.
AlarmingDecision = namedtuple("AlarmingDecision", ["baseline", "should_fire", "next_consecutive", "should_record"])

# ---------------------------------------------------------------------------
# Pure functions (testable)
# ---------------------------------------------------------------------------


def compute_staleness(status_rows, timeframe_intervals):
    """Given collection_status rows + {tf: interval_s}, return rows where
    seconds_since_success > threshold or last_success is NULL.

    Threshold is interval_s + max(interval_s, 1800). The 30-minute floor
    absorbs the serial-fetch cycle time across hundreds of pairs per TF —
    without it, short-TF pairs near the end of a fetch cycle trip the 2x
    interval threshold every cycle, drowning real signal in noise.
    """
    stale = []
    for row in status_rows:
        tf = row["timeframe"]
        interval_s = timeframe_intervals.get(tf)
        if interval_s is None:
            continue
        threshold = interval_s + max(interval_s, 1800)
        since = row.get("seconds_since_success")
        if since is None or since > threshold:
            stale.append(row)
    return stale


def compute_alarming(status_rows, timeframe_intervals, error_threshold=3):
    """Candle collectors that are *alarming-stale* — the high-signal subset of staleness.

    Two channels, deliberately NOT a plain staleness filter (see SPEC Decision):
      - error channel (`error_count >= error_threshold`): catches the stuck-collector class,
        whose rows fail every fetch and so never succeed (`last_success` NULL → `seconds_since_success`
        NULL) — a lone time gate would miss them. `error_count` is CONSECUTIVE errors (reset to 0 on
        any success), so the threshold gate separates a genuinely stuck collector (count climbs
        monotonically) from a transient blip: a rate-limit wave (e.g. bybit `retCode 10006` when many
        timeframe boundaries align) bumps hundreds of healthy pairs to `error_count=1` for a single
        cycle, then clears on their next success. Gating at `>0` counted that wave as alarming and
        paged on backfilled-losslessly noise (measured spikes to 356); `>=3` requires sustained
        failure, restoring the design's baseline≈0 assumption.
      - time channel (`seconds_since_success > max(4×interval, ALARMING_DARK_FLOOR_S)`): a
        previously-working collector gone dark, past a gate well above the within-cycle
        collection cursor.
    Excludes the benign new-listing floor (no errors, never succeeded), the sub-4×interval cursor,
    and the transient single-error blip. Loop-heartbeat pseudo-TFs are skipped — they page via their
    own presence alert and a single dead loop must not pad this count.
    """
    alarming = []
    for row in status_rows:
        tf = row["timeframe"]
        if tf in LOOP_TIMEFRAMES:
            continue
        interval_s = timeframe_intervals.get(tf)
        if interval_s is None:
            continue
        since = row.get("seconds_since_success")
        dark_gate_s = max(4 * interval_s, ALARMING_DARK_FLOOR_S)
        if row.get("error_count", 0) >= error_threshold or (since is not None and since > dark_gate_s):
            alarming.append(row)
    return alarming


def evaluate_alarming(count, history, delta_threshold, sustain_cycles, prior_consecutive):
    """Decide whether the alarming count constitutes an alert, and own the state-transition rules.

    Baseline is the median of `history` — prior *quiescent* counts only. The `should_record`
    field is the load-bearing invariant: a breaching count must NOT enter the baseline (else a
    mid-outage restart self-suppresses — a steady high count from an empty history would raise its
    own median past the gate before `sustain_cycles` completes). The caller records the count into
    its rolling window iff `should_record`, and carries `next_consecutive` into the next cycle —
    so the transition rules live here, not split across the call site.
    """
    baseline = statistics.median(history) if history else 0
    is_breach = count > baseline + delta_threshold
    next_consecutive = prior_consecutive + 1 if is_breach else 0
    return AlarmingDecision(
        baseline=baseline,
        should_fire=next_consecutive >= sustain_cycles,
        next_consecutive=next_consecutive,
        should_record=not is_breach,
    )


def format_stale_alert(stale_pairs, max_rows=15):
    """Human-readable stale alert — summary header + first `max_rows` rows.

    Discord caps webhook content at 2000 chars; a full 500-row dump 403s.
    We send a headline count plus a sample; the dashboard/DB is the
    authoritative source when investigating.
    """
    if not stale_pairs:
        return ""
    lines = [f"🔴 {len(stale_pairs)} stale collector(s) detected:"]
    for p in stale_pairs[:max_rows]:
        since = p.get("seconds_since_success")
        since_str = f"{since:.0f}s ago" if since is not None else "never"
        errors = p.get("error_count", 0)
        lines.append(f"  • {p['exchange']}/{p['symbol']}/{p['timeframe']} — last success: {since_str}, errors: {errors}")
    if len(stale_pairs) > max_rows:
        lines.append(f"  … plus {len(stale_pairs) - max_rows} more (see /status for full list)")
    return "\n".join(lines)


def format_alarming_alert(alarming_rows, count, baseline, delta_threshold, max_rows=15):
    """Human-readable alarming-collector alert — headline (count, baseline, gate) + first
    `max_rows` rows. Same 2000-char Discord cap discipline as format_stale_alert.
    """
    if not alarming_rows:
        return ""
    lines = [f"🔴 {count} alarming-stale collector(s) (baseline ~{baseline:.0f}, threshold +{delta_threshold}):"]
    for p in alarming_rows[:max_rows]:
        since = p.get("seconds_since_success")
        since_str = f"{since:.0f}s ago" if since is not None else "never"
        errors = p.get("error_count", 0)
        lines.append(f"  • {p['exchange']}/{p['symbol']}/{p['timeframe']} — last success: {since_str}, errors: {errors}")
    if len(alarming_rows) > max_rows:
        lines.append(f"  … plus {len(alarming_rows) - max_rows} more (see /status for full list)")
    return "\n".join(lines)


def format_gap_alert(exchange, timeframe, gaps):
    """Human-readable gap alert. Returns empty string if no gaps."""
    if not gaps:
        return ""
    lines = [f"⚠️ {len(gaps)} candle gap(s) on {exchange}/{timeframe}:"]
    for g in gaps:
        lines.append(f"  • {g['symbol']}: {int(g['missing_count'])} missing after {g['gap_after']}")
    return "\n".join(lines)


def should_alert(key, cooldowns, cooldown_s):
    """Dedup check. Returns True if alert should fire, updates cooldowns dict."""
    now = time.time()
    last = cooldowns.get(key)
    if last is not None and (now - last) < cooldown_s:
        return False
    cooldowns[key] = now
    return True


def gap_scan_due(last_gap_scan, key, scan_interval_s, now):
    """True if `key`'s tiered gap scan is due. `key` is `(exchange, timeframe)`, NOT timeframe
    alone — keying by timeframe would let the first exchange scanned for a timeframe suppress
    every other exchange sharing it, so gap detection would silently cover one exchange per TF.
    """
    return now - last_gap_scan.get(key, 0) >= scan_interval_s


def build_webhook_payload(text):
    """JSON-encoded payload with both Slack (text) and Discord (content) keys."""
    return json.dumps({"text": text, "content": text}).encode("utf-8")


def build_heartbeat_text(stats):
    """One-line summary for the daily heartbeat.

    stats keys: fresh_1h, stale_3h, total_active, candles_24h, total_errors.
    """
    return (
        "candle-collector alive: "
        f"{stats['fresh_1h']}/{stats['total_active']} collectors fresh (<1h), "
        f"{stats['stale_3h']} stale (>3h), "
        f"{stats['candles_24h']:,} candles/24h, "
        f"{stats['total_errors']} errors"
    )


def should_send_heartbeat(now_utc, last_sent_date, target_hour, target_weekday):
    """True if the weekly status heartbeat should fire right now.

    Fires once per week: on `target_weekday` (Mon=0 … Sun=6) when `now_utc.hour == target_hour`.
    A weekly cadence, not daily — a daily "all-ok" ping trained the operator to mute the alert
    channel, which then hid a real 7h outage's pages (T-013). `last_sent_date` is the idempotency
    key so the same hour-window doesn't fire on every monitor cycle within that hour.
    """
    if now_utc.weekday() != target_weekday:
        return False
    if now_utc.hour != target_hour:
        return False
    return last_sent_date != now_utc.date()


# ---------------------------------------------------------------------------
# I/O functions
# ---------------------------------------------------------------------------

STALENESS_SQL = """
    SELECT exchange, symbol, timeframe, last_success, error_count, last_error,
           EXTRACT(EPOCH FROM NOW() - last_success) AS seconds_since_success
    FROM collection_status
    WHERE active = TRUE
    ORDER BY exchange, timeframe, symbol
"""

# One gap-scan query per candle table (candles, candles_1m). The FROM table is substituted at
# import from the closed CANDLE_TABLES allowlist — NOT interpolated from a runtime value — so 1m
# gap detection scans candles_1m (else it would query `candles`, find no 1m rows, and be silently
# blind to 1m bar-gaps). Mirrors the collector's INSERT_SQL routing (same single source).
_GAP_TEMPLATE = """
    WITH consecutive AS (
        SELECT symbol, timestamp,
               LEAD(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_ts
        FROM {table}
        WHERE exchange = %(exchange)s
          AND timeframe = %(timeframe)s
          AND timestamp > NOW() - INTERVAL '1 second' * %(lookback_s)s
    )
    SELECT symbol, timestamp AS gap_after, next_ts AS gap_before,
           EXTRACT(EPOCH FROM next_ts - timestamp) / %(interval_s)s - 1 AS missing_count
    FROM consecutive
    WHERE next_ts IS NOT NULL
      AND EXTRACT(EPOCH FROM next_ts - timestamp) > %(interval_s)s * 1.5
    ORDER BY symbol, timestamp
"""
GAP_SQL_BY_TABLE = {table: _GAP_TEMPLATE.format(table=table) for table in CANDLE_TABLES}


def send_webhook(url, text):
    """POST alert to Slack/Discord webhook. Returns True on success.

    Cloudflare sits in front of Discord webhooks and blocks the default
    `Python-urllib` User-Agent (returns 403 with cf error 1010). A real
    UA string bypasses that.
    """
    if not url:
        return False
    payload = build_webhook_payload(text)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "candle-collector-monitor/1.0 (+https://github.com/holarchic/crypto-data-warehouse)",
    }
    req = urllib.request.Request(url, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        log.warning("Webhook POST failed: %s", exc)
        return False


HEARTBEAT_STATS_SQL = """
    SELECT
        count(*) FILTER (WHERE last_success > NOW() - INTERVAL '1 hour') AS fresh_1h,
        count(*) FILTER (WHERE last_success IS NULL OR last_success < NOW() - INTERVAL '3 hours') AS stale_3h,
        count(*) AS total_active,
        COALESCE(sum(error_count), 0) AS total_errors
    FROM collection_status
    WHERE active = TRUE
"""

# Sum the 24h candle count across every candle table (candles + candles_1m) so the weekly heartbeat
# doesn't undercount — 1m is the densest feed (~1.4M rows/day, > all other TFs combined). Built from
# the closed CANDLE_TABLES allowlist (literal, no runtime interpolation).
HEARTBEAT_CANDLES_SQL = "SELECT " + " + ".join(
    f"(SELECT count(*) FROM {table} WHERE timestamp > NOW() - INTERVAL '24 hours')"
    for table in CANDLE_TABLES
) + " AS candles_24h"


def fetch_heartbeat_stats(conn):
    """Query DB for daily heartbeat summary stats."""
    with conn.cursor() as cur:
        cur.execute(HEARTBEAT_STATS_SQL)
        fresh_1h, stale_3h, total_active, total_errors = cur.fetchone()
        cur.execute(HEARTBEAT_CANDLES_SQL)
        (candles_24h,) = cur.fetchone()
    return {
        "fresh_1h": fresh_1h,
        "stale_3h": stale_3h,
        "total_active": total_active,
        "total_errors": int(total_errors),
        "candles_24h": candles_24h,
    }


def ping_healthcheck(url):
    """Hit an uptime-monitor URL (e.g. healthchecks.io) as a dead-man's switch.
    Silent failure is intentional — the point is the absence of pings signals
    to an external watcher that the monitor is down. Returns True on success.
    """
    if not url:
        return False
    req = urllib.request.Request(
        url, headers={"User-Agent": "candle-collector-monitor/1.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        log.warning("Healthcheck ping failed: %s", exc)
        return False


def fetch_status_rows(conn):
    """Fetch all active collection_status rows with derived seconds_since_success.

    One fetch feeds both staleness (gap-skip + loop-liveness presence) and the alarming
    subset — alarming is NOT a subset of staleness (its 4×interval gate is below the staleness
    threshold for short TFs), so both compute over the same full row set.
    """
    with conn.cursor() as cur:
        cur.execute(STALENESS_SQL)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def detect_gaps(conn, exchange, timeframe, interval_s, table="candles", lookback_count=10):
    """Detect candle gaps for an exchange/timeframe in its storage table. Returns list of gap dicts."""
    lookback_s = lookback_count * interval_s
    params = {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_s": lookback_s,
        "interval_s": interval_s,
    }
    with conn.cursor() as cur:
        cur.execute(GAP_SQL_BY_TABLE[table], params)
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    # Convert timestamps to ISO strings for readability
    for row in rows:
        for key in ("gap_after", "gap_before"):
            if row.get(key) and hasattr(row[key], "isoformat"):
                row[key] = row[key].isoformat()
    return rows


def _get_exchange_timeframe_pairs(conn):
    """Get distinct (exchange, timeframe) combos from active collection_status."""
    sql = "SELECT DISTINCT exchange, timeframe FROM collection_status WHERE active = TRUE ORDER BY exchange, timeframe"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


# Slower timeframes checked less frequently — map of minimum cycle interval in seconds
_TF_SCAN_INTERVALS = {
    "1m": 300,    # every cycle (candles_1m; the 300s monitor tick is the finest cadence anyway)
    "5m": 300,    # every cycle
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
    "1w": 604800,
}


def monitor_loop(dsn, timeframe_intervals, webhook_url, poll_s=300, cooldown_s=3600,
                 healthcheck_url="", heartbeat_hour_utc=0, delta_threshold=25, sustain_cycles=2,
                 heartbeat_weekday=0, error_threshold=3):
    """Main monitor loop. Checks staleness + gaps on each cycle.

    Also hits healthcheck_url every cycle (dead-man's switch) and posts a weekly status
    summary on heartbeat_weekday at heartbeat_hour_utc.
    """
    import psycopg

    cooldowns = {}
    last_gap_scan = {}  # {(exchange, timeframe): last_scan_epoch}
    last_heartbeat_date = None  # date of last heartbeat send
    # Rolling-1h baseline of the alarming count (quiescent counts only) + the run of
    # consecutive breaches. In-memory like cooldowns — resets on restart, rebuilds over 1h.
    alarming_history = deque(maxlen=max(1, 3600 // poll_s))
    alarming_consecutive = 0

    # Timeframe → candle storage table (candles / candles_1m). Loaded once; routes the gap-scan so
    # 1m gaps are detected against candles_1m, not the (1m-empty) candles table.
    candle_tables = load_candle_tables()

    while True:
        try:
            conn = psycopg.connect(dsn, options="-c statement_timeout=30000")
        except Exception as exc:
            log.error("Monitor DB connect failed: %s", exc)
            time.sleep(poll_s)
            continue

        try:
            # --- Staleness + alarming (one fetch, two signals) ---
            rows = fetch_status_rows(conn)
            stale = compute_staleness(rows, timeframe_intervals)

            # Signal A — funding/OI loop-heartbeat presence: any stale sentinel row pages.
            # Kept separate from the candle count-delta below: a single dead loop is one row and
            # could never trip +delta_threshold, so folding it in would regress T-004.
            loop_stale = [s for s in stale if s["symbol"] == LOOP_SENTINEL_SYMBOL]
            if loop_stale and should_alert("loop_staleness", cooldowns, cooldown_s):
                if send_webhook(webhook_url, format_stale_alert(loop_stale)):
                    log.warning("Loop-liveness alert sent: %d stale loop(s)", len(loop_stale))
                else:
                    log.warning("Loop-liveness alert delivery failed")

            # Signal B — candle alarming-mass: alert on a rolling-1h-median delta sustained N
            # cycles. The raw stale count swings benignly (HL 5m cursor + new-listing floor), so
            # it is logged for visibility but never pages on its own.
            alarming = compute_alarming(rows, timeframe_intervals, error_threshold)
            decision = evaluate_alarming(
                len(alarming), alarming_history, delta_threshold, sustain_cycles, alarming_consecutive
            )
            alarming_consecutive = decision.next_consecutive
            log.info(
                "Collectors: %d stale (raw), %d alarming (baseline ~%.0f, +%d gate)",
                len(stale), len(alarming), decision.baseline, delta_threshold,
            )
            if decision.should_fire and should_alert("alarming", cooldowns, cooldown_s):
                alert_text = format_alarming_alert(alarming, len(alarming), decision.baseline, delta_threshold)
                if send_webhook(webhook_url, alert_text):
                    log.warning("Alarming alert sent: %d collectors", len(alarming))
                else:
                    log.warning("Alarming alert delivery failed")
            if decision.should_record:
                alarming_history.append(len(alarming))

            # --- Gap detection (tiered) ---
            stale_keys = {(s["exchange"], s["timeframe"]) for s in stale}
            et_pairs = _get_exchange_timeframe_pairs(conn)
            now = time.time()

            for exchange, timeframe in et_pairs:
                # Skip gap scan for pairs already flagged stale
                if (exchange, timeframe) in stale_keys:
                    continue

                # Loop heartbeat pseudo-TFs have no candle series — nothing to gap-check
                if timeframe in LOOP_TIMEFRAMES:
                    continue

                # Tiered scanning — check slower TFs less often, per (exchange, timeframe)
                scan_interval = _TF_SCAN_INTERVALS.get(timeframe, 300)
                if not gap_scan_due(last_gap_scan, (exchange, timeframe), scan_interval, now):
                    continue

                interval_s = timeframe_intervals.get(timeframe)
                if interval_s is None:
                    continue

                gaps = detect_gaps(conn, exchange, timeframe, interval_s, candle_tables.get(timeframe, "candles"))
                if gaps:
                    alert_text = format_gap_alert(exchange, timeframe, gaps)
                    alert_key = f"gaps:{exchange}:{timeframe}"
                    if should_alert(alert_key, cooldowns, cooldown_s):
                        if send_webhook(webhook_url, alert_text):
                            log.warning("Gap alert: %s/%s — %d gaps", exchange, timeframe, len(gaps))
                        else:
                            log.warning("Gap alert delivery failed: %s/%s", exchange, timeframe)
                last_gap_scan[(exchange, timeframe)] = now

            # --- Weekly status heartbeat (Discord summary) ---
            now_utc = datetime.now(timezone.utc)
            if should_send_heartbeat(now_utc, last_heartbeat_date, heartbeat_hour_utc, heartbeat_weekday):
                try:
                    stats = fetch_heartbeat_stats(conn)
                    text = build_heartbeat_text(stats)
                    send_webhook(webhook_url, text)
                    last_heartbeat_date = now_utc.date()
                    log.info("Heartbeat sent: %s", text)
                except Exception:
                    log.exception("Heartbeat send failed")

        except Exception:
            log.exception("Monitor cycle error")
        finally:
            conn.close()

        # --- Dead-man's switch ping (every cycle, end-of-cycle) ---
        ping_healthcheck(healthcheck_url)

        log.info("Monitor cycle complete, sleeping %ds", poll_s)
        time.sleep(poll_s)


def run():
    """Entry point — loads config, env vars, starts loop."""
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    dsn_parts = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "host": os.environ.get("POSTGRES_HOST", "db"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "db": os.environ.get("POSTGRES_DB", "candle_collector"),
    }
    dsn = f"postgresql://{dsn_parts['user']}:{dsn_parts['password']}@{dsn_parts['host']}:{dsn_parts['port']}/{dsn_parts['db']}"

    webhook_url = os.environ.get("WEBHOOK_URL", "")
    poll_s = int(os.environ.get("MONITOR_INTERVAL_S", "300"))
    cooldown_s = int(os.environ.get("ALERT_COOLDOWN_S", "3600"))
    healthcheck_url = os.environ.get("HEALTHCHECK_PING_URL", "")
    heartbeat_hour_utc = int(os.environ.get("HEARTBEAT_HOUR_UTC", "0"))
    heartbeat_weekday = int(os.environ.get("HEARTBEAT_DAY_UTC", "0"))  # 0=Mon … 6=Sun; weekly status ping
    delta_threshold = int(os.environ.get("ALARMING_DELTA_THRESHOLD", "25"))
    sustain_cycles = int(os.environ.get("ALARMING_SUSTAIN_CYCLES", "3"))
    error_threshold = int(os.environ.get("ALARMING_ERROR_THRESHOLD", "3"))

    # Candle TFs + per-loop heartbeat pseudo-TFs (funding/oi/liq_ws/premium/candle_ws) — so
    # compute_staleness flags a silently-dead loop (poll or WS) via the same staleness path.
    timeframe_intervals = {**load_timeframe_intervals(), **LOOP_INTERVALS}

    _weekdays = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    log.info(
        "Monitor starting: poll=%ds cooldown=%ds webhook=%s healthcheck=%s heartbeat=weekly %s %02d:00 UTC alarming=+%d×%d cycles error>=%d",
        poll_s, cooldown_s,
        "configured" if webhook_url else "none",
        "configured" if healthcheck_url else "none",
        _weekdays[heartbeat_weekday % 7], heartbeat_hour_utc, delta_threshold, sustain_cycles, error_threshold,
    )
    monitor_loop(dsn, timeframe_intervals, webhook_url, poll_s, cooldown_s,
                 healthcheck_url, heartbeat_hour_utc, delta_threshold, sustain_cycles,
                 heartbeat_weekday=heartbeat_weekday, error_threshold=error_threshold)


if __name__ == "__main__":
    run()
