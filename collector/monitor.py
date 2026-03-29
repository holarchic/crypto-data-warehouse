"""Health monitor — stale collector detection, gap detection, webhook alerts.

Runs as always-on container: `python -m monitor`
"""

import json
import logging
import os
import time
import urllib.request

from config import load_timeframe_intervals
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pure functions (testable)
# ---------------------------------------------------------------------------


def compute_staleness(status_rows, timeframe_intervals):
    """Given collection_status rows + {tf: interval_s}, return rows where
    seconds_since_success > 2 * interval_s or last_success is NULL."""
    stale = []
    for row in status_rows:
        tf = row["timeframe"]
        interval_s = timeframe_intervals.get(tf)
        if interval_s is None:
            continue
        threshold = 2 * interval_s
        since = row.get("seconds_since_success")
        if since is None or since > threshold:
            stale.append(row)
    return stale


def format_stale_alert(stale_pairs):
    """Human-readable stale alert message."""
    if not stale_pairs:
        return ""
    lines = [f"🔴 {len(stale_pairs)} stale collector(s) detected:"]
    for p in stale_pairs:
        since = p.get("seconds_since_success")
        since_str = f"{since:.0f}s ago" if since is not None else "never"
        errors = p.get("error_count", 0)
        lines.append(f"  • {p['exchange']}/{p['symbol']}/{p['timeframe']} — last success: {since_str}, errors: {errors}")
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


def build_webhook_payload(text):
    """JSON-encoded payload with both Slack (text) and Discord (content) keys."""
    return json.dumps({"text": text, "content": text}).encode("utf-8")


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

GAP_SQL = """
    WITH consecutive AS (
        SELECT symbol, timestamp,
               LEAD(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_ts
        FROM candles
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


def send_webhook(url, text):
    """POST alert to Slack/Discord webhook. Returns True on success."""
    if not url:
        return False
    payload = build_webhook_payload(text)
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        log.warning("Webhook POST failed: %s", exc)
        return False


def check_staleness(conn, timeframe_intervals):
    """Query collection_status and return stale pairs."""
    with conn.cursor() as cur:
        cur.execute(STALENESS_SQL)
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return compute_staleness(rows, timeframe_intervals)


def detect_gaps(conn, exchange, timeframe, interval_s, lookback_count=10):
    """Detect candle gaps for an exchange/timeframe. Returns list of gap dicts."""
    lookback_s = lookback_count * interval_s
    params = {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_s": lookback_s,
        "interval_s": interval_s,
    }
    with conn.cursor() as cur:
        cur.execute(GAP_SQL, params)
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


def monitor_loop(dsn, timeframe_intervals, webhook_url, poll_s=300, cooldown_s=3600):
    """Main monitor loop. Checks staleness + gaps on each cycle."""
    import psycopg

    cooldowns = {}
    last_gap_scan = {}  # {timeframe: last_scan_epoch}

    while True:
        try:
            conn = psycopg.connect(dsn, options="-c statement_timeout=30000")
        except Exception as exc:
            log.error("Monitor DB connect failed: %s", exc)
            time.sleep(poll_s)
            continue

        try:
            # --- Staleness check ---
            stale = check_staleness(conn, timeframe_intervals)
            if stale:
                log.warning("Stale collectors: %d", len(stale))
                alert_text = format_stale_alert(stale)
                if should_alert("staleness", cooldowns, cooldown_s):
                    send_webhook(webhook_url, alert_text)
                    log.info("Stale alert sent")
            else:
                log.info("All collectors healthy")

            # --- Gap detection (tiered) ---
            stale_keys = {(s["exchange"], s["timeframe"]) for s in stale}
            et_pairs = _get_exchange_timeframe_pairs(conn)
            now = time.time()

            for exchange, timeframe in et_pairs:
                # Skip gap scan for pairs already flagged stale
                if (exchange, timeframe) in stale_keys:
                    continue

                # Tiered scanning — check slower TFs less often
                scan_interval = _TF_SCAN_INTERVALS.get(timeframe, 300)
                last_scan = last_gap_scan.get(timeframe, 0)
                if now - last_scan < scan_interval:
                    continue

                interval_s = timeframe_intervals.get(timeframe)
                if interval_s is None:
                    continue

                gaps = detect_gaps(conn, exchange, timeframe, interval_s)
                if gaps:
                    alert_text = format_gap_alert(exchange, timeframe, gaps)
                    alert_key = f"gaps:{exchange}:{timeframe}"
                    if should_alert(alert_key, cooldowns, cooldown_s):
                        send_webhook(webhook_url, alert_text)
                        log.warning("Gap alert: %s/%s — %d gaps", exchange, timeframe, len(gaps))
                last_gap_scan[timeframe] = now

        except Exception:
            log.exception("Monitor cycle error")
        finally:
            conn.close()

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

    timeframe_intervals = load_timeframe_intervals()

    log.info("Monitor starting: poll=%ds cooldown=%ds webhook=%s", poll_s, cooldown_s, "configured" if webhook_url else "none")
    monitor_loop(dsn, timeframe_intervals, webhook_url, poll_s, cooldown_s)


if __name__ == "__main__":
    run()
