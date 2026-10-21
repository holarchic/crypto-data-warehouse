"""Feed-health rules the monitor and the api share: the two staleness rules and the candle gap query.

Both images import this module (api/Dockerfile copies it beside config.py), so it imports only config
and the stdlib.
"""

from config import CANDLE_TABLES

# A REST candle loop fetches its pairs serially, so a short-timeframe pair near the end of a cycle is
# routinely more than two intervals old. The floor keeps that cursor out of the monitor's stale count.
PAGING_STALE_FLOOR_S = 1800


def paging_threshold_s(interval_s: int) -> int:
    """Seconds since the last success after which the monitor counts a row stale."""
    return interval_s + max(interval_s, PAGING_STALE_FLOOR_S)


def is_stale_for_paging(seconds_since_success: float | None, interval_s: int | None) -> bool:
    """The monitor's raw-staleness and loop-liveness rule: no success, or none within
    `paging_threshold_s`. A stale loop sentinel pages; candle pages go through the monitor's separate
    alarming gate. /status uses the stricter `is_stale_for_status`. An unknown interval is never stale."""
    if interval_s is None:
        return False
    return seconds_since_success is None or seconds_since_success > paging_threshold_s(interval_s)


def is_stale_for_status(seconds_since_success: float | None, interval_s: int | None) -> bool:
    """/status's per-pair freshness for a reader: no success, or none within two intervals (120 s at
    1m). Stricter than the monitor's `is_stale_for_paging`, which waits out a serial fetch cycle. An
    unknown interval is never stale."""
    if interval_s is None:
        return False
    return seconds_since_success is None or seconds_since_success > 2 * interval_s


_GAP_TEMPLATE = """
    WITH consecutive AS (
        SELECT symbol, timestamp,
               LEAD(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_ts
        FROM {table}
        WHERE exchange = %(exchange)s
          AND timeframe = %(timeframe)s
          AND timestamp > NOW() - INTERVAL '1 second' * %(lookback_s)s{asset_filter}
    )
    SELECT symbol, timestamp AS gap_after, next_ts AS gap_before,
           EXTRACT(EPOCH FROM next_ts - timestamp) / %(interval_s)s - 1 AS missing_count
    FROM consecutive
    WHERE next_ts IS NOT NULL
      AND EXTRACT(EPOCH FROM next_ts - timestamp) > %(interval_s)s * 1.5
    ORDER BY symbol, timestamp
"""
# Keyed by (table, by_asset). The table comes from the closed CANDLE_TABLES allowlist at import, never
# from a runtime value; psycopg can't bind an identifier.
_GAP_SQL = {
    (table, by_asset): _GAP_TEMPLATE.format(
        table=table, asset_filter="\n          AND asset_id = %(asset_id)s" if by_asset else "")
    for table in CANDLE_TABLES
    for by_asset in (False, True)
}


def gap_query(table: str, *, exchange: str, timeframe: str, interval_s: int, lookback_count: int,
              asset_id: str | None = None) -> tuple[str, dict]:
    """The candle gap query over the last `lookback_count` intervals of one exchange and timeframe, and
    its parameters: one row per run of missing bars. `table` is a config-validated candle table."""
    params = {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_s": lookback_count * interval_s,
        "interval_s": interval_s,
    }
    if asset_id is not None:
        params["asset_id"] = asset_id
    return _GAP_SQL[(table, asset_id is not None)], params
