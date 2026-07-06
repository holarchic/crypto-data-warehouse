"""T-009.3 — HL node_fills Grade-A liquidation extraction tests.

Pure extraction/mapping logic + a real-lz4 streaming test + an INSERT-SQL source pin. S3 and the DB
are never touched (the module's `boto3`/`db`/`psycopg` are only reached by `run`/`_process_key`, which
these tests don't exercise); `lz4` is real so the streaming path is genuinely exercised.
"""

import datetime as dt
import json
from datetime import timezone

import liquidations_hl as hl
import lz4.frame


def _liq_fill(**over):
    fill = {
        "time": 1700000000123,
        "coin": "BTC",
        "side": "A",
        "dir": "Close Long",
        "px": "42000.5",
        "sz": "0.5",
        "oid": 111,
        "tid": 222,
        "closedPnl": "-100.0",
        "liquidation": {"liquidatedUser": "0xLIQ", "markPx": "42010.0", "method": "market"},
    }
    fill.update(over)
    return fill


# ---- format handling ----


def test_iter_fills_legacy_and_by_block():
    legacy = json.dumps(["0xADDR", _liq_fill()]).encode()
    assert list(hl._iter_fills(legacy)) == [("0xADDR", _liq_fill())]
    by_block = json.dumps({"block_number": 9, "events": [["0xA", _liq_fill()], ["0xB", _liq_fill()]]}).encode()
    got = list(hl._iter_fills(by_block))
    assert [a for a, _ in got] == ["0xA", "0xB"]


# ---- dedup-at-source + mapping ----


def test_dedup_emits_only_liquidated_side():
    fill = _liq_fill()  # liquidation.liquidatedUser == "0xLIQ"
    assert hl._row("0xLIQ", fill, "20260705") is not None  # the liquidated side → emitted
    assert hl._row("0xCOUNTERPARTY", fill, "20260705") is None  # counterparty → dropped (dedup at source)


def test_non_liquidation_fill_dropped():
    assert hl._row("0xADDR", {"time": 1, "coin": "BTC", "oid": 1, "tid": 2}, "20260705") is None


def test_row_mapping_close_long():
    r = hl._row("0xLIQ", _liq_fill(dir="Close Long"), "20260705")
    assert r["liquidated_side"] == "long"
    assert r["asset_id"] == "BTC" and r["side"] == "A" and r["dir"] == "Close Long"
    assert r["px"] == 42000.5 and r["sz"] == 0.5 and r["mark_px"] == 42010.0
    assert r["closed_pnl"] == -100.0 and r["method"] == "market"
    assert r["liquidated_user"] == "0xLIQ" and r["oid"] == 111 and r["tid"] == 222 and r["day"] == "20260705"


def test_row_mapping_close_short():
    assert hl._row("0xLIQ", _liq_fill(dir="Close Short"), "20260705")["liquidated_side"] == "short"


def test_liquidated_side_falls_back_to_side_when_dir_blank():
    # dir absent → fall back to side (A→long), with the anomaly logged.
    assert hl._row("0xLIQ", _liq_fill(dir=""), "20260705")["liquidated_side"] == "long"


def test_ms_to_epoch_seconds():
    # the load-bearing conversion: to_timestamp receives SECONDS, not ms.
    assert hl._row("0xLIQ", _liq_fill(time=1700000000123), "20260705")["ts"] == 1700000000.123


# ---- parse-or-drop ----


def test_oid_or_tid_missing_is_dropped_not_defaulted():
    assert hl._row("0xLIQ", _liq_fill(oid=None), "20260705") is None
    fill = _liq_fill()
    del fill["tid"]
    assert hl._row("0xLIQ", fill, "20260705") is None


def test_missing_coin_dropped():
    fill = _liq_fill()
    del fill["coin"]
    assert hl._row("0xLIQ", fill, "20260705") is None


def test_bad_magnitude_lands_null():
    r = hl._row("0xLIQ", _liq_fill(px="not-a-number"), "20260705")
    assert r is not None and r["px"] is None  # magnitude → NULL, row still lands


# ---- day selection + cutover ----


def test_default_days_last_3_complete_utc():
    now = dt.datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
    assert hl.default_days(now) == ("20260703", "20260705")  # yesterday-2 .. yesterday, today excluded


def test_prefix_for_cutover():
    assert hl._prefix_for("20250726") == hl.LEGACY_PREFIX
    assert hl._prefix_for("20250727") == hl.BLOCK_PREFIX  # by-block owns the overlap day
    assert hl._prefix_for("20250728") == hl.BLOCK_PREFIX


# ---- real streaming decompress end-to-end ----


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def iter_chunks(self, chunk_size):
        for i in range(0, len(self._data), chunk_size):
            yield self._data[i : i + chunk_size]


def test_extract_rows_streams_lz4_and_emits_liquidated_side_only():
    lines = [
        json.dumps(["0xLIQ", _liq_fill(oid=1, tid=1)]),  # liquidated side → 1 row
        json.dumps(["0xCP", _liq_fill(oid=1, tid=1)]),  # counterparty (same event) → dropped
        json.dumps(["0xX", {"time": 5, "coin": "ETH", "side": "B", "oid": 9, "tid": 9}]),  # non-liq → dropped
        json.dumps({"events": [["0xLIQ", _liq_fill(coin="SOL", dir="Close Short", oid=2, tid=2)]]}),  # by-block
    ]
    raw = ("\n".join(lines) + "\n").encode()
    body = _FakeBody(lz4.frame.compress(raw))
    rows = list(hl.extract_rows(body, "20260705"))
    assert len(rows) == 2
    assert {r["asset_id"] for r in rows} == {"BTC", "SOL"}
    assert next(r for r in rows if r["asset_id"] == "SOL")["liquidated_side"] == "short"


# ---- INSERT SQL contract ----


def test_insert_sql_shape():
    sql = hl.INSERT_HL_LIQ_SQL
    for col in ("timestamp", "venue", "grade", "asset_id", "liquidated_side", "side", "dir", "px", "sz",
                "mark_px", "closed_pnl", "method", "liquidated_user", "oid", "tid", "day"):
        assert col in sql
    assert "to_timestamp(%(ts)s)" in sql  # seconds, not ms
    assert "'hyperliquid'" in sql and "'a'" in sql
    assert "ON CONFLICT (venue, asset_id, timestamp, oid, tid) DO NOTHING" in sql
