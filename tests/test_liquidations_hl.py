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


def test_asset_id_is_hl_native_not_canonicalized():
    # liquidations mirrors the seed's HL-native coin (SPEC:240/247, matches hl_asset_ctxs) — a 1000x
    # token stays `kPEPE`, NOT the ccxt-canonical `KPEPE`, so the T-011 export is a seed passthrough.
    r = hl._row("0xLIQ", _liq_fill(coin="kPEPE"), "20260705")
    assert r["asset_id"] == "kPEPE"


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


def test_anomalies_counted_in_stats_not_logged_per_row():
    stats = hl._new_stats()
    hl._row("0xLIQ", _liq_fill(oid=None), "20260705", stats)  # malformed identity → dropped
    assert stats["dropped"] == 1
    list(hl._emit_line(b"{ not valid json", "20260705", stats))  # malformed line → bad_lines (no raise)
    assert stats["bad_lines"] == 1
    hl._row("0xLIQ", _liq_fill(dir="", side="A"), "20260705", stats)  # dir blank → side fallback used
    assert stats["side_fallback"] == 1


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
    """Emits FIXED small chunks regardless of the requested `chunk_size`, so a JSON line spans several
    chunks — exercising `extract_rows`' rolling-buffer line split across chunk boundaries (mirrors S3
    returning arbitrary small reads)."""

    def __init__(self, data: bytes, emit_chunk: int = 16):
        self._data = data
        self._emit = emit_chunk

    def iter_chunks(self, chunk_size):
        for i in range(0, len(self._data), self._emit):
            yield self._data[i : i + self._emit]


def test_extract_rows_streams_lz4_and_emits_liquidated_side_only():
    lines = [
        json.dumps(["0xLIQ", _liq_fill(oid=1, tid=1)]),  # liquidated side → 1 row
        json.dumps(["0xCP", _liq_fill(oid=1, tid=1)]),  # counterparty (same event) → dropped
        json.dumps(["0xX", {"time": 5, "coin": "ETH", "side": "B", "oid": 9, "tid": 9}]),  # non-liq → dropped
        json.dumps({"events": [["0xLIQ", _liq_fill(coin="SOL", dir="Close Short", oid=2, tid=2)]]}),  # by-block
    ]
    raw = ("\n".join(lines) + "\n").encode()
    body = _FakeBody(lz4.frame.compress(raw))  # 16-byte chunks force line splits across boundaries
    rows = list(hl.extract_rows(body, "20260705"))
    assert len(rows) == 2
    assert {r["asset_id"] for r in rows} == {"BTC", "SOL"}
    assert next(r for r in rows if r["asset_id"] == "SOL")["liquidated_side"] == "short"


def test_by_block_dict_missing_events_is_counted_not_silently_dropped():
    # a by-block dict with no `events` list is format drift → counted, NOT a silent zero-row drop.
    raw = (json.dumps({"block_number": 9}) + "\n").encode()  # no "events" key
    stats = hl._new_stats()
    rows = list(hl.extract_rows(_FakeBody(lz4.frame.compress(raw)), "20260705", stats))
    assert rows == [] and stats["bad_lines"] == 1


def test_malformed_fill_shape_is_counted_not_raised():
    # a non-dict fill in each format must be a counted bad-line skip, never an AttributeError.
    raw = (
        json.dumps(["0xA", "not-a-dict-fill"]) + "\n"  # legacy, bad fill
        + json.dumps({"events": [["0xB", 12345]]}) + "\n"  # by-block, bad fill
    ).encode()
    stats = hl._new_stats()
    rows = list(hl.extract_rows(_FakeBody(lz4.frame.compress(raw)), "20260705", stats))
    assert rows == []
    assert stats["bad_lines"] == 2


# ---- error classification ----


def test_corrupt_lz4_raises_runtimeerror_not_db_unavailable():
    # a corrupt object is an object-level failure (run() counts it + continues) — it must surface as a
    # RuntimeError, NOT be misclassified as the run-fatal _DBUnavailable (DB-down) path.
    body = _FakeBody(b"this is not a valid lz4 frame at all")
    try:
        list(hl.extract_rows(body, "20260705"))
    except hl._DBUnavailable:
        raise AssertionError("a corrupt lz4 object must not be classified as DB-unavailable")
    except RuntimeError:
        return
    raise AssertionError("expected a RuntimeError from a corrupt lz4 frame")


def test_connect_converts_retry_exhaustion_to_db_unavailable(monkeypatch):
    def _boom(_dsn):
        raise RuntimeError("connect retries exhausted")

    # Patch build_dsn too so the test is self-contained (build_dsn reads os.environ; whether the db
    # module is stubbed leaks across the suite — the conftest-isolation fragility).
    monkeypatch.setattr(hl, "build_dsn", lambda: "dummy-dsn")
    monkeypatch.setattr(hl, "connect_db", _boom)
    try:
        hl._connect()
    except hl._DBUnavailable:
        return
    raise AssertionError("expected _DBUnavailable from connect_db exhaustion")


# ---- INSERT SQL contract ----


def test_insert_sql_shape():
    sql = hl.INSERT_HL_LIQ_SQL
    for col in ("timestamp", "venue", "grade", "asset_id", "liquidated_side", "side", "dir", "px", "sz",
                "mark_px", "closed_pnl", "method", "liquidated_user", "oid", "tid", "day"):
        assert col in sql
    assert "to_timestamp(%(ts)s)" in sql  # seconds, not ms
    assert "'hyperliquid'" in sql and "'a'" in sql
    assert "ON CONFLICT (venue, asset_id, timestamp, oid, tid) DO NOTHING" in sql
