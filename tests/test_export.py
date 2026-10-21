"""T-011.1 forward-feed parquet export — byte-contract tests.

Two layers:
- **Pure module** (`api/export.py`) tested against REAL pyarrow — a stubbed lib can't verify a
  byte-contract (schema names/types, NULL→NaN value, epoch-ms conversion).
- **Endpoint** (`api/main.py`) source-pinned via `ast` — the api container can't be imported under
  the collector test harness (no fastapi), the same reason `test_api_feed_metadata.py` parses source.
"""

import ast
import math
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import export
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# The real backtester seed schema (`hl-asset-ctxs.parquet`), read-verified 2026-07-06 against the
# 240,336,969-row file. Names + types are the backtester-hash contract (it name-sorts, so order is not
# pinned by the hash); cc writes seed order as a parity convention, and this copy pins that too.
_SEED_SCHEMA = pa.schema(
    [
        ("coin", pa.string()),
        ("time_ms", pa.int64()),
        ("funding", pa.float64()),
        ("open_interest", pa.float64()),
        ("prev_day_px", pa.float64()),
        ("day_ntl_vlm", pa.float64()),
        ("premium", pa.float64()),
        ("oracle_px", pa.float64()),
        ("mark_px", pa.float64()),
        ("mid_px", pa.float64()),
        ("impact_bid_px", pa.float64()),
        ("impact_ask_px", pa.float64()),
    ]
)


def _row(**overrides) -> dict:
    """A full hl_asset_ctxs cc dict row (all mapped cc columns present), with overrides."""
    row = {
        "asset_id": "BTC",
        "timestamp": datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc),
        "funding": 1.2e-5,
        "open_interest": 123456.0,
        "prev_day_px": 42000.0,
        "day_ntl_vlm": 9.87e8,
        "premium": 0.0001,
        "oracle_px": 42010.5,
        "mark_px": 42011.0,
        "mid_px": 42010.75,
        "impact_bid_px": 42009.0,
        "impact_ask_px": 42012.0,
    }
    row.update(overrides)
    return row


def _read(data: bytes) -> pa.Table:
    return pq.read_table(BytesIO(data))


_T0 = datetime(2026, 7, 6, 10, 30, tzinfo=timezone.utc)
_T1 = datetime(2026, 7, 6, 12, 0, tzinfo=timezone.utc)


def _select_list(table: str) -> list[str]:
    """The SELECT list `feed_query` builds for `table` (the cc names the serializer will look up)."""
    sql, _ = export.feed_query(table, _T0, _T1)
    return sql[len("SELECT "):sql.index(" FROM ")].split(", ")


# ---- pure module: schema contract ----


def test_feed_schema_byte_matches_seed():
    # names + types must match the seed (the hash contract); order is checked too as a cc parity pin.
    schema = export.feed_schema("hl_asset_ctxs")
    assert schema.equals(_SEED_SCHEMA)
    assert schema.names == _SEED_SCHEMA.names  # seed order (convention, not a hash requirement)


def test_select_columns_are_cc_names_not_seed_names():
    cols = _select_list("hl_asset_ctxs")
    # cc DB column names (so the SELECT is valid) — NOT the seed names.
    assert cols[0] == "asset_id" and cols[1] == "timestamp"
    assert "coin" not in cols and "time_ms" not in cols
    assert "symbol" not in cols  # dropped (not in the seed)


# ---- pure module: row → parquet contract ----


def test_asset_id_maps_to_coin_including_1000x_token():
    data = export.rows_to_parquet("hl_asset_ctxs", [_row(asset_id="kPEPE")])
    table = _read(data)
    assert table.column("coin").to_pylist() == ["kPEPE"]  # HL-native, no transform


def test_timestamp_maps_to_epoch_ms_int64_sub_second_floor():
    # microsecond=123456 must floor to ...123 ms (proves int-component rule, not float*1000 drift).
    ts = datetime(2026, 7, 6, 12, 0, 0, 123456, tzinfo=timezone.utc)
    data = export.rows_to_parquet("hl_asset_ctxs", [_row(timestamp=ts)])
    table = _read(data)
    ms = table.column("time_ms").to_pylist()[0]
    assert table.schema.field("time_ms").type == pa.int64()
    assert ms == int(ts.timestamp()) * 1000 + 123
    assert ms % 1000 == 123


def test_null_float_becomes_nan_value_not_parquet_null():
    data = export.rows_to_parquet("hl_asset_ctxs", [_row(funding=None)])
    table = _read(data)
    col = table.column("funding")
    assert col.null_count == 0  # NaN is a VALUE, not a parquet-null — required by the backtester hash
    assert math.isnan(col.to_pylist()[0])


def test_zero_float_survives_not_coerced_to_nan():
    # the `nan if v is None else v` guard (never `v or nan`) must preserve a legitimate 0.0.
    data = export.rows_to_parquet("hl_asset_ctxs", [_row(oracle_px=0.0)])
    table = _read(data)
    assert table.column("oracle_px").to_pylist() == [0.0]


def test_roundtrip_preserves_schema_names_types_order():
    data = export.rows_to_parquet("hl_asset_ctxs", [_row(), _row(asset_id="ETH")])
    assert _read(data).schema.equals(_SEED_SCHEMA)


def test_empty_rows_is_total_valid_zero_row_parquet():
    # module totality — the endpoint guards empty with 204, but the pure fn must not crash on [].
    table = _read(export.rows_to_parquet("hl_asset_ctxs", []))
    assert table.num_rows == 0
    assert table.schema.equals(_SEED_SCHEMA)


def test_missing_mapped_column_fails_closed():
    # direct-key access surfaces a spec/SELECT drift immediately (vs silent None-fill).
    bad = _row()
    del bad["mid_px"]
    try:
        export.rows_to_parquet("hl_asset_ctxs", [bad])
    except KeyError:
        return
    raise AssertionError("expected KeyError on a row missing a mapped cc column")


def test_unknown_column_kind_fails_closed():
    # the transform is chosen by explicit `kind`, not inferred from the arrow type — an unregistered
    # kind (e.g. a future feed's column left mis-declared) must raise, not silently mis-convert.
    with pytest.raises(ValueError, match="unknown column kind"):
        export.Column("x", "x", "bogus")


# ---- binance_oi feed (open_interest -> binance-oi-metrics seed) ----

# The real binance-oi-metrics.parquet schema (140,033,320 rows, 694 all-USDT coins), read-verified
# 2026-07-06. coin is the Binance-native symbol; cc derives it from the ccxt `symbol` column.
_OI_SEED_SCHEMA = pa.schema(
    [
        ("coin", pa.string()),
        ("create_time_ms", pa.int64()),
        ("sum_open_interest", pa.float64()),
        ("sum_open_interest_value", pa.float64()),
    ]
)


def _oi_row(**overrides) -> dict:
    row = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc),
        "open_interest": 12345.0,
        "oi_value_usd": 5.4e8,
    }
    row.update(overrides)
    return row


def test_binance_oi_feed_schema_matches_seed():
    assert export.feed_schema("binance_oi").equals(_OI_SEED_SCHEMA)


def test_binance_oi_select_columns_are_cc_names():
    # coin derives from `symbol` (native transform), so the SELECT pulls symbol, not asset_id.
    assert _select_list("binance_oi") == ["symbol", "timestamp", "open_interest", "oi_value_usd"]


def test_native_symbol_transform_ccxt_to_binance_native():
    data = export.rows_to_parquet(
        "binance_oi", [_oi_row(symbol="BTC/USDT:USDT"), _oi_row(symbol="1000PEPE/USDT:USDT")]
    )
    # BTC/USDT:USDT -> BTCUSDT ; 1000PEPE/USDT:USDT -> 1000PEPEUSDT (matches the seed's native coins).
    assert _read(data).column("coin").to_pylist() == ["BTCUSDT", "1000PEPEUSDT"]


def test_binance_oi_roundtrip_schema_and_ms():
    t = _read(export.rows_to_parquet("binance_oi", [_oi_row()]))
    assert t.schema.equals(_OI_SEED_SCHEMA)
    assert t.column("create_time_ms").to_pylist()[0] == int(_oi_row()["timestamp"].timestamp()) * 1000


# ---- endpoint source pins (ast — api/main.py can't import under the collector harness) ----

_API_MAIN = Path(__file__).resolve().parents[1] / "api" / "main.py"


def _export_endpoint_source() -> str:
    src = _API_MAIN.read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "export_parquet":
            seg = ast.get_source_segment(src, node)
            assert seg is not None
            return seg
    raise AssertionError("export_parquet endpoint not found in api/main.py")


def test_endpoint_route_registered():
    src = _API_MAIN.read_text()
    assert "@app.get(" in src and '"/export/parquet"' in src


def test_endpoint_whitelists_table_via_feed_specs():
    body = _export_endpoint_source()
    assert "FEED_SPECS.get(table)" in body
    assert "status_code=400" in body  # unknown table rejected


def test_endpoint_rejects_naive_datetimes():
    body = _export_endpoint_source()
    assert "tzinfo is None" in body


def test_endpoint_normalizes_bounds_to_utc():
    # an accepted +offset input must map to UTC before the query/filename (no caller-local date stamp).
    body = _export_endpoint_source()
    assert "astimezone(timezone.utc)" in body


def test_endpoint_rejects_reversed_window():
    assert "start >= end" in _export_endpoint_source()


def test_endpoint_queries_through_feed_query():
    # the endpoint builds no SQL itself: export.feed_query owns the window, filters and row cap.
    body = _export_endpoint_source()
    assert "export.feed_query(table, start, end, asset_id)" in body
    assert "SELECT" not in body


def test_plain_feed_query_uses_half_open_window_not_inclusive():
    sql, params = export.feed_query("hl_asset_ctxs", _T0, _T1)
    assert "timestamp >= %(start)s AND timestamp < %(end)s" in sql
    assert "<= %(end)s" not in sql  # guards the copy-paste-from-/candles-inclusive regression
    assert params["start"] == _T0 and params["end"] == _T1  # a plain feed's bounds are not rounded
    assert sql.endswith("ORDER BY timestamp LIMIT %(limit)s")
    assert params["limit"] == export.EXPORT_MAX_ROWS + 1


def test_feed_query_parameterizes_asset_id_and_venue():
    sql, params = export.feed_query("hl_liquidations", _T0, _T1, asset_id="BTC")
    assert "venue = %(venue)s" in sql and params["venue"] == "hyperliquid"
    assert "asset_id = %(asset_id)s" in sql and params["asset_id"] == "BTC"
    assert "BTC" not in sql and "hyperliquid" not in sql


def test_endpoint_returns_204_on_empty_not_empty_parquet():
    body = _export_endpoint_source()
    assert "status_code=204" in body
    # the 204 guard must precede serialization — no rows_to_parquet on an empty result.
    assert body.index("status_code=204") < body.index("export.rows_to_parquet(")


def test_endpoint_caps_rows_with_413():
    body = _export_endpoint_source()
    assert "EXPORT_MAX_ROWS" in body
    assert "status_code=413" in body


# ---- hl_liquidations feed (liquidations WHERE venue='hyperliquid' -> hl-liquidations seed) ----

# The backtester's hl-liquidations.parquet schema (fetch_hl_node_fills_liquidations.py:70-85), 13 cols in order.
_LIQ_SEED_SCHEMA = pa.schema(
    [
        ("time_ms", pa.int64()),
        ("coin", pa.string()),
        ("side", pa.string()),
        ("dir", pa.string()),
        ("px", pa.float64()),
        ("sz", pa.float64()),
        ("mark_px", pa.float64()),
        ("closed_pnl", pa.float64()),
        ("method", pa.string()),
        ("liquidated_user", pa.string()),
        ("oid", pa.int64()),
        ("tid", pa.int64()),
        ("day", pa.string()),
    ]
)


def _liq_row(**overrides) -> dict:
    """A full hl_liquidations cc dict row (the 13 mapped cc columns), with overrides."""
    row = {
        "timestamp": datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc),
        "asset_id": "BTC",
        "side": "A",
        "dir": "Close Long",
        "px": 42000.0,
        "sz": 1.5,
        "mark_px": 42010.0,
        "closed_pnl": -12.5,
        "method": "market",
        "liquidated_user": "0xabc",
        "oid": 111,
        "tid": 222,
        "day": "20260706",
    }
    row.update(overrides)
    return row


def test_hl_liquidations_feed_schema_matches_seed():
    assert export.feed_schema("hl_liquidations").equals(_LIQ_SEED_SCHEMA)


def test_hl_liquidations_select_columns_are_cc_names():
    assert _select_list("hl_liquidations") == [
        "timestamp", "asset_id", "side", "dir", "px", "sz", "mark_px",
        "closed_pnl", "method", "liquidated_user", "oid", "tid", "day",
    ]


def test_int_kind_passes_through_int64_not_datetime():
    # oid/tid are the `int` kind: identity int64, NOT epoch-ms datetime-converted (the fan-out trap).
    t = _read(export.rows_to_parquet("hl_liquidations", [_liq_row(oid=111, tid=222)]))
    assert t.schema.field("oid").type == pa.int64()
    assert t.column("oid").to_pylist() == [111]
    assert t.column("tid").to_pylist() == [222]


def test_hl_liquidations_asset_id_passthrough_native_coin():
    # asset_id is the HL-native coin verbatim (kPEPE) == seed coin; no canonical/native transform.
    data = export.rows_to_parquet("hl_liquidations", [_liq_row(asset_id="kPEPE")])
    assert _read(data).column("coin").to_pylist() == ["kPEPE"]


def test_hl_liquidations_optional_nulls_fill_seed_defaults():
    # seed writes "" for a blank dir/method and 0.0 for a blank closed_pnl (:146,151,150) — NOT parquet-null/NaN.
    t = _read(export.rows_to_parquet("hl_liquidations", [_liq_row(dir=None, method=None, closed_pnl=None)]))
    assert t.column("dir").to_pylist() == [""]
    assert t.column("method").to_pylist() == [""]
    assert t.column("closed_pnl").to_pylist() == [0.0]
    assert t.column("dir").null_count == 0  # a value, not a parquet-null (null-mask parity with the seed)
    assert t.column("method").null_count == 0


def test_hl_liquidations_closed_pnl_values_survive():
    # a legit 0.0 must NOT be coerced (the on_null path is `v is None`, never `v or 0.0`); a negative survives.
    t = _read(export.rows_to_parquet("hl_liquidations", [_liq_row(closed_pnl=0.0), _liq_row(closed_pnl=-1.5)]))
    assert t.column("closed_pnl").to_pylist() == [0.0, -1.5]


def test_hl_liquidations_required_null_raises():
    # seed-required columns have no seed default: a cc NULL there is a row the seed never contains -> fail closed.
    for col in ("side", "px", "sz", "mark_px", "oid", "tid"):
        try:
            export.rows_to_parquet("hl_liquidations", [_liq_row(**{col: None})])
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError on NULL {col!r}")


def test_hl_liquidations_roundtrip_schema_and_ms():
    t = _read(export.rows_to_parquet("hl_liquidations", [_liq_row()]))
    assert t.schema.equals(_LIQ_SEED_SCHEMA)
    assert t.column("time_ms").to_pylist()[0] == int(_liq_row()["timestamp"].timestamp()) * 1000


def test_liquidations_endpoint_registered_reads_liquidations_table():
    # /liquidations JSON range endpoint (monitoring surface) — source-pinned like the export endpoint.
    src = _API_MAIN.read_text()
    assert '"/liquidations"' in src
    assert "FROM liquidations" in src
    # limit is bounded at BOTH ends (ge=1 rejects a non-positive LIMIT at the boundary, not in PostgreSQL).
    assert "ge=1, le=5000" in src


def test_feed_query_applies_symbol_like_universe_filter():
    # a feed with `symbol_like` (binance_oi) restricts to the seed universe via a parameterized LIKE.
    sql, params = export.feed_query("binance_oi", _T0, _T1)
    assert "symbol LIKE %(symbol_like)s" in sql and params["symbol_like"] == "%/USDT:USDT"


# ---- binance_premium_index feed (premium_index 5m resampled to the 1h binance-premium-index seed) ----

# The backtester's binance-premium-index.parquet schema (13,229,212 rows, 696 all-USDT coins), read-verified
# 2026-10-20; close_time_ms - open_time_ms is 3,599,999 on every row.
_PREMIUM_SEED_SCHEMA = pa.schema(
    [
        ("coin", pa.string()),
        ("open_time_ms", pa.int64()),
        ("premium_open", pa.float64()),
        ("premium_high", pa.float64()),
        ("premium_low", pa.float64()),
        ("premium_close", pa.float64()),
        ("close_time_ms", pa.int64()),
    ]
)


def _premium_row(**overrides) -> dict:
    """One resampled premium row as `feed_query` selects it (cc aliases)."""
    row = {
        "native_symbol": "BTCUSDT",
        "bucket_open": datetime(2026, 7, 6, 11, 0, tzinfo=timezone.utc),
        "premium_open": -0.0001,
        "premium_high": 0.0003,
        "premium_low": -0.0004,
        "premium_close": 0.0002,
        "bucket_close": datetime(2026, 7, 6, 11, 59, 59, 999000, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_premium_feed_schema_matches_seed():
    schema = export.feed_schema("binance_premium_index")
    assert schema.equals(_PREMIUM_SEED_SCHEMA)
    assert schema.names == _PREMIUM_SEED_SCHEMA.names


def test_premium_query_rounds_bounds_up_to_the_hour():
    # an hour is in [start, end) iff its open is: source rows are read over [ceil(start), ceil(end)).
    _, params = export.feed_query("binance_premium_index", _T0, _T1)
    assert params["start"] == datetime(2026, 7, 6, 11, 0, tzinfo=timezone.utc)
    assert params["end"] == _T1  # already on the hour: unchanged


def test_premium_query_resamples_complete_hours_only():
    sql, params = export.feed_query("binance_premium_index", _T0, _T1)
    bucket = "time_bucket(INTERVAL '3600 seconds', timestamp)"
    assert f"{bucket} AS bucket_open" in sql
    assert f"{bucket} + INTERVAL '3600 seconds' - INTERVAL '1 millisecond' AS bucket_close" in sql
    assert "CASE WHEN count(premium_open) = count(*) THEN first(premium_open, timestamp) END AS premium_open" in sql
    assert "CASE WHEN count(premium_high) = count(*) THEN max(premium_high) END AS premium_high" in sql
    assert "CASE WHEN count(premium_low) = count(*) THEN min(premium_low) END AS premium_low" in sql
    assert "CASE WHEN count(premium_close) = count(*) THEN last(premium_close, timestamp) END AS premium_close" in sql
    assert f"GROUP BY native_symbol, {bucket} HAVING count(*) = 12" in sql
    assert "exchange = %(venue)s" in sql and params["venue"] == "binance"
    assert "symbol LIKE %(symbol_like)s" in sql and params["symbol_like"] == "%/USDT:USDT"
    assert sql.endswith(f"ORDER BY {bucket} LIMIT %(limit)s")


def test_premium_row_maps_bucket_times_to_seed_ms():
    t = _read(export.rows_to_parquet("binance_premium_index", [_premium_row()]))
    assert t.schema.equals(_PREMIUM_SEED_SCHEMA)
    row = t.to_pylist()[0]
    assert row["coin"] == "BTCUSDT"
    assert row["open_time_ms"] == int(datetime(2026, 7, 6, 11, 0, tzinfo=timezone.utc).timestamp()) * 1000
    assert row["close_time_ms"] - row["open_time_ms"] == 3_599_999  # as on every seed row
    assert (row["premium_open"], row["premium_high"], row["premium_low"], row["premium_close"]) == (
        -0.0001, 0.0003, -0.0004, 0.0002)


def test_premium_null_price_raises_not_nan():
    # the seed has no NaN: a NULL aggregate (a NULL anywhere in the hour) is a writer bug, not a missing value.
    with pytest.raises(ValueError, match="premium_high"):
        export.rows_to_parquet("binance_premium_index", [_premium_row(premium_high=None)])


@pytest.mark.parametrize("bucket_s, source_s", [(3600, 7), (3600.0, 300), (True, 1), (0, 300), (3600, -300)])
def test_resample_rejects_widths_unsafe_to_format_into_sql(bucket_s, source_s):
    with pytest.raises(ValueError, match="Resample widths"):
        export.Resample(bucket_s=bucket_s, source_s=source_s)


def test_unknown_agg_fails_at_spec_construction():
    with pytest.raises(ValueError, match="unknown agg"):
        export.Column("x", "x", "float", agg="median")


def test_premium_bound_just_past_the_hour_rounds_to_the_next():
    on = datetime(2026, 7, 6, 11, 0, tzinfo=timezone.utc)
    _, params = export.feed_query("binance_premium_index", on.replace(microsecond=1), on + timedelta(hours=2))
    assert params["start"] == on + timedelta(hours=1)
    assert params["end"] == on + timedelta(hours=2)
