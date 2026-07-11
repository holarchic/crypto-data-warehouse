"""Pure parquet-export contract module for the backtester's forward-ingest (T-011).

Owns the byte-contract between cc's landed tables and the backtester's frozen seed parquets. The
backtester does NOT convert on ingest — it reads the raw exported parquet directly and validates each
feed by a structural hash over schema(cols+types) + row content. The hash name-sorts the schema and
sums per-row digests commutatively (feed_provenance.rs), so column and row ORDER do not affect it —
what must match the seed is the column names + types, and SQL NULL must be written as a NaN *value*
(null_count stays 0), not a parquet-null (a parquet-null changes the column's null-mask, which the
per-cell row digest sees). cc writes columns in seed order anyway as a parity convention. Per feed,
one FeedSpec holds the seed schema + the cc->seed column map.

Kept free of fastapi/psycopg so it unit-tests against REAL pyarrow (a stubbed lib can't verify a
byte-contract); the /export/parquet endpoint is a thin wrapper that pulls rows and calls in here.
"""

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from math import nan

import pyarrow as pa
import pyarrow.parquet as pq

# Max rows one /export/parquet request materializes. The endpoint buffers the whole path at once —
# the psycopg dict_row list + arrow arrays + parquet bytes + the response copy — inside the api
# container's 512 MB limit (docker-compose.prod.yml). Sized by a peak-RSS smoke run INSIDE the api
# image (Linux, pyarrow native included): 150k rows → 209.8 MB peak in the worker; ≈270 MB for the
# full serving process (+ uvicorn/fastapi/psycopg baseline), leaving >240 MB headroom under 512 MB.
# Larger spans use the memory-streaming bulk export script (T-011 fan-out).
EXPORT_MAX_ROWS = 150_000


# A column's `kind` is the SINGLE source of both its seed arrow type AND its cc->seed transform
# (see `_column_array`). Declared explicitly per column so the FeedSpec author chooses the transform
# deliberately — NOT inferred from the arrow type. That inference would be a fan-out trap: a future
# non-timestamp int64 field (e.g. liquidations `oid`/`tid` BIGINT) would be silently datetime-converted.
_ARROW_BY_KIND: dict[str, pa.DataType] = {
    "string": pa.string(),
    "epoch_ms": pa.int64(),  # cc timestamptz -> floor-to-ms epoch int64
    "float": pa.float64(),  # SQL NULL -> NaN value (default; overridable per column via on_null)
    "native_symbol": pa.string(),  # ccxt symbol -> exchange-native (BTC/USDT:USDT -> BTCUSDT)
    "int": pa.int64(),  # BIGINT identity pass-through (e.g. liquidations oid/tid) — NOT epoch-converted
}

# The SQL-NULL policy is a per-column property (`Column.on_null`), because the byte-contract's missing-value
# representation VARIES by column and seed: hl_asset_ctxs/binance_oi floats use NaN, but the liquidations seed
# writes "" for a blank dir/method, 0.0 for a blank closed_pnl, and has NO default for its required
# side/px/sz/mark_px/oid/tid (a NULL there is a row the seed never contains). `on_null` is a fill value, RAISE,
# or _UNSET -> the kind default below. RAISE is the strict default for every non-float kind: a NULL in a
# seed-required column must fail closed, never mint a NaN/parquet-null the extractor never emitted.
RAISE = object()  # on_null sentinel: a SQL NULL in this column is a contract violation -> raise
_UNSET = object()  # on_null unset -> use the kind default
_KIND_NULL_DEFAULT: dict[str, object] = {
    "float": nan,  # missing float -> NaN value (hl_asset_ctxs/binance_oi seed convention)
    "string": RAISE,
    "native_symbol": RAISE,
    "epoch_ms": RAISE,  # cc timestamp is NOT NULL
    "int": RAISE,  # a dedup-key int (oid/tid) going NULL is a contract violation
}


@dataclass(frozen=True)
class Column:
    """One seed column, its cc source, its transform kind, and its SQL-NULL policy (pure data)."""

    seed: str  # parquet/seed column name
    cc: str  # cc DB column to SELECT
    kind: str  # ∈ _ARROW_BY_KIND — drives BOTH the arrow type and the cc->seed transform
    on_null: object = _UNSET  # SQL NULL -> this fill value, or RAISE; _UNSET -> _KIND_NULL_DEFAULT[kind]

    @property
    def arrow(self) -> pa.DataType:
        return _ARROW_BY_KIND[self.kind]


@dataclass(frozen=True)
class FeedSpec:
    """A feed's byte-contract: seed schema + cc->seed map + optional venue filter (pure data,
    mirrors collector/ws.py WSFeedSpec). `columns` is in SEED ORDER — a parity convention (the backtester
    hash name-sorts, so order is not pinned; kept for human-diffability). `venue_col`/`venue` filter a
    multi-venue table to one venue; both None for a single-venue table (hl_asset_ctxs is HL-only)."""

    db_table: str
    ts_col: str  # cc timestamp column: range filter + ->time_ms source
    columns: tuple[Column, ...]  # seed order
    venue_col: str | None = None
    venue: str | None = None
    symbol_like: str | None = None  # SQL LIKE on `symbol` to restrict to the seed universe (e.g.
    #                                 binance OI seed is USDT-perps only, cc also holds USDC/coin-margined)


# hl_asset_ctxs -> the backtester's hl-asset-ctxs.parquet seed. Column names/types verified 2026-07-06
# against the real 240,336,969-row seed. asset_id->coin (HL-native == seed coin, T-008, no transform);
# timestamp->time_ms (cc poll wall-clock, floor-to-ms epoch int64); symbol dropped (not in the seed).
# The 10 context floats map same-named; SQL NULL -> NaN value at array build.
_HL_ASSET_CTXS = FeedSpec(
    db_table="hl_asset_ctxs",
    ts_col="timestamp",
    columns=(
        Column("coin", "asset_id", "string"),
        Column("time_ms", "timestamp", "epoch_ms"),
        Column("funding", "funding", "float"),
        Column("open_interest", "open_interest", "float"),
        Column("prev_day_px", "prev_day_px", "float"),
        Column("day_ntl_vlm", "day_ntl_vlm", "float"),
        Column("premium", "premium", "float"),
        Column("oracle_px", "oracle_px", "float"),
        Column("mark_px", "mark_px", "float"),
        Column("mid_px", "mid_px", "float"),
        Column("impact_bid_px", "impact_bid_px", "float"),
        Column("impact_ask_px", "impact_ask_px", "float"),
    ),
)

# open_interest (binance USDT-perps) -> the backtester's binance-oi-metrics.parquet seed. Seed coin is the
# Binance-**native** symbol (`BTCUSDT`, `1000PEPEUSDT` — the `1000×` prefix, not HL's `k`), verified
# all-USDT across the seed's 694 coins, so cc's ccxt `symbol` is transformed to native (strip settle +
# slash). Filter: exchange='binance' AND USDT-perp symbols only — cc also collects USDC/coin-margined OI
# which the seed excludes, so `symbol_like` restricts to the seed universe. timestamp -> create_time_ms
# (epoch-ms); open_interest -> sum_open_interest; oi_value_usd -> sum_open_interest_value.
_BINANCE_OI = FeedSpec(
    db_table="open_interest",
    ts_col="timestamp",
    columns=(
        Column("coin", "symbol", "native_symbol"),
        Column("create_time_ms", "timestamp", "epoch_ms"),
        Column("sum_open_interest", "open_interest", "float"),
        Column("sum_open_interest_value", "oi_value_usd", "float"),
    ),
    venue_col="exchange",
    venue="binance",
    symbol_like="%/USDT:USDT",
)

# liquidations (HL node_fills, venue='hyperliquid') -> the backtester's hl-liquidations.parquet seed. cc's writer
# (collector/liquidations_hl.py) is DERIVED from the seed extractor (fetch_hl_node_fills_liquidations.py) but
# stores SQL NULL where the seed writes per-column defaults, so the export owns parity via on_null: the seed
# writes dir/method="" (:146,151) and closed_pnl=0.0 (:150) for a blank, and has NO default for the required
# side/px/sz/mark_px/oid/tid -> those RAISE (a cc NULL there is a row the seed never contains; side/px/sz/mark_px
# are nullable in cc, a fail-closed guard gated by a prod NULL-count pre-check). oid/tid are the `int` kind
# (BIGINT identity, RAISE default). asset_id is the HL-native coin verbatim (a passthrough, :170-174). Only
# venue='hyperliquid' (the Grade-A tape) is exported; the Grade-C WS rows have no seed. Seed schema verified
# against fetch_hl_node_fills_liquidations.py:70-85 (13 cols, this order).
_HL_LIQUIDATIONS = FeedSpec(
    db_table="liquidations",
    ts_col="timestamp",
    columns=(
        Column("time_ms", "timestamp", "epoch_ms"),
        Column("coin", "asset_id", "string"),
        Column("side", "side", "string"),
        Column("dir", "dir", "string", on_null=""),
        Column("px", "px", "float", on_null=RAISE),
        Column("sz", "sz", "float", on_null=RAISE),
        Column("mark_px", "mark_px", "float", on_null=RAISE),
        Column("closed_pnl", "closed_pnl", "float", on_null=0.0),
        Column("method", "method", "string", on_null=""),
        Column("liquidated_user", "liquidated_user", "string"),
        Column("oid", "oid", "int"),
        Column("tid", "tid", "int"),
        Column("day", "day", "string"),
    ),
    venue_col="venue",
    venue="hyperliquid",
)

FEED_SPECS: dict[str, FeedSpec] = {
    "hl_asset_ctxs": _HL_ASSET_CTXS,
    "binance_oi": _BINANCE_OI,
    "hl_liquidations": _HL_LIQUIDATIONS,
}


def feed_schema(table: str) -> pa.Schema:
    """The seed parquet schema (names/types) for `table` — the single source of the contract."""
    return pa.schema([(c.seed, c.arrow) for c in FEED_SPECS[table].columns])


def select_columns(table: str) -> list[str]:
    """The cc DB columns to SELECT for `table` (cc names, not seed names), in seed order."""
    return [c.cc for c in FEED_SPECS[table].columns]


def _to_ms(dt: datetime) -> int:
    """Epoch-ms (floor) of a tz-aware datetime, from integer components — avoids the sub-ms float
    drift of `int(dt.timestamp() * 1000)` at epoch-scale magnitudes."""
    return int(dt.timestamp()) * 1000 + dt.microsecond // 1000


def _seed_value(col: Column, v: object) -> object:
    """One cc value -> its seed representation for `col`. Resolves the SQL-NULL policy FIRST (a fill value /
    RAISE / the kind default), THEN applies the non-null transform. The single source of the null policy;
    dispatch is on `col.kind` (explicit, NOT inferred from the arrow type — int64 shares its type with
    epoch_ms). NULL handling uses `v is None` (never `v or default`, which would corrupt a legitimate
    0.0/"")."""
    if v is None:
        policy = col.on_null if col.on_null is not _UNSET else _KIND_NULL_DEFAULT[col.kind]
        if policy is RAISE:
            raise ValueError(
                f"SQL NULL for seed column {col.seed!r} (cc {col.cc!r}) has no seed default "
                f"— the writer must not land a NULL here"
            )
        return policy  # a fill value: nan (float) / "" (blank string) / 0.0 (closed_pnl)
    if col.kind == "epoch_ms":
        return _to_ms(v)
    if col.kind == "native_symbol":
        # ccxt symbol -> exchange-native: BTC/USDT:USDT -> BTCUSDT (drop the :settle, then the /).
        return v.split(":")[0].replace("/", "")
    return v  # string / float / int pass the value through


def _column_array(col: Column, rows: list[dict]) -> pa.Array:
    """Build one seed column's arrow array from the dict rows via `_seed_value`. Direct-key access
    `row[col.cc]` is fail-closed (a missing key raises, surfacing a spec/SELECT drift, vs `.get()`
    silently None-filling). An unregistered `kind` raises here (not a silent mis-convert) — the kind
    is explicit, never inferred."""
    if col.kind not in _ARROW_BY_KIND:
        raise ValueError(f"unknown column kind {col.kind!r} for seed column {col.seed!r}")
    return pa.array([_seed_value(col, row[col.cc]) for row in rows], type=col.arrow)


def rows_to_parquet(table: str, rows: list[dict]) -> bytes:
    """Serialize cc dict rows to seed-schema parquet bytes. TOTAL — empty `rows` yields a valid 0-row
    parquet carrying the schema (a pure-function property; the endpoint returns 204 on empty rather
    than shipping one, since the backtester hash rejects an empty feed)."""
    spec = FEED_SPECS[table]
    arrays = [_column_array(col, rows) for col in spec.columns]
    buf = BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=feed_schema(table)), buf)
    return buf.getvalue()
