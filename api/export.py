"""Pure parquet-export contract module for the backtester's forward-ingest (T-011).

Owns the byte-contract between cc's landed tables and the backtester's frozen seed parquets. The
backtester does NOT convert on ingest — it reads the raw exported parquet directly and validates each
feed by a structural hash over schema(cols+types) + row content. The hash name-sorts the schema and
sums per-row digests commutatively (feed_provenance.rs), so column and row ORDER do not affect it —
what must match the seed is the column names + types, and a SQL NULL must become exactly what the seed
holds there (a NaN *value*, a seed default, or a refusal: per column, see `_KINDS`), never a parquet-null
(a parquet-null changes the column's null-mask, which the per-cell row digest sees). cc writes columns in
seed order anyway as a parity convention. Per feed, one FeedSpec holds the seed schema + the cc->seed
column map.

The module also builds each feed's SQL (`feed_query`), so the query and the serializer that consumes its
rows are one contract. Kept free of fastapi/psycopg so it unit-tests against REAL pyarrow (a stubbed lib
can't verify a byte-contract); the /export/parquet endpoint validates the request, runs `feed_query` and
calls `rows_to_parquet`.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from math import nan
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

# Max rows one /export/parquet request materializes. The endpoint buffers the whole path at once —
# the psycopg dict_row list + arrow arrays + parquet bytes + the response copy — inside the api
# container's 512 MB limit (docker-compose.prod.yml). Sized by a peak-RSS smoke run INSIDE the api
# image (Linux, pyarrow native included): 150k rows → 209.8 MB peak in the worker; ≈270 MB for the
# full serving process (+ uvicorn/fastapi/psycopg baseline), leaving >240 MB headroom under 512 MB.
# Larger spans use the memory-streaming bulk export script (T-011 fan-out).
EXPORT_MAX_ROWS = 150_000


# Each column `kind` -> (its seed arrow type, its DEFAULT SQL-NULL policy). One entry per kind — the single
# place a kind is defined. The kind is EXPLICIT per column, NEVER inferred from the arrow type: int64 is
# shared by `int` and `epoch_ms` (which datetime-converts), so inferring would be a fan-out trap (a BIGINT
# oid/tid silently datetime-converted). The null policy is a per-column property (`Column.on_null`) that
# OVERRIDES this default; the default is the common case per kind — `float` missing -> NaN value
# (hl_asset_ctxs/binance_oi seed convention), every other kind -> RAISE (a NULL in a seed-required column
# must fail closed, never mint a NaN/parquet-null the extractor never emitted). The non-identity transforms
# (epoch_ms floor, native_symbol strip) live in `_seed_value` — only 2 kinds transform, so a callable-per-kind
# would over-abstract.
RAISE = object()  # on_null sentinel: a SQL NULL in this column is a contract violation -> raise
_UNSET = object()  # on_null unset -> use the kind's default null policy


@dataclass(frozen=True)
class KindSpec:
    """A column kind's seed arrow type + its default SQL-NULL policy (pure data, named fields)."""

    arrow: pa.DataType
    default_on_null: object  # a fill value (nan) or RAISE


_KINDS = MappingProxyType({
    "string": KindSpec(pa.string(), RAISE),
    "epoch_ms": KindSpec(pa.int64(), RAISE),  # cc timestamptz -> floor-to-ms epoch int64; ts is NOT NULL
    "float": KindSpec(pa.float64(), nan),  # SQL NULL -> NaN value (overridable per column via on_null)
    "native_symbol": KindSpec(pa.string(), RAISE),  # ccxt symbol -> exchange-native (BTC/USDT:USDT -> BTCUSDT)
    "int": KindSpec(pa.int64(), RAISE),  # BIGINT identity pass-through (oid/tid) — NOT epoch-converted
})

# A resampled column's aggregate over its bucket's source rows (TimescaleDB first/last order by time).
# Each is wrapped so a NULL anywhere in the bucket makes the result NULL: max/min skip NULLs, and a
# skipped writer-bug NULL would otherwise export a plausible bar instead of reaching the column's RAISE.
_AGG_SQL = MappingProxyType({
    "first": "first({cc}, {ts})",
    "last": "last({cc}, {ts})",
    "max": "max({cc})",
    "min": "min({cc})",
})
_BUCKET_TIMES = ("bucket_open", "bucket_close")  # a resampled column holding its bucket's open/close time


@dataclass(frozen=True)
class Column:
    """One seed column, its cc source, its transform kind, and its SQL-NULL policy (pure data)."""

    seed: str  # parquet/seed column name
    cc: str  # cc DB column to SELECT (a resampled feed: the source column, or the bucket alias)
    kind: str  # ∈ _KINDS — drives BOTH the arrow type and the cc->seed transform
    on_null: object = _UNSET  # SQL NULL -> this fill value, or RAISE; _UNSET -> the kind's _KINDS default
    agg: str | None = None  # resampled feeds only: ∈ _AGG_SQL or _BUCKET_TIMES; None = a group key

    def __post_init__(self):
        # Validated once, when the literal spec is built: an unregistered kind would mis-convert silently
        # (the kind is explicit, never inferred), and an unknown agg would reach the SQL.
        if self.kind not in _KINDS:
            raise ValueError(f"unknown column kind {self.kind!r} for seed column {self.seed!r}")
        if self.agg is not None and self.agg not in _AGG_SQL and self.agg not in _BUCKET_TIMES:
            raise ValueError(f"unknown agg {self.agg!r} for seed column {self.seed!r}")

    @property
    def arrow(self) -> pa.DataType:
        return _KINDS[self.kind].arrow


@dataclass(frozen=True)
class Resample:
    """Resample source bars of `source_s` seconds into seed bars of `bucket_s` (pure data). A bucket is
    exported only when all bucket_s / source_s source bars are stored: a partial bucket's OHLC is wrong
    yet hash-valid. Both widths are formatted into SQL, so they must be ints from this literal spec."""

    bucket_s: int
    source_s: int

    def __post_init__(self):
        if not all(type(v) is int and v > 0 for v in (self.bucket_s, self.source_s)) or self.bucket_s % self.source_s:
            raise ValueError(f"Resample widths must be positive ints, bucket a multiple of source: {self}")


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
    resample: Resample | None = None  # None: one source row per seed row


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

# premium_index (binance 5m) -> the backtester's binance-premium-index.parquet seed, which holds Binance's own
# 1h premiumIndexKlines (fetch_binance_premium_index.py --interval 1h: k[0], k[1..4], k[6]). Resampled
# first/max/min/last per hour; close_time_ms = open + 3,599,999 as in every seed row. coin is native_symbol
# (`BTCUSDT`); the seed is all USDT. The seed has no NaN, so a NULL price raises.
_BINANCE_PREMIUM_INDEX = FeedSpec(
    db_table="premium_index",
    ts_col="timestamp",
    columns=(
        Column("coin", "native_symbol", "string"),
        Column("open_time_ms", "bucket_open", "epoch_ms", agg="bucket_open"),
        Column("premium_open", "premium_open", "float", on_null=RAISE, agg="first"),
        Column("premium_high", "premium_high", "float", on_null=RAISE, agg="max"),
        Column("premium_low", "premium_low", "float", on_null=RAISE, agg="min"),
        Column("premium_close", "premium_close", "float", on_null=RAISE, agg="last"),
        Column("close_time_ms", "bucket_close", "epoch_ms", agg="bucket_close"),
    ),
    venue_col="exchange",
    venue="binance",
    symbol_like="%/USDT:USDT",
    resample=Resample(bucket_s=3600, source_s=300),
)

FEED_SPECS = MappingProxyType({
    "hl_asset_ctxs": _HL_ASSET_CTXS,
    "binance_oi": _BINANCE_OI,
    "hl_liquidations": _HL_LIQUIDATIONS,
    "binance_premium_index": _BINANCE_PREMIUM_INDEX,
})
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def feed_schema(table: str) -> pa.Schema:
    """The seed parquet schema (names/types) for `table` — the single source of the contract."""
    return pa.schema([(c.seed, c.arrow) for c in FEED_SPECS[table].columns])


def feed_query(table: str, start: datetime, end: datetime, asset_id: str | None = None) -> tuple[str, dict]:
    """The SQL and params for `table`'s rows whose seed timestamp is in the half-open [start, end) (tz-aware),
    keyed by cc column name, at most EXPORT_MAX_ROWS + 1 rows so the caller can tell an over-cap window.
    Half-open so consecutive windows never double-count the seam row (the backtester's row hash is
    duplicate-sensitive). Table, columns and bucket widths come from the FEED_SPECS literal, never the
    caller, so they are formatted in; every value is a parameter."""
    spec = FEED_SPECS[table]
    params: dict = {"limit": EXPORT_MAX_ROWS + 1}
    filters = []
    if spec.venue_col is not None:
        filters.append(f"{spec.venue_col} = %(venue)s")
        params["venue"] = spec.venue
    if spec.symbol_like is not None:
        filters.append("symbol LIKE %(symbol_like)s")
        params["symbol_like"] = spec.symbol_like
    if asset_id is not None:
        filters.append("asset_id = %(asset_id)s")
        params["asset_id"] = asset_id
    if spec.resample is None:
        params.update(start=start, end=end)
        where = " AND ".join([f"{spec.ts_col} >= %(start)s", f"{spec.ts_col} < %(end)s", *filters])
        cols = ", ".join(c.cc for c in spec.columns)
        return f"SELECT {cols} FROM {spec.db_table} WHERE {where} ORDER BY {spec.ts_col} LIMIT %(limit)s", params
    return _resampled_query(spec, start, end, filters, params)


def _resampled_query(spec: FeedSpec, start: datetime, end: datetime, filters: list, params: dict) -> tuple[str, dict]:
    """A resampled feed's query. A bucket is in [start, end) iff its open time is, so the source rows are read
    over [ceil(start), ceil(end)) on bucket boundaries: any split point is seamless, never halving a bucket."""
    r = spec.resample
    params.update(start=_ceil_to(start, r.bucket_s), end=_ceil_to(end, r.bucket_s))
    ts = spec.ts_col
    bucket = f"time_bucket(INTERVAL '{r.bucket_s} seconds', {ts})"
    where = " AND ".join([f"{ts} >= %(start)s", f"{ts} < %(end)s", *filters])
    selects = ", ".join(f"{_resampled_expr(c, ts, bucket, r)} AS {c.cc}" for c in spec.columns)
    keys = ", ".join([c.cc for c in spec.columns if c.agg is None] + [bucket])
    sql = (
        f"SELECT {selects} FROM {spec.db_table} WHERE {where} GROUP BY {keys} "
        f"HAVING count(*) = {r.bucket_s // r.source_s} ORDER BY {bucket} LIMIT %(limit)s"
    )
    return sql, params


def _resampled_expr(col: Column, ts: str, bucket: str, r: Resample) -> str:
    if col.agg is None:
        return col.cc
    if col.agg == "bucket_open":
        return bucket
    if col.agg == "bucket_close":
        return f"{bucket} + INTERVAL '{r.bucket_s} seconds' - INTERVAL '1 millisecond'"
    return f"CASE WHEN count({col.cc}) = count(*) THEN {_AGG_SQL[col.agg].format(cc=col.cc, ts=ts)} END"


def _ceil_to(dt: datetime, width_s: int) -> datetime:
    """`dt` rounded up to a multiple of `width_s` seconds since the epoch (unchanged when already on one)."""
    rem = (dt - _EPOCH) % timedelta(seconds=width_s)
    return dt if not rem else dt - rem + timedelta(seconds=width_s)


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
        policy = col.on_null if col.on_null is not _UNSET else _KINDS[col.kind].default_on_null
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
    silently None-filling)."""
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
