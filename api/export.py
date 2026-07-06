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
    "float": pa.float64(),  # SQL NULL -> NaN value
}


@dataclass(frozen=True)
class Column:
    """One seed column, its cc source, and its transform kind (pure data)."""

    seed: str  # parquet/seed column name
    cc: str  # cc DB column to SELECT
    kind: str  # ∈ _ARROW_BY_KIND — drives BOTH the arrow type and the cc->seed transform

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

FEED_SPECS: dict[str, FeedSpec] = {
    "hl_asset_ctxs": _HL_ASSET_CTXS,
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


def _column_array(col: Column, rows: list[dict]) -> pa.Array:
    """Build one seed column's arrow array from the dict rows, dispatching on `col.kind` (explicit,
    NOT inferred from the arrow type). Direct-key access `row[col.cc]` (fail-closed: a missing key
    raises, surfacing a spec/SELECT drift, vs `.get()` silently None-filling). Floats: NULL -> NaN
    *value* (null_count 0, matching the seed), via `nan if v is None else v` — never `v or nan`, which
    would corrupt a legitimate 0.0."""
    if col.kind == "string":
        return pa.array([row[col.cc] for row in rows], type=col.arrow)
    if col.kind == "epoch_ms":
        return pa.array([_to_ms(row[col.cc]) for row in rows], type=col.arrow)
    if col.kind == "float":
        return pa.array([nan if row[col.cc] is None else row[col.cc] for row in rows], type=col.arrow)
    raise ValueError(f"unknown column kind {col.kind!r} for seed column {col.seed!r}")


def rows_to_parquet(table: str, rows: list[dict]) -> bytes:
    """Serialize cc dict rows to seed-schema parquet bytes. TOTAL — empty `rows` yields a valid 0-row
    parquet carrying the schema (a pure-function property; the endpoint returns 204 on empty rather
    than shipping one, since the backtester hash rejects an empty feed)."""
    spec = FEED_SPECS[table]
    arrays = [_column_array(col, rows) for col in spec.columns]
    buf = BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=feed_schema(table)), buf)
    return buf.getvalue()
