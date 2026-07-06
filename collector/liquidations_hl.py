"""HyperLiquid Grade-A liquidation daily pull (T-009.3).

Extracts forced-liquidation EVENTS from HL's `node_fills` tape (`s3://hl-mainnet-node-data`,
requester-pays) into the `liquidations` table (`venue='hyperliquid'`, `grade='a'`). A one-shot daily
batch (`python -m liquidations_hl`, run by cron) — NOT a live feed: node_fills is an hourly lz4 tape,
so a daily batch is the correct cadence. cc captures FORWARD from the backtester seed's 2026-06-27 end
(the seed holds the deep history); the default pulls the last few complete days, idempotently.

Ports the backtester's proven extractor (`fetch_hl_node_fills_liquidations.py`, 5.99M events):
- Two on-disk formats across the 2025-07-27 cutover: legacy `node_fills/hourly` (line = `[addr, fill]`)
  and by-block `node_fills_by_block/hourly` (line = `{"events": [[addr, fill], ...]}`).
- Streaming lz4 decompress (1 MiB chunks + rolling-buffer line split) so a peak cascade hour never lands
  whole in memory (raw decompresses ~4.8x).
- The **dedup-at-source** rule: a liquidation fill carries `fill["liquidation"] = {"liquidatedUser",
  "markPx", "method"}`, present on BOTH the liquidated party's fill and the counterparty's fill with the
  same `liquidatedUser` — so a discrete event is emitted ONCE, from the fill whose owning
  `addr == liquidation["liquidatedUser"]` (the liquidated side).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from datetime import timezone

import boto3
import lz4.frame
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from db import build_dsn, connect_db, safe_rollback

log = logging.getLogger(__name__)

BUCKET = "hl-mainnet-node-data"
LEGACY_PREFIX = "node_fills/hourly"
BLOCK_PREFIX = "node_fills_by_block/hourly"
CUTOVER = "20250727"  # by-block owns the overlap day — exactly one source per day (no double-count)
LOOKBACK_DAYS = 3  # cron default: the last N complete UTC days (heals ≤N missed runs, idempotent)
BATCH_SIZE = 5000  # rows per insert flush — bounds emitted-row memory on a cascade hour

# `dir` is the authoritative source of the liquidated side; `side` (A=sell closes a long, B=buy closes a
# short) is a defensive fallback whose use is logged as a node_fills shape anomaly.
_SIDE_FALLBACK = {"A": "long", "B": "short"}

INSERT_HL_LIQ_SQL = """
    INSERT INTO liquidations (
        timestamp, venue, grade, asset_id, liquidated_side, side, dir, px, sz,
        mark_px, closed_pnl, method, liquidated_user, oid, tid, day
    ) VALUES (
        to_timestamp(%(ts)s), 'hyperliquid', 'a', %(asset_id)s, %(liquidated_side)s, %(side)s, %(dir)s,
        %(px)s, %(sz)s, %(mark_px)s, %(closed_pnl)s, %(method)s, %(liquidated_user)s, %(oid)s, %(tid)s,
        %(day)s
    )
    ON CONFLICT (venue, asset_id, timestamp, oid, tid) DO NOTHING
"""


def _s3():
    # Adaptive retries — requester-pays throttling under repeated GET.
    return boto3.client("s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))


def _prefix_for(day: str) -> str:
    return BLOCK_PREFIX if day >= CUTOVER else LEGACY_PREFIX


def _daterange(start: str, end: str):
    d = dt.datetime.strptime(start, "%Y%m%d").date()
    last = dt.datetime.strptime(end, "%Y%m%d").date()
    while d <= last:
        yield d.strftime("%Y%m%d")
        d += dt.timedelta(days=1)


def default_days(now: dt.datetime, lookback: int = LOOKBACK_DAYS) -> tuple[str, str]:
    """The last `lookback` COMPLETE UTC days `[yesterday-lookback+1 .. yesterday]`. `now` (UTC) is
    injected so the boundary is deterministic + testable. 'Today' is excluded — it is still being
    written to the tape."""
    yesterday = now.date() - dt.timedelta(days=1)
    start = yesterday - dt.timedelta(days=lookback - 1)
    return start.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")


def list_keys(s3, day: str) -> list[str]:
    """The hourly `.lz4` object keys for one day (paginated)."""
    prefix = f"{_prefix_for(day)}/{day}/"
    keys: list[str] = []
    token = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, RequestPayer="requester")
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".lz4")]
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            return keys


def _iter_fills(line: bytes):
    """Yield `(address, fill)` from a tape line in either on-disk format. A by-block dict MUST carry an
    `events` list — a dict without one is format drift and raises (the caller counts it as a bad line),
    NOT a silent zero-row drop that would lose a whole block with no anomaly signal."""
    rec = json.loads(line)
    if isinstance(rec, dict):  # by-block
        events = rec.get("events")
        if not isinstance(events, list):
            raise ValueError("by-block record missing an 'events' list")
        for ev in events:
            yield ev[0], ev[1]
    else:  # legacy
        yield rec[0], rec[1]


def _num(x):
    """Magnitude field → float or None (never raises; a NULL px/sz still lands a row)."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _int_or_none(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _liquidated_side(dir_: str, side: str) -> str | None:
    """Normalized `long`/`short` from `dir` (authoritative); `side` is a fallback (anomaly-counted by
    the caller). Pure — no logging in this per-row helper."""
    if dir_ == "Close Long":
        return "long"
    if dir_ == "Close Short":
        return "short"
    return _SIDE_FALLBACK.get(side)


def _row(addr: str, fill: dict, day: str, stats: dict | None = None) -> dict | None:
    """A cc-shaped row from the LIQUIDATED-side fill, or None if this fill is not the liquidated side
    (expected — the counterparty carries the same liquidation) or a required identity field is missing.
    `oid`/`tid` are DROP-not-default: they are the dedup key, so a missing one is dropped as malformed
    (never `0`, which would collide two events). Anomalies increment `stats` (summarized once per object,
    not logged per row); a throwaway is used when None."""
    if stats is None:
        stats = _new_stats()
    liq = fill.get("liquidation")
    if not isinstance(liq, dict) or liq.get("liquidatedUser") != addr:
        return None  # counterparty side, or not a liquidation — expected, not an anomaly
    ts_ms = _int_or_none(fill.get("time"))
    coin = fill.get("coin")
    oid = _int_or_none(fill.get("oid"))
    tid = _int_or_none(fill.get("tid"))
    if ts_ms is None or not coin or oid is None or tid is None:
        stats["dropped"] += 1
        return None
    dir_ = fill.get("dir", "")
    side = fill.get("side", "")
    ls = _liquidated_side(dir_, side)
    if ls is not None and dir_ not in ("Close Long", "Close Short"):
        stats["side_fallback"] += 1  # dir was unexpected; liquidated_side came from the side fallback
    return {
        "ts": ts_ms / 1000.0,  # epoch SECONDS for to_timestamp — NOT ms
        # asset_id is the HL-NATIVE coin (`kPEPE`/`kSHIB`), stored verbatim — NOT canonicalized to the
        # ccxt-uppercase form. `liquidations` deliberately mirrors the seed's `coin` (SPEC:240/247, same
        # convention as `hl_asset_ctxs`), so the T-011 export is a passthrough of hl-liquidations.parquet;
        # canonicalizing here would break that byte-contract. (`open_interest` uses the canonical form —
        # a documented per-table divergence, SPEC:441. Nothing joins liquidations cross-venue on asset_id.)
        "asset_id": coin,
        "liquidated_side": ls,
        "side": fill.get("side"),
        "dir": dir_ or None,
        "px": _num(fill.get("px")),
        "sz": _num(fill.get("sz")),
        "mark_px": _num(liq.get("markPx")),
        "closed_pnl": _num(fill.get("closedPnl")),
        "method": liq.get("method") or None,
        "liquidated_user": addr,
        "oid": oid,
        "tid": tid,
        "day": day,
    }


def _emit_line(line: bytes, day: str, stats: dict):
    """Parse ONE tape line at the boundary (parse-don't-validate) and yield its liquidated-side rows.
    A malformed line (bad JSON / unexpected shape) is a classified skip — counted in `stats`, never a
    generic exception that would fail the whole object."""
    try:
        fills = list(_iter_fills(line))
    except (ValueError, IndexError, KeyError, TypeError):  # ValueError covers JSONDecodeError + drift
        stats["bad_lines"] += 1
        return
    for addr, fill in fills:
        # the fill must be a dict — a malformed payload (e.g. a string where a fill object is expected)
        # is a counted skip at the boundary, not an AttributeError that fails the whole object.
        if not isinstance(fill, dict):
            stats["bad_lines"] += 1
            continue
        row = _row(addr, fill, day, stats)
        if row is not None:
            yield row


def extract_rows(body, day: str, stats: dict | None = None):
    """Stream-decompress one hourly object body, yielding cc-shaped liquidated-side rows. The rolling
    buffer keeps at most one partial line resident, so a peak hour's raw fills never land whole. `stats`
    (a mutable counter dict) accrues per-object anomalies; a throwaway is used when None."""
    if stats is None:
        stats = _new_stats()
    dctx = lz4.frame.LZ4FrameDecompressor()
    buf = b""
    for chunk in body.iter_chunks(chunk_size=1 << 20):
        buf += dctx.decompress(chunk)
        start = 0
        nl = buf.find(b"\n")
        while nl != -1:
            if nl > start:
                yield from _emit_line(buf[start:nl], day, stats)
            start = nl + 1
            nl = buf.find(b"\n", start)
        buf = buf[start:]
    if buf.strip():
        yield from _emit_line(buf, day, stats)


def _new_stats() -> dict:
    return {"dropped": 0, "side_fallback": 0, "bad_lines": 0}


def _stream_insert(s3, conn, key: str, day: str) -> tuple[int, int]:
    """GET + stream-extract one hourly object, flushing inserts in `BATCH_SIZE` chunks. Returns
    `(inserted, seen)`. Raises on a DB error (caller reconnects/retries)."""
    body = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")["Body"]
    stats = _new_stats()
    inserted = seen = 0
    batch: list[dict] = []

    def flush():
        nonlocal inserted
        if not batch:
            return
        with conn.cursor() as cur:
            cur.executemany(INSERT_HL_LIQ_SQL, batch)
            inserted += cur.rowcount  # psycopg3 sums affected rows across the batch
        conn.commit()
        batch.clear()

    for row in extract_rows(body, day, stats):
        seen += 1
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            flush()
    flush()
    if any(stats.values()):
        log.info("anomalies key=%s dropped=%d side_fallback=%d bad_lines=%d",
                 key, stats["dropped"], stats["side_fallback"], stats["bad_lines"])
    return inserted, seen


class _DBUnavailable(Exception):
    """The DB could not be reached (connect_db exhausted its retries) — a run-fatal condition, distinct
    from an object-level failure. A dedicated type so `run()` classifies a DB outage separately from a
    per-object `RuntimeError` (e.g. a corrupt lz4 frame), which must NOT be mislabelled as DB-down."""


def _connect():
    """`connect_db(build_dsn())`, converting its retry-exhaustion `RuntimeError` into `_DBUnavailable`
    so the run-fatal DB-down path is a distinct, catchable type (not conflated with a corrupt-object
    `RuntimeError` that should be a counted per-object failure)."""
    try:
        return connect_db(build_dsn())
    except RuntimeError as e:
        raise _DBUnavailable(str(e)) from e


def _process_key(s3, conn, key: str, day: str):
    """`_stream_insert` with a single reconnect-and-retry on a dropped connection (a one-shot must survive
    a mid-run db blip; a partial batch already committed is idempotent on retry). Retries ONLY on an actual
    connection loss (`conn.closed`) — a non-connection `OperationalError` (a server/query error) is
    re-raised for `run()` to roll back and count, not reconnected. Reconnect exhaustion raises
    `_DBUnavailable` (run-fatal). Returns `(inserted, seen, conn)`."""
    try:
        ins, seen = _stream_insert(s3, conn, key, day)
        return ins, seen, conn
    except psycopg.OperationalError:
        if not conn.closed:
            raise  # transient non-connection DB error → run() rolls back + counts (don't reconnect)
        log.warning("db connection lost on %s — reconnecting once and retrying", key)
        try:
            conn.close()  # release the broken handle before replacing it
        except Exception:  # noqa: BLE001 — closing an already-broken conn must never mask the retry
            pass
        conn = _connect()  # exhaustion → _DBUnavailable → run()'s run-fatal handler
        ins, seen = _stream_insert(s3, conn, key, day)  # a second failure propagates → run() counts it
        return ins, seen, conn


def _recover_conn(conn):
    """After an object-level DB failure, return a usable connection for the next key: a fresh one if the
    connection dropped, else the same one with its aborted transaction rolled back."""
    if conn.closed:
        return _connect()
    safe_rollback(conn)
    return conn


def run(start: str, end: str) -> int:
    """Pull `[start, end]` (inclusive, YYYYMMDD) into `liquidations`. Returns a process exit code: 0 on
    a clean run, 1 if any object failed (surfaced, not silently swallowed — the idempotent lookback
    re-pulls a failed day on the next run)."""
    s3 = _s3()
    conn = None
    total_ins = total_seen = failures = 0
    try:
        conn = _connect()
        for day in _daterange(start, end):
            try:
                keys = list_keys(s3, day)
            except (BotoCoreError, ClientError) as e:
                # A day-level S3 listing failure is an object-level (per-day) failure — count it and move
                # to the next day, don't abort the whole run + remaining lookback days.
                log.error("FAIL listing day %s: %s", day, e)
                failures += 1
                continue
            if len(keys) < 24:
                log.warning("day %s lists %d hourly objects (<24) — may be upstream-partial", day, len(keys))
            day_ins = day_seen = 0
            for key in keys:
                try:
                    ins, seen, conn = _process_key(s3, conn, key, day)
                    day_ins += ins
                    day_seen += seen
                except (psycopg.Error, BotoCoreError, ClientError, RuntimeError) as e:
                    # Expected object-level failures — isolate the bad object, DON'T abort the run:
                    # DB errors (a non-connection error leaves the txn ABORTED → recover the conn before
                    # the next key), S3 GETs, and a corrupt lz4 frame (`RuntimeError`). A run-fatal DB
                    # outage is `_DBUnavailable` (not caught here → handled below); a genuine logic bug
                    # (TypeError/KeyError/…) propagates → aborts loudly.
                    log.error("FAIL %s: %s", key, e)
                    failures += 1
                    conn = _recover_conn(conn)
            log.info("day %s: %d inserted / %d events / %d objects", day, day_ins, day_seen, len(keys))
            total_ins += day_ins
            total_seen += day_seen
    except _DBUnavailable as e:
        # connect_db exhausted its retries (initial connect or a mid-run reconnect) → the DB is down,
        # a run-fatal condition. Exit non-zero cleanly; the next cron run's lookback re-pulls.
        log.error("DB unreachable — aborting run %s..%s: %s", start, end, e)
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 — a close failure must not mask the run's real exit code
                pass
    log.info("DONE %s..%s: %d inserted / %d seen / %d object failures", start, end, total_ins, total_seen, failures)
    return 1 if failures else 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="HL node_fills Grade-A daily liquidation pull")
    ap.add_argument("--start", help="first day YYYYMMDD (backfill); default = last 3 complete UTC days")
    ap.add_argument("--end", help="last day YYYYMMDD inclusive")
    args = ap.parse_args()
    if args.start and args.end:
        start, end = args.start, args.end
    elif args.start or args.end:
        ap.error("--start and --end must be given together")
    else:
        start, end = default_days(dt.datetime.now(timezone.utc))
    return run(start, end)


if __name__ == "__main__":
    raise SystemExit(main())
