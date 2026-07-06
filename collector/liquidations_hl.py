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

from db import build_dsn, connect_db

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
    """Yield `(address, fill)` from a tape line in either on-disk format."""
    rec = json.loads(line)
    if isinstance(rec, dict):  # by-block
        for ev in rec.get("events", []):
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
    """Normalized `long`/`short` from `dir` (authoritative); `side` is a logged-anomaly fallback."""
    if dir_ == "Close Long":
        return "long"
    if dir_ == "Close Short":
        return "short"
    fb = _SIDE_FALLBACK.get(side)
    if fb is not None:
        log.warning("liquidated_side fell back to side=%r (dir=%r) — unexpected node_fills shape", side, dir_)
    return fb


def _row(addr: str, fill: dict, day: str) -> dict | None:
    """A cc-shaped row from the LIQUIDATED-side fill, or None if this fill is not the liquidated side
    or a required identity field is missing. `oid`/`tid` are DROP-not-default: they are the dedup key,
    so a missing one is dropped as malformed (never `0`, which would collide two events)."""
    liq = fill.get("liquidation")
    if not isinstance(liq, dict) or liq.get("liquidatedUser") != addr:
        return None  # counterparty side, or not a liquidation
    ts_ms = _int_or_none(fill.get("time"))
    coin = fill.get("coin")
    oid = _int_or_none(fill.get("oid"))
    tid = _int_or_none(fill.get("tid"))
    if ts_ms is None or not coin or oid is None or tid is None:
        log.warning("dropping malformed liquidation fill: coin=%r oid=%r tid=%r time=%r",
                    coin, fill.get("oid"), fill.get("tid"), fill.get("time"))
        return None
    dir_ = fill.get("dir", "")
    return {
        "ts": ts_ms / 1000.0,  # epoch SECONDS for to_timestamp — NOT ms
        "asset_id": coin,
        "liquidated_side": _liquidated_side(dir_, fill.get("side", "")),
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


def extract_rows(body, day: str):
    """Stream-decompress one hourly object body, yielding cc-shaped liquidated-side rows. The rolling
    buffer keeps at most one partial line resident, so a peak hour's raw fills never land whole."""
    dctx = lz4.frame.LZ4FrameDecompressor()
    buf = b""
    for chunk in body.iter_chunks(chunk_size=1 << 20):
        buf += dctx.decompress(chunk)
        start = 0
        nl = buf.find(b"\n")
        while nl != -1:
            if nl > start:
                for addr, fill in _iter_fills(buf[start:nl]):
                    row = _row(addr, fill, day)
                    if row is not None:
                        yield row
            start = nl + 1
            nl = buf.find(b"\n", start)
        buf = buf[start:]
    if buf.strip():
        for addr, fill in _iter_fills(buf):
            row = _row(addr, fill, day)
            if row is not None:
                yield row


def _stream_insert(s3, conn, key: str, day: str) -> tuple[int, int]:
    """GET + stream-extract one hourly object, flushing inserts in `BATCH_SIZE` chunks. Returns
    `(inserted, seen)`. Raises on a DB error (caller reconnects/retries)."""
    body = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")["Body"]
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

    for row in extract_rows(body, day):
        seen += 1
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            flush()
    flush()
    return inserted, seen


def _process_key(s3, conn, key: str, day: str):
    """`_stream_insert` with a single DB-reconnect-and-retry on a dropped connection (a one-shot must
    survive a mid-run db blip; a partial batch already committed is idempotent on retry). Returns
    `(inserted, seen, conn)` — conn may be a fresh handle after a reconnect."""
    try:
        ins, seen = _stream_insert(s3, conn, key, day)
        return ins, seen, conn
    except psycopg.OperationalError as e:
        log.warning("db connection lost on %s — reconnecting once and retrying: %s", key, e)
        conn = connect_db(build_dsn())
        ins, seen = _stream_insert(s3, conn, key, day)  # a second failure propagates → run() fails non-zero
        return ins, seen, conn


def run(start: str, end: str) -> int:
    """Pull `[start, end]` (inclusive, YYYYMMDD) into `liquidations`. Returns a process exit code: 0 on
    a clean run, 1 if any object failed (surfaced, not silently swallowed — the idempotent lookback
    re-pulls a failed day on the next run)."""
    s3 = _s3()
    conn = connect_db(build_dsn())
    total_ins = total_seen = failures = 0
    try:
        for day in _daterange(start, end):
            keys = list_keys(s3, day)
            if len(keys) < 24:
                log.warning("day %s lists %d hourly objects (<24) — may be upstream-partial", day, len(keys))
            day_ins = day_seen = 0
            for key in keys:
                try:
                    ins, seen, conn = _process_key(s3, conn, key, day)
                    day_ins += ins
                    day_seen += seen
                except Exception as e:  # noqa: BLE001 — one bad object must not abort the run; counted + surfaced
                    log.error("FAIL %s: %s", key, e)
                    failures += 1
            log.info("day %s: %d inserted / %d events / %d objects", day, day_ins, day_seen, len(keys))
            total_ins += day_ins
            total_seen += day_seen
    finally:
        conn.close()
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
