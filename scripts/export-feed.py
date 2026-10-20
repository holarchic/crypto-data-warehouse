#!/usr/bin/env python3
"""Export a span of one /export/parquet feed, larger than one request may return, as a single parquet.

Walks [start, end) in windows over the API's /export/parquet endpoint and streams each window into one
file, so a catch-up span of any size holds one window in memory. The API owns the seed contract; this
client checks only that every window has the first window's schema.

Usage:
    # 1. Reach the API, e.g. an SSH tunnel: ssh -L 8000:localhost:8000 <server>
    # 2. python3 scripts/export-feed.py --api http://localhost:8000 --table hl_asset_ctxs \
    #        --start 2026-07-01T00:00:00Z --end 2026-10-01T00:00:00Z --out hl-asset-ctxs-forward.parquet

- 413 (window over the row cap): the window halves, and the smaller step is kept for the rest of the run.
- 204 (no rows): the window is skipped.
- Any other non-200 status, a timeout, or a schema change aborts the run.
- The file is written as <out>.partial and renamed to <out> only after the last window, so an aborted
  run leaves nothing that looks complete. A span with no rows writes nothing and exits 1, since the
  backtester's hash rejects an empty feed.
- Windows are separate requests, not one database snapshot: a row backfilled into a window after it
  was read is missing, so re-export a span after a known backfill.

Requires: pyarrow
"""

import argparse
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import pyarrow.parquet as pq

MIN_STEP = timedelta(minutes=1)


class ExportError(Exception):
    """The span cannot be exported as one complete file."""


def fetch_window(api: str, table: str, start: datetime, end: datetime, asset_id: str | None,
                 timeout: float) -> tuple[int, bytes]:
    """One /export/parquet request: (HTTP status, body). Timeouts and connection errors propagate."""
    query = {"table": table, "start": start.isoformat(), "end": end.isoformat()}
    if asset_id is not None:
        query["asset_id"] = asset_id
    url = f"{api.rstrip('/')}/export/parquet?{urllib.parse.urlencode(query)}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, exc.read()


def export_span(api: str, table: str, start: datetime, end: datetime, out: Path, *,
                asset_id: str | None = None, step: timedelta = timedelta(hours=24), timeout: float = 300,
                log=print) -> int:
    """Write `table`'s rows in [start, end) to `out`; returns the row count. Raises ExportError (or a
    network error) with no `out` written and no partial file left."""
    partial = out.with_name(out.name + ".partial")
    writer = None
    total = 0
    try:
        t = start
        while t < end:
            w_end = min(t + step, end)
            status, body = fetch_window(api, table, t, w_end, asset_id, timeout)
            if status == 413:
                step /= 2
                log(f"{t.isoformat()} → {w_end.isoformat()}  413, step now {step}")
                if step < MIN_STEP:
                    raise ExportError(f"a {MIN_STEP} window still exceeds the row cap at {t.isoformat()}")
                continue
            if status == 204:
                log(f"{t.isoformat()} → {w_end.isoformat()}  204")
                t = w_end
                continue
            if status != 200:
                raise ExportError(f"{t.isoformat()} → {w_end.isoformat()}: HTTP {status} {body[:300]!r}")
            window = pq.read_table(BytesIO(body))
            if writer is None:
                writer = pq.ParquetWriter(partial, window.schema, compression="zstd")
            elif not window.schema.equals(writer.schema):
                raise ExportError(f"schema changed at {t.isoformat()}: {window.schema} != {writer.schema}")
            writer.write_table(window)
            total += window.num_rows
            log(f"{t.isoformat()} → {w_end.isoformat()}  200 rows={window.num_rows}")
            t = w_end
    except BaseException:
        if writer is not None:
            writer.close()
        partial.unlink(missing_ok=True)
        raise
    if writer is None:
        raise ExportError(f"no rows in {start.isoformat()} → {end.isoformat()}; nothing written")
    writer.close()
    partial.replace(out)
    return total


def _utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} has no timezone; use e.g. 2026-07-01T00:00:00Z")
    return dt


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--api", required=True, help="API base URL, e.g. http://localhost:8000")
    ap.add_argument("--table", required=True, help="feed name, as /export/parquet's `table`")
    ap.add_argument("--start", required=True, type=_utc, help="inclusive, timezone-aware ISO time")
    ap.add_argument("--end", required=True, type=_utc, help="exclusive, timezone-aware ISO time")
    ap.add_argument("--asset-id", help="one asset only")
    ap.add_argument("--step-hours", type=float, default=24, help="initial window width (default 24)")
    ap.add_argument("--timeout", type=float, default=300, help="seconds per request (default 300)")
    ap.add_argument("--out", required=True, type=Path, help="output parquet path")
    args = ap.parse_args(argv)
    if args.start >= args.end:
        ap.error("--start must be before --end")
    try:
        rows = export_span(args.api, args.table, args.start, args.end, args.out, asset_id=args.asset_id,
                           step=timedelta(hours=args.step_hours), timeout=args.timeout)
    except (ExportError, OSError) as exc:
        print(f"export failed: {exc}", file=sys.stderr)
        return 1
    print(f"{rows} rows → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
