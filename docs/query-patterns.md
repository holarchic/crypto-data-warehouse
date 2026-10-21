# Query patterns

How to read the warehouse for analysis and backtesting: what the stored data means, how to pull long histories through the API, and how to join feeds without lookahead. Examples assume the API on `http://localhost:8000`; the endpoint list is in the [README](../README.md#api).

## What a row means

- **Time:** every timestamp is UTC. A candle's `timestamp` is its open time; it covers `[timestamp, timestamp + interval)`.
- **Names:** `symbol` is the ccxt symbol (`BTC/USDT:USDT`) and `asset_id` the base asset (`BTC`). `asset_id` joins the same asset across exchanges; `native_symbol` is the venue's own name (`BTCUSDT`).
- **Closed bars only**, except HyperLiquid 1m. Its newest bar is still forming until the minute ends, and a daily job replaces recent bars with HyperLiquid's closed values. Drop the newest HyperLiquid 1m bar unless you want a live value.
- **A missing row is not always a gap:**
  - HyperLiquid 1m arrives over a WebSocket that only reports traded minutes, so an untraded minute has no row.
  - Coinbase's candle API omits intervals with no trades.
  - Elsewhere, a missing bar is a gap; `GET /status/gaps` lists recent ones.
- **Nulls:** `quote_volume` and `trades` are null where the venue doesn't report them.

## Timeframes

`1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `12h` are collected (`GET /candles/timeframes` lists them). `1m` lives in its own table, and the API routes to it.

`1d`, `3d` and `1w` are no longer collected. Their stored history is kept but frozen. Derive them from `4h`:

```sql
-- 1d from 4h, keeping only complete days (six 4h bars).
SELECT exchange, symbol, time_bucket('1 day', timestamp) AS day,
       first(open, timestamp) AS open, max(high) AS high, min(low) AS low, last(close, timestamp) AS close,
       sum(volume) AS volume
FROM candles
WHERE exchange = 'binance' AND asset_id = 'BTC' AND timeframe = '4h'
  AND timestamp >= '2026-07-01' AND timestamp < '2026-10-01'
GROUP BY 1, 2, 3
HAVING count(*) = 6
ORDER BY day;
```

`time_bucket('1 week', …)` starts weeks on Monday 00:00 UTC, as the venues' weekly candles do. A `3d` bar has no common anchor across venues, so state the origin you choose (`time_bucket('3 days', timestamp, origin => …)`).

## Pulling a long history through the API

`/candles`, `/funding`, `/oi` and `/liquidations` return the newest rows first, up to `limit` (at most 5,000), between `start` and `end` (both inclusive). For a long span, walk forward in fixed windows sized to the cap, so every request is complete and nothing is fetched twice:

```python
import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "http://localhost:8000"
STEP = timedelta(minutes=5)          # the timeframe's interval
CAP = 5000

def candles(exchange, asset_id, timeframe, start, end):
    """Every bar with open time in [start, end), oldest first."""
    rows = []
    window_start = start
    while window_start < end:
        window_end = min(window_start + (CAP - 1) * STEP, end - STEP)   # inclusive bound
        query = urllib.parse.urlencode({"exchange": exchange, "asset_id": asset_id, "timeframe": timeframe,
                                        "start": window_start.isoformat(), "end": window_end.isoformat(),
                                        "limit": CAP})
        with urllib.request.urlopen(f"{API}/candles?{query}") as resp:
            rows.extend(reversed(json.load(resp)))
        window_start = window_end + STEP
    return rows

bars = candles("binance", "BTC", "5m",
               datetime(2026, 7, 1, tzinfo=timezone.utc), datetime(2026, 10, 1, tzinfo=timezone.utc))
```

Check what came back before using it. The expected count is `(end - start) / interval`, less any untraded bars on the venues above, and a shortfall is a gap.

## Funding

- **`timestamp`** is the settlement time. **`rate`** is the rate paid at that settlement, as a fraction (`0.0001` = 0.01 %). **`predicted_rate`** is the venue's estimate for the next one, where it gives one.
- **`interval_hours`** is the interval the collector assumes for the venue: 1 for HyperLiquid, 8 elsewhere. It is not a per-symbol schedule. Before annualising, or before treating a symbol's rows as one per settlement, check the venue's schedule for that symbol: some venues settle some symbols more often.
- **In a backtest,** a funding payment is known at its settlement time. Apply it to positions open across that instant, never earlier.

## Open interest

Snapshots arrive at each venue's own cadence:
- HyperLiquid about every minute;
- Bybit and OKX about every 30 s;
- Binance every 5 minutes, plus about every 60 s within 15 minutes of each funding settlement.

`open_interest` is as the venue reports it. `oi_value_usd` is its USD value where the venue or a price gives one.

Binance's 5-minute values come from its REST history, which stamps each value 5 minutes after the `create_time` that Binance's public data archive gives the same value. Shift by 300 s before joining the two sources.

To put OI on bars, take the latest snapshot at or before each bar's close (an as-of join), not the nearest one, which can be from the future:

```sql
SELECT c.timestamp AS bar_open, c.close, oi.open_interest
FROM candles c
LEFT JOIN LATERAL (
    SELECT o.open_interest FROM open_interest o
    WHERE o.exchange = c.exchange AND o.symbol = c.symbol
      AND o.timestamp <= c.timestamp + interval '5 minutes'      -- the bar's close
      AND o.timestamp >  c.timestamp - interval '1 hour'         -- a bound, so a dead feed shows as null
    ORDER BY o.timestamp DESC LIMIT 1
) oi ON true
WHERE c.exchange = 'binance' AND c.asset_id = 'BTC' AND c.timeframe = '5m'
  AND c.timestamp >= '2026-10-01' AND c.timestamp < '2026-10-02'
ORDER BY c.timestamp;
```

With pandas, `pd.merge_asof(bars, oi, left_on="close_time", right_on="timestamp", direction="backward", tolerance=pd.Timedelta("1h"))` does the same.

## Liquidations

- **Binance and Bybit** come from live streams. History starts when collection started, and an outage is a hole that can't be refilled.
- **HyperLiquid's** come from its public archive, pulled daily for the last three complete days.

## Feeds for a forward-ingesting backtester

`GET /export/parquet?table=…&start=…&end=…` returns one feed window as parquet: `hl_asset_ctxs`, `binance_oi`, `hl_liquidations` or `binance_premium_index`.
- The window is half-open, `[start, end)`, and both bounds must carry a timezone.
- An empty window answers `204`, not an empty file.
- A window over the row cap answers `413`. For long spans, `scripts/export-feed.py` walks the windows and writes one file.

## Larger pulls on the server

Through the API, ask for one exchange, asset and timeframe per request. For research across many assets, read the database directly on the server and stream to a file:

```bash
docker compose exec -T db psql -U candles candle_collector -c "COPY (
  SELECT exchange, asset_id, timestamp, open, high, low, close, volume FROM candles
  WHERE timeframe = '1h' AND timestamp >= '2026-07-01' AND timestamp < '2026-10-01'
  ORDER BY exchange, asset_id, timestamp
) TO STDOUT WITH CSV HEADER" > candles-1h.csv
```

Always bound the time range: the hypertables are partitioned by time, and a bounded query reads only the chunks it needs.

## Knowing what you have

- **`GET /status`** shows each collector's last success and whether it is stale.
- **`GET /status/gaps?exchange=…&timeframe=…&lookback=N`** lists missing bars in the last N intervals (up to 1,000).
- **`scripts/stability-scorecard.sh`** reports candle, funding and OI capture over a window, per venue. [deploy/README.md](../deploy/README.md#stability-scorecard) describes it.
