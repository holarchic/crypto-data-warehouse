"""A fake funding venue for funding.py and funding_repair.py tests: real ccxt with only the HTTP
transport stubbed, answering from a history that honours each request's bounds."""

import json
import urllib.parse
from datetime import UTC, datetime

import ccxt
from funding import HOUR_MS

T0 = 1_792_713_600_000  # 2026-10-21 00:00 UTC
MARKETS = {
    "binance": {"symbol": "BTCUSDT"},
    "bybit": {"category": "linear", "symbol": "BTCUSDT"},
    "okx": {"instId": "BTC-USDT-SWAP"},
    "hyperliquid": {"coin": "BTC"},
    "dydx": {"market": "BTC-USD"},
}


def hourly(n, step_h=1, start=T0, jitter_ms=5):
    """`n` settlements `step_h` apart from `start`, each `jitter_ms` late, rates 1e-5, 2e-5, …"""
    return [(start + i * step_h * HOUR_MS + jitter_ms, (i + 1) * 1e-5) for i in range(n)]


def _iso(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class FakeVenue:
    """Answers each venue's funding-history request from `history` [(raw_ms, rate)], honouring
    the request's bounds and page size the way the venue does. `extra` records are appended to
    every answer, `override` replaces the answer for the n-th request."""

    def __init__(self, venue, history, extra=(), override=None):
        self.venue, self.history, self.extra, self.override = venue, history, list(extra), override or {}
        self.requests, self.urls = [], []

    def __call__(self, url, method="GET", headers=None, body=None):
        params = json.loads(body) if body else {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
        self.requests.append(params)
        self.urls.append(url)
        if len(self.requests) - 1 in self.override:
            return self.override[len(self.requests) - 1]
        return getattr(self, f"_{self.venue}")(params)

    def _between(self, lo, hi, newest_first=False, limit=None):
        rows = [(t, r) for t, r in self.history if lo <= t <= hi]
        rows = sorted(rows, reverse=newest_first)
        return rows[:limit] if limit else rows

    def _binance(self, p):
        rows = self._between(int(p["startTime"]), int(p.get("endTime", 10**15)), limit=int(p["limit"]))
        return [{"symbol": p.get("symbol", "BTCUSDT"), "fundingTime": t, "fundingRate": str(r)} for t, r in rows] + self.extra

    def _bybit(self, p):
        rows = self._between(int(p["startTime"]), int(p["endTime"]), newest_first=True, limit=int(p["limit"]))
        recs = [{"symbol": p["symbol"], "fundingRateTimestamp": str(t), "fundingRate": str(r)} for t, r in rows]
        return {"retCode": 0, "result": {"list": recs + self.extra}}

    def _okx(self, p):
        rows = self._between(int(p["before"]) + 1, int(p["after"]) - 1, newest_first=True, limit=int(p["limit"]))
        recs = [{"fundingTime": str(t), "realizedRate": str(r), "fundingRate": "0.0001"} for t, r in rows]
        return {"code": "0", "data": recs + self.extra}

    def _hyperliquid(self, p):
        return [{"coin": p["coin"], "time": t, "fundingRate": str(r)} for t, r in self._between(p["startTime"], p["endTime"])] + self.extra

    def _dydx(self, p):
        until = int(datetime.fromisoformat(p["effectiveBeforeOrAt"].replace("Z", "+00:00")).timestamp() * 1000)
        rows = self._between(0, until, newest_first=True, limit=int(p["limit"]))
        return {"historicalFunding": [{"effectiveAt": _iso(t), "rate": str(r)} for t, r in rows] + self.extra}


CCXT_CLASS = {"binance": "binanceusdm"}  # as exchanges.yaml runs it


def exchange_for(venue, fake):
    ex = getattr(ccxt, CCXT_CLASS.get(venue, venue))({"enableRateLimit": False})
    ex.fetch = fake
    return ex
