# data/massive_client.py
# Polygon.io data client (accessed via MASSIVE_API_KEY)
#
# Primary endpoints used:
#   /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}  — live quote + day OHLCV + prevDay
#   /v2/aggs/ticker/{ticker}/prev                           — guaranteed prev-day close
#   /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}        — historical daily OHLCV
#   /v3/reference/tickers/{ticker}                          — name, market cap, description
#
# All functions return None on failure — callers must fall through to yfinance / Finnhub.

import os
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

POLYGON_BASE = "https://api.polygon.io"


# ─── Core HTTP helper ─────────────────────────────────────────────────────────

def _key() -> str:
    return os.environ.get("MASSIVE_API_KEY", "")


def _get(path: str, params: Optional[dict] = None, timeout: int = 8) -> Optional[dict]:
    key = _key()
    if not key:
        return None
    if params is None:
        params = {}
    params["apiKey"] = key
    try:
        resp = requests.get(f"{POLYGON_BASE}{path}", params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            # Rate limited — back off once and retry
            time.sleep(1.0)
            resp = requests.get(f"{POLYGON_BASE}{path}", params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        return None
    except Exception:
        return None


# ─── Quote / snapshot ─────────────────────────────────────────────────────────

def get_snapshot(ticker: str) -> Optional[dict]:
    """
    Single-call snapshot for one ticker.
    Returns the raw Polygon 'ticker' sub-object, e.g.:
      {day: {o,h,l,c,v}, prevDay: {c,...}, lastTrade: {p,...}, todaysChangePerc}
    """
    data = _get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}")
    if not data:
        return None
    # Polygon returns {"status": "OK", "ticker": {...}}
    return data.get("ticker")


def get_quote(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Normalized quote dict with keys:
      price, prev_close, volume, high, low, open, change_pct, source

    Uses snapshot as primary; falls back to /prev for prev_close if snapshot
    prevDay is missing.  Returns None if Polygon has no data for this ticker.
    """
    snap = get_snapshot(ticker)
    if not snap:
        return None

    day        = snap.get("day") or {}
    prev_day   = snap.get("prevDay") or {}
    last_trade = snap.get("lastTrade") or {}

    # Best available price: lastTrade.p > day.c
    price = float(last_trade.get("p") or day.get("c") or 0)
    if price <= 0:
        return None

    prev_close = float(prev_day.get("c") or 0)
    if prev_close <= 0:
        # Try dedicated /prev endpoint as fallback for prev_close
        prev_close = _get_prev_close_raw(ticker) or price

    change_pct = (price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

    return {
        "price":      price,
        "prev_close": prev_close,
        "volume":     float(day.get("v") or 0),
        "high":       float(day.get("h") or price),
        "low":        float(day.get("l") or price),
        "open":       float(day.get("o") or price),
        "change_pct": round(change_pct, 2),
        "source":     "massive",
    }


def _get_prev_close_raw(ticker: str) -> Optional[float]:
    """Previous trading-day closing price via /v2/aggs/ticker/{ticker}/prev."""
    data = _get(f"/v2/aggs/ticker/{ticker.upper()}/prev")
    if not data or not data.get("results"):
        return None
    result = data["results"][0]
    return float(result.get("c") or 0) or None


def get_prev_close(ticker: str) -> Optional[float]:
    """Public wrapper — previous trading-day close."""
    return _get_prev_close_raw(ticker)


# ─── Batch snapshots (for pre-screener) ──────────────────────────────────────

def batch_quotes(tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch quotes for multiple tickers in one HTTP call.
    Polygon supports up to ~250 tickers per request via the tickers= param.
    Returns {ticker: quote_dict} — missing tickers are absent from the dict.
    """
    if not tickers or not _key():
        return {}

    results: Dict[str, Dict[str, Any]] = {}

    # Polygon allows comma-separated tickers; chunk to be safe
    chunk_size = 200
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data  = _get(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            {"tickers": ",".join(t.upper() for t in chunk)},
        )
        if not data or not data.get("tickers"):
            continue
        for snap in data["tickers"]:
            sym        = snap.get("ticker", "")
            day        = snap.get("day") or {}
            prev_day   = snap.get("prevDay") or {}
            last_trade = snap.get("lastTrade") or {}
            price      = float(last_trade.get("p") or day.get("c") or 0)
            if price <= 0:
                continue
            prev_close = float(prev_day.get("c") or price)
            change_pct = (price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
            results[sym] = {
                "price":      price,
                "prev_close": prev_close,
                "volume":     float(day.get("v") or 0),
                "high":       float(day.get("h") or price),
                "low":        float(day.get("l") or price),
                "open":       float(day.get("o") or price),
                "change_pct": round(change_pct, 2),
                "source":     "massive",
            }

    return results


# ─── Daily OHLCV aggregates ───────────────────────────────────────────────────

def get_daily_aggs(ticker: str, days: int = 5) -> Optional[List[dict]]:
    """
    Last `days` trading-day OHLCV bars.
    Returns list of {o, h, l, c, v, t (epoch ms)} sorted newest-first, or None.
    """
    to_date   = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days + 7)).strftime("%Y-%m-%d")
    data = _get(
        f"/v2/aggs/ticker/{ticker.upper()}/range/1/day/{from_date}/{to_date}",
        {"adjusted": "true", "sort": "desc", "limit": days},
    )
    if not data or not data.get("results"):
        return None
    return data["results"]


def get_intraday_aggs(ticker: str, resolution_min: int = 1) -> Optional[List[dict]]:
    """
    Today's intraday OHLCV bars from 4 AM ET to now, at `resolution_min`-minute bars.
    Returns list of {o, h, l, c, v, t (epoch ms)}, or None.
    """
    from zoneinfo import ZoneInfo
    et     = ZoneInfo("America/New_York")
    now_et = datetime.now(et)
    # Start from 4 AM ET today
    start  = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
    from_ts = int(start.timestamp() * 1000)
    to_ts   = int(now_et.timestamp() * 1000)

    data = _get(
        f"/v2/aggs/ticker/{ticker.upper()}/range/{resolution_min}/minute"
        f"/{start.strftime('%Y-%m-%d')}/{now_et.strftime('%Y-%m-%d')}",
        {"adjusted": "true", "sort": "asc", "limit": 1000},
    )
    if not data or not data.get("results"):
        return None
    # Filter to today's session window
    return [bar for bar in data["results"] if from_ts <= bar.get("t", 0) <= to_ts]


# ─── Ticker details / fundamentals ───────────────────────────────────────────

def get_ticker_details(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Company reference data: name, market_cap, description, sic_code, etc.
    Returns Polygon's 'results' sub-object, or None.

    Key fields:
      name, market_cap (USD), description, sic_description,
      total_employees, list_date, primary_exchange
    """
    data = _get(f"/v3/reference/tickers/{ticker.upper()}")
    if not data or data.get("status") not in ("OK", "ok"):
        return None
    return data.get("results")


# ─── Convenience: availability check ─────────────────────────────────────────

def is_available() -> bool:
    """True if MASSIVE_API_KEY is set and the API is reachable."""
    if not _key():
        return False
    data = _get("/v2/snapshot/locale/us/markets/stocks/tickers/SPY")
    return data is not None
