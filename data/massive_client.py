# data/massive_client.py
# Massive.com market data client (MASSIVE_API_KEY)
# polygon.io redirects to massive.com — same URL path structure, different host.
#
# Free-tier endpoints (work with any key):
#   /v2/aggs/ticker/{ticker}/prev                           — prev-day close ✅
#   /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}        — historical daily OHLCV ✅
#   /v3/reference/tickers/{ticker}                          — name, market cap ✅
#
# Paid-tier only (403 NOT_AUTHORIZED on free):
#   /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}  — real-time quote ❌
#
# Use Massive for historical/reference data; fall through to yfinance/Finnhub for live price.

import os
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

MASSIVE_BASE = "https://api.massive.com"


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
        resp = requests.get(f"{MASSIVE_BASE}{path}", params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            # Rate limited — back off once and retry
            time.sleep(1.0)
            resp = requests.get(f"{MASSIVE_BASE}{path}", params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        return None
    except Exception:
        return None


# ─── Quote / snapshot ─────────────────────────────────────────────────────────

def get_snapshot(ticker: str) -> Optional[dict]:
    """
    Real-time snapshot — requires Massive paid plan.
    Returns None on free tier (403 NOT_AUTHORIZED). Use get_prev_close() instead.
    """
    # Snapshot is paid-tier only. Skip the call to avoid 403 noise.
    return None


def get_quote(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Live quote — requires Massive paid plan snapshot endpoint.
    Returns None on free tier. Callers fall through to yfinance / Finnhub.
    """
    return None


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
    Batch snapshot — requires Massive paid plan.
    Returns empty dict on free tier. Callers fall through to yfinance / Finnhub.
    """
    return {}


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
    """True if MASSIVE_API_KEY is set and the API is reachable (tests free-tier /prev endpoint)."""
    if not _key():
        return False
    data = _get("/v2/aggs/ticker/SPY/prev")
    return data is not None and bool(data.get("results"))
