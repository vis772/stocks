# resilient_fetcher.py
# Multi-source quote fetcher: tiingo → finnhub → yfinance → alphavantage → DB stale cache.
# Never returns None. Logs every failure to data_quality table.

import os
import time
import threading
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

import requests
import yfinance as yf


@dataclass
class QuoteResult:
    ticker: str
    price: float
    prev_close: float
    volume: float
    high: float
    low: float
    source: str
    source_quality: str  # 'live' | 'stale_<N>m' | 'unavailable'
    latency_ms: float


class _RateLimiter:
    """Sliding-window token-bucket. Auto-throttles at 80% budget."""
    def __init__(self, calls: int, period_s: int):
        self.max_calls = calls
        self.period    = period_s
        self._calls: list = []
        self._lock = threading.Lock()

    def check(self, name: str) -> bool:
        now = time.time()
        with self._lock:
            self._calls = [t for t in self._calls if t > now - self.period]
            pct = len(self._calls) / self.max_calls
            if pct >= 1.0:
                return False
            if pct >= 0.8:
                print(f"  [rate] {name} at {pct:.0%} budget — throttling")
                time.sleep(0.5)
            self._calls.append(now)
            return True


_RL = {
    "tiingo":       _RateLimiter(500,    3600),
    "finnhub":      _RateLimiter(60,     60),
    "yfinance":     _RateLimiter(10_000, 3600),
    "alphavantage": _RateLimiter(5,      60),
}


def _log_quality(ticker: str, source: str, status: str,
                 latency_ms: float, error: str = "") -> None:
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO data_quality (ticker, source, status, latency_ms, error)
                VALUES (%s, %s, %s, %s, %s)
            """, (ticker, source, status, round(latency_ms, 1), error[:200]))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                INSERT INTO data_quality (ticker, source, status, latency_ms, error)
                VALUES (?, ?, ?, ?, ?)
            """, (ticker, source, status, round(latency_ms, 1), error[:200]))
            conn.commit(); conn.close()
    except Exception:
        pass


def _store_quote(r: QuoteResult) -> None:
    """Upsert live quote into quote_cache for future stale-fallback use."""
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO quote_cache (ticker, price, prev_close, volume, high, low, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ticker) DO UPDATE SET
                    price=EXCLUDED.price, prev_close=EXCLUDED.prev_close,
                    volume=EXCLUDED.volume, high=EXCLUDED.high, low=EXCLUDED.low,
                    source=EXCLUDED.source, created_at=NOW()
            """, (r.ticker, r.price, r.prev_close, r.volume, r.high, r.low, r.source))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                INSERT OR REPLACE INTO quote_cache
                    (ticker, price, prev_close, volume, high, low, source, created_at)
                VALUES (?,?,?,?,?,?,?,datetime('now'))
            """, (r.ticker, r.price, r.prev_close, r.volume, r.high, r.low, r.source))
            conn.commit(); conn.close()
    except Exception:
        pass


def _load_stale(ticker: str) -> Optional[QuoteResult]:
    """Return last known price from quote_cache with staleness label."""
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT price, prev_close, volume, high, low, created_at
                FROM quote_cache WHERE ticker = %s
                ORDER BY created_at DESC LIMIT 1
            """, (ticker,))
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT price, prev_close, volume, high, low, created_at
                FROM quote_cache WHERE ticker = ?
                ORDER BY created_at DESC LIMIT 1
            """, (ticker,))
            row = cur.fetchone(); conn.close()

        if not row or not row[0]:
            return None
        price, prev_close, volume, high, low, created_at = row
        ts = created_at if isinstance(created_at, datetime) else datetime.fromisoformat(str(created_at))
        age_m = int((datetime.now() - ts.replace(tzinfo=None)).total_seconds() / 60)
        return QuoteResult(
            ticker=ticker, price=float(price), prev_close=float(prev_close or price),
            volume=float(volume or 0), high=float(high or price), low=float(low or price),
            source="db_cache", source_quality=f"stale_{age_m}m", latency_ms=0,
        )
    except Exception:
        return None


# ─── Per-source fetchers ──────────────────────────────────────────────────────

def _tiingo(ticker: str) -> Optional[QuoteResult]:
    token = os.environ.get("TIINGO_TOKEN", "")
    if not token or not _RL["tiingo"].check("tiingo"):
        return None
    t0 = time.time()
    try:
        r = requests.get(
            f"https://api.tiingo.com/iex/{ticker.lower()}",
            headers={"Authorization": f"Token {token}", "Content-Type": "application/json"},
            timeout=6,
        )
        ms = (time.time() - t0) * 1000
        if r.status_code != 200:
            _log_quality(ticker, "tiingo", f"http_{r.status_code}", ms)
            return None
        data = r.json()
        if not data or not isinstance(data, list):
            _log_quality(ticker, "tiingo", "empty", ms)
            return None
        d = data[0]
        price = float(d.get("last") or d.get("tngoLast") or 0)
        if price <= 0:
            _log_quality(ticker, "tiingo", "empty", ms)
            return None
        _log_quality(ticker, "tiingo", "ok", ms)
        return QuoteResult(
            ticker=ticker, price=price,
            prev_close=float(d.get("prevClose") or price),
            volume=float(d.get("lastVolume") or 0),
            high=float(d.get("high") or price),
            low=float(d.get("low") or price),
            source="tiingo", source_quality="live", latency_ms=ms,
        )
    except Exception as e:
        ms = (time.time() - t0) * 1000
        _log_quality(ticker, "tiingo", "error", ms, str(e))
        return None


def _finnhub(ticker: str) -> Optional[QuoteResult]:
    key = os.environ.get("FINNHUB_API_KEY", "")
    if not key or not _RL["finnhub"].check("finnhub"):
        return None
    t0 = time.time()
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": ticker, "token": key}, timeout=6,
        )
        ms = (time.time() - t0) * 1000
        if r.status_code != 200:
            _log_quality(ticker, "finnhub", f"http_{r.status_code}", ms)
            return None
        d = r.json()
        if not d or not d.get("c") or d["c"] <= 0:
            _log_quality(ticker, "finnhub", "empty", ms)
            return None
        _log_quality(ticker, "finnhub", "ok", ms)
        return QuoteResult(
            ticker=ticker, price=float(d["c"]),
            prev_close=float(d.get("pc") or d["c"]),
            volume=float(d.get("v") or 0),
            high=float(d.get("h") or d["c"]),
            low=float(d.get("l") or d["c"]),
            source="finnhub", source_quality="live", latency_ms=ms,
        )
    except Exception as e:
        ms = (time.time() - t0) * 1000
        _log_quality(ticker, "finnhub", "error", ms, str(e))
        return None


def _yfinance(ticker: str) -> Optional[QuoteResult]:
    if not _RL["yfinance"].check("yfinance"):
        return None
    t0 = time.time()
    try:
        info = yf.Ticker(ticker).info
        ms = (time.time() - t0) * 1000
        price = (info.get("currentPrice") or info.get("regularMarketPrice")
                 or info.get("ask") or 0)
        if not price:
            _log_quality(ticker, "yfinance", "empty", ms)
            return None
        _log_quality(ticker, "yfinance", "ok", ms)
        return QuoteResult(
            ticker=ticker, price=float(price),
            prev_close=float(info.get("previousClose") or price),
            volume=float(info.get("volume") or info.get("regularMarketVolume") or 0),
            high=float(info.get("dayHigh") or info.get("regularMarketDayHigh") or price),
            low=float(info.get("dayLow") or info.get("regularMarketDayLow") or price),
            source="yfinance", source_quality="live", latency_ms=ms,
        )
    except Exception as e:
        ms = (time.time() - t0) * 1000
        _log_quality(ticker, "yfinance", "error", ms, str(e))
        return None


def _alphavantage(ticker: str) -> Optional[QuoteResult]:
    key = os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not key or not _RL["alphavantage"].check("alphavantage"):
        return None
    t0 = time.time()
    try:
        r = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "GLOBAL_QUOTE", "symbol": ticker, "apikey": key},
            timeout=10,
        )
        ms = (time.time() - t0) * 1000
        if r.status_code != 200:
            _log_quality(ticker, "alphavantage", f"http_{r.status_code}", ms)
            return None
        d = r.json().get("Global Quote", {})
        price = float(d.get("05. price") or 0)
        if not price:
            _log_quality(ticker, "alphavantage", "empty", ms)
            return None
        _log_quality(ticker, "alphavantage", "ok", ms)
        return QuoteResult(
            ticker=ticker, price=price,
            prev_close=float(d.get("08. previous close") or price),
            volume=float(d.get("06. volume") or 0),
            high=float(d.get("03. high") or price),
            low=float(d.get("04. low") or price),
            source="alphavantage", source_quality="live", latency_ms=ms,
        )
    except Exception as e:
        ms = (time.time() - t0) * 1000
        _log_quality(ticker, "alphavantage", "error", ms, str(e))
        return None


_SOURCES = [_tiingo, _finnhub, _yfinance, _alphavantage]


class ResilientQuoteFetcher:
    """
    Multi-source cascading fetcher. SOURCES tried in order with 3× exponential backoff.
    Falls back to DB stale cache. Never returns None.
    """
    _mem_cache: dict = {}
    _lock = threading.Lock()
    TTL = 60  # seconds

    def fetch(self, ticker: str) -> QuoteResult:
        ticker = ticker.upper()

        # 1. In-memory cache
        with self._lock:
            entry = self._mem_cache.get(ticker)
            if entry and time.time() < entry[1]:
                return entry[0]

        # 2. Cascade through sources with exponential backoff
        for fn in _SOURCES:
            for attempt in range(3):
                result = fn(ticker)
                if result and result.price > 0:
                    with self._lock:
                        self._mem_cache[ticker] = (result, time.time() + self.TTL)
                    _store_quote(result)
                    return result
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))  # 0.5s, 1.0s

        # 3. DB stale cache
        stale = _load_stale(ticker)
        if stale and stale.price > 0:
            print(f"  [fetcher] {ticker}: all live sources failed — {stale.source_quality}")
            return stale

        # 4. Unavailable sentinel — caller never gets None
        print(f"  [fetcher] {ticker}: no data from any source")
        return QuoteResult(
            ticker=ticker, price=0.0, prev_close=0.0,
            volume=0.0, high=0.0, low=0.0,
            source="none", source_quality="unavailable", latency_ms=0,
        )


_fetcher = ResilientQuoteFetcher()


def fetch_quote(ticker: str) -> QuoteResult:
    return _fetcher.fetch(ticker)
