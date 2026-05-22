# data/market_data.py
# Primary: Massive/Polygon.io (MASSIVE_API_KEY)
# Secondary: yfinance
# Tertiary: Finnhub (fallback only, FINNHUB_API_KEY)

import os
import requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
import warnings
warnings.filterwarnings("ignore")

from config import MIN_MARKET_CAP, MAX_MARKET_CAP, MIN_AVG_VOLUME
from data.massive_client import (
    get_quote      as _massive_quote,
    get_ticker_details,
    get_daily_aggs as _massive_daily_aggs,
    get_intraday_aggs as _massive_intraday_aggs,
)

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Sector ETFs for relative strength comparison
SECTOR_ETFS = {
    "Technology":          "QQQ",
    "Healthcare":          "XLV",
    "Financial Services":  "XLF",
    "Energy":              "XLE",
    "Consumer Cyclical":   "XLY",
    "Industrials":         "XLI",
    "Communication":       "XLC",
    "Crypto/Mining":       "BITQ",
    "AI/Innovation":       "ARKK",
    "Default":             "SPY",
}


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fh_get(endpoint: str, params: dict) -> Optional[dict]:
    key = _fh_key()
    if not key:
        return None
    try:
        params["token"] = key
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return data
        return None
    except Exception as e:
        print(f"  [finnhub] {endpoint} failed: {e}")
        return None


def fetch_ticker_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
    ticker = ticker.upper().strip()

    # ── 1. Massive/Polygon: live quote + company details ──────────────────────
    massive_q      = _massive_quote(ticker)           # {price, prev_close, volume, high, low, open, source}
    massive_detail = get_ticker_details(ticker)        # {name, market_cap, description, ...}

    # ── 2. yfinance: history + fundamentals (always fetched for 60-day OHLCV) ─
    try:
        yf_ticker = yf.Ticker(ticker)
        hist      = yf_ticker.history(period="60d", auto_adjust=True)
        yf_info   = yf_ticker.info
    except Exception:
        hist    = pd.DataFrame()
        yf_info = {}

    # ── 3. Finnhub: fallback for quote/profile only when Massive failed ────────
    fh_quote   = None
    fh_profile = None
    fh_metric  = None
    if not massive_q:
        fh_quote   = _fh_get("quote",          {"symbol": ticker})
        fh_profile = _fh_get("stock/profile2", {"symbol": ticker})
        fh_metric  = _fh_get("stock/metric",   {"symbol": ticker, "metric": "all"})
        if fh_quote:
            print(f"  [snapshot/finnhub] {ticker}: Massive unavailable — fell back to Finnhub")

    # Determine active quote source for logging
    quote_source = "massive" if massive_q else ("finnhub" if fh_quote else "yfinance")

    # ── Price ─────────────────────────────────────────────────────────────────
    price = None
    if massive_q:
        price = massive_q["price"]
    elif fh_quote and fh_quote.get("c") and fh_quote["c"] > 0:
        price = fh_quote["c"]
    elif yf_info.get("currentPrice"):
        price = yf_info["currentPrice"]
    elif yf_info.get("regularMarketPrice"):
        price = yf_info["regularMarketPrice"]
    elif not hist.empty:
        price = float(hist["Close"].iloc[-1])

    if not price or price <= 0:
        return None

    # ── Market cap ────────────────────────────────────────────────────────────
    market_cap = 0
    if massive_detail and massive_detail.get("market_cap"):
        market_cap = massive_detail["market_cap"]          # Polygon already in USD
    elif fh_profile and fh_profile.get("marketCapitalization"):
        market_cap = fh_profile["marketCapitalization"] * 1_000_000
    elif yf_info.get("marketCap"):
        market_cap = yf_info["marketCap"]

    # ── Volume ────────────────────────────────────────────────────────────────
    today_volume = 0
    avg_vol_20   = 0
    if massive_q and massive_q.get("volume"):
        today_volume = int(massive_q["volume"])
    elif fh_quote and fh_quote.get("v"):
        today_volume = int(fh_quote["v"])
    elif yf_info.get("regularMarketVolume"):
        today_volume = int(yf_info["regularMarketVolume"])
    elif yf_info.get("volume"):
        today_volume = int(yf_info["volume"])
    elif not hist.empty:
        today_volume = int(hist["Volume"].iloc[-1])

    if not hist.empty and len(hist) >= 20:
        avg_vol_20 = int(hist["Volume"].tail(20).mean())
    elif yf_info.get("averageVolume10days"):
        avg_vol_20 = int(yf_info["averageVolume10days"])
    elif yf_info.get("averageVolume"):
        avg_vol_20 = int(yf_info["averageVolume"])
    elif yf_info.get("threeMonthAverageVolume"):
        avg_vol_20 = int(yf_info["threeMonthAverageVolume"])

    rel_volume = round(today_volume / avg_vol_20, 2) if avg_vol_20 > 0 else 0

    # ── Identity ──────────────────────────────────────────────────────────────
    # Massive/Polygon detail > Finnhub profile > yfinance
    poly_name = (massive_detail or {}).get("name")
    company_name = poly_name or (fh_profile or {}).get("name") or yf_info.get("longName") or ticker
    sector       = yf_info.get("sector") or (fh_profile or {}).get("finnhubIndustry") or "Default"
    industry     = yf_info.get("industry") or (fh_profile or {}).get("finnhubIndustry") or "Unknown"

    # ── 52-week range ─────────────────────────────────────────────────────────
    fh_m        = (fh_metric or {}).get("metric", {})
    week52_high = yf_info.get("fiftyTwoWeekHigh") or fh_m.get("52WeekHigh")
    week52_low  = yf_info.get("fiftyTwoWeekLow")  or fh_m.get("52WeekLow")

    # ── Fundamentals (all from yfinance — most complete) ─────────────────────
    revenue             = yf_info.get("totalRevenue")
    revenue_growth      = yf_info.get("revenueGrowth")
    gross_margins       = yf_info.get("grossMargins")
    total_cash          = yf_info.get("totalCash")
    total_debt          = yf_info.get("totalDebt")
    free_cashflow       = yf_info.get("freeCashflow")
    operating_cashflow  = yf_info.get("operatingCashflow")
    ebitda              = yf_info.get("ebitda")
    shares_out          = yf_info.get("sharesOutstanding")
    float_shares        = yf_info.get("floatShares")

    # ── Valuation ─────────────────────────────────────────────────────────────
    pe_ratio = yf_info.get("trailingPE")               or fh_m.get("peTTM")
    ps_ratio = yf_info.get("priceToSalesTrailing12Months") or fh_m.get("psTTM")
    pb_ratio = yf_info.get("priceToBook")              or fh_m.get("pbAnnual")

    # ── Short interest ────────────────────────────────────────────────────────
    short_pct   = yf_info.get("shortPercentOfFloat")
    short_ratio = yf_info.get("shortRatio")

    # ── Analyst ───────────────────────────────────────────────────────────────
    analyst_rec    = yf_info.get("recommendationKey", "none")
    analyst_target = yf_info.get("targetMeanPrice")
    analyst_count  = yf_info.get("numberOfAnalystOpinions", 0)

    # ── Earnings Calendar (Finnhub still used — no Polygon free-tier equiv) ───
    earnings_date    = None
    earnings_warning = False
    days_to_earnings = None
    fh_earnings = _fh_get("calendar/earnings", {
        "symbol": ticker,
        "from":   datetime.now().strftime("%Y-%m-%d"),
        "to":     (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
    })
    if fh_earnings and fh_earnings.get("earningsCalendar"):
        upcoming = fh_earnings["earningsCalendar"]
        if upcoming:
            earnings_date = upcoming[0].get("date")
            try:
                ed = datetime.strptime(earnings_date, "%Y-%m-%d")
                days_to_earnings = (ed - datetime.now()).days
                earnings_warning = days_to_earnings <= 7
            except Exception:
                pass

    # ── Sector Relative Strength ──────────────────────────────────────────────
    sector_rs = get_sector_relative_strength(ticker, sector, hist)

    # ── OHLCV fields from best available source ───────────────────────────────
    open_px    = (massive_q or {}).get("open",       (fh_quote or {}).get("o",  price))
    day_high   = (massive_q or {}).get("high",       (fh_quote or {}).get("h",  price))
    day_low    = (massive_q or {}).get("low",        (fh_quote or {}).get("l",  price))
    prev_close = (massive_q or {}).get("prev_close", (fh_quote or {}).get("pc", price))

    print(f"  [snapshot/{quote_source}] {ticker}: ${price:.2f}  mcap=${market_cap/1e6:.0f}M")

    return {
        "ticker":              ticker,
        "company_name":        company_name,
        "sector":              sector,
        "industry":            industry,
        "price":               round(price, 4),
        "open":                open_px,
        "day_high":            day_high,
        "day_low":             day_low,
        "prev_close":          prev_close,
        "week_52_high":        week52_high,
        "week_52_low":         week52_low,
        "market_cap":          market_cap,
        "shares_outstanding":  shares_out,
        "float_shares":        float_shares,
        "volume":              today_volume,
        "avg_volume":          avg_vol_20,
        "relative_volume":     rel_volume,
        "pe_ratio":            pe_ratio,
        "ps_ratio":            ps_ratio,
        "pb_ratio":            pb_ratio,
        "revenue":             revenue,
        "revenue_growth":      revenue_growth,
        "gross_margins":       gross_margins,
        "ebitda":              ebitda,
        "total_cash":          total_cash,
        "total_debt":          total_debt,
        "free_cashflow":       free_cashflow,
        "operating_cashflow":  operating_cashflow,
        "short_percent_float": short_pct,
        "short_ratio":         short_ratio,
        "analyst_recommendation": analyst_rec,
        "analyst_mean_target":    analyst_target,
        "analyst_count":          analyst_count,
        "earnings_date":          earnings_date,
        "earnings_warning":       earnings_warning,
        "days_to_earnings":       days_to_earnings,
        "sector_etf":             sector_rs.get("etf"),
        "sector_return_20d":      sector_rs.get("sector_return_20d"),
        "stock_vs_sector":        sector_rs.get("stock_vs_sector"),
        "sector_rs_label":        sector_rs.get("label"),
        "_history":               hist,
        "_info":                  yf_info,
        "data_sources":           [quote_source, "yfinance (history/fundamentals)"],
        "data_fetched_at":        datetime.now().isoformat(),
    }


def get_sector_relative_strength(ticker: str, sector: str, hist: pd.DataFrame) -> dict:
    """
    Compare the stock's 20-day return against its sector ETF.
    Tells you if the stock is leading or lagging its peers.
    """
    try:
        if hist is None or len(hist) < 21:
            return {}

        # Stock 20-day return
        stock_ret = (hist["Close"].iloc[-1] / hist["Close"].iloc[-21] - 1) * 100

        # Pick the right sector ETF
        etf = SECTOR_ETFS.get(sector, SECTOR_ETFS["Default"])

        # Fetch ETF history
        etf_data = yf.Ticker(etf).history(period="30d", auto_adjust=True)
        if etf_data.empty or len(etf_data) < 21:
            return {}

        sector_ret = (etf_data["Close"].iloc[-1] / etf_data["Close"].iloc[-21] - 1) * 100
        vs_sector  = round(stock_ret - sector_ret, 2)

        if vs_sector > 10:
            label = f"⚡ Strongly outperforming {etf} by {vs_sector:+.1f}%"
        elif vs_sector > 3:
            label = f"↑ Outperforming {etf} by {vs_sector:+.1f}%"
        elif vs_sector > -3:
            label = f"→ In line with {etf} ({vs_sector:+.1f}%)"
        elif vs_sector > -10:
            label = f"↓ Underperforming {etf} by {abs(vs_sector):.1f}%"
        else:
            label = f"⚠ Significantly underperforming {etf} by {abs(vs_sector):.1f}%"

        return {
            "etf":              etf,
            "sector_return_20d": round(sector_ret, 2),
            "stock_vs_sector":  vs_sector,
            "label":            label,
        }
    except Exception:
        return {}


def passes_universe_filter(snapshot: Dict[str, Any]) -> Tuple[bool, str]:
    mc  = snapshot.get("market_cap", 0)
    vol = snapshot.get("avg_volume", 0)
    px  = snapshot.get("price", 0)

    if mc < MIN_MARKET_CAP:
        return False, f"Market cap ${mc/1e6:.1f}M below ${MIN_MARKET_CAP/1e6:.0f}M floor"
    if mc > MAX_MARKET_CAP:
        return False, f"Market cap ${mc/1e9:.1f}B above ${MAX_MARKET_CAP/1e9:.0f}B ceiling"
    if vol < MIN_AVG_VOLUME:
        return False, f"Avg volume {vol:,} below {MIN_AVG_VOLUME:,} — liquidity risk"
    if px < 0.50:
        return False, f"Price ${px:.4f} below $0.50 — near-OTC territory"

    return True, "Passes universe filter"


def get_price_history(ticker: str, days: int = 90) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        return t.history(period=f"{days}d", auto_adjust=True)
    except Exception:
        return pd.DataFrame()


def get_intraday_candles(ticker: str, resolution: str = "1") -> pd.DataFrame:
    """
    Fetch today's intraday candles (4 AM ET → now).
    Primary: Massive/Polygon minute aggregates.
    Fallback: Finnhub /stock/candle.
    """
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")

    # ── 1. Massive/Polygon minute aggs ────────────────────────────────────────
    try:
        res_min = int(resolution) if resolution.isdigit() else 1
        bars    = _massive_intraday_aggs(ticker, resolution_min=res_min)
        if bars:
            df = pd.DataFrame({
                "Open":   [b["o"] for b in bars],
                "High":   [b["h"] for b in bars],
                "Low":    [b["l"] for b in bars],
                "Close":  [b["c"] for b in bars],
                "Volume": [b["v"] for b in bars],
            }, index=pd.to_datetime([b["t"] for b in bars], unit="ms", utc=True).tz_convert(et))
            print(f"  [candles/massive] {ticker}: {len(df)} bars")
            return df
    except Exception as e:
        print(f"  [candles/massive] {ticker} failed: {e}")

    # ── 2. Finnhub fallback ───────────────────────────────────────────────────
    try:
        import pytz
        key = _fh_key()
        if not key:
            return pd.DataFrame()
        et_pytz  = pytz.timezone("America/New_York")
        now_et   = datetime.now(et_pytz)
        from_et  = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
        resp = requests.get(
            f"{FINNHUB_BASE}/stock/candle",
            params={"symbol": ticker.upper(), "resolution": resolution,
                    "from": int(from_et.timestamp()), "to": int(now_et.timestamp()),
                    "token": key},
            timeout=12,
        )
        data = resp.json()
        if data.get("s") != "ok" or not data.get("t"):
            return pd.DataFrame()
        df = pd.DataFrame({
            "Open": data["o"], "High": data["h"],
            "Low":  data["l"], "Close": data["c"], "Volume": data["v"],
        }, index=pd.to_datetime(data["t"], unit="s", utc=True).tz_convert(et_pytz))
        print(f"  [candles/finnhub] {ticker}: {len(df)} bars (Massive unavailable)")
        return df
    except Exception as e:
        print(f"  [candles/finnhub] {ticker} failed: {e}")
        return pd.DataFrame()


def get_chart_data(ticker: str, timeframe: str) -> pd.DataFrame:
    """Return OHLCV DataFrame for the requested timeframe."""
    if timeframe == "1D":
        return get_intraday_candles(ticker, resolution="1")
    elif timeframe == "5D":
        try:
            df = yf.Ticker(ticker.upper()).history(period="5d", interval="15m", auto_adjust=True)
            if not df.empty:
                return df
        except Exception:
            pass
        return get_intraday_candles(ticker, resolution="15")
    elif timeframe == "1M":
        return get_price_history(ticker, 30)
    else:
        return get_price_history(ticker, 90)
