# data/market_data.py
# Primary: Finnhub (real-time, reliable, free tier 60 calls/min)
# Fallback: yfinance (unlimited but unreliable)
# New: Earnings calendar warning, sector relative strength

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

    fh_quote   = _fh_get("quote", {"symbol": ticker})
    fh_profile = _fh_get("stock/profile2", {"symbol": ticker})
    fh_metric  = _fh_get("stock/metric", {"symbol": ticker, "metric": "all"})

    try:
        yf_ticker = yf.Ticker(ticker)
        hist      = yf_ticker.history(period="60d", auto_adjust=True)
        yf_info   = yf_ticker.info
    except Exception:
        hist    = pd.DataFrame()
        yf_info = {}

    # Price
    price = None
    if fh_quote and fh_quote.get("c") and fh_quote["c"] > 0:
        price = fh_quote["c"]
    elif yf_info.get("currentPrice"):
        price = yf_info["currentPrice"]
    elif yf_info.get("regularMarketPrice"):
        price = yf_info["regularMarketPrice"]
    elif not hist.empty:
        price = float(hist["Close"].iloc[-1])

    if not price or price <= 0:
        return None

    # Market cap
    market_cap = 0
    if fh_profile and fh_profile.get("marketCapitalization"):
        market_cap = fh_profile["marketCapitalization"] * 1_000_000
    elif yf_info.get("marketCap"):
        market_cap = yf_info["marketCap"]

    # Volume
    today_volume = 0
    avg_vol_20   = 0
    if fh_quote and fh_quote.get("v"):
        today_volume = int(fh_quote["v"])
    elif not hist.empty:
        today_volume = int(hist["Volume"].iloc[-1])

    if not hist.empty and len(hist) >= 20:
        avg_vol_20 = int(hist["Volume"].tail(20).mean())
    elif yf_info.get("averageVolume"):
        avg_vol_20 = yf_info["averageVolume"]

    rel_volume = round(today_volume / avg_vol_20, 2) if avg_vol_20 > 0 else 0

    # Identity
    company_name = (fh_profile or {}).get("name") or yf_info.get("longName") or ticker
    sector       = (fh_profile or {}).get("finnhubIndustry") or yf_info.get("sector") or "Default"
    industry     = (fh_profile or {}).get("finnhubIndustry") or yf_info.get("industry") or "Unknown"

    # 52-week range
    fh_m        = (fh_metric or {}).get("metric", {})
    week52_high = fh_m.get("52WeekHigh") or yf_info.get("fiftyTwoWeekHigh")
    week52_low  = fh_m.get("52WeekLow")  or yf_info.get("fiftyTwoWeekLow")

    # Fundamentals
    revenue        = yf_info.get("totalRevenue")
    revenue_growth = yf_info.get("revenueGrowth")
    gross_margins  = yf_info.get("grossMargins")
    total_cash     = yf_info.get("totalCash")
    total_debt     = yf_info.get("totalDebt")
    free_cashflow  = yf_info.get("freeCashflow")
    ebitda         = yf_info.get("ebitda")
    shares_out     = yf_info.get("sharesOutstanding")
    float_shares   = yf_info.get("floatShares")

    # Valuation
    pe_ratio = fh_m.get("peTTM")    or yf_info.get("trailingPE")
    ps_ratio = fh_m.get("psTTM")    or yf_info.get("priceToSalesTrailing12Months")
    pb_ratio = fh_m.get("pbAnnual") or yf_info.get("priceToBook")

    # Short interest
    short_pct   = yf_info.get("shortPercentOfFloat")
    short_ratio = yf_info.get("shortRatio")

    # Analyst
    analyst_rec    = yf_info.get("recommendationKey", "none")
    analyst_target = yf_info.get("targetMeanPrice")
    analyst_count  = yf_info.get("numberOfAnalystOpinions", 0)

    # ── OPTION A: Earnings Calendar ───────────────────────────────────────────
    earnings_date       = None
    earnings_warning    = False
    days_to_earnings    = None
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

    # ── OPTION C: Sector Relative Strength ────────────────────────────────────
    sector_rs = get_sector_relative_strength(ticker, sector, hist)

    return {
        "ticker":              ticker,
        "company_name":        company_name,
        "sector":              sector,
        "industry":            industry,
        "price":               round(price, 4),
        "open":                (fh_quote or {}).get("o", price),
        "day_high":            (fh_quote or {}).get("h", price),
        "day_low":             (fh_quote or {}).get("l", price),
        "prev_close":          (fh_quote or {}).get("pc", price),
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
        "short_percent_float": short_pct,
        "short_ratio":         short_ratio,
        "analyst_recommendation": analyst_rec,
        "analyst_mean_target":    analyst_target,
        "analyst_count":          analyst_count,
        # Option A
        "earnings_date":       earnings_date,
        "earnings_warning":    earnings_warning,
        "days_to_earnings":    days_to_earnings,
        # Option C
        "sector_etf":          sector_rs.get("etf"),
        "sector_return_20d":   sector_rs.get("sector_return_20d"),
        "stock_vs_sector":     sector_rs.get("stock_vs_sector"),
        "sector_rs_label":     sector_rs.get("label"),
        "_history":            hist,
        "_info":               yf_info,
        "data_sources":        ["Finnhub (real-time)", "Yahoo Finance (history/fundamentals)"],
        "data_fetched_at":     datetime.now().isoformat(),
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
