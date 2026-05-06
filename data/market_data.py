# data/market_data.py
# Fetches price, volume, fundamentals, and technicals from yfinance.
# yfinance is free but rate-limited and occasionally unreliable.
# All functions return None or empty DataFrame on failure — never crash the scanner.

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time
import warnings
warnings.filterwarnings("ignore")

from config import MIN_MARKET_CAP, MAX_MARKET_CAP, MIN_AVG_VOLUME


def fetch_ticker_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Pull a complete snapshot for one ticker: price, volume, market cap,
    fundamentals, and enough history for technical analysis.

    Returns a dict or None if the ticker is invalid/unavailable.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info

        # Bail early if we can't get basic price data
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price or price <= 0:
            return None

        market_cap = info.get("marketCap", 0)
        avg_volume  = info.get("averageVolume", 0) or info.get("averageDailyVolume10Day", 0)

        # Pull 60 days of OHLCV for technical analysis
        hist = t.history(period="60d", auto_adjust=True)
        if hist.empty or len(hist) < 10:
            return None

        today_volume = int(hist["Volume"].iloc[-1]) if len(hist) > 0 else 0
        avg_vol_20   = int(hist["Volume"].tail(20).mean()) if len(hist) >= 20 else avg_volume

        return {
            # Identity
            "ticker":           ticker.upper(),
            "company_name":     info.get("longName", ticker),
            "sector":           info.get("sector", "Unknown"),
            "industry":         info.get("industry", "Unknown"),

            # Price
            "price":            round(price, 4),
            "open":             info.get("open", price),
            "day_high":         info.get("dayHigh", price),
            "day_low":          info.get("dayLow", price),
            "week_52_high":     info.get("fiftyTwoWeekHigh", price),
            "week_52_low":      info.get("fiftyTwoWeekLow", price),

            # Size & liquidity
            "market_cap":       market_cap,
            "shares_outstanding": info.get("sharesOutstanding", 0),
            "float_shares":     info.get("floatShares", 0),
            "volume":           today_volume,
            "avg_volume":       avg_vol_20,
            "relative_volume":  round(today_volume / avg_vol_20, 2) if avg_vol_20 > 0 else 0,

            # Fundamentals
            "pe_ratio":         info.get("trailingPE"),
            "forward_pe":       info.get("forwardPE"),
            "ps_ratio":         info.get("priceToSalesTrailing12Months"),
            "pb_ratio":         info.get("priceToBook"),
            "revenue":          info.get("totalRevenue"),
            "revenue_growth":   info.get("revenueGrowth"),       # YoY as decimal
            "gross_margins":    info.get("grossMargins"),
            "ebitda":           info.get("ebitda"),
            "total_cash":       info.get("totalCash"),
            "total_debt":       info.get("totalDebt"),
            "free_cashflow":    info.get("freeCashflow"),
            "short_ratio":      info.get("shortRatio"),           # Days to cover
            "short_percent_float": info.get("shortPercentOfFloat"),  # 0.0–1.0

            # Analyst sentiment
            "analyst_recommendation": info.get("recommendationKey", "none"),
            "analyst_mean_target":    info.get("targetMeanPrice"),
            "analyst_count":          info.get("numberOfAnalystOpinions", 0),

            # Raw history for technical calculations
            "_history":         hist,
            "_info":            info,

            "data_fetched_at":  datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"  [market_data] Failed to fetch {ticker}: {e}")
        return None


def passes_universe_filter(snapshot: Dict[str, Any]) -> tuple[bool, str]:
    """
    Check if a ticker belongs in our scan universe.
    Returns (passes: bool, reason: str).

    This is a hard filter — if it fails, we skip scoring entirely.
    """
    mc  = snapshot.get("market_cap", 0)
    vol = snapshot.get("avg_volume", 0)
    px  = snapshot.get("price", 0)

    if mc < MIN_MARKET_CAP:
        return False, f"Market cap ${mc/1e6:.1f}M below ${MIN_MARKET_CAP/1e6:.0f}M floor"
    if mc > MAX_MARKET_CAP:
        return False, f"Market cap ${mc/1e9:.1f}B above ${MAX_MARKET_CAP/1e9:.0f}B ceiling — too large"
    if vol < MIN_AVG_VOLUME:
        return False, f"Avg volume {vol:,} below {MIN_AVG_VOLUME:,} — liquidity risk"
    if px < 0.50:
        return False, f"Price ${px:.4f} below $0.50 — near-OTC territory"

    return True, "Passes universe filter"


def batch_fetch(tickers: list, delay: float = 0.5) -> Dict[str, Dict]:
    """
    Fetch snapshots for a list of tickers with a small delay to be
    polite to Yahoo Finance's servers and avoid getting rate-limited.

    Returns a dict of {ticker: snapshot} for successful fetches only.
    """
    results = {}
    for ticker in tickers:
        snap = fetch_ticker_snapshot(ticker)
        if snap is not None:
            results[ticker] = snap
        time.sleep(delay)   # Be a good citizen
    return results


def get_price_history(ticker: str, days: int = 90) -> pd.DataFrame:
    """Return OHLCV history for charting. Separate from snapshot for UI use."""
    try:
        t = yf.Ticker(ticker)
        return t.history(period=f"{days}d", auto_adjust=True)
    except Exception:
        return pd.DataFrame()
