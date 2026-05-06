# analysis/technicals.py
# Computes technical indicators and scores from price/volume history.
#
# IMPORTANT: Technical indicators are descriptive, not predictive.
# An RSI of 30 doesn't mean "it will bounce." It means "it has been
# selling off recently." Use these to understand context, not as signals.

import pandas as pd
import numpy as np
from typing import Dict, Optional


def compute_technicals(hist: pd.DataFrame) -> Dict:
    """
    Compute all technical indicators from an OHLCV DataFrame.

    Args:
        hist: DataFrame with columns Open, High, Low, Close, Volume
              (as returned by yfinance .history())

    Returns:
        Dict of indicator values and a technical_score (0-100).
    """
    if hist is None or len(hist) < 14:
        return _empty_technicals()

    close   = hist["Close"]
    volume  = hist["Volume"]
    high    = hist["High"]
    low     = hist["Low"]

    result = {}

    # ── Moving Averages ────────────────────────────────────────────────────────
    result["sma_20"]  = round(close.tail(20).mean(), 4) if len(close) >= 20 else None
    result["sma_50"]  = round(close.tail(50).mean(), 4) if len(close) >= 50 else None
    result["ema_9"]   = round(_ema(close, 9).iloc[-1], 4) if len(close) >= 9 else None

    price_now = close.iloc[-1]
    result["price"]   = round(price_now, 4)

    # Price vs moving averages
    result["above_sma20"] = price_now > result["sma_20"] if result["sma_20"] else None
    result["above_sma50"] = price_now > result["sma_50"] if result["sma_50"] else None

    # ── RSI ────────────────────────────────────────────────────────────────────
    result["rsi_14"] = round(_rsi(close, 14), 1) if len(close) >= 15 else None

    # ── MACD ───────────────────────────────────────────────────────────────────
    macd_line, signal_line = _macd(close)
    if macd_line is not None:
        result["macd"]        = round(macd_line, 4)
        result["macd_signal"] = round(signal_line, 4)
        result["macd_bullish"] = macd_line > signal_line
    else:
        result["macd"]         = None
        result["macd_signal"]  = None
        result["macd_bullish"] = None

    # ── Volume Analysis ────────────────────────────────────────────────────────
    avg_vol_20 = volume.tail(20).mean()
    today_vol  = volume.iloc[-1]
    result["relative_volume"] = round(today_vol / avg_vol_20, 2) if avg_vol_20 > 0 else 0
    result["avg_volume_20d"]  = int(avg_vol_20)
    result["today_volume"]    = int(today_vol)

    # ── Momentum ───────────────────────────────────────────────────────────────
    result["return_1d"]  = round((close.iloc[-1] / close.iloc[-2] - 1) * 100, 2) if len(close) >= 2 else 0
    result["return_5d"]  = round((close.iloc[-1] / close.iloc[-6] - 1) * 100, 2) if len(close) >= 6 else 0
    result["return_20d"] = round((close.iloc[-1] / close.iloc[-21] - 1) * 100, 2) if len(close) >= 21 else 0

    # ── Volatility ─────────────────────────────────────────────────────────────
    # Annualized standard deviation of daily returns
    daily_returns = close.pct_change().dropna()
    result["volatility_30d_ann"] = round(daily_returns.tail(30).std() * np.sqrt(252) * 100, 1) if len(daily_returns) >= 10 else None

    # ── Breakout / Gap Detection ───────────────────────────────────────────────
    # Gap-up: today's open > yesterday's high
    if len(hist) >= 2:
        yesterday_high = high.iloc[-2]
        today_open     = hist["Open"].iloc[-1]
        gap_pct = (today_open - yesterday_high) / yesterday_high * 100
        result["gap_up"]   = gap_pct > 2.0    # >2% gap up
        result["gap_down"] = gap_pct < -2.0   # >2% gap down
        result["gap_pct"]  = round(gap_pct, 2)
    else:
        result["gap_up"] = result["gap_down"] = False
        result["gap_pct"] = 0

    # ── Support / Resistance (simple: 20-day high/low) ─────────────────────────
    result["resistance_20d"] = round(high.tail(20).max(), 4)
    result["support_20d"]    = round(low.tail(20).min(), 4)
    result["near_resistance"] = price_now >= result["resistance_20d"] * 0.97  # Within 3%
    result["near_support"]    = price_now <= result["support_20d"] * 1.03     # Within 3%

    # ── Technical Score (0-100) ────────────────────────────────────────────────
    result["technical_score"] = _compute_technical_score(result)

    return result


def _compute_technical_score(t: Dict) -> float:
    """
    Translate technical indicators into a 0-100 score.

    This is intentionally transparent — each component is documented.
    THESE WEIGHTS ARE ASSUMPTIONS. Tune them based on your own results.
    """
    score = 50.0  # Start neutral

    # ── RSI Component (–15 to +15) ──────────────────────────────────────────
    rsi = t.get("rsi_14")
    if rsi is not None:
        if 45 <= rsi <= 65:
            score += 10    # Healthy momentum range
        elif rsi < 35:
            score += 5     # Oversold — potential bounce setup
        elif rsi > 75:
            score -= 10    # Overbought — risky entry
        elif rsi > 65:
            score -= 3     # Getting extended

    # ── MACD Component (–10 to +10) ─────────────────────────────────────────
    if t.get("macd_bullish") is True:
        score += 8
    elif t.get("macd_bullish") is False:
        score -= 8

    # ── Moving Average Alignment (–10 to +10) ───────────────────────────────
    above_20 = t.get("above_sma20")
    above_50 = t.get("above_sma50")
    if above_20 and above_50:
        score += 10    # Price above both MAs = uptrend
    elif above_20 and not above_50:
        score += 3     # Short-term bullish, medium bearish
    elif not above_20 and above_50:
        score -= 3
    elif not above_20 and not above_50:
        score -= 10    # Below both = downtrend

    # ── Relative Volume (0 to +12) ───────────────────────────────────────────
    rvol = t.get("relative_volume", 0)
    if rvol >= 3.0:
        score += 12    # Major volume spike — something is happening
    elif rvol >= 2.0:
        score += 8
    elif rvol >= 1.5:
        score += 4
    elif rvol < 0.5:
        score -= 5     # Very low volume — weak conviction

    # ── Momentum (–10 to +10) ───────────────────────────────────────────────
    ret5 = t.get("return_5d", 0)
    if ret5 > 15:
        score += 10
    elif ret5 > 5:
        score += 6
    elif ret5 > 0:
        score += 2
    elif ret5 < -20:
        score -= 10
    elif ret5 < -10:
        score -= 6
    elif ret5 < 0:
        score -= 2

    # ── Gap-Up Bonus ─────────────────────────────────────────────────────────
    if t.get("gap_up"):
        score += 8
    elif t.get("gap_down"):
        score -= 8

    # Cap to 0-100
    return round(max(0, min(100, score)), 1)


# ─── Indicator Helpers ────────────────────────────────────────────────────────

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def _macd(series: pd.Series, fast=12, slow=26, signal=9):
    if len(series) < slow + signal:
        return None, None
    ema_fast   = _ema(series, fast)
    ema_slow   = _ema(series, slow)
    macd_line  = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1])


def _empty_technicals() -> Dict:
    """Return empty technicals dict when we don't have enough data."""
    return {
        "technical_score": 50,
        "rsi_14": None, "macd": None, "macd_bullish": None,
        "above_sma20": None, "above_sma50": None,
        "relative_volume": 0, "return_5d": 0, "return_20d": 0,
        "volatility_30d_ann": None, "gap_up": False, "gap_down": False,
        "gap_pct": 0, "resistance_20d": None, "support_20d": None,
    }


def suggest_entry_and_stops(snapshot: Dict, technicals: Dict) -> Dict:
    """
    Suggest entry zone, stop-loss, and profit targets based on recent price action.

    These are STARTING POINTS for your own analysis, not trade instructions.
    Always use your own judgment and risk management.
    """
    price = snapshot.get("price", 0)
    if price == 0:
        return {}

    support = technicals.get("support_20d") or price * 0.85
    resistance = technicals.get("resistance_20d") or price * 1.15

    return {
        "entry_zone_low":   round(price * 0.97, 4),    # Within 3% of current
        "entry_zone_high":  round(price * 1.02, 4),    # Up to 2% above current
        "stop_loss":        round(max(support, price * 0.88), 4),  # Support or -12%
        "target_1":         round(resistance, 4),                   # First resistance
        "target_2":         round(price * 1.25, 4),                # +25%
        "target_3":         round(price * 1.50, 4),                # +50%
        "risk_reward_at_t1": round((resistance - price) / (price - max(support, price * 0.88)), 2),
    }
