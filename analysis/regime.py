# analysis/regime.py
# Market regime detection using IWM (Russell 2000) as the small-cap benchmark.
#
# Regimes:
#   TRENDING_UP    IWM 20-MA > 50-MA, positive 20-day momentum
#   TRENDING_DOWN  IWM 20-MA < 50-MA, negative 20-day momentum
#   MEAN_REVERSION IWM in tight range, ADX < 20
#   HIGH_VOL       Realized 20-day volatility > 25% annualized
#
# Regime is recomputed daily and cached in market_regime table.

import numpy as np
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from typing import Optional


def _safe(v, fb: float = 0.0) -> float:
    try:
        f = float(v)
        return fb if (f != f or f == float("inf") or f == float("-inf")) else f
    except Exception:
        return fb


def _compute_adx(high: pd.Series, low: pd.Series, close: pd.Series,
                 period: int = 14) -> float:
    """Wilder's ADX using pandas. Returns ADX value or 20 (neutral) on failure."""
    try:
        n = min(len(high), len(low), len(close))
        if n < period * 2:
            return 20.0
        h = high.tail(n).reset_index(drop=True)
        lo = low.tail(n).reset_index(drop=True)
        c = close.tail(n).reset_index(drop=True)

        tr_list, plus_dm_list, minus_dm_list = [], [], []
        for i in range(1, n):
            tr = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
            plus  = max(h[i] - h[i - 1], 0) if (h[i] - h[i - 1]) > (lo[i - 1] - lo[i]) else 0
            minus = max(lo[i - 1] - lo[i], 0) if (lo[i - 1] - lo[i]) > (h[i] - h[i - 1]) else 0
            tr_list.append(tr)
            plus_dm_list.append(plus)
            minus_dm_list.append(minus)

        def _wilder_smooth(data: list, p: int) -> list:
            result = [sum(data[:p])]
            for i in range(p, len(data)):
                result.append(result[-1] - result[-1] / p + data[i])
            return result

        atr  = _wilder_smooth(tr_list, period)
        pdm  = _wilder_smooth(plus_dm_list, period)
        mdm  = _wilder_smooth(minus_dm_list, period)

        dx_list = []
        for a, p_, m in zip(atr, pdm, mdm):
            if a == 0:
                continue
            pdi = p_ / a * 100
            mdi = m / a * 100
            dsum = pdi + mdi
            dx_list.append(abs(pdi - mdi) / dsum * 100 if dsum > 0 else 0)

        if len(dx_list) < period:
            return 20.0
        adx_raw = sum(dx_list[-period:]) / period
        return _safe(adx_raw, 20.0)
    except Exception:
        return 20.0


def detect_regime(iwm_hist: pd.DataFrame = None) -> dict:
    """
    Detect market regime from IWM OHLCV data.
    Fetches fresh IWM data if iwm_hist is not provided.

    Returns dict with: regime, iwm_price, iwm_ma20, iwm_ma50, adx_14, volatility_20d
    """
    if iwm_hist is None or iwm_hist.empty:
        try:
            iwm_hist = yf.Ticker("IWM").history(period="6mo", auto_adjust=True)
        except Exception as e:
            print(f"  [regime] IWM fetch failed: {e}")
            return {"regime": "UNKNOWN", "iwm_price": 0, "iwm_ma20": 0,
                    "iwm_ma50": 0, "adx_14": 20, "volatility_20d": 0}

    if len(iwm_hist) < 50:
        return {"regime": "UNKNOWN", "iwm_price": 0, "iwm_ma20": 0,
                "iwm_ma50": 0, "adx_14": 20, "volatility_20d": 0}

    close = iwm_hist["Close"].dropna()
    high  = iwm_hist["High"].dropna()
    low   = iwm_hist["Low"].dropna()

    iwm_price = _safe(float(close.iloc[-1]))
    ma20 = _safe(float(close.tail(20).mean()))
    ma50 = _safe(float(close.tail(50).mean()))
    adx  = _compute_adx(high, low, close)

    # 20-day realized volatility (annualized)
    log_rets = np.log(close / close.shift(1)).dropna()
    vol20 = _safe(float(log_rets.tail(20).std()) * np.sqrt(252) * 100)

    # 20-day momentum
    mom20 = _safe((float(close.iloc[-1]) - float(close.iloc[-21])) / float(close.iloc[-21]) * 100
                  if len(close) >= 21 and close.iloc[-21] > 0 else 0)

    # Regime classification
    high_vol_threshold = 25.0  # % annualized

    if vol20 > high_vol_threshold:
        regime = "HIGH_VOL"
    elif adx < 20:
        regime = "MEAN_REVERSION"
    elif ma20 > ma50 and mom20 > 0:
        regime = "TRENDING_UP"
    elif ma20 < ma50 and mom20 < 0:
        regime = "TRENDING_DOWN"
    else:
        regime = "NEUTRAL"

    return {
        "regime":        regime,
        "iwm_price":     round(iwm_price, 2),
        "iwm_ma20":      round(ma20, 2),
        "iwm_ma50":      round(ma50, 2),
        "adx_14":        round(adx, 1),
        "volatility_20d": round(vol20, 1),
        "mom_20d":       round(mom20, 2),
    }


def update_daily_regime() -> dict:
    """
    Detect today's regime and persist to DB. Call once at market open.
    Returns the regime dict.
    """
    result = detect_regime()
    if result.get("regime") and result["regime"] != "UNKNOWN":
        try:
            from db.database import save_market_regime
            save_market_regime(
                regime_date   = date.today().isoformat(),
                regime        = result["regime"],
                iwm_price     = result["iwm_price"],
                iwm_ma20      = result["iwm_ma20"],
                iwm_ma50      = result["iwm_ma50"],
                adx_14        = result["adx_14"],
                volatility_20d = result["volatility_20d"],
            )
            print(f"  [regime] {result['regime']} | IWM ${result['iwm_price']} | "
                  f"MA20/50 ${result['iwm_ma20']}/{result['iwm_ma50']} | "
                  f"ADX {result['adx_14']} | Vol {result['volatility_20d']}%")
        except Exception as e:
            print(f"  [regime] DB save failed: {e}")
    return result


def get_current_regime() -> dict:
    """
    Return cached regime from DB if today's row exists, else recompute.
    """
    try:
        from db.database import get_latest_regime
        cached = get_latest_regime()
        if cached.get("date") == date.today().isoformat():
            return cached
    except Exception:
        pass
    return update_daily_regime()


REGIME_DISPLAY = {
    "TRENDING_UP":    ("Trending Up",    "#16a34a", "↑"),
    "TRENDING_DOWN":  ("Trending Down",  "#dc2626", "↓"),
    "MEAN_REVERSION": ("Mean Reversion", "#c2610f", "↔"),
    "HIGH_VOL":       ("High Volatility","#6d28d9", "⚡"),
    "NEUTRAL":        ("Neutral",        "#94a3b8", "—"),
    "UNKNOWN":        ("Unknown",        "#94a3b8", "?"),
}


def regime_label(regime: str) -> tuple:
    """Return (display_name, color_hex, symbol) for a regime string."""
    return REGIME_DISPLAY.get(regime, REGIME_DISPLAY["UNKNOWN"])
