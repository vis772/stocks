# quant/factor_engine.py
# Statistically grounded multi-factor scoring engine.
# Factors are normalized to z-scores before combining.
# Weights are dynamically updated using rolling 20-day IC (Information Coefficient).
# If no IC history exists, equal weights are used as bootstrap.
#
# Factor groups (default weight shares):
#   Mean Reversion  30%
#   Momentum        30%
#   Quality         20%
#   Catalyst        20%

import json
import time
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from typing import Optional


# ─── Constants ────────────────────────────────────────────────────────────────

QUANT_MODE = True  # Can be overridden by AXIOM_QUANT_MODE env var

# Default weights when no IC history exists
DEFAULT_WEIGHTS: dict = {
    # Mean Reversion
    "z_sma20":        0.055,
    "z_sma50":        0.045,
    "bb_pct":         0.070,
    "rsi_z":          0.075,
    "dist_52w_low":   0.055,
    # Momentum
    "mom_1m":         0.080,
    "mom_3m":         0.070,
    "mom_6m":         0.060,
    "vol_mom":        0.050,
    "rs_iwm_1m":      0.040,
    # Quality
    "gross_margin":   0.045,
    "rev_growth":     0.055,
    "current_ratio":  0.040,
    "de_ratio_inv":   0.030,
    # Catalyst / Sentiment
    "news_sentiment": 0.085,
    "sec_catalyst":   0.095,
}

# Factors where higher raw value = worse (inverted before scoring)
_INVERSE_FACTORS = {"de_ratio_inv"}

# ─── IWM cache (shared across all tickers in a scan cycle) ────────────────────

_IWM_CACHE: dict = {"prices": [], "ts": 0.0}
_IWM_CACHE_TTL = 3600  # seconds


def _get_iwm_prices(period: str = "1y") -> list:
    global _IWM_CACHE
    if time.time() - _IWM_CACHE["ts"] < _IWM_CACHE_TTL and _IWM_CACHE["prices"]:
        return _IWM_CACHE["prices"]
    try:
        hist = yf.Ticker("IWM").history(period=period, auto_adjust=True)
        if hist is None or hist.empty:
            return _IWM_CACHE["prices"]
        closes = [float(c) for c in hist["Close"].dropna().tolist() if c > 0]
        _IWM_CACHE = {"prices": closes, "ts": time.time()}
        return closes
    except Exception:
        return _IWM_CACHE["prices"]


def _safe(v, fb: float = 0.0) -> float:
    try:
        f = float(v)
        return fb if (f != f or f == float("inf") or f == float("-inf")) else f
    except Exception:
        return fb


# ─── Factor Computer ──────────────────────────────────────────────────────────

class FactorComputer:
    """
    Computes all individual factor z-scores for a single ticker.

    Each factor returns {"raw": float, "z": float} where z is the
    cross-sectional z-score (normalized vs universe mean/std).
    """

    def compute_all(self, ticker: str, snapshot: dict, hist: pd.DataFrame,
                    iwm_prices: list) -> dict:
        """
        Returns {factor_name: {"raw": float, "z": float}} for all factors.
        hist: yfinance OHLCV DataFrame (at least 200 rows for full coverage).
        snapshot: output from data.market_data.fetch_ticker_snapshot().
        """
        factors: dict = {}
        try:
            factors.update(self._mean_reversion(snapshot, hist))
        except Exception as e:
            print(f"  [factor] {ticker} mean_reversion error: {e}")

        try:
            factors.update(self._momentum(hist, iwm_prices))
        except Exception as e:
            print(f"  [factor] {ticker} momentum error: {e}")

        try:
            factors.update(self._quality(snapshot))
        except Exception as e:
            print(f"  [factor] {ticker} quality error: {e}")

        try:
            factors.update(self._catalyst(snapshot))
        except Exception as e:
            print(f"  [factor] {ticker} catalyst error: {e}")

        return factors

    # ── Mean Reversion ────────────────────────────────────────────────────────

    def _mean_reversion(self, snapshot: dict, hist: pd.DataFrame) -> dict:
        out: dict = {}
        if hist is None or len(hist) < 20:
            return out

        close = hist["Close"].dropna()
        price = float(close.iloc[-1])
        if price <= 0:
            return out

        # z-score vs SMA20
        if len(close) >= 20:
            window20 = close.tail(20)
            sma20 = float(window20.mean())
            std20 = float(window20.std())
            z20 = _safe((price - sma20) / std20) if std20 > 0 else 0.0
            out["z_sma20"] = {"raw": round((price - sma20) / sma20 * 100, 2) if sma20 > 0 else 0, "z": round(z20, 3)}

        # z-score vs SMA50
        if len(close) >= 50:
            window50 = close.tail(50)
            sma50 = float(window50.mean())
            std50 = float(window50.std())
            z50 = _safe((price - sma50) / std50) if std50 > 0 else 0.0
            out["z_sma50"] = {"raw": round((price - sma50) / sma50 * 100, 2) if sma50 > 0 else 0, "z": round(z50, 3)}

        # Bollinger Band position (0 = lower band, 1 = upper band)
        if len(close) >= 20:
            window20 = close.tail(20)
            sma = float(window20.mean())
            std = float(window20.std())
            upper = sma + 2 * std
            lower = sma - 2 * std
            span = upper - lower
            bb_pct = _safe((price - lower) / span) if span > 0 else 0.5
            bb_pct = float(np.clip(bb_pct, 0.0, 1.0))
            # Map to z-score: 0.5 = neutral, 0 = oversold (+), 1 = overbought (-)
            bb_z = _safe((0.5 - bb_pct) * 4)  # 0.0 → +2, 1.0 → -2
            out["bb_pct"] = {"raw": round(bb_pct, 4), "z": round(bb_z, 3)}

        # RSI (14-day) normalized around 50
        if len(close) >= 15:
            rsi = self._compute_rsi(close, 14)
            rsi_z = _safe((rsi - 50) / 20)  # 30 RSI → +1.0, 70 RSI → -1.0
            # For mean reversion: lower RSI = more oversold = positive signal
            out["rsi_z"] = {"raw": round(rsi, 1), "z": round(-rsi_z, 3)}

        # Distance from 52-week low (normalized)
        if len(close) >= 50:
            prices_yr = close.tail(252)
            low52 = float(prices_yr.min())
            high52 = float(prices_yr.max())
            span52 = high52 - low52
            if span52 > 0:
                dist_low_pct = (price - low52) / span52  # 0 = at low, 1 = at high
                # For mean reversion setup: being near low is a positive signal
                dist_z = _safe((0.2 - dist_low_pct) * 5)  # near low → positive
                out["dist_52w_low"] = {"raw": round(dist_low_pct, 4), "z": round(dist_z, 3)}

        return out

    # ── Momentum ─────────────────────────────────────────────────────────────

    def _momentum(self, hist: pd.DataFrame, iwm_prices: list) -> dict:
        out: dict = {}
        if hist is None or len(hist) < 22:
            return out

        close = hist["Close"].dropna().tolist()
        price = close[-1]
        if price <= 0:
            return out

        # Skip last 5 days to avoid short-term reversal bias
        skip = 5

        # 1-month momentum (21 trading days, skipping last 5)
        if len(close) >= 21 + skip:
            base = close[-(21 + skip)]
            mom_1m = _safe((close[-skip] - base) / base * 100) if base > 0 else 0.0
            out["mom_1m"] = {"raw": round(mom_1m, 2), "z": round(mom_1m / 15, 3)}

        # 3-month momentum (63 trading days)
        if len(close) >= 63 + skip:
            base = close[-(63 + skip)]
            mom_3m = _safe((close[-skip] - base) / base * 100) if base > 0 else 0.0
            out["mom_3m"] = {"raw": round(mom_3m, 2), "z": round(mom_3m / 25, 3)}

        # 6-month momentum (126 trading days)
        if len(close) >= 126 + skip:
            base = close[-(126 + skip)]
            mom_6m = _safe((close[-skip] - base) / base * 100) if base > 0 else 0.0
            out["mom_6m"] = {"raw": round(mom_6m, 2), "z": round(mom_6m / 40, 3)}

        # Volume momentum (relative volume)
        if "Volume" in hist.columns:
            vol = hist["Volume"].dropna()
            if len(vol) >= 20:
                adv20 = float(vol.tail(20).mean())
                today_vol = float(vol.iloc[-1])
                rvol = _safe(today_vol / adv20) if adv20 > 0 else 1.0
                vol_z = _safe((rvol - 1.0) / 0.8)  # rvol=2x → z=+1.25
                out["vol_mom"] = {"raw": round(rvol, 3), "z": round(float(np.clip(vol_z, -3, 3)), 3)}

        # Relative strength vs IWM (1-month)
        if len(iwm_prices) >= 26 and len(close) >= 26:
            stock_ret = _safe((close[-skip] - close[-(21 + skip)]) / close[-(21 + skip)] * 100) if close[-(21 + skip)] > 0 else 0
            iwm_ret   = _safe((iwm_prices[-skip] - iwm_prices[-(21 + skip)]) / iwm_prices[-(21 + skip)] * 100) if iwm_prices[-(21 + skip)] > 0 else 0
            rs_excess = stock_ret - iwm_ret
            out["rs_iwm_1m"] = {"raw": round(rs_excess, 2), "z": round(rs_excess / 20, 3)}

        return out

    # ── Quality ───────────────────────────────────────────────────────────────

    def _quality(self, snapshot: dict) -> dict:
        out: dict = {}

        # Gross margin
        gm = snapshot.get("gross_margins")
        if gm is not None:
            gm_f = _safe(gm)
            gm_z = _safe((gm_f - 0.35) / 0.20)  # 35% = neutral
            out["gross_margin"] = {"raw": round(gm_f, 4), "z": round(float(np.clip(gm_z, -3, 3)), 3)}

        # Revenue growth (QoQ or YoY)
        rev_growth = snapshot.get("revenue_growth")
        if rev_growth is not None:
            rg = _safe(rev_growth)
            rg_z = _safe((rg - 0.10) / 0.30)  # 10% growth = neutral
            out["rev_growth"] = {"raw": round(rg, 4), "z": round(float(np.clip(rg_z, -3, 3)), 3)}

        # Current ratio (higher = more liquid)
        cr = snapshot.get("current_ratio")
        if cr is not None:
            cr_f = _safe(cr)
            cr_z = _safe((cr_f - 1.5) / 1.0)  # 1.5 = neutral
            out["current_ratio"] = {"raw": round(cr_f, 3), "z": round(float(np.clip(cr_z, -3, 3)), 3)}

        # Debt-to-equity (inverted: lower D/E = better)
        de = snapshot.get("debtToEquity") or snapshot.get("debt_to_equity")
        if de is not None:
            de_f = _safe(de)
            de_z = _safe(-(de_f - 50) / 50)  # 50% D/E = neutral, lower = positive
            out["de_ratio_inv"] = {"raw": round(de_f, 3), "z": round(float(np.clip(de_z, -3, 3)), 3)}

        return out

    # ── Catalyst ─────────────────────────────────────────────────────────────

    def _catalyst(self, snapshot: dict) -> dict:
        out: dict = {}

        # News sentiment score (0-100 scale from news_data module)
        ns = snapshot.get("news_sentiment_score") or snapshot.get("sentiment_score")
        if ns is not None:
            ns_f = _safe(ns)
            # Map 0-100 scale to z-score: 50 = neutral
            ns_z = _safe((ns_f - 50) / 20)
            out["news_sentiment"] = {"raw": round(ns_f, 2), "z": round(float(np.clip(ns_z, -3, 3)), 3)}

        # SEC catalyst: 0 (no recent filing) to 1 (fresh 8-K)
        sec_score = snapshot.get("sec_catalyst_score", 0.0)
        if sec_score is not None:
            sc_f = _safe(sec_score)
            sc_z = _safe((sc_f - 0.3) / 0.3)  # 0.3 = neutral baseline
            out["sec_catalyst"] = {"raw": round(sc_f, 3), "z": round(float(np.clip(sc_z, -3, 3)), 3)}

        return out

    # ── RSI helper ────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_rsi(series: pd.Series, period: int = 14) -> float:
        delta = series.diff()
        gain = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return _safe(float(rsi.iloc[-1]), 50.0)


# ─── IC Weight Engine ─────────────────────────────────────────────────────────

class ICWeightEngine:
    """
    Loads rolling IC data from the DB and returns normalized factor weights.
    Falls back to DEFAULT_WEIGHTS when there is insufficient history.
    """

    MIN_HISTORY_DAYS = 5  # need at least this many IC data points per factor

    def get_weights(self, horizon_days: int = 5, regime: str = "NEUTRAL") -> dict:
        """Return {factor_name: weight} normalised to sum=1."""
        from db.database import get_rolling_ic
        ic_data = get_rolling_ic(horizon_days=horizon_days, lookback_days=20)

        if len(ic_data) < len(DEFAULT_WEIGHTS) // 2:
            weights = dict(DEFAULT_WEIGHTS)
        else:
            # Use absolute IC as weight proxy (both + and - IC are informative)
            weights = {k: abs(v) for k, v in ic_data.items() if k in DEFAULT_WEIGHTS}
            # Fill missing factors with small default weight
            for k, v in DEFAULT_WEIGHTS.items():
                if k not in weights:
                    weights[k] = v * 0.5

        weights = self._apply_regime_tilt(weights, regime)
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    def _apply_regime_tilt(self, weights: dict, regime: str) -> dict:
        """Tilt weights based on current market regime."""
        w = dict(weights)
        if regime == "TRENDING_UP":
            for f in ["mom_1m", "mom_3m", "mom_6m", "rs_iwm_1m"]:
                if f in w:
                    w[f] *= 1.4
            for f in ["z_sma20", "bb_pct", "rsi_z"]:
                if f in w:
                    w[f] *= 0.7
        elif regime == "MEAN_REVERSION":
            for f in ["z_sma20", "z_sma50", "bb_pct", "rsi_z", "dist_52w_low"]:
                if f in w:
                    w[f] *= 1.5
            for f in ["mom_1m", "mom_3m"]:
                if f in w:
                    w[f] *= 0.6
        elif regime == "HIGH_VOL":
            # De-risk: upweight quality and catalyst, downweight pure momentum
            for f in ["gross_margin", "current_ratio", "de_ratio_inv"]:
                if f in w:
                    w[f] *= 1.3
            for f in ["mom_1m", "mom_3m", "mom_6m"]:
                if f in w:
                    w[f] *= 0.75
        return w


# ─── Main Scorer ──────────────────────────────────────────────────────────────

class MultiFactorScorer:
    """
    Orchestrates factor computation, IC weighting, and composite score calculation.
    Score is 0-100.
    """

    def __init__(self):
        self._computer = FactorComputer()
        self._weighter = ICWeightEngine()

    def score_ticker(self, ticker: str, snapshot: dict,
                     hist: pd.DataFrame = None,
                     regime: str = "NEUTRAL",
                     log_factors: bool = True) -> dict:
        """
        Compute the multi-factor composite score for a single ticker.

        Returns dict with:
          composite_score   float 0-100
          factor_z_scores   {name: z}
          factor_raw_values {name: raw}
          weights_used      {name: weight}
          top_factors       list of (name, contribution) sorted desc
        """
        import os
        if os.environ.get("AXIOM_QUANT_MODE", "1") != "1":
            return {"composite_score": 50.0, "factor_z_scores": {}, "factor_raw_values": {}}

        t0 = time.time()

        # Fetch history if not provided
        if hist is None or hist.empty:
            try:
                hist = yf.Ticker(ticker).history(period="1y", auto_adjust=True)
            except Exception:
                hist = pd.DataFrame()

        # Get IWM prices
        iwm_prices = _get_iwm_prices()

        # Compute all factors
        factors = self._computer.compute_all(ticker, snapshot, hist, iwm_prices)

        if not factors:
            return {"composite_score": 50.0, "factor_z_scores": {}, "factor_raw_values": {}}

        # Get IC-weighted factor weights
        weights = self._weighter.get_weights(horizon_days=5, regime=regime)

        # Compute weighted composite
        z_scores = {k: _safe(v["z"]) for k, v in factors.items()}
        raw_vals  = {k: _safe(v["raw"]) for k, v in factors.items()}

        total_w = 0.0
        weighted_sum = 0.0
        contributions = []
        for fname, w in weights.items():
            if fname not in z_scores:
                continue
            z = float(np.clip(z_scores[fname], -3.0, 3.0))
            contrib = w * z
            weighted_sum += contrib
            total_w += w
            contributions.append((fname, round(contrib, 4)))

        # Map weighted_sum from [-3, +3] range → [0, 100]
        if total_w > 0:
            norm = weighted_sum / total_w  # still in z-score space
        else:
            norm = 0.0
        composite = float(np.clip((norm + 3) / 6 * 100, 0, 100))

        contributions.sort(key=lambda x: abs(x[1]), reverse=True)

        result = {
            "composite_score":   round(composite, 1),
            "factor_z_scores":   {k: round(v, 3) for k, v in z_scores.items()},
            "factor_raw_values": {k: round(v, 4) for k, v in raw_vals.items()},
            "weights_used":      {k: round(v, 4) for k, v in weights.items() if k in z_scores},
            "top_factors":       contributions[:5],
            "compute_ms":        round((time.time() - t0) * 1000, 1),
        }

        if log_factors:
            self._log_to_db(ticker, z_scores, raw_vals)

        return result

    def _log_to_db(self, ticker: str, z_scores: dict, raw_vals: dict) -> None:
        try:
            from db.database import save_factor_scores
            today = date.today().isoformat()
            combined = {k: {"raw": raw_vals.get(k), "z": z_scores[k]} for k in z_scores}
            save_factor_scores(today, ticker, combined)
        except Exception as e:
            print(f"  [factor_engine] log_to_db failed for {ticker}: {e}")


# ─── IC Computation ───────────────────────────────────────────────────────────

def compute_and_store_ic(horizon_days: int = 5) -> dict:
    """
    Join factor_scores with signal_outcomes to compute per-factor IC.
    Stores results in factor_ic_history. Call daily after market close.
    Returns {factor_name: ic_value}.
    """
    try:
        from db.database import (
            _is_postgres, _get_pg_conn, _get_sqlite_conn, save_ic_history
        )
        calc_date = date.today().isoformat()
        cutoff_date = (date.today() - timedelta(days=horizon_days + 2)).isoformat()

        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT fs.factor_name, fs.z_score, so.ret_5d
                FROM factor_scores fs
                JOIN signal_log sl
                  ON sl.ticker = fs.ticker
                  AND DATE(sl.created_at) = fs.scan_date
                JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE fs.scan_date <= %s
                  AND so.ret_5d IS NOT NULL
                  AND fs.z_score IS NOT NULL
            """, (cutoff_date,))
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT fs.factor_name, fs.z_score, so.ret_5d
                FROM factor_scores fs
                JOIN signal_log sl
                  ON sl.ticker = fs.ticker
                  AND DATE(sl.created_at) = fs.scan_date
                JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE fs.scan_date <= ?
                  AND so.ret_5d IS NOT NULL
                  AND fs.z_score IS NOT NULL
            """, (cutoff_date,))
            rows = cur.fetchall(); conn.close()

        if not rows:
            print("  [ic] No data yet for IC computation")
            return {}

        # Group by factor
        factor_data: dict = {}
        for fname, z, ret in rows:
            if fname not in factor_data:
                factor_data[fname] = {"zs": [], "rets": []}
            factor_data[fname]["zs"].append(float(z))
            factor_data[fname]["rets"].append(float(ret))

        ic_results: dict = {}
        for fname, d in factor_data.items():
            if len(d["zs"]) < 10:
                continue
            z_arr = np.array(d["zs"])
            r_arr = np.array(d["rets"])
            if np.std(z_arr) == 0 or np.std(r_arr) == 0:
                continue
            ic = float(np.corrcoef(z_arr, r_arr)[0, 1])
            if not np.isnan(ic):
                ic_results[fname] = round(ic, 4)
                save_ic_history(calc_date, fname, ic, len(d["zs"]), horizon_days)

        print(f"  [ic] Computed IC for {len(ic_results)} factors ({len(rows)} data points)")
        return ic_results

    except Exception as e:
        print(f"  [ic] compute_and_store_ic failed: {e}")
        return {}


# ─── Batch Screener (for 2000+ universe morning scan) ─────────────────────────

def batch_pre_screen(tickers: list, max_workers: int = 20,
                     timeout_per_ticker: float = 3.0) -> list:
    """
    Lightweight pre-screen across entire universe using only Finnhub quotes.
    Computes a quick activity score (gap + rvol) to select top N for full scoring.

    Returns list of (ticker, activity_score, price, change_pct, rvol) sorted desc.
    Uses utils.fh_get so 429 rate-limit responses are automatically retried once.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        from utils import fh_get as _fhg
    except Exception:
        # Fallback if utils not available: plain requests with no retry
        import requests as _requests
        def _fhg(endpoint, params, timeout=8):
            key = os.environ.get("FINNHUB_API_KEY", "")
            if not key:
                return None
            try:
                r = _requests.get(
                    f"https://finnhub.io/api/v1/{endpoint}",
                    params={**params, "token": key},
                    timeout=timeout,
                )
                return r.json() if r.status_code == 200 else None
            except Exception:
                return None

    if not os.environ.get("FINNHUB_API_KEY", ""):
        print("  [batch_screen] No FINNHUB_API_KEY — skipping pre-screen")
        return [(t, 0.0, 0.0, 0.0, 1.0) for t in tickers[:200]]

    def _fetch_one(ticker: str):
        try:
            d = _fhg("quote", {"symbol": ticker}, timeout=timeout_per_ticker)
            if not d:
                return None
            price = float(d.get("c") or 0)
            prev  = float(d.get("pc") or 0)
            vol   = float(d.get("v") or 0)
            if price <= 0 or prev <= 0:
                return None
            chg_pct = (price - prev) / prev * 100
            # Activity score: |gap| + rvol proxy
            activity = abs(chg_pct) * 2 + min(vol / 500_000, 5.0)
            return (ticker, round(activity, 2), round(price, 4), round(chg_pct, 2), 1.0)
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_one, t): t for t in tickers}
        for fut in as_completed(futures, timeout=120):
            try:
                res = fut.result()
                if res:
                    results.append(res)
            except Exception:
                pass

    results.sort(key=lambda x: x[1], reverse=True)
    print(f"  [batch_screen] Screened {len(tickers)} → {len(results)} active tickers")
    return results
