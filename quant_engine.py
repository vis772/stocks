# quant_engine.py
# Additive quant scoring layer. Produces quant_adjustment ∈ [-25, +25].
# Never replaces the existing composite score — only adjusts it.
# All per-ticker computation must complete in <500ms or the block is skipped.

import time
import numpy as np
import yfinance as yf
from typing import Optional


def _safe(val: float, fallback: float = 0.0) -> float:
    """Return fallback if val is NaN / inf / None."""
    try:
        v = float(val)
        return fallback if (v != v or v == float("inf") or v == float("-inf")) else v
    except Exception:
        return fallback


def _prices(ticker: str, period: str = "60d") -> list:
    """Fetch adjusted close prices as a plain list. Returns [] on failure."""
    try:
        hist = yf.Ticker(ticker).history(period=period, auto_adjust=True)
        if hist is None or hist.empty:
            return []
        closes = hist["Close"].dropna().tolist()
        return [float(c) for c in closes if c > 0]
    except Exception:
        return []


def _hist_ohlcv(ticker: str, period: str = "60d"):
    """Return (closes, highs, lows) lists. All empty on failure."""
    try:
        hist = yf.Ticker(ticker).history(period=period, auto_adjust=True)
        if hist is None or hist.empty:
            return [], [], []
        c = hist["Close"].dropna().tolist()
        h = hist["High"].dropna().tolist()
        lo = hist["Low"].dropna().tolist()
        n = min(len(c), len(h), len(lo))
        return [float(x) for x in c[:n]], [float(x) for x in h[:n]], [float(x) for x in lo[:n]]
    except Exception:
        return [], [], []


class QuantEngine:
    """
    Additive quant layer. Returns quant_adjustment ∈ [-25, +25].
    Integrated into the prediction scan after the existing composite scorer.
    """

    # Shared universe momentum stats, updated each scan cycle
    _universe_mom: list = []
    _universe_mu: float = 0.0
    _universe_sigma: float = 1.0
    _universe_ps: list = []

    @classmethod
    def update_universe_stats(cls, all_tickers: list, scan_results: dict = None) -> None:
        """Compute cross-sectional momentum mean/std across all tickers. Call once per cycle."""
        moms = []
        for t in all_tickers:
            p = _prices(t, "30d")
            if len(p) < 20:
                continue
            roc5  = (p[-1] - p[-5])  / p[-5]  * 100 if len(p) >= 5  and p[-5]  > 0 else 0
            roc10 = (p[-1] - p[-10]) / p[-10] * 100 if len(p) >= 10 and p[-10] > 0 else 0
            roc20 = (p[-1] - p[-20]) / p[-20] * 100 if len(p) >= 20 and p[-20] > 0 else 0
            moms.append(0.5 * roc5 + 0.3 * roc10 + 0.2 * roc20)
        cls._universe_mom = moms
        if len(moms) >= 2:
            arr = np.array(moms)
            cls._universe_mu    = float(np.mean(arr))
            cls._universe_sigma = float(np.std(arr)) or 1.0
        else:
            cls._universe_mu    = 0.0
            cls._universe_sigma = 1.0
        cls._universe_ps = []
        if scan_results:
            for t in all_tickers:
                sr = scan_results.get(t, {})
                ps = sr.get("ps_ratio") or sr.get("priceToSalesTrailing12Months")
                if ps is not None:
                    cls._universe_ps.append(_safe(ps))

    # ── Momentum block (max +12 pts) ─────────────────────────────────────────

    def momentum_score(self, ticker: str, prices: list, iwm_prices: list) -> float:
        if len(prices) < 20:
            return 0.0

        p = prices
        roc5  = _safe((p[-1] - p[-5])  / p[-5]  * 100) if len(p) >= 5  and p[-5]  > 0 else 0
        roc10 = _safe((p[-1] - p[-10]) / p[-10] * 100) if len(p) >= 10 and p[-10] > 0 else 0
        roc20 = _safe((p[-1] - p[-20]) / p[-20] * 100) if len(p) >= 20 and p[-20] > 0 else 0

        mom = 0.5 * roc5 + 0.3 * roc10 + 0.2 * roc20
        sigma = self.__class__._universe_sigma or 1.0
        z = (mom - self.__class__._universe_mu) / sigma
        mom_contrib = float(np.clip(z * 4, -8, 8))

        # Relative strength vs IWM (Russell 2000 proxy)
        rs_contrib = 0.0
        if len(iwm_prices) >= 20 and iwm_prices[-20] > 0 and p[-20] > 0:
            stock_ret = (p[-1] - p[-20]) / p[-20]
            iwm_ret   = (iwm_prices[-1] - iwm_prices[-20]) / iwm_prices[-20]
            if iwm_ret != 0:
                rs = stock_ret / iwm_ret
            else:
                rs = 1.0 if stock_ret > 0 else 0.5
            if rs > 1.3:
                rs_contrib = 4.0
            elif rs > 1.0:
                rs_contrib = 2.0
            elif rs < 0.6:
                rs_contrib = -4.0
            elif rs < 0.8:
                rs_contrib = -2.0

        return float(np.clip(mom_contrib + rs_contrib, -12, 12))

    # ── Volume block (max +8 pts) ─────────────────────────────────────────────

    def volume_score(self, ticker: str, volume_today: float, adv_20: float) -> float:
        if adv_20 <= 0:
            return 0.0
        rvol = _safe(volume_today / adv_20)
        if rvol >= 3.0:
            return 8.0
        if rvol >= 2.0:
            return 6.0
        if rvol >= 1.5:
            return 4.0
        if rvol >= 1.0:
            return 0.0
        if rvol < 0.5:
            return -4.0
        return 0.0

    # ── Mean reversion block (max +5 pts) ─────────────────────────────────────

    def mean_reversion_score(self, ticker: str, prices: list) -> float:
        if len(prices) < 20:
            return 0.0

        window = prices[-20:]
        sma20 = _safe(np.mean(window))
        s20   = _safe(np.std(window))
        if s20 == 0:
            return 0.0
        upper   = sma20 + 2 * s20
        lower   = sma20 - 2 * s20
        span    = upper - lower
        bb_pct  = _safe((prices[-1] - lower) / span) if span > 0 else 0.5
        bb_pct  = float(np.clip(bb_pct, 0.0, 1.0))

        # Wilder's RSI
        rets = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        rets14 = rets[-14:] if len(rets) >= 14 else rets
        if not rets14:
            return 0.0
        gains  = [max(r, 0) for r in rets14]
        losses = [abs(min(r, 0)) for r in rets14]
        avg_g  = _safe(np.mean(gains))
        avg_l  = _safe(np.mean(losses))
        rs     = avg_g / avg_l if avg_l > 0 else 100.0
        rsi    = _safe(100 - 100 / (1 + rs))

        if rsi < 30 and bb_pct < 0.1:
            return 5.0
        if rsi < 35 and bb_pct < 0.2:
            return 3.0
        if rsi > 75 and bb_pct > 0.9:
            return -5.0
        if rsi > 70 and bb_pct > 0.85:
            return -3.0
        return 0.0

    # ── Volatility/risk penalty (max -10 pts) ─────────────────────────────────

    def volatility_penalty(self, prices: list, highs: list = None, lows: list = None) -> float:
        if len(prices) < 2:
            return 0.0

        log_rets = [np.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))
                    if prices[i - 1] > 0 and prices[i] > 0]
        if len(log_rets) < 5:
            return 0.0
        sigma_hist = _safe(float(np.std(log_rets[-20:])) * np.sqrt(252))

        if sigma_hist <= 0.80:
            return 0.0
        if sigma_hist <= 1.20:
            return -3.0
        if sigma_hist <= 1.60:
            return -6.0
        return -10.0

    # ── Factor exposure block (max +5 pts) ────────────────────────────────────

    def factor_score(self, fundamentals: dict) -> float:
        score = 0.0
        mktcap = fundamentals.get("market_cap", 0) or 0
        pb     = fundamentals.get("pb_ratio") or fundamentals.get("priceToBook")
        ps     = fundamentals.get("ps_ratio") or fundamentals.get("priceToSalesTrailing12Months")
        gm     = fundamentals.get("gross_margins") or fundamentals.get("grossMargins")
        de     = fundamentals.get("debt_to_equity") or fundamentals.get("debtToEquity")

        # Size factor
        if 100_000_000 <= mktcap <= 500_000_000:
            score += 2.0
        elif 500_000_000 < mktcap <= 1_000_000_000:
            score += 1.0

        # Value factor
        pb_val = float(pb) if pb is not None else None
        ps_val = float(ps) if ps is not None else None
        if pb_val is not None and ps_val is not None:
            if pb_val < 3.0 and ps_val < 5.0:
                score += 2.0
            elif pb_val < 5.0:
                score += 1.0
            if pb_val > 20:
                score -= 2.0

        # Quality factor
        gm_val = float(gm) if gm is not None else None
        de_val = float(de) if de is not None else None
        if gm_val is not None and gm_val > 0.40:
            score += 1.0
        if de_val is not None and de_val > 3.0:
            score -= 2.0

        return float(np.clip(score, -5, 5))

    # ── ATR helper (used by ConvictionEngine too) ─────────────────────────────

    @staticmethod
    def compute_atr(prices: list, highs: list, lows: list, period: int = 14) -> float:
        """Wilder's ATR(14). Returns 0 on insufficient data."""
        n = min(len(prices), len(highs), len(lows))
        if n < period + 1:
            return 0.0
        trs = []
        for i in range(1, n):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - prices[i - 1]),
                abs(lows[i] - prices[i - 1]),
            )
            trs.append(tr)
        if len(trs) < period:
            return 0.0
        # Wilder smoothing: seed with SMA then EMA α=1/period
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return _safe(atr)

    # ── Main entry point ──────────────────────────────────────────────────────

    def compute(self, ticker: str, prices: list, volume: float, adv_20: float,
                fundamentals: dict, iwm_prices: list) -> float:
        """
        Returns quant_adjustment ∈ [-25, +25]. Handles all edge cases internally.
        """
        m  = self.momentum_score(ticker, prices, iwm_prices)
        v  = self.volume_score(ticker, volume, adv_20)
        mr = self.mean_reversion_score(ticker, prices)
        vp = self.volatility_penalty(prices)
        f  = self.factor_score(fundamentals)
        raw = m + v + mr + vp + f
        return round(float(np.clip(raw, -25, 25)), 2)


def run_quant_for_ticker(ticker: str, scan_result: dict) -> tuple:
    """
    Convenience wrapper called from scanner_loop.
    Returns (quant_adj, atr_14, rsi, sigma_hist).
    Times itself — returns (0, 0, None, None) if >500ms.
    """
    t0 = time.time()
    try:
        engine = QuantEngine()
        closes, highs, lows = _hist_ohlcv(ticker, "60d")
        if not closes:
            return 0.0, 0.0, None, None

        # IWM prices for RS calculation
        iwm = _prices("IWM", "60d")

        volume_today = float(scan_result.get("volume") or 0)
        adv_20       = float(scan_result.get("avg_volume") or 0)
        fundamentals = {
            "market_cap":    scan_result.get("market_cap") or 0,
            "pb_ratio":      scan_result.get("pb_ratio"),
            "ps_ratio":      scan_result.get("ps_ratio"),
            "gross_margins": scan_result.get("gross_margins"),
            "debt_to_equity": None,
        }

        elapsed = (time.time() - t0) * 1000
        if elapsed > 450:
            print(f"  [quant] {ticker}: data fetch took {elapsed:.0f}ms — skipping quant block")
            return 0.0, 0.0, None, None

        adj = engine.compute(ticker, closes, volume_today, adv_20, fundamentals, iwm)
        atr = QuantEngine.compute_atr(closes, highs, lows)

        # Quick RSI and sigma from already-fetched prices for ConvictionEngine
        rsi_val    = None
        sigma_hist = None
        if len(closes) >= 15:
            rets = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
            gains  = [max(r, 0) for r in rets[-14:]]
            losses = [abs(min(r, 0)) for r in rets[-14:]]
            avg_g  = float(np.mean(gains))
            avg_l  = float(np.mean(losses))
            rs     = avg_g / avg_l if avg_l > 0 else 100.0
            rsi_val = _safe(100 - 100 / (1 + rs))

        if len(closes) >= 21:
            log_rets = [np.log(closes[i] / closes[i - 1])
                        for i in range(1, len(closes))
                        if closes[i - 1] > 0 and closes[i] > 0]
            sigma_hist = _safe(float(np.std(log_rets[-20:])) * np.sqrt(252))

        total = (time.time() - t0) * 1000
        if total > 500:
            print(f"  [quant] {ticker}: quant block {total:.0f}ms — exceeded 500ms budget")

        return adj, atr, rsi_val, sigma_hist
    except Exception as e:
        print(f"  [quant] {ticker}: error — {e}")
        return 0.0, 0.0, None, None
