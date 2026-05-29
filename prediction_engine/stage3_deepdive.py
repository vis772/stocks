# prediction_engine/stage3_deepdive.py
# Stage 3 (6:00-8:00 AM ET): All 4 SLMs perform independent deep-dive analysis
# on the top 50 candidates in parallel. Each model has a specialized role.

import json
import time
import logging
import requests
import yfinance as yf
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .config import (
    OLLAMA_BASE_URL, OLLAMA_TIMEOUT, DEEPDIVE_MAX_WORKERS, DEEPDIVE_RETRIES,
    MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL, MODEL_OPTIONS,
    FINNHUB_API_KEY, ROLE_BEAR,
)

logger = logging.getLogger("pe.stage3")

FINNHUB_BASE = "https://finnhub.io/api/v1"


# ─── System prompts per model ─────────────────────────────────────────────────

_SYS_QWEN = (
    "You are a momentum and fundamentals analyst. "
    "Evaluate the stock's revenue trend, margin quality, balance sheet strength, "
    "and momentum indicators. Return only valid JSON — no markdown, no explanation."
)

_SYS_PHI = (
    "You are a technical analysis and institutional activity analyst. "
    "Evaluate technical confluence, SEC filing quality, insider activity, "
    "short interest, and squeeze potential. Return only valid JSON — no markdown."
)

_SYS_GEMMA = (
    "You are a price action and chart pattern analyst. "
    "Evaluate chart structure, support/resistance levels, pattern quality, "
    "and risk/reward setup. Return only valid JSON — no markdown, no explanation."
)

_SYS_SMOL = (
    "You are a news catalyst and market sentiment analyst. "
    "Evaluate news catalyst strength, sentiment quality, and signal reliability. "
    "Return only valid JSON — no markdown, no explanation."
)

_SYS_BEAR = (
    "You are an adversarial risk analyst and short-seller. "
    "Your ONLY job is to find reasons this pre-market setup will FAIL. "
    "Be cynical. Ignore the bull case entirely. Return only valid JSON — no markdown."
)

_PROMPT_BEAR_DEEP = """\
You are a short-seller. Find every reason this trade will go wrong.

Ticker: {ticker}
Premarket Gap: {gap_pct:+.1f}%  (Price: ${price:.2f})
Volume Ratio: {volume_ratio:.1f}x average
RSI(14): {rsi:.1f}
Short Interest: {short_float_pct:.1f}% of float
Days to Cover: {days_to_cover:.1f}
Price vs 20-day SMA: {vs_sma20:+.1f}%
52-week High: ${high_52w:.2f}  (current is {pct_from_52wh:+.1f}% from it)
Earnings in {earnings_days} days
Has Options Market: {has_options}

Recent News:
{news_text}

Assign a RISK score 0-100 (higher = more likely to fail):
- 70-100 HIGH RISK: gap >20%, earnings imminent, at 52w high, no real catalyst, RSI>75
- 40-70 MODERATE: some concern but manageable
- 0-40 LOW: clean setup, bear case is weak

Return exactly this JSON:
{{"risk_score": <0-100>, "verdict": "<dangerous|moderate|low>", "primary_risk": "<single biggest risk in 8 words>", "risk_type": "<dilution|halt_risk|overbought|resistance|weak_catalyst|earnings_gamble|other>"}}
"""


# ─── Prompt templates per model ───────────────────────────────────────────────

_PROMPT_QWEN_DEEP = """\
Analyze this stock for pre-market trading conviction.

Ticker: {ticker}
Premarket Price: ${price:.2f}  (Gap: {gap_pct:+.1f}%)
Volume Ratio: {volume_ratio:.1f}x 30-day average
Sector: {sector}
Market Cap: ${market_cap_m:.0f}M

Fundamentals:
- Revenue Growth YoY: {revenue_growth:.1f}%
- Gross Margin: {gross_margin:.1f}%
- Cash on Hand: ${cash_m:.0f}M
- Total Debt: ${debt_m:.0f}M
- Cash/Debt Ratio: {cash_debt_ratio:.2f}

Technicals:
- RSI(14): {rsi:.1f}
- Price vs 20-day SMA: {vs_sma20:+.1f}%
- Price vs 50-day SMA: {vs_sma50:+.1f}%
- 5-day return: {ret_5d:+.1f}%

Score this setup 0-100. High scores (70+) for: strong revenue growth, healthy margins,
low debt, RSI 40-65, fresh gap not chasing an extended move.
Low scores (<40) for: declining revenue, heavy debt, RSI>72 (overbought), or weak volume.

Return exactly this JSON:
{{"score": <0-100>, "verdict": "<high|medium|low>", "fundamental_strength": "<strong|moderate|weak>", "momentum": "<strong|moderate|weak>", "thesis": "<one sentence max>", "key_risk": "<one sentence max>"}}
"""

_PROMPT_PHI_DEEP = """\
Analyze technical confluence and institutional signals for this stock.

Ticker: {ticker}
Premarket Price: ${price:.2f}  (Gap: {gap_pct:+.1f}%)
Short Interest: {short_float_pct:.1f}% of float
Days to Cover: {days_to_cover:.1f}

Recent SEC Filings (last 30 days):
{filings_text}

Recent Insider Activity (last 30 days):
{insider_text}

Technical Indicators:
- RSI(14): {rsi:.1f}
- MACD Signal: {macd_signal}
- Price vs VWAP: {vs_vwap:+.1f}%
- Bollinger Band Position: {bb_position:.2f} (0=lower, 1=upper)
- ATR(14): ${atr:.2f}

Score this setup 0-100. High scores (70+) for: positive SEC catalyst, insider buying,
short squeeze potential (>15% float short), clean MACD signal, price near support.
Low scores (<40) for: dilution filings, insider selling, near resistance with no catalyst.

Return exactly this JSON:
{{"score": <0-100>, "verdict": "<high|medium|low>", "technical_confluence": "<strong|moderate|weak>", "filing_quality": "<positive|neutral|negative>", "insider_direction": "<buying|selling|neutral>", "squeeze_potential": "<high|medium|low>", "thesis": "<one sentence max>"}}
"""

_PROMPT_GEMMA_DEEP = """\
Analyze the chart pattern and price action for this stock.

Ticker: {ticker}
Premarket Price: ${price:.2f}  (Gap: {gap_pct:+.1f}%)

Recent Price History (last 10 sessions, newest first):
{price_history}

Key Levels:
- 52-week High: ${high_52w:.2f}
- 52-week Low:  ${low_52w:.2f}
- 20-day SMA:   ${sma20:.2f}
- 50-day SMA:   ${sma50:.2f}
- Estimated Support: ${support:.2f}
- Estimated Resistance: ${resistance:.2f}

Score this setup 0-100. High scores (70+) for: clean breakout above resistance,
gap-up with prior consolidation, clear support below entry, favorable R/R.
Low scores (<40) for: gap into major resistance, no defined support, messy price action.

Return exactly this JSON:
{{"score": <0-100>, "verdict": "<high|medium|low>", "pattern": "<pattern name or description>", "setup_quality": "<clean|moderate|messy>", "risk_reward": <float like 2.5>, "entry_zone": "${entry_low:.2f}-${entry_high:.2f}", "thesis": "<one sentence max>"}}
"""

_PROMPT_SMOL_DEEP = """\
Analyze news catalysts and market sentiment for this stock.

Ticker: {ticker}
Premarket Price: ${price:.2f}  (Gap: {gap_pct:+.1f}%)
Volume Ratio: {volume_ratio:.1f}x  (volume spike confirmed: {volume_spike})

Recent News Headlines (newest first):
{news_text}

Score this setup 0-100. High scores (70+) for: confirmed earnings beat, FDA approval,
major contract win, buyout rumor — catalysts that justify the gap.
Medium scores (40-60) for: sector tailwinds, general positive news.
Low scores (<40) for: no news found, social media hype with no fundamental catalyst,
or negative news contradicting the gap direction.

Return exactly this JSON:
{{"score": <0-100>, "verdict": "<high|medium|low>", "sentiment_score": <float -1 to 1>, "catalyst_quality": "<strong|moderate|weak|none>", "signal_strength": "<high|medium|low>", "catalyst_type": "<earnings|fda|contract|sector|social|unknown>", "thesis": "<one sentence max>"}}
"""


# ─── Main entry point ─────────────────────────────────────────────────────────

def run(candidates: list) -> dict:
    """
    Run all 4 SLMs on each candidate SEQUENTIALLY (one model at a time).

    CPU mode rationale (t3.xlarge): Ollama on CPU is single-threaded.
    Parallel threads just queue behind each other and each incurs cold-start
    overhead as the model is swapped in/out of RAM.

    Sequential per-model strategy:
      - Load Qwen → score all 50 tickers → Qwen stays resident in RAM
      - Load Phi  → score all 50 tickers → etc.
    This keeps each model hot for its full batch, maximising throughput.

    Timing estimate: 4 models × 50 tickers × ~25s avg = ~83 minutes.
    Fits within the 6:00-8:00 AM window.

    Returns {ticker: {model_name: {score, verdict, raw_data, ...}}}
    """
    logger.info(
        "[stage3] Deep dive on %d candidates — sequential per-model (CPU mode)",
        len(candidates),
    )
    start   = time.monotonic()
    results = {c["ticker"]: {} for c in candidates}

    # Fetch supplemental data for all candidates upfront (parallelised — this is I/O not Ollama)
    logger.info("[stage3] Fetching supplemental data (fundamentals, news, SEC)...")
    enriched = _enrich_all(candidates)

    model_tasks = [
        (MODEL_QWEN,  _build_qwen_prompt,  _SYS_QWEN),
        (MODEL_PHI,   _build_phi_prompt,   _SYS_PHI),
        (MODEL_GEMMA, _build_gemma_prompt, _SYS_GEMMA),
        (MODEL_SMOL,  _build_smol_prompt,  _SYS_SMOL),
    ]

    for model_idx, (model_name, prompt_fn, sys_prompt) in enumerate(model_tasks, 1):
        model_start = time.monotonic()
        logger.info(
            "[stage3] Model %d/4: %s — scoring %d tickers",
            model_idx, model_name, len(enriched),
        )

        for ticker_idx, c in enumerate(enriched, 1):
            ticker = c["ticker"]
            score, verdict, raw = _run_model(model_name, sys_prompt, prompt_fn, c)
            results[ticker][model_name] = {
                "score":   score,
                "verdict": verdict,
                "raw":     raw or {},
            }

            if ticker_idx % 10 == 0 or ticker_idx == len(enriched):
                elapsed_m = time.monotonic() - model_start
                rate = ticker_idx / elapsed_m if elapsed_m > 0 else 0
                eta  = (len(enriched) - ticker_idx) / rate if rate > 0 else 0
                logger.info(
                    "[stage3] %s: %d/%d tickers (%.1f/min, ETA %.0fm)",
                    model_name, ticker_idx, len(enriched), rate * 60, eta / 60,
                )
            else:
                logger.debug("[stage3] %s / %s: score=%s verdict=%s",
                             model_name, ticker, score, verdict)

        model_elapsed = time.monotonic() - model_start
        scored_count  = sum(
            1 for t in results.keys()
            if results[t].get(model_name, {}).get("score") is not None
        )
        logger.info(
            "[stage3] %s complete — %d/%d tickers scored in %.1fs",
            model_name, scored_count, len(enriched), model_elapsed,
        )

    # ── Bear-case pass (5th) ─────────────────────────────────────────────────
    # Uses MODEL_QWEN with adversarial framing. Results stored under ROLE_BEAR.
    # NOT counted in consensus voting — used only to penalise/kill risky picks.
    bear_start = time.monotonic()
    logger.info("[stage3] Bear-case pass: adversarial risk scoring %d tickers...", len(enriched))

    for ticker_idx, c in enumerate(enriched, 1):
        ticker = c["ticker"]
        risk_score, risk_verdict, risk_raw = _run_model(
            MODEL_QWEN, _SYS_BEAR, _build_bear_prompt, c
        )
        results[ticker][ROLE_BEAR] = {
            "score":   risk_score,
            "verdict": risk_verdict,
            "raw":     risk_raw or {},
        }
        if risk_score is not None and risk_score >= 65:
            logger.info("[stage3] BEAR %-8s  risk=%3.0f  [%s]  %s",
                        ticker, risk_score, risk_verdict,
                        (risk_raw or {}).get("primary_risk", "")[:50])

    logger.info("[stage3] Bear-case pass complete in %.0fs",
                time.monotonic() - bear_start)

    elapsed = time.monotonic() - start
    total_scores = sum(
        1 for t in results.values()
        for k, v in t.items()
        if k != ROLE_BEAR and v.get("score") is not None
    )
    logger.info(
        "[stage3] All 4+bear models complete — %d bull scores across %d tickers in %.1fs (%.0fm)",
        total_scores, len(candidates), elapsed, elapsed / 60,
    )
    return results


# ─── Supplemental data fetch ──────────────────────────────────────────────────

def _enrich_all(candidates: list) -> list:
    """
    Fetch supplemental data for all candidates in parallel (I/O-bound — threads help).
    yfinance + Finnhub are network calls, not CPU — safe to parallelise here.
    """
    enriched_map = {}

    def enrich_one(c: dict) -> dict:
        ticker = c["ticker"]
        try:
            yf_data = _fetch_yf_data(ticker)
            news    = _fetch_finnhub_news(ticker)
            insider = _fetch_finnhub_insider(ticker)
            filings = _fetch_finnhub_filings(ticker)
            c.update({"yf_data": yf_data, "news": news,
                      "insider": insider, "filings": filings})
        except Exception as e:
            logger.debug("[stage3] Enrich failed for %s: %s", ticker, e)
            c.setdefault("yf_data", {})
            c.setdefault("news",    [])
            c.setdefault("insider", [])
            c.setdefault("filings", [])
        return c

    # 8 workers — enough for Finnhub rate limits (60 req/min free tier)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(enrich_one, c): c["ticker"] for c in candidates}
        for future in as_completed(futures):
            try:
                enriched_c = future.result()
                enriched_map[enriched_c["ticker"]] = enriched_c
            except Exception as e:
                logger.debug("[stage3] Enrich future error: %s", e)

    # Preserve original ordering
    return [enriched_map.get(c["ticker"], c) for c in candidates]


def _fetch_yf_data(ticker: str) -> dict:
    try:
        tk   = yf.Ticker(ticker)
        info = tk.info or {}

        # Historical prices for pattern analysis
        hist = tk.history(period="60d", progress=False)

        # Technicals
        closes = hist["Close"].dropna().values if not hist.empty else np.array([])
        rsi    = _calc_rsi(closes) if len(closes) >= 14 else 50.0
        sma20  = float(np.mean(closes[-20:])) if len(closes) >= 20 else 0
        sma50  = float(np.mean(closes[-50:])) if len(closes) >= 50 else 0
        atr    = _calc_atr(hist) if not hist.empty else 0
        high_52w = float(info.get("fiftyTwoWeekHigh", 0) or 0)
        low_52w  = float(info.get("fiftyTwoWeekLow",  0) or 0)

        current_price = float(closes[-1]) if len(closes) > 0 else 0
        vs_sma20 = ((current_price - sma20) / sma20 * 100) if sma20 > 0 else 0
        vs_sma50 = ((current_price - sma50) / sma50 * 100) if sma50 > 0 else 0
        ret_5d   = ((current_price - closes[-6]) / closes[-6] * 100) if len(closes) >= 6 else 0

        # Bollinger band position
        bb_pos = 0.5
        if len(closes) >= 20:
            upper = sma20 + 2 * np.std(closes[-20:])
            lower = sma20 - 2 * np.std(closes[-20:])
            bb_pos = ((current_price - lower) / (upper - lower)) if upper > lower else 0.5
            bb_pos = max(0.0, min(1.0, bb_pos))

        # Support / resistance (simple pivot)
        support    = float(np.percentile(closes[-20:], 10)) if len(closes) >= 10 else current_price * 0.95
        resistance = float(np.percentile(closes[-20:], 90)) if len(closes) >= 10 else current_price * 1.05

        # Price history text (last 10 sessions)
        recent   = closes[-10:] if len(closes) >= 10 else closes
        ph_lines = [f"  Session {len(recent)-i}: ${p:.2f}" for i, p in enumerate(reversed(recent))]
        price_history_text = "\n".join(ph_lines)

        # Fundamentals from yfinance
        revenue_growth = float((info.get("revenueGrowth") or 0) * 100)
        gross_margin   = float((info.get("grossMargins")  or 0) * 100)
        total_cash     = float((info.get("totalCash")     or 0) / 1_000_000)
        total_debt     = float((info.get("totalDebt")     or 0) / 1_000_000)
        cash_debt_ratio = (total_cash / total_debt) if total_debt > 0 else 99.0
        short_pct      = float((info.get("shortPercentOfFloat") or 0) * 100)
        days_to_cover  = float(info.get("shortRatio") or 0)
        market_cap_m   = float((info.get("marketCap") or 0) / 1_000_000)
        float_shares   = int(info.get("floatShares") or 0)

        # ── Earnings calendar (days until next earnings) ──────────────────
        earnings_days = 999  # default: no upcoming earnings known
        try:
            cal = tk.calendar
            if cal is not None and not cal.empty:
                from datetime import date as _date
                earn_col = cal.columns[0] if not cal.empty else None
                if earn_col is not None:
                    earn_val = cal.iloc[0, 0]
                    if hasattr(earn_val, "date"):
                        earnings_days = (earn_val.date() - _date.today()).days
                    elif hasattr(earn_val, "year"):
                        earnings_days = (earn_val - _date.today()).days
        except Exception:
            pass

        # ── Options market check ──────────────────────────────────────────
        has_options = False
        options_iv  = 0.0
        try:
            expirations = tk.options
            has_options = bool(expirations)
            if has_options and current_price > 0:
                chain      = tk.option_chain(expirations[0])
                calls      = chain.calls
                atm_calls  = calls[abs(calls["strike"] - current_price) < current_price * 0.08]
                if not atm_calls.empty:
                    options_iv = float(atm_calls["impliedVolatility"].median())
        except Exception:
            pass

        return {
            "rsi":              rsi,
            "sma20":            sma20,
            "sma50":            sma50,
            "atr":              atr,
            "vs_sma20":         vs_sma20,
            "vs_sma50":         vs_sma50,
            "ret_5d":           ret_5d,
            "bb_position":      bb_pos,
            "support":          support,
            "resistance":       resistance,
            "high_52w":         high_52w,
            "low_52w":          low_52w,
            "price_history":    price_history_text,
            "revenue_growth":   revenue_growth,
            "gross_margin":     gross_margin,
            "cash_m":           total_cash,
            "debt_m":           total_debt,
            "cash_debt_ratio":  cash_debt_ratio,
            "short_float_pct":  short_pct,
            "days_to_cover":    days_to_cover,
            "market_cap_m":     market_cap_m,
            "float_shares":     float_shares,
            "earnings_days":    earnings_days,   # NEW: days until next earnings
            "has_options":      has_options,     # NEW: options market exists
            "options_iv":       options_iv,      # NEW: ATM implied volatility
        }
    except Exception as e:
        logger.debug("[stage3/yf] %s error: %s", ticker, e)
        return {}


def _fetch_finnhub_news(ticker: str) -> list:
    if not FINNHUB_API_KEY:
        return []
    try:
        from datetime import datetime, timedelta
        end   = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        resp  = requests.get(
            f"{FINNHUB_BASE}/company-news",
            params={"symbol": ticker, "from": start, "to": end, "token": FINNHUB_API_KEY},
            timeout=8,
        )
        if resp.status_code == 200:
            items = resp.json()[:8]
            return [{"headline": n.get("headline", ""), "summary": n.get("summary", "")[:100]}
                    for n in items]
    except Exception:
        pass
    return []


def _fetch_finnhub_insider(ticker: str) -> list:
    if not FINNHUB_API_KEY:
        return []
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/stock/insider-transactions",
            params={"symbol": ticker, "token": FINNHUB_API_KEY},
            timeout=8,
        )
        if resp.status_code == 200:
            data  = resp.json().get("data", [])
            items = []
            for t in data[:5]:
                items.append({
                    "name":   t.get("name", ""),
                    "change": t.get("change", 0),
                    "share":  t.get("share", 0),
                    "date":   str(t.get("transactionDate", ""))[:10],
                })
            return items
    except Exception:
        pass
    return []


def _fetch_finnhub_filings(ticker: str) -> list:
    if not FINNHUB_API_KEY:
        return []
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/stock/filings",
            params={"symbol": ticker, "token": FINNHUB_API_KEY},
            timeout=8,
        )
        if resp.status_code == 200:
            filings = resp.json()[:5]
            return [{"type": f.get("type", ""), "date": str(f.get("filedDate", ""))[:10],
                     "description": str(f.get("description", ""))[:80]}
                    for f in filings]
    except Exception:
        pass
    return []


# ─── Prompt builders ──────────────────────────────────────────────────────────

def _build_qwen_prompt(c: dict) -> str:
    yf = c.get("yf_data", {})
    return _PROMPT_QWEN_DEEP.format(
        ticker         = c.get("ticker", "?"),
        price          = c.get("premarket_price") or c.get("price", 0),
        gap_pct        = c.get("gap_pct", 0),
        volume_ratio   = c.get("volume_ratio", 0),
        sector         = c.get("sector", "Unknown"),
        market_cap_m   = yf.get("market_cap_m") or (c.get("market_cap", 0) or 0) / 1e6,
        revenue_growth = yf.get("revenue_growth", 0),
        gross_margin   = yf.get("gross_margin", 0),
        cash_m         = yf.get("cash_m", 0),
        debt_m         = yf.get("debt_m", 0),
        cash_debt_ratio= yf.get("cash_debt_ratio", 0),
        rsi            = yf.get("rsi", 50),
        vs_sma20       = yf.get("vs_sma20", 0),
        vs_sma50       = yf.get("vs_sma50", 0),
        ret_5d         = yf.get("ret_5d", 0),
    )


def _build_phi_prompt(c: dict) -> str:
    yf = c.get("yf_data", {})
    filings = c.get("filings", [])
    insider = c.get("insider", [])

    filings_text = "\n".join(
        f"  {f['date']} {f['type']}: {f['description']}" for f in filings
    ) or "  No recent filings found."

    insider_text = "\n".join(
        f"  {t['date']} {t['name']}: {'BUY' if (t.get('change') or 0) > 0 else 'SELL'} "
        f"{abs(t.get('change') or 0):,} shares"
        for t in insider
    ) or "  No recent insider transactions found."

    macd_signal = "Bullish" if yf.get("vs_sma20", 0) > 0 else "Bearish"

    return _PROMPT_PHI_DEEP.format(
        ticker          = c.get("ticker", "?"),
        price           = c.get("premarket_price") or c.get("price", 0),
        gap_pct         = c.get("gap_pct", 0),
        short_float_pct = yf.get("short_float_pct", 0),
        days_to_cover   = yf.get("days_to_cover", 0),
        filings_text    = filings_text,
        insider_text    = insider_text,
        rsi             = yf.get("rsi", 50),
        macd_signal     = macd_signal,
        vs_vwap         = yf.get("vs_sma20", 0),
        bb_position     = yf.get("bb_position", 0.5),
        atr             = yf.get("atr", 0),
    )


def _build_gemma_prompt(c: dict) -> str:
    yf = c.get("yf_data", {})
    price = c.get("premarket_price") or c.get("price", 0)
    support    = yf.get("support",    price * 0.95)
    resistance = yf.get("resistance", price * 1.05)

    return _PROMPT_GEMMA_DEEP.format(
        ticker        = c.get("ticker", "?"),
        price         = price,
        gap_pct       = c.get("gap_pct", 0),
        price_history = yf.get("price_history", "  No history available."),
        high_52w      = yf.get("high_52w", price * 1.5),
        low_52w       = yf.get("low_52w",  price * 0.5),
        sma20         = yf.get("sma20", price),
        sma50         = yf.get("sma50", price),
        support       = support,
        resistance    = resistance,
        entry_low     = price * 0.995,
        entry_high    = price * 1.005,
    )


def _build_smol_prompt(c: dict) -> str:
    news = c.get("news", [])
    news_text = "\n".join(
        f"  - {n['headline']}" for n in news
    ) or "  No recent news found."
    volume_spike = "YES" if c.get("volume_ratio", 0) >= 2.0 else "NO"

    return _PROMPT_SMOL_DEEP.format(
        ticker       = c.get("ticker", "?"),
        price        = c.get("premarket_price") or c.get("price", 0),
        gap_pct      = c.get("gap_pct", 0),
        volume_ratio = c.get("volume_ratio", 0),
        volume_spike = volume_spike,
        news_text    = news_text,
    )


def _build_bear_prompt(c: dict) -> str:
    yf_d  = c.get("yf_data", {})
    news  = c.get("news", [])
    price = c.get("premarket_price") or c.get("price", 0)
    high_52w     = yf_d.get("high_52w", price * 1.5)
    pct_from_52wh = ((price - high_52w) / high_52w * 100) if high_52w > 0 else 0
    news_text    = "\n".join(f"  - {n['headline']}" for n in news[:5]) or "  No news found."
    earnings_days = yf_d.get("earnings_days", 999)
    earn_str      = str(earnings_days) if earnings_days < 90 else "90+"

    return _PROMPT_BEAR_DEEP.format(
        ticker          = c.get("ticker", "?"),
        price           = price,
        gap_pct         = c.get("gap_pct", 0),
        volume_ratio    = c.get("volume_ratio", 0),
        rsi             = yf_d.get("rsi", 50),
        short_float_pct = yf_d.get("short_float_pct", 0),
        days_to_cover   = yf_d.get("days_to_cover", 0),
        vs_sma20        = yf_d.get("vs_sma20", 0),
        high_52w        = high_52w,
        pct_from_52wh   = pct_from_52wh,
        earnings_days   = earn_str,
        has_options     = "YES" if yf_d.get("has_options") else "NO",
        news_text       = news_text,
    )


# ─── Model call with retries ──────────────────────────────────────────────────

def _run_model(model: str, system: str, prompt_fn, c: dict):
    """Returns (score, verdict, raw_dict) for one ticker from one model."""
    ticker = c.get("ticker", "?")
    try:
        prompt = prompt_fn(c)
    except Exception as e:
        logger.warning("[stage3] %s prompt build failed for %s: %s", model, ticker, e)
        return None, None, {}

    for attempt in range(DEEPDIVE_RETRIES + 1):
        try:
            options = MODEL_OPTIONS.get(model, {})
            payload = {
                "model":   model,
                "system":  system,
                "prompt":  prompt,
                "format":  "json",
                "stream":  False,
                "options": options,
            }
            resp = requests.post(
                f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate",
                json=payload,
                timeout=OLLAMA_TIMEOUT,
            )
            resp.raise_for_status()
            raw_str = resp.json().get("response", "")
            parsed  = _parse_json(raw_str)

            if parsed is None:
                raise ValueError("null JSON parse")

            score   = _clamp(parsed.get("score"), 0, 100)
            verdict = str(parsed.get("verdict", "medium")).lower()
            if verdict not in ("high", "medium", "low"):
                verdict = "high" if score >= 70 else ("medium" if score >= 50 else "low")

            return score, verdict, parsed

        except Exception as e:
            logger.debug("[stage3] %s/%s attempt %d: %s", model, ticker, attempt + 1, e)
            if attempt < DEEPDIVE_RETRIES:
                time.sleep(2 ** attempt)

    logger.warning("[stage3] %s/%s: all attempts failed", model, ticker)
    return None, None, {}


def _parse_json(raw: str) -> Optional[dict]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text  = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass
    return None


def _clamp(val, lo, hi):
    try:
        return max(lo, min(hi, float(val)))
    except (TypeError, ValueError):
        return (lo + hi) / 2


# ─── Technical helpers ────────────────────────────────────────────────────────

def _calc_rsi(closes: np.ndarray, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-(period + 1):])
    gains  = np.maximum(deltas, 0)
    losses = np.abs(np.minimum(deltas, 0))
    avg_g  = np.mean(gains) or 1e-9
    avg_l  = np.mean(losses) or 1e-9
    rs     = avg_g / avg_l
    return float(100 - 100 / (1 + rs))


def _calc_atr(hist, period: int = 14) -> float:
    try:
        hi = hist["High"].values[-period:]
        lo = hist["Low"].values[-period:]
        cl = hist["Close"].values[-period:]
        pc = hist["Close"].values[-(period + 1):-1]
        tr = np.maximum(hi - lo, np.maximum(np.abs(hi - pc), np.abs(lo - pc)))
        return float(np.mean(tr))
    except Exception:
        return 0.0
