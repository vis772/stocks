# core/scanner.py
# The main scanner orchestrator.
# This is the engine that runs the full scan pipeline for each ticker.
#
# Pipeline for each ticker:
#   1. Fetch market data snapshot (yfinance)
#   2. Apply universe filter (size, volume, price)
#   3. Compute technicals
#   4. Fetch SEC filings (EDGAR)
#   5. Fetch news (RSS)
#   6. Score: technical, catalyst, fundamental, risk, sentiment
#   7. Compute final weighted score
#   8. Assign signal label
#   9. Flag risks
#  10. Persist to database

import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import traceback

from config import SCORING_WEIGHTS, SIGNAL_THRESHOLDS, RISK_FLAGS
from data.market_data import fetch_ticker_snapshot, passes_universe_filter
from data.sec_data import get_recent_filings, analyze_filing_risk
from data.news_data import fetch_ticker_news, analyze_news_sentiment
from analysis.technicals import compute_technicals, suggest_entry_and_stops
from analysis.fundamentals import score_fundamentals, score_risk, score_catalyst
from db.database import save_scan_result


def scan_ticker(ticker: str, save: bool = True) -> Optional[Dict]:
    """
    Run the full scan pipeline for a single ticker.
    Returns a complete result dict or None if the ticker fails filters.

    Args:
        ticker: Stock symbol (e.g. "SOUN")
        save:   If True, persist result to SQLite database
    """
    ticker = ticker.upper().strip()
    print(f"  → Scanning {ticker}...")

    result = {
        "ticker":     ticker,
        "scan_date":  datetime.now().strftime("%Y-%m-%d"),
        "scan_time":  datetime.now().strftime("%H:%M:%S"),
        "data_sources": [],
        "risk_flags":   [],
        "warnings":     [],
    }

    # ── Step 1: Fetch Market Data ──────────────────────────────────────────────
    try:
        snapshot = fetch_ticker_snapshot(ticker)
        if snapshot is None:
            return {**result, "error": "Could not fetch market data — ticker may be invalid or delisted"}
    except Exception as e:
        return {**result, "error": f"Market data fetch failed: {str(e)}"}

    result["data_sources"].append("Yahoo Finance (yfinance)")

    # ── Step 2: Universe Filter ────────────────────────────────────────────────
    passes, reason = passes_universe_filter(snapshot)
    if not passes:
        return {
            **result,
            "company_name": snapshot.get("company_name", ticker),
            "price":        snapshot.get("price"),
            "market_cap":   snapshot.get("market_cap"),
            "filtered_out": True,
            "filter_reason": reason,
        }

    # ── Step 3: Technical Analysis ─────────────────────────────────────────────
    hist = snapshot.get("_history")
    technicals = compute_technicals(hist)

    # Merge volatility back into snapshot for risk scoring
    snapshot["volatility_30d_ann"] = technicals.get("volatility_30d_ann")
    snapshot["relative_volume"]    = technicals.get("relative_volume", 0)

    # ── Step 4: SEC Filings ────────────────────────────────────────────────────
    try:
        filings     = get_recent_filings(ticker, days_back=30)
        sec_analysis = analyze_filing_risk(filings)
        result["data_sources"].append("SEC EDGAR (free)")
    except Exception as e:
        filings      = []
        sec_analysis = {"active_flags": [], "flag_details": {}, "filing_summary": []}
        result["warnings"].append(f"SEC data unavailable: {str(e)}")

    # ── Step 5: News ──────────────────────────────────────────────────────────
    try:
        articles = fetch_ticker_news(ticker, max_articles=10)
        news_sentiment = analyze_news_sentiment(articles)
        result["data_sources"].append("Yahoo Finance RSS news")
    except Exception as e:
        articles = []
        news_sentiment = {"sentiment_score": 50, "headline_count": 0, "hype_alert": False,
                          "catalyst_keywords": [], "risk_keywords": [], "recent_headlines": []}
        result["warnings"].append(f"News data unavailable: {str(e)}")

    # ── Step 6: Scoring ────────────────────────────────────────────────────────

    # Technical score (0-100)
    tech_score = technicals.get("technical_score", 50)

    # Catalyst score (0-100)
    catalyst_result = score_catalyst(filings, news_sentiment, snapshot)
    cat_score = catalyst_result["catalyst_score"]

    # Fundamental score (0-100)
    fund_result = score_fundamentals(snapshot)
    fund_score  = fund_result["fundamental_score"]

    # Risk score (0-100, higher = more risky)
    # Pass SEC flags + snapshot into risk scorer
    risk_result = score_risk(snapshot, sec_analysis["active_flags"])
    raw_risk    = risk_result["risk_score"]
    # INVERT for final scoring: low risk = high contribution
    risk_contribution = 100 - raw_risk

    # Sentiment score (0-100)
    sent_score = news_sentiment.get("sentiment_score", 50)

    # ── Step 7: Final Weighted Score ───────────────────────────────────────────
    w = SCORING_WEIGHTS
    final_score = (
        tech_score         * w["technical"]   +
        cat_score          * w["catalyst"]    +
        fund_score         * w["fundamental"] +
        risk_contribution  * w["risk"]        +
        sent_score         * w["sentiment"]
    )
    final_score = round(final_score, 1)

    # ── Step 8: Signal Label ───────────────────────────────────────────────────
    signal = _score_to_signal(final_score)

    # ── Step 9: Risk Flags (collect all active flags) ─────────────────────────
    all_flags = list(set(
        sec_analysis.get("active_flags", []) +
        risk_result.get("active_flags", [])
    ))

    # Hype flag from news
    if news_sentiment.get("hype_alert"):
        all_flags.append("pump_signal")

    # Option A: Earnings warning flag
    if snapshot.get("earnings_warning"):
        all_flags.append("earnings_imminent")

    # Override signal if critical flags are present
    critical_flags = {"going_concern", "reverse_split_risk"}
    if critical_flags.intersection(set(all_flags)):
        if signal not in ("Sell", "Avoid"):
            signal = "Avoid"  # Don't recommend buying into a going concern

    # ── Step 10: Entry / Stop Suggestions ────────────────────────────────────
    entries = suggest_entry_and_stops(snapshot, technicals)

    # ── Assemble Full Result ──────────────────────────────────────────────────
    full_result = {
        **result,

        # Identity
        "company_name":  snapshot.get("company_name", ticker),
        "sector":        snapshot.get("sector", "Unknown"),
        "industry":      snapshot.get("industry", "Unknown"),

        # Price & Size
        "price":         snapshot.get("price"),
        "market_cap":    snapshot.get("market_cap"),
        "volume":        snapshot.get("volume"),
        "avg_volume":    snapshot.get("avg_volume"),
        "relative_volume": snapshot.get("relative_volume"),

        # Scores (all 0-100)
        "technical_score":    round(tech_score, 1),
        "catalyst_score":     round(cat_score, 1),
        "fundamental_score":  round(fund_score, 1),
        "risk_score":         round(raw_risk, 1),       # Raw risk (higher = worse)
        "sentiment_score":    round(sent_score, 1),
        "final_score":        final_score,

        # Signal
        "signal":        signal,

        # Risk
        "risk_flags":    all_flags,
        "flag_details":  {**sec_analysis.get("flag_details", {}), **risk_result.get("flag_details", {})},

        # Technicals
        "rsi":           technicals.get("rsi_14"),
        "macd_bullish":  technicals.get("macd_bullish"),
        "above_sma20":   technicals.get("above_sma20"),
        "above_sma50":   technicals.get("above_sma50"),
        "return_1d":     technicals.get("return_1d"),
        "return_5d":     technicals.get("return_5d"),
        "return_20d":    technicals.get("return_20d"),
        "volatility":    technicals.get("volatility_30d_ann"),
        "gap_up":        technicals.get("gap_up"),
        "gap_pct":       technicals.get("gap_pct"),

        # Entry zones
        "entry_zone":    entries.get("entry_zone_low"),
        "stop_loss":     entries.get("stop_loss"),
        "target_1":      entries.get("target_1"),
        "target_2":      entries.get("target_2"),

        # Fundamentals
        "revenue_growth":   fund_result.get("revenue_growth"),
        "gross_margins":    fund_result.get("gross_margins"),
        "runway_months":    fund_result.get("runway_months"),
        "total_cash":       fund_result.get("total_cash"),

        # Catalysts & News
        "catalyst_notes":   catalyst_result.get("catalyst_notes", []),
        "filing_summary":   sec_analysis.get("filing_summary", []),
        "recent_headlines": news_sentiment.get("recent_headlines", []),
        "hype_alert":       news_sentiment.get("hype_alert", False),
        "sentiment_score":  sent_score,

        # Analyst
        "analyst_recommendation": snapshot.get("analyst_recommendation"),
        "analyst_target":         snapshot.get("analyst_mean_target"),

        # Short interest
        "short_percent_float": snapshot.get("short_percent_float"),
        "short_ratio":         snapshot.get("short_ratio"),

        # Option A: Earnings calendar
        "earnings_date":    snapshot.get("earnings_date"),
        "earnings_warning": snapshot.get("earnings_warning", False),
        "days_to_earnings": snapshot.get("days_to_earnings"),

        # Option C: Sector relative strength
        "sector_etf":        snapshot.get("sector_etf"),
        "sector_return_20d": snapshot.get("sector_return_20d"),
        "stock_vs_sector":   snapshot.get("stock_vs_sector"),
        "sector_rs_label":   snapshot.get("sector_rs_label"),

        # AI filing summary
        "filing_ai_summary": filing_ai_summary,

        # Score breakdown for display
        "score_breakdown": {
            "Technical":    f"{round(tech_score, 1)}/100  (weight: {w['technical']:.0%})",
            "Catalyst":     f"{round(cat_score, 1)}/100  (weight: {w['catalyst']:.0%})",
            "Fundamental":  f"{round(fund_score, 1)}/100  (weight: {w['fundamental']:.0%})",
            "Risk (inverted)": f"{round(risk_contribution, 1)}/100  (raw risk: {round(raw_risk,1)}, weight: {w['risk']:.0%})",
            "Sentiment":    f"{round(sent_score, 1)}/100  (weight: {w['sentiment']:.0%})",
        },

        "summary": _generate_summary(ticker, snapshot, final_score, signal, all_flags,
                                     catalyst_result, news_sentiment, technicals, fund_result),
    }

    # Persist
    if save:
        try:
            save_scan_result(full_result)
        except Exception as e:
            full_result["warnings"].append(f"DB save failed: {str(e)}")

    return full_result


def scan_universe(tickers: List[str], delay: float = 1.0) -> List[Dict]:
    """
    Scan a list of tickers sequentially.
    Returns list of result dicts, sorted by final_score descending.

    delay: seconds between tickers (be polite to free APIs)
    """
    results = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{total}] Scanning {ticker}...")
        try:
            result = scan_ticker(ticker)
            if result:
                results.append(result)
        except Exception as e:
            print(f"  ✗ {ticker} failed: {e}")
            traceback.print_exc()

        if i < total:
            time.sleep(delay)

    # Sort by score, filtered-out stocks go to bottom
    results.sort(
        key=lambda r: (not r.get("filtered_out", False), r.get("final_score", 0)),
        reverse=True
    )

    return results


def _score_to_signal(score: float) -> str:
    """Map a final score to a signal label."""
    for label, (low, high) in SIGNAL_THRESHOLDS.items():
        if low <= score <= high:
            return label
    return "Watchlist"


def _generate_summary(
    ticker, snapshot, score, signal, flags,
    catalyst_result, news_sentiment, technicals, fund_result
) -> str:
    """
    Generate a plain-English summary paragraph for the stock.
    This is shown in the dashboard card view.
    """
    parts = []

    mc = snapshot.get("market_cap", 0)
    mc_str = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
    price = snapshot.get("price", 0)
    rvol  = snapshot.get("relative_volume", 1)
    rsi   = technicals.get("rsi_14")

    parts.append(f"{ticker} ({snapshot.get('company_name', ticker)}) trades at ${price:.4f} with a market cap of {mc_str}.")

    if rvol >= 2.0:
        parts.append(f"Volume is running {rvol:.1f}x the 20-day average — elevated activity today.")
    elif rvol < 0.6:
        parts.append(f"Volume is quiet ({rvol:.1f}x avg) — low conviction move.")

    if rsi:
        if rsi > 70:
            parts.append(f"RSI of {rsi:.0f} signals overbought conditions — extended entry risk.")
        elif rsi < 35:
            parts.append(f"RSI of {rsi:.0f} signals oversold — potential bounce candidate but not confirmed.")
        else:
            parts.append(f"RSI is {rsi:.0f} — within normal range.")

    cat_notes = catalyst_result.get("catalyst_notes", [])
    if cat_notes:
        parts.append("Catalysts: " + ". ".join(cat_notes[:2]) + ".")

    if flags:
        readable = [RISK_FLAGS.get(f, f) for f in flags[:3]]
        parts.append("Risk flags: " + " | ".join(readable))

    runway = fund_result.get("runway_months")
    if runway and runway < 18:
        parts.append(f"Estimated cash runway: ~{runway:.0f} months — watch for dilutive financing.")

    analyst = snapshot.get("analyst_recommendation")
    target  = snapshot.get("analyst_mean_target")
    if analyst and analyst != "none":
        target_str = f" (target: ${target:.2f})" if target else ""
        parts.append(f"Analyst consensus: {analyst.upper()}{target_str}.")

    parts.append(
        f"Scanner signal: {signal} | Score: {score}/100. "
        f"This is a research flag, not a trade recommendation. Always verify before acting."
    )

    return " ".join(parts)
