# core/scanner.py
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import traceback

from config import SCORING_WEIGHTS, SIGNAL_THRESHOLDS, RISK_FLAGS
from data.market_data import fetch_ticker_snapshot, passes_universe_filter
from data.sec_data import get_recent_filings, analyze_filing_risk, summarize_filing_with_claude
from data.news_data import fetch_ticker_news, analyze_news_sentiment
from analysis.technicals import compute_technicals, suggest_entry_and_stops
from analysis.fundamentals import score_fundamentals, score_risk, score_catalyst
from db.database import save_scan_result


def scan_ticker(ticker: str, save: bool = True, weights: Optional[Dict] = None) -> Optional[Dict]:
    ticker = ticker.upper().strip()
    print(f"  → Scanning {ticker}...")

    result = {
        "ticker":       ticker,
        "scan_date":    datetime.now().strftime("%Y-%m-%d"),
        "scan_time":    datetime.now().strftime("%H:%M:%S"),
        "data_sources": [],
        "risk_flags":   [],
        "warnings":     [],
    }

    # Step 1: Market Data
    try:
        snapshot = fetch_ticker_snapshot(ticker)
        if snapshot is None:
            return {**result, "error": "Could not fetch market data — ticker may be invalid or delisted"}
    except Exception as e:
        return {**result, "error": f"Market data fetch failed: {str(e)}"}

    result["data_sources"].append("Finnhub (real-time) · Yahoo Finance (history/fundamentals)")

    # Step 2: Universe Filter
    passes, reason = passes_universe_filter(snapshot)
    if not passes:
        return {
            **result,
            "company_name":  snapshot.get("company_name", ticker),
            "price":         snapshot.get("price"),
            "market_cap":    snapshot.get("market_cap"),
            "filtered_out":  True,
            "filter_reason": reason,
        }

    # Step 3: Technicals
    hist       = snapshot.get("_history")
    technicals = compute_technicals(hist)
    snapshot["volatility_30d_ann"] = technicals.get("volatility_30d_ann")
    snapshot["relative_volume"]    = technicals.get("relative_volume", 0)

    # Step 4: SEC Filings
    filings      = []
    sec_analysis = {"active_flags": [], "flag_details": {}, "filing_summary": []}
    try:
        filings      = get_recent_filings(ticker, days_back=30)
        sec_analysis = analyze_filing_risk(filings)
        result["data_sources"].append("SEC EDGAR (free)")
    except Exception as e:
        result["warnings"].append(f"SEC data unavailable: {str(e)}")

    # AI filing summary
    filing_ai_summary = ""
    try:
        important = [f for f in filings if f.get("form_type") in ("8-K", "S-3", "424B3", "424B4")]
        if important:
            filing_ai_summary = summarize_filing_with_claude(
                filing_url=important[0]["url"],
                form_type=important[0]["form_type"],
                ticker=ticker,
            )
    except Exception as e:
        result["warnings"].append(f"AI filing summary unavailable: {str(e)}")

    # Step 5: News
    articles       = []
    news_sentiment = {"sentiment_score": 50, "headline_count": 0, "hype_alert": False,
                      "catalyst_keywords": [], "risk_keywords": [], "recent_headlines": []}
    try:
        articles       = fetch_ticker_news(ticker, max_articles=10)
        news_sentiment = analyze_news_sentiment(articles)
        result["data_sources"].append("Yahoo Finance RSS news")
    except Exception as e:
        result["warnings"].append(f"News data unavailable: {str(e)}")

    # Step 6: Scoring
    tech_score       = technicals.get("technical_score", 50)
    catalyst_result  = score_catalyst(filings, news_sentiment, snapshot)
    cat_score        = catalyst_result["catalyst_score"]
    fund_result      = score_fundamentals(snapshot)
    fund_score       = fund_result["fundamental_score"]
    risk_result      = score_risk(snapshot, sec_analysis["active_flags"])
    raw_risk         = risk_result["risk_score"]
    risk_contribution = 100 - raw_risk
    sent_score       = news_sentiment.get("sentiment_score", 50)

    # Step 7: Final Score
    w = weights if weights is not None else SCORING_WEIGHTS
    final_score = round(
        tech_score        * w["technical"]   +
        cat_score         * w["catalyst"]    +
        fund_score        * w["fundamental"] +
        risk_contribution * w["risk"]        +
        sent_score        * w["sentiment"],
        1
    )

    # Step 8: Signal
    signal = _score_to_signal(final_score)

    # Step 9: Risk Flags
    all_flags = list(set(
        sec_analysis.get("active_flags", []) +
        risk_result.get("active_flags", [])
    ))
    if news_sentiment.get("hype_alert"):
        all_flags.append("pump_signal")
    if snapshot.get("earnings_warning"):
        all_flags.append("earnings_imminent")

    critical_flags = {"going_concern", "reverse_split_risk"}
    if critical_flags.intersection(set(all_flags)):
        if signal not in ("Sell", "Avoid"):
            signal = "Avoid"

    # Step 10: Entry/Stop
    entries = suggest_entry_and_stops(snapshot, technicals)

    full_result = {
        **result,
        "company_name":    snapshot.get("company_name", ticker),
        "sector":          snapshot.get("sector", "Unknown"),
        "industry":        snapshot.get("industry", "Unknown"),
        "price":           snapshot.get("price"),
        "market_cap":      snapshot.get("market_cap"),
        "volume":          snapshot.get("volume"),
        "avg_volume":      snapshot.get("avg_volume"),
        "relative_volume": snapshot.get("relative_volume"),
        "technical_score":   round(tech_score, 1),
        "catalyst_score":    round(cat_score, 1),
        "fundamental_score": round(fund_score, 1),
        "risk_score":        round(raw_risk, 1),
        "sentiment_score":   round(sent_score, 1),
        "final_score":       final_score,
        "signal":            signal,
        "risk_flags":        all_flags,
        "flag_details":      {**sec_analysis.get("flag_details", {}), **risk_result.get("flag_details", {})},
        "rsi":               technicals.get("rsi_14"),
        "macd_bullish":      technicals.get("macd_bullish"),
        "above_sma20":       technicals.get("above_sma20"),
        "above_sma50":       technicals.get("above_sma50"),
        "return_1d":         technicals.get("return_1d"),
        "return_5d":         technicals.get("return_5d"),
        "return_20d":        technicals.get("return_20d"),
        "volatility":        technicals.get("volatility_30d_ann"),
        "gap_up":            technicals.get("gap_up"),
        "gap_pct":           technicals.get("gap_pct"),
        "entry_zone":        entries.get("entry_zone_low"),
        "stop_loss":         entries.get("stop_loss"),
        "target_1":          entries.get("target_1"),
        "target_2":          entries.get("target_2"),
        "revenue_growth":    fund_result.get("revenue_growth"),
        "gross_margins":     fund_result.get("gross_margins"),
        "runway_months":     fund_result.get("runway_months"),
        "total_cash":        fund_result.get("total_cash"),
        "catalyst_notes":    catalyst_result.get("catalyst_notes", []),
        "filing_summary":    sec_analysis.get("filing_summary", []),
        "filing_ai_summary": filing_ai_summary,
        "recent_headlines":  news_sentiment.get("recent_headlines", []),
        "hype_alert":        news_sentiment.get("hype_alert", False),
        "analyst_recommendation": snapshot.get("analyst_recommendation"),
        "analyst_target":         snapshot.get("analyst_mean_target"),
        "short_percent_float":    snapshot.get("short_percent_float"),
        "short_ratio":            snapshot.get("short_ratio"),
        "earnings_date":          snapshot.get("earnings_date"),
        "earnings_warning":       snapshot.get("earnings_warning", False),
        "days_to_earnings":       snapshot.get("days_to_earnings"),
        "sector_etf":             snapshot.get("sector_etf"),
        "sector_return_20d":      snapshot.get("sector_return_20d"),
        "stock_vs_sector":        snapshot.get("stock_vs_sector"),
        "sector_rs_label":        snapshot.get("sector_rs_label"),
        "score_breakdown": {
            "Technical":       f"{round(tech_score, 1)}/100  (weight: {w['technical']:.0%})",
            "Catalyst":        f"{round(cat_score, 1)}/100  (weight: {w['catalyst']:.0%})",
            "Fundamental":     f"{round(fund_score, 1)}/100  (weight: {w['fundamental']:.0%})",
            "Risk (inverted)": f"{round(risk_contribution, 1)}/100  (raw risk: {round(raw_risk,1)}, weight: {w['risk']:.0%})",
            "Sentiment":       f"{round(sent_score, 1)}/100  (weight: {w['sentiment']:.0%})",
        },
        "summary": _generate_summary(ticker, snapshot, final_score, signal, all_flags,
                                     catalyst_result, news_sentiment, technicals, fund_result),
    }

    if save:
        try:
            save_scan_result(full_result)
        except Exception as e:
            full_result["warnings"].append(f"DB save failed: {str(e)}")

    return full_result


def scan_universe(tickers: List[str], delay: float = 1.0) -> List[Dict]:
    results = []
    total   = len(tickers)

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

    results.sort(
        key=lambda r: (not r.get("filtered_out", False), r.get("final_score", 0)),
        reverse=True
    )
    return results


def _score_to_signal(score: float) -> str:
    for label, (low, high) in SIGNAL_THRESHOLDS.items():
        if low <= score <= high:
            return label
    return "Watchlist"


def _generate_summary(
    ticker, snapshot, score, signal, flags,
    catalyst_result, news_sentiment, technicals, fund_result
) -> str:
    parts = []

    mc    = snapshot.get("market_cap", 0)
    mc_str = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
    price = snapshot.get("price", 0)
    rvol  = snapshot.get("relative_volume", 1)
    rsi   = technicals.get("rsi_14")
    ret1  = technicals.get("return_1d", 0)
    ret5  = technicals.get("return_5d", 0)
    ret20 = technicals.get("return_20d", 0)
    vol   = technicals.get("volatility_30d_ann")
    above20 = technicals.get("above_sma20")
    above50 = technicals.get("above_sma50")
    macd  = technicals.get("macd_bullish")
    gap   = technicals.get("gap_pct", 0)
    short = snapshot.get("short_percent_float")
    short_ratio = snapshot.get("short_ratio")
    cash  = fund_result.get("total_cash")
    debt  = fund_result.get("total_debt")
    fcf   = fund_result.get("free_cashflow")
    rev   = fund_result.get("revenue")
    rev_g = fund_result.get("revenue_growth")
    gm    = fund_result.get("gross_margins")
    runway = fund_result.get("runway_months")
    analyst_rec = snapshot.get("analyst_recommendation")
    analyst_tgt = snapshot.get("analyst_mean_target")
    analyst_cnt = snapshot.get("analyst_count", 0)
    sector   = snapshot.get("sector", "Unknown")
    industry = snapshot.get("industry", "Unknown")
    week52h  = snapshot.get("week_52_high")
    week52l  = snapshot.get("week_52_low")
    pe  = snapshot.get("pe_ratio")
    ps  = snapshot.get("ps_ratio")
    pb  = snapshot.get("pb_ratio")
    float_sh   = snapshot.get("float_shares")
    shares_out = snapshot.get("shares_outstanding")

    parts.append(
        f"IDENTITY: {ticker} ({snapshot.get('company_name', ticker)}) operates in the "
        f"{industry} industry within the {sector} sector. "
        f"Current price: ${price:.4f} | Market cap: {mc_str}."
    )

    if week52h and week52l:
        pct_from_high = (price - week52h) / week52h * 100
        pct_from_low  = (price - week52l) / week52l * 100
        parts.append(
            f"52-WEEK RANGE: Low ${week52l:.4f} — High ${week52h:.4f}. "
            f"Currently {abs(pct_from_high):.1f}% below 52-week high "
            f"and {pct_from_low:.1f}% above 52-week low."
        )

    ma_status = []
    if above20 is True:   ma_status.append("above SMA20 (short-term uptrend)")
    elif above20 is False: ma_status.append("below SMA20 (short-term downtrend)")
    if above50 is True:   ma_status.append("above SMA50 (medium-term uptrend)")
    elif above50 is False: ma_status.append("below SMA50 (medium-term downtrend)")
    macd_str = "MACD bullish (momentum positive)" if macd else "MACD bearish (momentum negative)" if macd is False else "MACD neutral"

    parts.append(
        f"TECHNICALS: Price is {', '.join(ma_status) if ma_status else 'trend unclear'}. "
        f"{macd_str}. "
        f"RSI(14): {rsi:.1f} — {'overbought, risky entry' if rsi and rsi > 70 else 'oversold, watch for reversal' if rsi and rsi < 35 else 'neutral range'}. "
        f"Returns: 1D {ret1:+.2f}% | 5D {ret5:+.2f}% | 20D {ret20:+.2f}%."
    )

    if gap and abs(gap) > 2:
        parts.append(
            f"GAP: {'Gap-UP' if gap > 0 else 'Gap-DOWN'} of {abs(gap):.1f}% today — "
            f"{'strong buying pressure at open' if gap > 0 else 'selling pressure at open'}."
        )

    if vol:
        vol_label = "extreme — position size very carefully" if vol > 150 else "high" if vol > 100 else "elevated" if vol > 70 else "moderate"
        parts.append(f"VOLATILITY: {vol:.0f}% annualized ({vol_label}).")

    avg_vol   = snapshot.get("avg_volume", 0)
    today_vol = snapshot.get("volume", 0)
    if rvol >= 3.0:
        parts.append(f"VOLUME: Extremely elevated at {rvol:.1f}x average ({today_vol:,} vs avg {avg_vol:,}). Verify against SEC filings and news before acting.")
    elif rvol >= 2.0:
        parts.append(f"VOLUME: Unusual at {rvol:.1f}x average ({today_vol:,} today). Check for catalyst.")
    elif rvol < 0.5:
        parts.append(f"VOLUME: Very quiet at {rvol:.1f}x average. Low conviction.")
    else:
        parts.append(f"VOLUME: Normal at {rvol:.1f}x average ({today_vol:,} today).")

    if short is not None:
        squeeze = short > 0.20 and short_ratio and short_ratio > 5
        parts.append(
            f"SHORT INTEREST: {short*100:.1f}% of float shorted "
            f"({'days to cover: ' + str(round(short_ratio,1)) if short_ratio else ''}). "
            f"{'HIGH — significant bearish conviction. ' if short > 0.20 else ''}"
            f"{'Short squeeze potential if positive catalyst emerges.' if squeeze else ''}"
            f"{'Low short interest.' if short < 0.05 else ''}"
        )

    if float_sh and shares_out:
        float_pct = float_sh / shares_out * 100
        float_str = f"{float_sh/1e6:.1f}M" if float_sh < 1e9 else f"{float_sh/1e9:.2f}B"
        parts.append(
            f"FLOAT: {float_str} shares ({float_pct:.1f}% of shares outstanding). "
            f"{'Low float — price moves dramatically on volume.' if float_sh < 20_000_000 else 'Moderate float.' if float_sh < 100_000_000 else 'Large float — harder to move.'}"
        )

    fund_parts = []
    if rev:
        rev_str = f"${rev/1e9:.2f}B" if rev >= 1e9 else f"${rev/1e6:.1f}M"
        fund_parts.append(f"Revenue: {rev_str}")
    if rev_g is not None:
        fund_parts.append(f"YoY growth: {rev_g*100:.0f}% ({'strong' if rev_g > 0.5 else 'solid' if rev_g > 0.2 else 'modest' if rev_g > 0 else 'DECLINING — red flag'})")
    if gm is not None:
        fund_parts.append(f"Gross margin: {gm*100:.1f}% ({'high quality' if gm > 0.6 else 'decent' if gm > 0.3 else 'thin' if gm > 0 else 'NEGATIVE — structurally broken'})")
    if fund_parts:
        parts.append("FUNDAMENTALS: " + " | ".join(fund_parts) + ".")

    bs_parts = []
    if cash is not None:
        bs_parts.append(f"Cash: ${cash/1e6:.1f}M" if cash < 1e9 else f"Cash: ${cash/1e9:.2f}B")
    if debt is not None:
        bs_parts.append(f"Debt: ${debt/1e6:.1f}M" if debt < 1e9 else f"Debt: ${debt/1e9:.2f}B")
    if fcf is not None:
        bs_parts.append(f"FCF: {'positive +' if fcf > 0 else 'burning -'}${abs(fcf)/1e6:.1f}M/yr")
    if runway:
        urgency = "CRITICAL" if runway < 6 else "concerning" if runway < 12 else "watch closely" if runway < 18 else "adequate"
        bs_parts.append(f"Runway: ~{runway:.0f} months ({urgency})")
    if bs_parts:
        parts.append("BALANCE SHEET: " + " | ".join(bs_parts) + ".")

    val_parts = []
    if pe: val_parts.append(f"P/E: {pe:.1f}x")
    if ps: val_parts.append(f"P/S: {ps:.1f}x {'(expensive)' if ps > 20 else '(reasonable)' if ps < 5 else ''}")
    if pb: val_parts.append(f"P/B: {pb:.1f}x")
    if val_parts:
        parts.append("VALUATION: " + " | ".join(val_parts) + ".")

    cat_notes = catalyst_result.get("catalyst_notes", [])
    kw = news_sentiment.get("catalyst_keywords", [])
    if cat_notes:
        parts.append("CATALYSTS: " + " | ".join(cat_notes[:4]) + ".")
    if kw:
        parts.append(f"CATALYST KEYWORDS in news: {', '.join(kw)}.")

    sent_score     = news_sentiment.get("sentiment_score", 50)
    headline_count = news_sentiment.get("headline_count", 0)
    pos_count      = news_sentiment.get("positive_count", 0)
    neg_count      = news_sentiment.get("negative_count", 0)
    risk_kw        = news_sentiment.get("risk_keywords", [])
    sent_label     = "strongly positive" if sent_score > 70 else "positive" if sent_score > 55 else "neutral" if sent_score > 45 else "negative" if sent_score > 30 else "strongly negative"
    parts.append(
        f"NEWS SENTIMENT: {sent_label} ({sent_score}/100). "
        f"{headline_count} articles — {pos_count} positive, {neg_count} negative. "
        f"{'⚠️ Hype keywords detected.' if news_sentiment.get('hype_alert') else ''}"
        f"{' Risk keywords: ' + ', '.join(risk_kw) + '.' if risk_kw else ''}"
    )

    if analyst_rec and analyst_rec != "none":
        tgt_str = f" | Target: ${analyst_tgt:.2f} ({((analyst_tgt/price)-1)*100:+.1f}% upside)" if analyst_tgt and price else ""
        parts.append(f"ANALYSTS: {analyst_rec.upper()} from {analyst_cnt} analyst(s){tgt_str}.")
    else:
        parts.append("ANALYSTS: No coverage — typical for small/micro-cap names.")

    if flags:
        parts.append("RISK FLAGS: " + " | ".join([RISK_FLAGS.get(f, f) for f in flags]) + ".")

    # Earnings warning in summary
    earnings_date = snapshot.get("earnings_date")
    days_to_earn  = snapshot.get("days_to_earnings")
    if earnings_date:
        warn = f" ⚠ EARNINGS IN {days_to_earn} DAYS — binary event risk." if snapshot.get("earnings_warning") else ""
        parts.append(f"EARNINGS: Next report expected {earnings_date}.{warn}")

    # Sector RS in summary
    rs_label = snapshot.get("sector_rs_label")
    if rs_label:
        parts.append(f"SECTOR RELATIVE STRENGTH: {rs_label}.")

    parts.append(
        f"VERDICT: Signal is {signal} | Score {score:.0f}/100. "
        f"Not a prediction — verify all data independently before acting."
    )

    return "\n\n".join(parts)
