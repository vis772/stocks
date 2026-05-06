# analysis/fundamentals.py
# Scores fundamentals and risk from yfinance data.
#
# For small-cap speculative stocks, fundamentals are often weak by definition —
# that's why they're speculative. The goal here is to:
#   1. Identify the WORST cases (going concern, extreme burn, no cash)
#   2. Find relative strengths (revenue growth, improving margins)
#   3. Flag dilution risk proactively

from typing import Dict, List, Optional
from config import RISK_FLAGS


def score_fundamentals(snapshot: Dict) -> Dict:
    """
    Compute a fundamental score (0-100) from financial data.

    Small-cap speculative stocks rarely score above 60 here — that's expected.
    A score of 40-55 means "typical speculative name." Below 30 means
    the balance sheet is a serious concern.
    """
    score = 50.0  # Start neutral

    # ── Revenue & Growth (–15 to +20) ─────────────────────────────────────────
    revenue = snapshot.get("revenue")
    rev_growth = snapshot.get("revenue_growth")

    if revenue is not None:
        if revenue > 100_000_000:    # >$100M revenue
            score += 10
        elif revenue > 50_000_000:
            score += 6
        elif revenue > 10_000_000:
            score += 2
        elif revenue < 1_000_000:    # <$1M = pre-revenue
            score -= 10

    if rev_growth is not None:
        if rev_growth > 0.50:        # >50% YoY growth
            score += 20
        elif rev_growth > 0.25:
            score += 12
        elif rev_growth > 0.10:
            score += 6
        elif rev_growth < 0:
            score -= 15              # Declining revenue is a major red flag
        elif rev_growth < -0.20:
            score -= 20

    # ── Cash & Burn Rate (–20 to +15) ─────────────────────────────────────────
    cash = snapshot.get("total_cash")
    fcf  = snapshot.get("free_cashflow")

    if cash is not None:
        market_cap = snapshot.get("market_cap", 1)
        # Cash as % of market cap — high cash relative to market cap is good
        cash_ratio = cash / market_cap if market_cap > 0 else 0
        if cash_ratio > 0.30:
            score += 15
        elif cash_ratio > 0.15:
            score += 8
        elif cash_ratio > 0.05:
            score += 3
        elif cash_ratio < 0.02:
            score -= 15    # Nearly no cash = dilution or bankruptcy risk

    if fcf is not None:
        if fcf > 0:
            score += 10    # Positive free cash flow is rare and good for this universe
        elif fcf < -50_000_000:  # Burning >$50M/yr
            score -= 15
        elif fcf < -20_000_000:
            score -= 8

    # ── Debt (–15 to 0) ───────────────────────────────────────────────────────
    debt = snapshot.get("total_debt")
    if debt is not None and cash is not None:
        net_debt = debt - cash
        mc = snapshot.get("market_cap", 1)
        if mc > 0:
            net_debt_ratio = net_debt / mc
            if net_debt_ratio > 2.0:
                score -= 15
            elif net_debt_ratio > 1.0:
                score -= 10
            elif net_debt_ratio > 0.5:
                score -= 5

    # ── Gross Margins (–10 to +10) ─────────────────────────────────────────────
    gm = snapshot.get("gross_margins")
    if gm is not None:
        if gm > 0.60:
            score += 10    # High-margin business (software, biotech)
        elif gm > 0.40:
            score += 5
        elif gm > 0.20:
            score += 2
        elif gm < 0:
            score -= 10    # Negative gross margins = structurally broken

    # Cap to 0-100
    fundamental_score = round(max(0, min(100, score)), 1)

    # Compute runway estimate (how many months of cash at current burn)
    runway_months = None
    if cash is not None and fcf is not None and fcf < 0:
        monthly_burn = abs(fcf) / 12
        if monthly_burn > 0:
            runway_months = round(cash / monthly_burn, 1)

    return {
        "fundamental_score": fundamental_score,
        "revenue":           revenue,
        "revenue_growth":    rev_growth,
        "gross_margins":     gm,
        "total_cash":        cash,
        "total_debt":        debt,
        "free_cashflow":     fcf,
        "runway_months":     runway_months,
    }


def score_risk(snapshot: Dict, sec_flags: List[str]) -> Dict:
    """
    Compute a risk score (0-100) where HIGHER = MORE RISKY.
    This is inverted in the final scoring model: risk_contribution = 100 - risk_score.

    Why invert? Because we want the final score to be "higher = better."
    A stock with a risk score of 80 (very risky) contributes only 20 points
    to the final score from this category.
    """
    risk_score = 20.0  # Start at low-baseline risk (all small caps have some risk)
    active_flags = list(sec_flags)  # Copy the list from SEC analysis
    flag_details = {}

    # ── Short Interest (0 to +25 risk) ────────────────────────────────────────
    short_pct = snapshot.get("short_percent_float")
    short_ratio = snapshot.get("short_ratio")  # Days to cover

    if short_pct is not None:
        if short_pct > 0.30:          # >30% short
            risk_score += 25
            active_flags.append("high_short_interest")
            flag_details["high_short_interest"] = f"Short interest: {short_pct:.0%} of float"
        elif short_pct > 0.20:
            risk_score += 15
            active_flags.append("high_short_interest")
            flag_details["high_short_interest"] = f"Short interest: {short_pct:.0%} of float"
        elif short_pct > 0.10:
            risk_score += 8

    # ── Market Cap Size (0 to +10 risk) ───────────────────────────────────────
    mc = snapshot.get("market_cap", 0)
    if mc < 100_000_000:      # Under $100M — micro-cap risk
        risk_score += 10
    elif mc < 300_000_000:    # Under $300M — high small-cap risk
        risk_score += 5

    # ── Volatility (0 to +20 risk) ────────────────────────────────────────────
    # (Volatility comes from technicals module, passed in via snapshot)
    vol_ann = snapshot.get("volatility_30d_ann")
    if vol_ann is not None:
        if vol_ann > 150:
            risk_score += 20
            active_flags.append("extreme_volatility")
            flag_details["extreme_volatility"] = f"Annualized volatility: {vol_ann:.0f}%"
        elif vol_ann > 100:
            risk_score += 12
            active_flags.append("extreme_volatility")
            flag_details["extreme_volatility"] = f"Annualized volatility: {vol_ann:.0f}%"
        elif vol_ann > 70:
            risk_score += 6

    # ── Dilution / Shelf Risk from SEC (0 to +20) ─────────────────────────────
    if "shelf_registration" in sec_flags:
        risk_score += 15
        flag_details["shelf_registration"] = "Active shelf registration on file"
    if "atm_offering" in sec_flags:
        risk_score += 20
        flag_details["atm_offering"] = "Active ATM equity program"

    # ── Reverse Split Risk ────────────────────────────────────────────────────
    if "reverse_split_risk" in sec_flags:
        risk_score += 15
        flag_details["reverse_split_risk"] = "Proxy filing suggests possible reverse split"

    # ── Going Concern ─────────────────────────────────────────────────────────
    if "going_concern" in sec_flags:
        risk_score += 25
        flag_details["going_concern"] = "Going concern language detected in filing"

    # ── Liquidity (from avg volume) ───────────────────────────────────────────
    avg_vol = snapshot.get("avg_volume", 0)
    if avg_vol < 500_000:
        risk_score += 10
        active_flags.append("low_liquidity")
        flag_details["low_liquidity"] = f"Avg daily volume: {avg_vol:,}"

    # ── Pump Signal (volume spike without SEC catalyst) ────────────────────────
    rvol = snapshot.get("relative_volume", 1)
    if rvol >= 3.0 and "material_event" not in sec_flags and "dilution_risk" not in sec_flags:
        risk_score += 10
        active_flags.append("pump_signal")
        flag_details["pump_signal"] = f"Volume {rvol:.1f}x avg with no confirmed SEC catalyst"

    # Deduplicate flags
    active_flags = list(set(active_flags))

    risk_score = round(min(100, max(0, risk_score)), 1)

    return {
        "risk_score":   risk_score,   # Higher = more risky
        "active_flags": active_flags,
        "flag_details": flag_details,
    }


def score_catalyst(filings: List[Dict], news_sentiment: Dict, snapshot: Dict) -> Dict:
    """
    Score the strength of current catalysts (0-100).

    Catalysts are the MOST IMPORTANT factor for speculative small-caps.
    A stock moving without a real catalyst is much riskier than one with confirmed news.
    """
    score = 30.0  # Start low — assume no catalyst unless proven

    catalyst_notes = []

    # ── SEC Material Events (8-K filings) ─────────────────────────────────────
    recent_8ks = [f for f in filings if f.get("form_type") == "8-K"]
    if recent_8ks:
        days_ago = recent_8ks[0].get("days_ago", 99)
        if days_ago <= 3:
            score += 25    # Very fresh 8-K
            catalyst_notes.append(f"8-K filed {days_ago} days ago — material event")
        elif days_ago <= 7:
            score += 15
            catalyst_notes.append(f"8-K filed {days_ago} days ago")
        elif days_ago <= 14:
            score += 8

    # ── Positive News Sentiment ────────────────────────────────────────────────
    news_score = news_sentiment.get("sentiment_score", 50)
    kw = news_sentiment.get("catalyst_keywords", [])
    headline_count = news_sentiment.get("headline_count", 0)

    if headline_count == 0:
        score -= 5    # No news coverage = lower catalyst confidence
    elif news_score > 70 and headline_count >= 3:
        score += 20
        catalyst_notes.append(f"Positive news sentiment ({headline_count} articles)")
    elif news_score > 60:
        score += 10
    elif news_score < 35:
        score -= 15
        catalyst_notes.append("Negative news sentiment detected")

    # Bonus for high-value catalyst keywords
    high_value_kw = ["contract", "government", "dod", "nasa", "fda", "partnership", "awarded"]
    hits = [k for k in kw if k in high_value_kw]
    if hits:
        score += 15
        catalyst_notes.append(f"High-value catalyst keywords: {', '.join(hits)}")

    # ── Unusual Volume as Catalyst Proxy ──────────────────────────────────────
    rvol = snapshot.get("relative_volume", 1)
    if rvol >= 2.0 and news_score > 55:
        score += 10    # Volume + positive news = credible catalyst
        catalyst_notes.append(f"Unusual volume ({rvol:.1f}x) with positive news")

    # ── Hype Warning ──────────────────────────────────────────────────────────
    if news_sentiment.get("hype_alert"):
        score -= 15
        catalyst_notes.append("⚠️ Hype keywords detected in news — social media driven risk")

    catalyst_score = round(max(0, min(100, score)), 1)

    return {
        "catalyst_score": catalyst_score,
        "catalyst_notes": catalyst_notes,
    }
