# analysis/portfolio.py
# Analyzes your holdings in the context of current prices and scan results.
#
# IMPORTANT: These recommendations are mechanical outputs based on price math.
# They do not know your tax situation, total wealth, income, or risk tolerance.
# Treat them as a starting point for your own decision-making, not instructions.

import pandas as pd
from typing import Dict, Optional
from config import SIGNAL_THRESHOLDS


def analyze_holding(
    ticker: str,
    shares: float,
    avg_cost: float,
    current_price: float,
    final_score: float,
    active_flags: list,
    technicals: Dict,
    fundamentals: Dict,
) -> Dict:
    """
    Produce a portfolio-aware analysis for a single holding.

    Returns recommendation (Hold / Trim / Sell / Add), reasoning,
    P&L stats, and risk concentration context.
    """
    position_value    = shares * current_price
    cost_basis        = shares * avg_cost
    unrealized_pnl    = position_value - cost_basis
    unrealized_pnl_pct = (current_price / avg_cost - 1) * 100 if avg_cost > 0 else 0

    # ── Determine Recommendation ──────────────────────────────────────────────
    recommendation = _get_recommendation(
        score=final_score,
        pnl_pct=unrealized_pnl_pct,
        active_flags=active_flags,
        technicals=technicals,
    )

    # ── Build Reasoning ───────────────────────────────────────────────────────
    reasoning = _build_reasoning(
        ticker=ticker,
        recommendation=recommendation,
        pnl_pct=unrealized_pnl_pct,
        final_score=final_score,
        active_flags=active_flags,
        technicals=technicals,
        fundamentals=fundamentals,
    )

    # ── Stop Loss Suggestion ──────────────────────────────────────────────────
    # Based on your average cost, not current price — protects your basis
    suggested_stop = round(avg_cost * 0.80, 4)    # -20% from your avg cost
    hard_stop      = round(avg_cost * 0.70, 4)    # -30% hard stop

    return {
        "ticker":              ticker,
        "shares":              shares,
        "avg_cost":            avg_cost,
        "current_price":       current_price,
        "position_value":      round(position_value, 2),
        "cost_basis":          round(cost_basis, 2),
        "unrealized_pnl":      round(unrealized_pnl, 2),
        "unrealized_pnl_pct":  round(unrealized_pnl_pct, 2),
        "recommendation":      recommendation,
        "reasoning":           reasoning,
        "suggested_stop":      suggested_stop,
        "hard_stop":           hard_stop,
        "final_score":         final_score,
        "active_flags":        active_flags,
    }


def _get_recommendation(
    score: float,
    pnl_pct: float,
    active_flags: list,
    technicals: Dict,
) -> str:
    """
    Map score + context to a portfolio action recommendation.

    Logic hierarchy:
    1. Critical risk flags → immediate Sell regardless of score
    2. Large loss + weak score → Sell
    3. Large gain + weak score → Trim (lock in profit)
    4. Score drives Hold/Add for everything else
    """

    # ── Critical Overrides ────────────────────────────────────────────────────
    critical_flags = {"going_concern", "reverse_split_risk"}
    if critical_flags.intersection(set(active_flags)):
        return "Sell"

    # ── Loss-Based Rules ──────────────────────────────────────────────────────
    if pnl_pct < -30:
        if score < 40:
            return "Sell"      # Down >30% AND weak score — cut it
        elif score < 55:
            return "Hold"      # Down big but score is okay — reassess
        else:
            return "Hold"      # Down big but score is good — potential bounce

    if pnl_pct < -15 and score < 35:
        return "Sell"          # Down 15-30% with weak fundamentals

    # ── Gain-Based Rules ──────────────────────────────────────────────────────
    if pnl_pct > 100 and score < 50:
        return "Trim"          # Doubled + score declining = take some off

    if pnl_pct > 50 and score < 40:
        return "Trim"

    # ── Score-Based Rules ─────────────────────────────────────────────────────
    if score >= 65:
        if "shelf_registration" in active_flags or "atm_offering" in active_flags:
            return "Hold"      # Good score but dilution risk = don't add
        return "Add (if confirmed)"   # Strong signal — could add IF you have conviction

    if score >= 45:
        return "Hold"

    if score >= 30:
        return "Hold"          # Weak but not sell territory yet — watch closely

    return "Sell"              # Score below 30 = weak case for holding


def _build_reasoning(
    ticker: str,
    recommendation: str,
    pnl_pct: float,
    final_score: float,
    active_flags: list,
    technicals: Dict,
    fundamentals: Dict,
) -> str:
    """Build a plain-English explanation for the portfolio recommendation."""
    lines = []

    pnl_str = f"{'▲' if pnl_pct >= 0 else '▼'} {abs(pnl_pct):.1f}%"
    lines.append(f"Position is {pnl_str} from your average cost.")
    lines.append(f"Current scanner score: {final_score:.0f}/100.")

    # Flag warnings
    flag_messages = {
        "going_concern":       "⚠️ Going concern warning in recent SEC filing — high bankruptcy risk.",
        "shelf_registration":  "⚠️ Active shelf registration — dilution could happen any time.",
        "atm_offering":        "⚠️ ATM offering active — shares being continuously sold into market.",
        "reverse_split_risk":  "⚠️ Proxy filing suggests possible reverse split vote pending.",
        "high_short_interest": "⚠️ High short interest — volatile, but squeeze potential too.",
        "extreme_volatility":  "⚠️ Extreme volatility — position sizing should be conservative.",
        "pump_signal":         "⚠️ Volume spike without confirmed catalyst — watch for fade.",
        "low_liquidity":       "⚠️ Low average volume — large orders may move price against you.",
    }
    for flag in active_flags:
        if flag in flag_messages:
            lines.append(flag_messages[flag])

    # Recommendation context
    rec_context = {
        "Sell":              "Recommendation: SELL. Score is too weak and/or risk flags are critical.",
        "Trim":              "Recommendation: TRIM. Consider selling 25-50% to lock in gains while reducing risk.",
        "Hold":              "Recommendation: HOLD. Keep position but do not add at current levels.",
        "Add (if confirmed)": "Recommendation: ADD — only if you see a fresh catalyst or breakout confirmation. Never average up blindly.",
    }
    lines.append(rec_context.get(recommendation, f"Recommendation: {recommendation}."))

    # RSI context
    rsi = technicals.get("rsi_14")
    if rsi:
        if rsi > 70:
            lines.append(f"RSI is {rsi:.0f} — technically extended. Not an ideal add point.")
        elif rsi < 35:
            lines.append(f"RSI is {rsi:.0f} — oversold territory. Watch for stabilization before adding.")

    # Runway context
    runway = fundamentals.get("runway_months")
    if runway and runway < 12:
        lines.append(f"⚠️ Estimated cash runway: ~{runway:.0f} months. Dilution or financing needed soon.")

    lines.append("⚠️ This analysis does not account for your taxes, total portfolio size, or personal risk tolerance.")

    return " | ".join(lines)


def compute_portfolio_summary(holdings_analysis: list) -> Dict:
    """
    High-level portfolio statistics across all holdings.
    Useful for spotting concentration risk.
    """
    if not holdings_analysis:
        return {}

    total_value    = sum(h["position_value"] for h in holdings_analysis)
    total_cost     = sum(h["cost_basis"] for h in holdings_analysis)
    total_pnl      = total_value - total_cost
    total_pnl_pct  = (total_value / total_cost - 1) * 100 if total_cost > 0 else 0

    # Concentration: % of total portfolio per position
    for h in holdings_analysis:
        h["portfolio_pct"] = round(h["position_value"] / total_value * 100, 1) if total_value > 0 else 0

    # Flag over-concentrated positions (>25% of portfolio)
    concentrated = [h for h in holdings_analysis if h.get("portfolio_pct", 0) > 25]

    return {
        "total_value":       round(total_value, 2),
        "total_cost":        round(total_cost, 2),
        "total_pnl":         round(total_pnl, 2),
        "total_pnl_pct":     round(total_pnl_pct, 2),
        "num_holdings":      len(holdings_analysis),
        "concentrated_risk": [h["ticker"] for h in concentrated],
        "sell_count":        sum(1 for h in holdings_analysis if h["recommendation"] == "Sell"),
        "trim_count":        sum(1 for h in holdings_analysis if h["recommendation"] == "Trim"),
        "hold_count":        sum(1 for h in holdings_analysis if h["recommendation"] == "Hold"),
        "add_count":         sum(1 for h in holdings_analysis if "Add" in h["recommendation"]),
    }
