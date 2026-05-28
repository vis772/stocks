# prediction_engine/stage5_conviction.py
# Stage 5 (8:00-8:30 AM ET): Select final top-5 picks and compute
# entry price, target, and stop for each.

import logging
from typing import Optional

from .config import (
    TOP_N_PICKS, DEFAULT_STOP_PCT, DEFAULT_TARGET_PCT,
    ATR_MULTIPLIER, conviction_tier,
)

logger = logging.getLogger("pe.stage5")


def run(consensus_picks: list) -> list:
    """
    Take the sorted consensus list, select top 5, enrich with
    entry/target/stop levels and conviction metadata, and build the
    2-3 sentence thesis.

    Returns a list of up to TOP_N_PICKS pick dicts, each containing
    everything needed by stage6 (report) and stage7 (notify).
    """
    logger.info("[stage5] Finalizing top %d picks from %d consensus candidates",
                TOP_N_PICKS, len(consensus_picks))

    final_picks = []
    for rank, c in enumerate(consensus_picks[:TOP_N_PICKS], 1):
        pick = _build_pick(rank, c)
        final_picks.append(pick)
        logger.info(
            "[stage5] #%d %-8s  score=%.1f  tier=%-10s  %d/4 models  "
            "entry=$%.2f  target=$%.2f  stop=$%.2f  risk/reward=%.1fx",
            rank,
            pick["ticker"],
            pick["conviction_score"],
            pick["conviction_tier"],
            pick["model_agreement_count"],
            pick["entry"],
            pick["target"],
            pick["stop"],
            pick["risk_reward"],
        )

    return final_picks


def _build_pick(rank: int, c: dict) -> dict:
    ticker  = c["ticker"]
    price   = c.get("premarket_price") or c.get("price", 0)
    score   = c["conviction_score"]
    atr     = c.get("atr", 0)

    # Entry: use premarket price (slight 0.25% premium for slippage buffer)
    entry = round(price * 1.0025, 2)

    # Stop: ATR-based if available, else percentage default
    if atr and atr > 0:
        stop = round(entry - ATR_MULTIPLIER * atr, 2)
        stop = max(stop, entry * (1 - DEFAULT_STOP_PCT * 1.5))  # cap at 1.5× default
    else:
        stop = round(entry * (1 - DEFAULT_STOP_PCT), 2)
    stop = max(stop, 0.01)

    # Target: 2R minimum
    stop_dist  = entry - stop
    target_min = entry + (stop_dist * 2)
    target_pct = round(entry * (1 + DEFAULT_TARGET_PCT), 2)
    target     = round(max(target_min, target_pct), 2)

    stop_pct   = round((entry - stop)   / entry * 100, 1)
    target_pct_val = round((target - entry) / entry * 100, 1)
    risk_reward = round((target - entry) / (entry - stop), 2) if entry > stop else 0

    tier = conviction_tier(score)

    # Build 2-3 sentence thesis from model outputs
    thesis = _build_thesis(c, entry, target, stop)

    return {
        "rank":                 rank,
        "ticker":               ticker,
        "price":                price,
        "conviction_score":     score,
        "conviction_tier":      tier,
        "model_agreement_count": c.get("model_agreement_count", 0),
        "agreeing_models":      c.get("agreeing_models", []),
        "model_scores":         c.get("model_scores", {}),
        "catalyst":             c.get("catalyst", ""),
        "technical_setup":      c.get("technical_setup", ""),
        "pattern_summary":      c.get("pattern_summary", ""),
        "sentiment_score":      c.get("sentiment_score"),
        "short_interest":       c.get("short_float_pct", 0),
        "entry":                entry,
        "target":               target,
        "stop":                 stop,
        "stop_pct":             stop_pct,
        "target_pct":           target_pct_val,
        "risk_reward":          risk_reward,
        "gap_pct":              c.get("gap_pct", 0),
        "volume_ratio":         c.get("volume_ratio", 0),
        "rsi":                  c.get("rsi", 0),
        "sector":               c.get("sector", ""),
        "market_cap":           c.get("market_cap", 0),
        "float_shares":         c.get("float_shares", 0),
        "thesis":               thesis,
    }


def _build_thesis(c: dict, entry: float, target: float, stop: float) -> str:
    """Assemble a 2-3 sentence conviction thesis from model insights."""
    parts = []

    catalyst = c.get("catalyst", "").strip()
    technical = c.get("technical_setup", "").strip()
    pattern  = c.get("pattern_summary", "").strip()

    ticker    = c["ticker"]
    score     = c["conviction_score"]
    gap_pct   = c.get("gap_pct", 0)
    vol_ratio = c.get("volume_ratio", 0)
    short_pct = c.get("short_float_pct", 0)
    agreement = c.get("model_agreement_count", 0)

    # Sentence 1: catalyst or momentum driver
    if catalyst and len(catalyst) > 10:
        s1 = _cap_sentence(catalyst)
    else:
        s1 = (f"{ticker} is gapping {gap_pct:+.1f}%% premarket on "
              f"{vol_ratio:.1f}x average volume with {agreement} of 4 "
              f"models in agreement at conviction score {score:.0f}.")
    parts.append(s1)

    # Sentence 2: technical setup or pattern
    if technical and len(technical) > 10:
        s2 = _cap_sentence(technical)
    elif pattern and len(pattern) > 10:
        s2 = _cap_sentence(pattern)
    else:
        risk_reward = round((target - entry) / (entry - stop), 1) if entry > stop else 0
        s2 = (f"Entry at ${entry:.2f} offers a {risk_reward:.1f}R setup "
              f"with target ${target:.2f} and stop ${stop:.2f}.")
    parts.append(s2)

    # Sentence 3: short squeeze or risk note
    if short_pct and short_pct >= 10:
        s3 = (f"Short interest at {short_pct:.1f}%% of float adds squeeze "
              f"potential if momentum sustains above ${entry * 1.02:.2f}.")
        parts.append(s3)

    return " ".join(parts)


def _cap_sentence(text: str) -> str:
    """Ensure sentence ends with a period and starts capitalized."""
    text = text.strip()
    if text and text[0].islower():
        text = text[0].upper() + text[1:]
    if text and text[-1] not in ".!?":
        text += "."
    return text
