# prediction_engine/stage4_consensus.py
# Stage 4 (8:00-8:30 AM ET): Consensus voting across 4 SLMs.
# A ticker advances only if ≥3 models scored it above the agreement threshold.
# Final conviction score is the mean of only the agreeing models' scores.

import logging
from typing import Optional

from .config import (
    MIN_MODEL_AGREEMENT, MODEL_AGREE_THRESHOLD, TOP_N_PICKS,
    MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL,
    ROLE_BEAR, BEAR_RISK_THRESHOLD, BEAR_KILL_THRESHOLD,
)
from .db import save_model_vote, _et_date

logger = logging.getLogger("pe.stage4")

ALL_MODELS = [MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL]


def run(candidates: list, model_scores: dict) -> list:
    """
    Apply consensus voting to all deep-dive results.

    candidates   : list of candidate dicts from Stage 2
    model_scores : {ticker: {model_name: {score, verdict, raw}}} from Stage 3

    Returns list of dicts for all tickers that passed consensus,
    sorted by conviction_score desc.
    """
    logger.info("[stage4] Applying consensus voting to %d candidates", len(candidates))
    date_str = _et_date()

    consensus_results = []

    for c in candidates:
        ticker   = c["ticker"]
        votes    = model_scores.get(ticker, {})

        agreeing_models = []
        all_model_data  = {}

        for model in ALL_MODELS:
            mv    = votes.get(model, {})
            score = mv.get("score")
            raw   = mv.get("raw", {})

            # Persist every vote to DB for audit trail
            if score is not None:
                save_model_vote(
                    date       = date_str,
                    ticker     = ticker,
                    model_name = model,
                    score      = score,
                    verdict    = mv.get("verdict", ""),
                    raw_output = str(raw)[:500],
                )

            all_model_data[model] = {
                "score":   score,
                "verdict": mv.get("verdict"),
                "raw":     raw,
            }

            if score is not None and score >= MODEL_AGREE_THRESHOLD:
                agreeing_models.append((model, score, raw))

        agreement_count = len(agreeing_models)

        if agreement_count < MIN_MODEL_AGREEMENT:
            logger.debug(
                "[stage4] %s REJECTED — %d/%d models agreed (need %d)",
                ticker, agreement_count, len(ALL_MODELS), MIN_MODEL_AGREEMENT,
            )
            continue

        # ── Bear-case risk check ──────────────────────────────────────────
        bear_data    = votes.get(ROLE_BEAR, {})
        bear_risk    = bear_data.get("score") or 0
        primary_risk = (bear_data.get("raw") or {}).get("primary_risk", "")

        if bear_risk >= BEAR_KILL_THRESHOLD:
            logger.info(
                "[stage4] %s KILLED by bear analyst (risk=%d ≥ %d) — %s",
                ticker, bear_risk, BEAR_KILL_THRESHOLD, primary_risk[:60],
            )
            continue

        # Final conviction score = average of agreeing models only
        final_score = sum(s for _, s, _ in agreeing_models) / len(agreeing_models)

        # Bear penalty: risk ≥ threshold reduces conviction proportionally
        if bear_risk >= BEAR_RISK_THRESHOLD:
            penalty     = (bear_risk - BEAR_RISK_THRESHOLD) / (100 - BEAR_RISK_THRESHOLD) * 15
            final_score = max(0, final_score - penalty)
            logger.info(
                "[stage4] %s bear penalty: risk=%d → conviction %.1f → %.1f  (%s)",
                ticker, bear_risk, final_score + penalty, final_score, primary_risk[:50],
            )

        # Synthesize per-model insights into the consensus record
        catalyst        = _extract_field(all_model_data, MODEL_SMOL,  "thesis") or \
                          _extract_field(all_model_data, MODEL_QWEN,  "thesis")
        technical_setup = _extract_field(all_model_data, MODEL_PHI,   "thesis")
        pattern_summary = _extract_field(all_model_data, MODEL_GEMMA, "thesis")

        sentiment_score = _extract_float(all_model_data, MODEL_SMOL, "sentiment_score")
        short_interest  = _extract_float(all_model_data, MODEL_PHI,  "short_float_pct")

        consensus_results.append({
            "ticker":               ticker,
            "conviction_score":     round(final_score, 1),
            "model_agreement_count": agreement_count,
            "agreeing_models":      [m for m, _, _ in agreeing_models],
            "model_scores":         {m: s for m, s, _ in agreeing_models},
            "all_model_data":       all_model_data,
            "catalyst":             catalyst or "",
            "technical_setup":      technical_setup or "",
            "pattern_summary":      pattern_summary or "",
            "sentiment_score":      sentiment_score,
            "short_interest":       short_interest,
            "bear_risk_score":      bear_risk,
            "bear_primary_risk":    primary_risk,
            # Pass through premarket snapshot from Stage 1
            "premarket_price":      c.get("premarket_price") or c.get("price", 0),
            "price":                c.get("price", 0),
            "gap_pct":              c.get("gap_pct", 0),
            "volume_ratio":         c.get("volume_ratio", 0),
            "avg_volume":           c.get("avg_volume", 0),
            "sector":               c.get("sector", ""),
            "market_cap":           c.get("market_cap", 0),
            "float_shares":         c.get("yf_data", {}).get("float_shares", 0),
            "rsi":                  c.get("yf_data", {}).get("rsi", 50),
            "short_float_pct":      c.get("yf_data", {}).get("short_float_pct", 0),
            "sma20":                c.get("yf_data", {}).get("sma20", 0),
            "atr":                  c.get("yf_data", {}).get("atr", 0),
        })

        logger.info(
            "[stage4] %s PASSED — %d/4 models agreed — conviction_score=%.1f",
            ticker, agreement_count, final_score,
        )

    # Sort by conviction_score descending
    consensus_results.sort(key=lambda x: x["conviction_score"], reverse=True)

    top = consensus_results[:TOP_N_PICKS * 3]  # Pass 3x to Stage 5 for entry/stop calc
    logger.info(
        "[stage4] %d tickers passed consensus, %d passed to Stage 5",
        len(consensus_results), len(top),
    )
    return top


# ─── Field extraction helpers ─────────────────────────────────────────────────

def _extract_field(all_data: dict, model: str, field: str) -> Optional[str]:
    raw = all_data.get(model, {}).get("raw", {})
    val = raw.get(field)
    if val and isinstance(val, str) and len(val.strip()) > 0:
        return val.strip()
    return None


def _extract_float(all_data: dict, model: str, field: str) -> Optional[float]:
    raw = all_data.get(model, {}).get("raw", {})
    try:
        val = raw.get(field)
        if val is not None:
            return float(val)
    except (TypeError, ValueError):
        pass
    return None
