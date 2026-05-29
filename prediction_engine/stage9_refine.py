# prediction_engine/stage9_refine.py
# Stage 9: Pre-Open Refinement Loop
#
# After the initial top-5 picks are identified (~4:20 AM ET), this stage
# continuously re-analyzes them with all 4 Ollama models + fresh premarket data
# every REFINEMENT_INTERVAL_MIN minutes, until REFINEMENT_NOTIFY_HOUR:MINUTE.
#
# Purpose: picks found at 4 AM may be stale by 8 AM — news breaks, gaps shift,
# volume patterns evolve. Each pass uses the latest premarket prices and news.
# The PDF and Pushover notification are sent immediately after this stage exits.

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import yfinance as yf

from .config import (
    REFINEMENT_NOTIFY_HOUR, REFINEMENT_NOTIFY_MINUTE, REFINEMENT_INTERVAL_MIN,
    MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL,
    MIN_MODEL_AGREEMENT, MODEL_AGREE_THRESHOLD,
)
from . import stage3_deepdive, stage5_conviction

logger = logging.getLogger("pe.stage9")

_ALL_MODELS = [MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL]
_PROMPT_MAP = [
    (MODEL_QWEN,  stage3_deepdive._build_qwen_prompt,  stage3_deepdive._SYS_QWEN),
    (MODEL_PHI,   stage3_deepdive._build_phi_prompt,   stage3_deepdive._SYS_PHI),
    (MODEL_GEMMA, stage3_deepdive._build_gemma_prompt, stage3_deepdive._SYS_GEMMA),
    (MODEL_SMOL,  stage3_deepdive._build_smol_prompt,  stage3_deepdive._SYS_SMOL),
]


def _et_now() -> datetime:
    utc    = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    return utc + timedelta(hours=offset)


def run(picks: list, initial_model_scores: dict, skip_waits: bool = False) -> list:
    """
    Continuously re-analyze the top 5 picks until REFINEMENT_NOTIFY_HOUR:MINUTE ET.

    Args:
        picks:                Top-5 picks from Stage 5 (with all premarket fields).
        initial_model_scores: Stage 3 scores {ticker: {model: {score, verdict, raw}}}.
        skip_waits:           If True, skip the loop immediately (for --skip-waits testing).

    Returns:
        Final picks with refined conviction scores, ready for Stage 6 (PDF) and
        Stage 7 (Pushover notification).
    """
    if not picks:
        return picks

    if skip_waits:
        logger.info("[stage9] [SKIP-WAITS] Bypassing refinement loop — returning initial picks")
        return picks

    now    = _et_now()
    cutoff = now.replace(
        hour=REFINEMENT_NOTIFY_HOUR,
        minute=max(0, REFINEMENT_NOTIFY_MINUTE - 5),
        second=0, microsecond=0,
    )

    if now >= cutoff:
        logger.info("[stage9] Already past %02d:%02d ET — skipping refinement loop",
                    REFINEMENT_NOTIFY_HOUR, REFINEMENT_NOTIFY_MINUTE - 5)
        return picks

    minutes_remaining  = (cutoff - now).total_seconds() / 60
    iterations_planned = max(1, int(minutes_remaining / REFINEMENT_INTERVAL_MIN))
    tickers            = [p["ticker"] for p in picks]

    logger.info(
        "[stage9] Refinement loop started for %s — "
        "%.0f min until %02d:%02d ET → ~%d passes at %d-min intervals",
        ", ".join(tickers), minutes_remaining,
        REFINEMENT_NOTIFY_HOUR, REFINEMENT_NOTIFY_MINUTE,
        iterations_planned, REFINEMENT_INTERVAL_MIN,
    )

    current_picks         = list(picks)
    refinement_candidates = _picks_to_candidates(picks)
    iteration             = 0

    while True:
        if _et_now() >= cutoff:
            logger.info("[stage9] Cutoff reached after %d iterations", iteration)
            break

        # Sleep until next refinement slot
        next_slot = _et_now().replace(second=0, microsecond=0) + timedelta(minutes=REFINEMENT_INTERVAL_MIN)
        wait_secs = max(0.0, (next_slot - _et_now()).total_seconds())
        if wait_secs > 5:
            logger.info("[stage9] Next pass in %.0f min (%s ET)",
                        wait_secs / 60, next_slot.strftime("%H:%M"))
            time.sleep(wait_secs)
            if _et_now() >= cutoff:
                logger.info("[stage9] Cutoff reached after sleeping — exiting")
                break

        iteration += 1
        iter_start = time.monotonic()
        logger.info(
            "[stage9] ── Pass %d / ~%d  (%s ET) ──────────────────────────────",
            iteration, iterations_planned, _et_now().strftime("%H:%M"),
        )

        # 1. Refresh live premarket prices
        try:
            refinement_candidates = _refresh_premarket_prices(refinement_candidates)
        except Exception as e:
            logger.warning("[stage9] Pass %d: price refresh failed (%s)", iteration, e)

        # 2. Re-enrich with fresh yfinance technicals + Finnhub news
        try:
            enriched = stage3_deepdive._enrich_all(refinement_candidates)
        except Exception as e:
            logger.warning("[stage9] Pass %d: data enrich failed (%s) — using cached data",
                           iteration, e)
            enriched = refinement_candidates

        # 3. Re-run all 4 models on the top 5 tickers
        new_scores: dict = {}
        for c in enriched:
            ticker = c["ticker"]
            new_scores[ticker] = {}
            for model_name, prompt_fn, sys_prompt in _PROMPT_MAP:
                score, verdict, raw = stage3_deepdive._run_model(
                    model_name, sys_prompt, prompt_fn, c
                )
                new_scores[ticker][model_name] = {
                    "score":   score,
                    "verdict": verdict,
                    "raw":     raw or {},
                }
                # Log notable score shifts
                prev = _prev_score(current_picks, ticker, model_name, initial_model_scores)
                if prev is not None and score is not None and abs(score - prev) >= 5:
                    logger.info("[stage9] Pass %d  %-6s / %-20s  %3.0f → %3.0f  (%+.0f)",
                                iteration, ticker, model_name, prev, score, score - prev)

        # 4. Re-compute consensus (no DB writes — refinement is ephemeral scoring)
        refined_consensus = _consensus(enriched, new_scores)

        # 5. Only update picks if at least half still pass consensus
        if len(refined_consensus) >= max(1, len(picks) // 2):
            try:
                refined_picks = stage5_conviction.run(refined_consensus)
                if refined_picks:
                    for p in refined_picks:
                        orig  = next((x for x in current_picks if x["ticker"] == p["ticker"]), {})
                        prev  = orig.get("conviction_score", 0)
                        new   = p.get("conviction_score", 0)
                        delta = new - prev
                        arrow = "↑" if delta > 1 else ("↓" if delta < -1 else "─")
                        logger.info(
                            "[stage9] Pass %d  %s %-6s  %.1f → %.1f  (%+.1f)  "
                            "[%s]  %d/4 models",
                            iteration, arrow, p["ticker"], prev, new, delta,
                            p.get("conviction_tier", ""),
                            p.get("model_agreement_count", 0),
                        )
                    current_picks         = refined_picks
                    refinement_candidates = _picks_to_candidates(current_picks)
            except Exception as e:
                logger.warning("[stage9] Pass %d: Stage 5 re-run failed: %s", iteration, e)
        else:
            logger.warning(
                "[stage9] Pass %d: only %d/%d tickers passed consensus — "
                "keeping previous picks",
                iteration, len(refined_consensus), len(picks),
            )

        logger.info(
            "[stage9] Pass %d done in %.0fs  — top: %s (%.1f)",
            iteration,
            time.monotonic() - iter_start,
            current_picks[0]["ticker"] if current_picks else "?",
            current_picks[0].get("conviction_score", 0) if current_picks else 0,
        )

    # Final summary
    logger.info("[stage9] Refinement complete — %d passes.  Initial → Final conviction:", iteration)
    for i, p in enumerate(current_picks, 1):
        orig  = next((x for x in picks if x["ticker"] == p["ticker"]), {})
        delta = p.get("conviction_score", 0) - orig.get("conviction_score", 0)
        logger.info(
            "  %d. %-6s  %.1f → %.1f  (%+.1f)  [%s]  gap=%+.1f%%  entry=$%.2f",
            i, p["ticker"],
            orig.get("conviction_score", 0), p.get("conviction_score", 0), delta,
            p.get("conviction_tier", ""),
            p.get("gap_pct", 0), p.get("entry", 0),
        )

    return current_picks


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _picks_to_candidates(picks: list) -> list:
    """Convert Stage 5 pick dicts back to the slim candidate format for re-enrichment."""
    return [
        {
            "ticker":          p["ticker"],
            "premarket_price": p.get("price") or p.get("entry", 0),
            "price":           p.get("price", 0),
            "gap_pct":         p.get("gap_pct", 0),
            "volume_ratio":    p.get("volume_ratio", 0),
            "sector":          p.get("sector", ""),
            "market_cap":      p.get("market_cap", 0),
            "avg_volume":      p.get("avg_volume", 0),
        }
        for p in picks
    ]


def _refresh_premarket_prices(candidates: list) -> list:
    """
    Update premarket_price and gap_pct for each candidate from live yfinance data.
    Only updates if a valid premarket price is available (active premarket session).
    """
    for c in candidates:
        try:
            info      = yf.Ticker(c["ticker"]).fast_info
            pre_price = getattr(info, "last_price", None)
            prev_close = getattr(info, "previous_close", None)
            if pre_price and pre_price > 0:
                c["premarket_price"] = float(pre_price)
                if prev_close and prev_close > 0:
                    c["gap_pct"] = (float(pre_price) - float(prev_close)) / float(prev_close) * 100
        except Exception:
            pass  # Keep previous values on any error
    return candidates


def _consensus(candidates: list, model_scores: dict) -> list:
    """
    Lightweight consensus computation — same logic as Stage 4 but no DB writes.
    Used during refinement iterations so we don't spam prediction_engine_model_votes.
    """
    results = []
    for c in candidates:
        ticker = c["ticker"]
        votes  = model_scores.get(ticker, {})

        agreeing = [
            (m, votes[m]["score"], votes[m].get("raw", {}))
            for m in _ALL_MODELS
            if m in votes
            and votes[m].get("score") is not None
            and votes[m]["score"] >= MODEL_AGREE_THRESHOLD
        ]

        if len(agreeing) < MIN_MODEL_AGREEMENT:
            logger.debug("[stage9] %s: %d/4 models agreed — below threshold, skipping",
                         ticker, len(agreeing))
            continue

        final_score     = sum(s for _, s, _ in agreeing) / len(agreeing)
        all_model_data  = {m: votes.get(m, {}) for m in _ALL_MODELS}

        result = dict(c)
        result.update({
            "conviction_score":      round(final_score, 1),
            "model_agreement_count": len(agreeing),
            "agreeing_models":       [m for m, _, _ in agreeing],
            "model_scores":          {m: s for m, s, _ in agreeing},
            "all_model_data":        all_model_data,
            "catalyst":              _thesis(all_model_data, MODEL_SMOL) or _thesis(all_model_data, MODEL_QWEN) or "",
            "technical_setup":       _thesis(all_model_data, MODEL_PHI)  or "",
            "pattern_summary":       _thesis(all_model_data, MODEL_GEMMA) or "",
            "sentiment_score":       None,
            "short_interest":        None,
            # Stage 5 needs atr at the top level
            "atr":                   c.get("yf_data", {}).get("atr", 0),
            "rsi":                   c.get("yf_data", {}).get("rsi", 50),
            "short_float_pct":       c.get("yf_data", {}).get("short_float_pct", 0),
            "sma20":                 c.get("yf_data", {}).get("sma20", 0),
            "float_shares":          c.get("yf_data", {}).get("float_shares", 0),
        })
        results.append(result)

    results.sort(key=lambda x: x["conviction_score"], reverse=True)
    return results


def _thesis(all_model_data: dict, model: str) -> Optional[str]:
    """Extract thesis string from a model's raw output."""
    raw = all_model_data.get(model, {}).get("raw", {})
    val = raw.get("thesis", "")
    return val.strip() if isinstance(val, str) and val.strip() else None


def _prev_score(current_picks: list, ticker: str, model_name: str,
                initial_model_scores: dict) -> Optional[float]:
    """Get the last known model score for a ticker (for delta logging)."""
    for p in current_picks:
        if p.get("ticker") == ticker:
            return p.get("model_scores", {}).get(model_name)
    return initial_model_scores.get(ticker, {}).get(model_name, {}).get("score")
