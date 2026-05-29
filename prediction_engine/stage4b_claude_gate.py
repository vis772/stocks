# prediction_engine/stage4b_claude_gate.py
# Stage 4b: Claude API Quality Gate
#
# After the 4 SLMs vote and the bear analyst penalises risky picks, this stage
# sends all surviving candidates to Claude for one final CONFIRM / KILL pass.
#
# Why: SLMs pattern-match prompts. They can't reason about whether a catalyst
# is real, whether a gap is chasing exhaustion, or whether short interest is
# likely to squeeze vs. compress. Claude can.
#
# Cost: ~15 candidates × ~300 tokens each + 500 token response = ~5000 tokens.
# At claude-haiku pricing that's < $0.01 per day. Negligible.
#
# Batched into ONE API call. No per-ticker loops.

import json
import logging
from typing import Optional

import anthropic

from .config import (
    ANTHROPIC_API_KEY, PE_CLAUDE_MODEL,
    CLAUDE_GATE_ENABLED, CLAUDE_GATE_MIN_KEEP,
)

logger = logging.getLogger("pe.stage4b")

_SYSTEM = (
    "You are the final risk manager for a pre-market stock scanner. "
    "You receive candidates that have already passed a 4-model SLM consensus. "
    "Your job is to eliminate setups that are likely traps, not genuine opportunities. "
    "Be concise and decisive. Return only valid JSON — no markdown, no explanation."
)

_GATE_PROMPT = """\
Review these pre-market conviction candidates. Each passed a 4-model AI consensus.
Your job: CONFIRM the genuine setups, KILL the traps and weak hands.

KILL if any apply:
  - Gap > 20% with no confirmed catalyst name (halt/climax risk)
  - Earnings within 3 days AND gap > 8% (binary event, uncontrollable)
  - Bear risk score ≥ 70 AND conviction < 72 (risky setup, marginal conviction)
  - Thesis is vague ("strong momentum", "positive sentiment") — no specific named catalyst
  - RSI > 75 (chasing an already-extended move)
  - Setup reads like social media hype with no fundamental anchor

CONFIRM if:
  - Specific named catalyst: earnings beat, FDA result, contract, M&A, partnership
  - Gap 3-15% with volume > 2x and defined support below entry
  - RSI 40-68 (momentum without being overbought)
  - Bear risk < 60 (clean from adversarial review)

Candidates:
{candidates_block}

Return ONLY a JSON array, one entry per candidate, same order as listed:
[{{"ticker": "X", "verdict": "CONFIRM", "reason": "named catalyst, clean setup"}}, ...]
Verdict must be exactly "CONFIRM" or "KILL".
"""


def run(candidates: list) -> list:
    """
    Final Claude quality gate. Returns confirmed candidates only.
    Falls back to returning all candidates if the API is unavailable.

    Args:
        candidates: Stage 4 consensus results (up to 15 candidates).

    Returns:
        Filtered list — only CONFIRM verdicts. Always returns at least
        CLAUDE_GATE_MIN_KEEP picks (by conviction score) even if Claude kills more.
    """
    if not CLAUDE_GATE_ENABLED:
        logger.info("[stage4b] Claude gate disabled (CLAUDE_GATE_ENABLED=False) — passing all through")
        return candidates

    if not ANTHROPIC_API_KEY:
        logger.warning("[stage4b] ANTHROPIC_API_KEY not set — skipping Claude gate")
        return candidates

    if not candidates:
        return candidates

    logger.info("[stage4b] Claude gate: reviewing %d candidates via %s",
                len(candidates), PE_CLAUDE_MODEL)

    candidates_block = _format_candidates(candidates)
    prompt           = _GATE_PROMPT.format(candidates_block=candidates_block)

    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model      = PE_CLAUDE_MODEL,
            max_tokens = 600,
            system     = _SYSTEM,
            messages   = [{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text if response.content else ""
        logger.debug("[stage4b] Raw response: %s", raw[:400])

        verdicts = _parse_verdicts(raw)

        if not verdicts:
            logger.warning("[stage4b] Could not parse Claude response — passing all candidates through")
            return candidates

        # Log and filter
        confirmed, killed = [], []
        for c in candidates:
            ticker  = c["ticker"]
            verdict = verdicts.get(ticker, {})
            action  = verdict.get("verdict", "CONFIRM").upper()
            reason  = verdict.get("reason", "")

            if action == "KILL":
                killed.append(ticker)
                logger.info("[stage4b] KILL %-6s — %s", ticker, reason[:70])
            else:
                confirmed.append(c)
                logger.info("[stage4b] CONFIRM %-6s — %s  (conviction=%.1f)",
                            ticker, reason[:60], c.get("conviction_score", 0))

        logger.info("[stage4b] Gate result: %d confirmed, %d killed",
                    len(confirmed), len(killed))

        # Safety floor: always keep at least CLAUDE_GATE_MIN_KEEP picks
        if len(confirmed) < CLAUDE_GATE_MIN_KEEP:
            missing = CLAUDE_GATE_MIN_KEEP - len(confirmed)
            # Add back the highest-conviction killed picks
            killed_candidates = [c for c in candidates if c["ticker"] in killed]
            killed_candidates.sort(key=lambda x: x.get("conviction_score", 0), reverse=True)
            rescued = killed_candidates[:missing]
            for c in rescued:
                logger.warning("[stage4b] Safety floor: reinstating %s (conviction=%.1f)",
                               c["ticker"], c.get("conviction_score", 0))
            confirmed = confirmed + rescued

        confirmed.sort(key=lambda x: x.get("conviction_score", 0), reverse=True)
        return confirmed

    except anthropic.AuthenticationError:
        logger.error("[stage4b] Invalid ANTHROPIC_API_KEY — skipping Claude gate")
        return candidates
    except Exception as e:
        logger.exception("[stage4b] Claude gate failed: %s — passing all candidates through", e)
        return candidates


def _format_candidates(candidates: list) -> str:
    """Format candidates as a readable block for the Claude prompt."""
    lines = []
    for i, c in enumerate(candidates, 1):
        ticker    = c["ticker"]
        score     = c.get("conviction_score", 0)
        agreement = c.get("model_agreement_count", 0)
        gap       = c.get("gap_pct", 0)
        vol       = c.get("volume_ratio", 0)
        rsi       = c.get("rsi", 0) or c.get("yf_data", {}).get("rsi", 0)
        sector    = c.get("sector", "?")
        entry     = c.get("premarket_price") or c.get("price", 0)
        bear_risk = c.get("bear_risk_score", 0)
        bear_note = c.get("bear_primary_risk", "")
        catalyst  = (c.get("catalyst") or "")[:80]
        thesis    = (c.get("technical_setup") or "")[:60]

        # Try to get earnings days from enriched data
        earn_days = c.get("yf_data", {}).get("earnings_days", 999)
        earn_str  = f"{earn_days}d" if earn_days < 90 else "90d+"

        lines.append(
            f"{i:2d}. {ticker:<6}  conviction={score:.1f}  {agreement}/4 models  "
            f"gap={gap:+.1f}%  vol={vol:.1f}x  RSI={rsi:.0f}  "
            f"entry=${entry:.2f}  sector={sector}\n"
            f"     bear_risk={bear_risk:.0f}  earnings_in={earn_str}\n"
            f"     catalyst: {catalyst or '(none provided)'}\n"
            f"     technical: {thesis or '(none provided)'}\n"
        )
    return "\n".join(lines)


def _parse_verdicts(raw: str) -> dict:
    """Parse Claude's JSON array response into {ticker: {verdict, reason}}."""
    text = raw.strip()

    # Strip markdown fences
    if text.startswith("```"):
        lines = text.split("\n")
        text  = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    # Try to extract JSON array
    parsed = None
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            parsed = obj
    except json.JSONDecodeError:
        start = text.find("[")
        end   = text.rfind("]")
        if start != -1 and end > start:
            try:
                parsed = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

    if not parsed:
        return {}

    return {
        str(item.get("ticker", "")).upper().strip(): {
            "verdict": str(item.get("verdict", "CONFIRM")).upper().strip(),
            "reason":  str(item.get("reason", "")),
        }
        for item in parsed
        if isinstance(item, dict) and item.get("ticker")
    }
