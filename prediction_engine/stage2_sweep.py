# prediction_engine/stage2_sweep.py
# Stage 2 (5:00-6:00 AM ET): Qwen2.5-1.5B sweeps 500 candidates → top 50.
#
# CPU mode (t3.xlarge): individual calls take ~15s each.
# Batched scoring (SWEEP_BATCH_SIZE tickers per prompt) reduces ~500 calls
# to ~63 calls → ~16 minutes total. Fits comfortably in the 1-hour window.

import json
import time
import logging
import requests
from typing import Optional

from .config import (
    OLLAMA_BASE_URL, OLLAMA_TIMEOUT, MODEL_QWEN, MODEL_OPTIONS,
    SWEEP_TOP_N, SWEEP_MIN_SCORE, SWEEP_BATCH_SIZE,
)

logger = logging.getLogger("pe.stage2")

_SYSTEM_QWEN_SWEEP = (
    "You are a pre-market stock scanner. Score each stock for intraday trading conviction "
    "based on premarket gap, volume, float, and sector momentum. "
    "Return ONLY a valid JSON array — no explanation, no markdown."
)

_PROMPT_BATCH_HEADER = """\
Score each stock for pre-market trading conviction. Return ONLY a JSON array of objects.

Scoring rules:
- Gap 3-15% with volume_ratio ≥ 2.0x = strong setup (score 65-85)
- Gap 2-3% with volume_ratio ≥ 1.5x = moderate setup (score 50-65)
- Gap >20% = potential halt/climax, cap score at 60 regardless
- Gap <2% or volume_ratio <1.2x = weak setup (score <40)
- verdict = "pass" if score >= 50, else "fail"

Stocks to score:
"""

_PROMPT_STOCK_LINE = (
    '  {i}. {ticker}: gap={gap_pct:+.1f}%, vol_ratio={volume_ratio:.1f}x, '
    'price=${price:.2f}, sector={sector}\n'
)

_PROMPT_BATCH_FOOTER = """
Return exactly this JSON array (one entry per stock, same order as listed):
[
  {{"ticker": "XXXX", "score": <0-100>, "verdict": "<pass|fail>", "reason": "<5 words max>"}},
  ...
]
"""


def run(candidates: list) -> list:
    """
    Batch-sweep all candidates with Qwen2.5-1.5B.
    Returns top SWEEP_TOP_N candidates with sweep scores appended.
    """
    logger.info("[stage2] Batched sweep of %d candidates (batch_size=%d) using %s",
                len(candidates), SWEEP_BATCH_SIZE, MODEL_QWEN)
    start  = time.monotonic()
    scored = {}  # ticker → {score, verdict, reason}

    # Split into batches
    batches = [
        candidates[i:i + SWEEP_BATCH_SIZE]
        for i in range(0, len(candidates), SWEEP_BATCH_SIZE)
    ]
    total_batches = len(batches)
    logger.info("[stage2] %d batches of ≤%d tickers", total_batches, SWEEP_BATCH_SIZE)

    failed_batches = 0
    for batch_idx, batch in enumerate(batches, 1):
        results = _score_batch(batch, batch_idx, total_batches)

        if results:
            for ticker, data in results.items():
                scored[ticker] = data
        else:
            failed_batches += 1
            # Fallback: assign a neutral score based on gap/vol momentum
            for c in batch:
                t = c["ticker"]
                gap = abs(c.get("gap_pct", 0))
                vol = c.get("volume_ratio", 1.0)
                fb_score = min(60, int(gap * 4 + vol * 5))
                scored[t] = {
                    "score":   fb_score,
                    "verdict": "pass" if fb_score >= 50 else "fail",
                    "reason":  "model_unavailable_fallback",
                }

        if batch_idx % 10 == 0 or batch_idx == total_batches:
            elapsed = time.monotonic() - start
            rate    = batch_idx / elapsed * SWEEP_BATCH_SIZE
            passed  = sum(1 for d in scored.values() if d.get("verdict") == "pass")
            logger.info("[stage2] %d/%d batches (%.0f tickers/min) — %d passing",
                        batch_idx, total_batches, rate * 60, passed)

    # Annotate candidates and filter
    passing = []
    for c in candidates:
        ticker = c["ticker"]
        data   = scored.get(ticker, {"score": 0, "verdict": "fail", "reason": ""})
        c["sweep_score"]   = data.get("score", 0)
        c["sweep_verdict"] = data.get("verdict", "fail")
        c["sweep_reason"]  = data.get("reason", "")[:100]
        if c["sweep_verdict"] == "pass" and c["sweep_score"] >= SWEEP_MIN_SCORE:
            passing.append(c)

    passing.sort(key=lambda x: x.get("sweep_score", 0), reverse=True)
    top = passing[:SWEEP_TOP_N]

    elapsed = time.monotonic() - start
    logger.info(
        "[stage2] Complete: %d passed, returning top %d "
        "(%.1fs elapsed, %d failed batches)",
        len(passing), len(top), elapsed, failed_batches,
    )
    for i, c in enumerate(top, 1):
        logger.info(
            "  %2d. %-8s  score=%-3d  gap=%+.1f%%  vol=%.1fx  %s",
            i, c["ticker"], c["sweep_score"], c.get("gap_pct", 0),
            c.get("volume_ratio", 0), c.get("sweep_reason", "")[:50],
        )
    return top


def _score_batch(batch: list, batch_idx: int, total: int,
                 retries: int = 2) -> Optional[dict]:
    """
    Score a batch of tickers in a single Ollama call.
    Returns {ticker: {score, verdict, reason}} or None on total failure.
    """
    if not batch:
        return {}

    # Build prompt
    prompt = _PROMPT_BATCH_HEADER
    for i, c in enumerate(batch, 1):
        prompt += _PROMPT_STOCK_LINE.format(
            i          = i,
            ticker     = c.get("ticker", "?"),
            gap_pct    = c.get("gap_pct", 0),
            volume_ratio = c.get("volume_ratio", 0),
            price      = c.get("premarket_price") or c.get("price", 0),
            sector     = (c.get("sector") or "Unknown")[:15],
        )
    prompt += _PROMPT_BATCH_FOOTER

    for attempt in range(retries + 1):
        try:
            options = dict(MODEL_OPTIONS.get(MODEL_QWEN, {}))
            # Each ticker entry ≈ 60 tokens in JSON. Add 50% headroom.
            options["num_predict"] = max(400, SWEEP_BATCH_SIZE * 60)

            resp = requests.post(
                f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate",
                json={
                    "model":   MODEL_QWEN,
                    "system":  _SYSTEM_QWEN_SWEEP,
                    "prompt":  prompt,
                    # No "format": "json" — that makes the model return a single
                    # object instead of the array the prompt requests.
                    # _parse_response handles extracting [...] from free text.
                    "stream":  False,
                    "options": options,
                },
                timeout=OLLAMA_TIMEOUT,
            )
            resp.raise_for_status()
            resp_json = resp.json()

            # Check for Ollama error in response body
            if "error" in resp_json:
                logger.warning("[stage2] Batch %d/%d attempt %d: Ollama error: %s",
                               batch_idx, total, attempt + 1, resp_json["error"])
                if attempt < retries:
                    time.sleep(3 * (attempt + 1))
                continue

            raw = resp_json.get("response", "")

            parsed = _parse_response(raw, batch)
            if parsed:
                return parsed

            # Log what the model actually returned so we can diagnose parse failures
            logger.warning("[stage2] Batch %d/%d attempt %d: parse failed. Raw (first 300): %s",
                           batch_idx, total, attempt + 1, raw[:300])

        except requests.RequestException as e:
            logger.warning("[stage2] Batch %d/%d attempt %d: request error: %s",
                           batch_idx, total, attempt + 1, e)
        except Exception as e:
            logger.warning("[stage2] Batch %d/%d attempt %d: unexpected error: %s",
                           batch_idx, total, attempt + 1, e)

        if attempt < retries:
            time.sleep(3 * (attempt + 1))

    logger.error("[stage2] Batch %d/%d: all %d attempts failed", batch_idx, total, retries + 1)
    return None


def _parse_response(raw: str, batch: list) -> Optional[dict]:
    """
    Parse the model's JSON array response.
    Tolerates markdown fences and partial arrays.
    Returns {ticker: {...}} keyed by ticker, or None if unparseable.
    """
    text = raw.strip()

    # Strip markdown fences
    if text.startswith("```"):
        lines = text.split("\n")
        text  = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    # The model may return a JSON object with a key wrapping the array.
    # Try direct array parse first, then ANY dict value that is a list.
    parsed_list = None
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            parsed_list = obj
        elif isinstance(obj, dict):
            # Try any value that is a non-empty list (model may use any key name)
            for val in obj.values():
                if isinstance(val, list) and val:
                    parsed_list = val
                    break
    except json.JSONDecodeError:
        pass

    # Fallback: extract the first [...] block from the raw text
    if not parsed_list:
        start = text.find("[")
        end   = text.rfind("]")
        if start != -1 and end > start:
            try:
                parsed_list = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

    # Last resort: truncated array recovery — extract all complete {...} objects
    if not parsed_list:
        import re
        objects = re.findall(r'\{[^{}]+\}', text)
        if objects:
            recovered = []
            for obj_str in objects:
                try:
                    recovered.append(json.loads(obj_str))
                except json.JSONDecodeError:
                    pass
            if recovered:
                logger.warning("[stage2] _parse_response: recovered %d partial objects from truncated array",
                               len(recovered))
                parsed_list = recovered

    if not parsed_list:
        logger.debug("[stage2] _parse_response: no list found in: %.200s", text[:200])
        return None

    result = {}
    batch_tickers = {c["ticker"].upper(): c for c in batch}

    for item in parsed_list:
        if not isinstance(item, dict):
            continue
        ticker  = str(item.get("ticker", "")).upper().strip()
        score   = _clamp(item.get("score"), 0, 100)
        verdict = str(item.get("verdict", "")).lower().strip()
        reason  = str(item.get("reason", ""))[:80]

        if not ticker:
            continue

        # If the model omits ticker in output, try to match by position
        if ticker not in batch_tickers:
            continue

        if verdict not in ("pass", "fail"):
            verdict = "pass" if score >= 50 else "fail"

        result[ticker] = {"score": score, "verdict": verdict, "reason": reason}

    # If we got fewer results than expected, fill gaps with positional fallback
    if len(result) < len(batch) and len(parsed_list) == len(batch):
        for i, (item, c) in enumerate(zip(parsed_list, batch)):
            t = c["ticker"]
            if t not in result and isinstance(item, dict):
                score   = _clamp(item.get("score"), 0, 100)
                verdict = "pass" if score >= 50 else "fail"
                result[t] = {
                    "score":   score,
                    "verdict": verdict,
                    "reason":  str(item.get("reason", "positional_match"))[:80],
                }

    return result if result else None


def _clamp(val, lo, hi) -> int:
    try:
        return max(lo, min(hi, int(val)))
    except (TypeError, ValueError):
        return (lo + hi) // 2
